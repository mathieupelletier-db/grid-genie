#
# # 04 · Lakebase operational serving
#
# Provisions (idempotently) a **Lakebase Postgres Autoscaling** project, registers its database in
# Unity Catalog, and syncs the `gold.ops_*` tables into Postgres schema `ops` as **synced tables**
# (Triggered mode, CDF-based incremental refresh, one shared sync pipeline).
#
# Synced tables do not support GEOMETRY columns, so the `ops_*` tables carry scalar `lon` / `lat`
# (derived with `ST_X` / `ST_Y` in gold). Geometry stays authoritative in Delta.

# ---------------------------------------------------------------------------------------------

import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists, ResourceConflict
from databricks.sdk.service import postgres as pg


def _arg(name, default):
    if f"--{name}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{name}") + 1]
    return default


CATALOG = _arg("catalog", "grid_genie")
PROJECT_ID = _arg("lakebase_project", "grid-genie")
LAKEBASE_CATALOG = _arg("lakebase_catalog", "grid_genie_lakebase")
PG_DATABASE = "databricks_postgres"
PG_SCHEMA = "ops"
BRANCH = f"projects/{PROJECT_ID}/branches/production"

SYNCED = {  # gold source table → primary key
    "ops_line_status": ["line_id"],
    "ops_transformer_status": ["transformer_id"],
    "ops_zone_risk": ["zone_id"],
    "ops_active_outages": ["outage_id"],
}

w = WorkspaceClient()

# ---------------------------------------------------------------------------------------------

# 1. Project (creates `production` branch + primary read-write endpoint)
try:
    w.postgres.get_project(name=f"projects/{PROJECT_ID}")
    print(f"Lakebase project {PROJECT_ID} exists")
except NotFound:
    print(f"Creating Lakebase project {PROJECT_ID} …")
    w.postgres.create_project(
        project_id=PROJECT_ID,
        project=pg.Project.from_dict({"spec": {
            "display_name": "Grid Genie — operational serving",
            "pg_version": 17,
            "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.5, "autoscaling_limit_max_cu": 2},
        }}),
    ).wait()

# 2. Register the Postgres database as a UC catalog (one-time)
try:
    w.postgres.get_catalog(name=f"catalogs/{LAKEBASE_CATALOG}")
    print(f"Lakebase catalog {LAKEBASE_CATALOG} exists")
except NotFound:
    w.postgres.create_catalog(
        catalog_id=LAKEBASE_CATALOG,
        catalog=pg.Catalog.from_dict({"spec": {"postgres_database": PG_DATABASE, "branch": BRANCH}}),
    ).wait()
    print(f"Registered {LAKEBASE_CATALOG}")

# ---------------------------------------------------------------------------------------------

# 3. Synced tables — first one creates the sync pipeline, the others join it
pipeline_id = None
for table, pk in SYNCED.items():
    target = f"{LAKEBASE_CATALOG}.{PG_SCHEMA}.{table}"
    try:
        st = w.postgres.get_synced_table(name=f"synced_tables/{target}")
        pipeline_id = pipeline_id or (st.status.pipeline_id if st.status else None)
        print(f"{target}: exists ({st.status.detailed_state if st.status else 'n/a'})")
        continue
    except NotFound:
        pass
    spec = {
        "source_table_full_name": f"{CATALOG}.gold.{table}",
        "primary_key_columns": pk,
        "scheduling_policy": "TRIGGERED",
        "branch": BRANCH,
        "postgres_database": PG_DATABASE,
        "create_database_objects_if_missing": True,
    }
    if pipeline_id:
        spec["existing_pipeline_id"] = pipeline_id
    else:
        spec["new_pipeline_spec"] = {"storage_catalog": CATALOG, "storage_schema": "governance"}
    try:
        st = w.postgres.create_synced_table(
            synced_table_id=target, synced_table=pg.SyncedTable.from_dict({"spec": spec})).wait()
    except (ResourceAlreadyExists, ResourceConflict) as e:
        print(f"{target}: {e}")
        st = w.postgres.get_synced_table(name=f"synced_tables/{target}")
    pipeline_id = pipeline_id or st.status.pipeline_id
    print(f"{target}: created, pipeline {pipeline_id}")

# ---------------------------------------------------------------------------------------------

# 4. Trigger an incremental refresh so Postgres reflects this run's gold tables
TERMINAL = ("COMPLETED", "FAILED", "CANCELED")


def wait_update(update_id):
    for _ in range(90):
        u = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update
        if u.state.value in TERMINAL:
            return u.state.value
        time.sleep(20)
    return "TIMEOUT"


if pipeline_id:
    # An update started by synced-table creation may not include tables attached afterwards:
    # let any in-flight update finish, then always run a fresh one covering every table.
    update_id = None
    for attempt in range(10):
        for u in w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=5).updates or []:
            if u.state and u.state.value not in TERMINAL:
                wait_update(u.update_id)
        try:
            update_id = w.pipelines.start_update(pipeline_id=pipeline_id).update_id
            break
        except ResourceConflict as e:  # an update started in between (e.g. source-change trigger): wait, retry
            print(f"start_update conflict (attempt {attempt + 1}): {e}")
            time.sleep(15)
    if not update_id:
        raise RuntimeError("Could not start a Lakebase sync update")
    state = wait_update(update_id)
    if state == "FAILED":
        # Typically a source table was replaced (new Delta table id breaks the CDF checkpoint):
        # re-snapshot once. Safe — synced tables are derived, read-only copies of gold.
        print(f"Incremental update {update_id} failed — running one full refresh of the sync pipeline")
        w.pipelines.stop_and_wait(pipeline_id=pipeline_id)  # cancels the pipeline's automatic retry loop
        update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True).update_id
        state = wait_update(update_id)
    print(f"Sync pipeline update {update_id}: {state}")
    if state != "COMPLETED":
        raise RuntimeError(f"Lakebase sync update {update_id} ended in {state}")

for table in SYNCED:
    st = w.postgres.get_synced_table(name=f"synced_tables/{LAKEBASE_CATALOG}.{PG_SCHEMA}.{table}")
    print(table, st.status.detailed_state, st.status.message)
