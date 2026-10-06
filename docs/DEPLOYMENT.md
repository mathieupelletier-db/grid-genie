# Grid Genie — Deployment & Execution Guide (4–8 h)

## Prerequisites (15 min)

| Requirement | Check |
|---|---|
| Databricks workspace with Unity Catalog, serverless compute, Databricks Apps, Lakebase Autoscaling | — |
| Serverless SQL warehouse (DBSQL 2025.x+ for `GEOMETRY` / `ST_*`) | `SELECT typeof(ST_Point(-71.2, 46.8, 4326))` → `geometry(4326)` |
| Rights: `CREATE CATALOG` (or an existing catalog), create jobs, pipelines, apps, Lakebase projects | — |
| Foundation-model endpoint for `ai_query` (default `databricks-claude-sonnet-4-6`; must support batch inference) | `databricks serving-endpoints list` |
| Databricks CLI ≥ 0.294, `jq`, `psql` | `databricks -v` |
| CLI profile authenticated | `databricks auth profiles` |

## Option A — one command (≈ 45 min wall-clock)

```bash
./scripts/deploy.sh <PROFILE> [<WAREHOUSE_ID>] [<MANAGED_LOCATION>]
```

Pass `MANAGED_LOCATION` when the metastore has no root storage (the error reads *"Metastore storage root URL does not exist"*).

## Option B — step by step

| # | Step | Time | Command / action |
|---|---|---|---|
| 1 | Create the catalog | 2 min | `CREATE CATALOG grid_genie [MANAGED LOCATION '…']` |
| 2 | Set the warehouse id | 2 min | Edit `targets.dev.variables.warehouse_id` in `databricks.yml` (and in `app/databricks.yml`) |
| 3 | Deploy the platform bundle | 2 min | `databricks bundle deploy -t dev -p <PROFILE>` |
| 4 | Run the setup job | 25–35 min | `databricks bundle run grid_genie_setup -t dev -p <PROFILE>` |
| 5 | Collect ids | 2 min | Genie space: `databricks api get /api/2.0/genie/spaces`. Lakebase DB: `databricks postgres list-databases projects/grid-genie/branches/production` |
| 6 | Deploy and start the app | 5 min | `cd app && databricks bundle deploy -t dev --var genie_space_id=… --var lakebase_database_id=… && databricks bundle run grid_genie_map -t dev …` |
| 7 | Grant the app service principal access | 5 min | See the grants block in `scripts/deploy.sh`: UC `USE`/`SELECT` on gold, `EXECUTE` on governance, a `ced_access` row, and Postgres `GRANT SELECT` on schema `ops` |
| 8 | Smoke test | 10 min | Open the app URL. Check the map layers and the Lakebase panel latency, then ask the 10 questions in the chat |

### What the setup job does

| Task | File | Output |
|---|---|---|
| `generate_raw_extracts` | `src/00_generate_raw.py` | Schemas, landing volume, raw JSON / CSV / Parquet extracts (23 lines, ~4.4 k poles, ~1.2 k transformers, ~8.4 k customers, 2 winters of daily peaks, ~830 outages including 5 ongoing) |
| `ingest_lakeflow` | `src/pipeline/01_ingest_lakeflow.py` | `bronze.*_raw` (Auto Loader), then `silver.*` with `GEOMETRY(4326)` and expectations |
| `governance_spatial_gold` | `src/02_governance_spatial.sql` | `gold.*` spatial tables, row filter, PII masks, dynamic view, bilingual comments |
| `intelligence` | `src/03_intelligence.py` | UC model `gold.transformer_overload_model@champion`, `gold.transformer_risk`, `gold.zone_risk` (+ `ai_query` recommendations), `gold.ops_*` |
| `lakebase_sync` | `src/04_lakebase_sync.py` | Lakebase project `grid-genie`, UC catalog `grid_genie_lakebase`, 4 synced tables in Postgres schema `ops` |
| `genie_space` | `src/05_genie_space.py` | Genie space « Grid Genie · Réseau de distribution » (idempotent: matched by title) |

## Governance demo script

1. As yourself, run `SELECT customer_name, ST_AsText(geom) FROM grid_genie.gold.customers LIMIT 3`. Names come back masked and points are H3 cell centres.
2. Add yourself to account group `grid_genie_pii_readers`. The same query now returns exact data.
3. Row-level security: remove the `*` row for your user from `governance.ced_access` and add your user to `ced_matapedia_ops`. `gold.outages` and `gold.customers` now return MAT only, in SQL, in Genie and in the app (when the app runs on behalf of the user).

## Re-running and resetting

* Re-run the job at any time. Every step is idempotent: Auto Loader picks up new extracts, gold is rebuilt, the Lakebase sync is triggered incrementally, and the Genie space is updated in place.
* Reset: `databricks bundle destroy` (in both bundles), `databricks postgres delete-project projects/grid-genie`, `DROP CATALOG grid_genie CASCADE`, then delete the Genie space. **Confirm each step first.** These are destructive.

## Known limits (prototype)

* GEOMETRY can't be synced to Lakebase or used as a UC column-mask type. Both are handled as described in `ARCHITECTURE.md`.
* The app queries the warehouse as its service principal, so row filters apply to the SP (mapped to `*`). For per-user RLS, switch to on-behalf-of-user auth (`x-forwarded-access-token`) and declare `user_api_scopes` (`sql`, `dashboards.genie`).
* The data is synthetic. Coordinates sit in plausible Laval / Beauce / Matapédia areas but don't follow real infrastructure.
