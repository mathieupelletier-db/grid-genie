"""Grid Genie — Databricks App (FastAPI + MapLibre GL).

* GeoJSON layer endpoints: native Delta GEOMETRY → ST_AsGeoJSON on a serverless SQL warehouse
* Spatial selection: ST_Intersects against a user-drawn polygon
* Operational panels: low-latency reads from Lakebase Postgres (synced gold.ops_* tables)
* Genie assistant: relays FR/EN questions to the Genie Conversation API and returns text, SQL and rows
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

import psycopg
from psycopg.rows import dict_row
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

log = logging.getLogger("grid_genie")
logging.basicConfig(level=logging.INFO)

CATALOG = os.getenv("GG_CATALOG", "grid_genie")
GOLD = f"{CATALOG}.gold"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")
PG_SCHEMA = os.getenv("GG_PG_SCHEMA", "ops")
CACHE_TTL_S = 600

cfg = Config()
w = WorkspaceClient(config=cfg)
app = FastAPI(title="Grid Genie", version="1.0")


# ---------------------------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------------------------

def _host() -> str:
    return cfg.host.replace("https://", "").rstrip("/")


def run_sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a query on the SQL warehouse (app service principal) and return rows as dicts."""
    with dbsql.connect(server_hostname=_host(), http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate) as conn, conn.cursor() as cur:
        cur.execute(query, params or {})
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _lakebase_endpoint() -> str:
    """Endpoint resource path: injected env var, else the endpoint of GG_LAKEBASE_BRANCH whose host is PGHOST."""
    if os.getenv("LAKEBASE_ENDPOINT"):
        return os.environ["LAKEBASE_ENDPOINT"]
    branch = os.getenv("GG_LAKEBASE_BRANCH", "projects/grid-genie/branches/production")
    for ep in w.postgres.list_endpoints(parent=branch):
        if ep.status and ep.status.hosts and ep.status.hosts.host == os.environ.get("PGHOST"):
            return ep.name
    return f"{branch}/endpoints/primary"


class _PgToken:
    """OAuth token for Lakebase, refreshed before its 1-hour expiry."""

    def __init__(self):
        self._lock, self._token, self._at, self._endpoint = threading.Lock(), None, 0.0, None

    def get(self) -> str:
        with self._lock:
            if not self._token or time.time() - self._at > 45 * 60:
                self._endpoint = self._endpoint or _lakebase_endpoint()
                self._token = w.postgres.generate_database_credential(endpoint=self._endpoint).token
                self._at = time.time()
            return self._token


_pg_token = _PgToken()


def run_pg(query: str, params: tuple | dict | None = None) -> tuple[list[dict[str, Any]], float]:
    t0 = time.perf_counter()
    with psycopg.connect(host=os.environ["PGHOST"], port=int(os.getenv("PGPORT", "5432")),
                         dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
                         password=_pg_token.get(), sslmode="require", connect_timeout=15) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return rows, round((time.perf_counter() - t0) * 1000, 1)


_cache: dict[str, tuple[float, Any]] = {}


def cached(key: str, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL_S:
        return hit[1]
    val = fn()
    _cache[key] = (time.time(), val)
    return val


def feature_collection(rows: list[dict[str, Any]], geom_col: str = "geojson") -> dict:
    feats = []
    for r in rows:
        g = r.pop(geom_col, None)
        if g:
            feats.append({"type": "Feature", "geometry": json.loads(g),
                          "properties": {k: _jsonable(v) for k, v in r.items()}})
    return {"type": "FeatureCollection", "features": feats}


def _jsonable(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


# ---------------------------------------------------------------------------------------------
# Map layers (native GEOMETRY → GeoJSON)
# ---------------------------------------------------------------------------------------------

LAYERS: dict[str, str] = {
    "substations": f"""SELECT substation_code, substation_name, ced_code, region_name, capacity_mva,
                              ST_AsGeoJSON(geom) AS geojson FROM {GOLD}.substations WHERE {{where}}""",
    "lines": f"""SELECT line_id, substation_name, ced_code, length_km, n_customers, n_transformers,
                        max_distance_to_substation_m, avg_pole_age_years, ST_AsGeoJSON(geom) AS geojson
                 FROM {GOLD}.lines WHERE {{where}}""",
    "right_of_way": f"""SELECT line_id, substation_name, ced_code, right_of_way_area_m2,
                               round(right_of_way_area_m2 / 10000, 2) AS right_of_way_ha,
                               ST_AsGeoJSON(right_of_way_geom) AS geojson FROM {GOLD}.lines WHERE {{where}}""",
    "spans": f"""SELECT span_id, line_id, ced_code, length_m, ST_AsGeoJSON(geom) AS geojson
                 FROM {GOLD}.spans WHERE {{where}}""",
    "poles": f"""SELECT pole_id, line_id, ced_code, install_year, age_years, material, branch_type,
                        round(network_distance_m) AS network_distance_m, ST_AsGeoJSON(geom) AS geojson
                 FROM {GOLD}.poles WHERE {{where}}""",
    "transformers": f"""SELECT t.transformer_id, t.line_id, t.ced_code, t.zone_id, t.kva, t.install_year, t.n_customers,
                               t.max_overload_pct_hiver_2025, t.max_overload_pct_hiver_2026,
                               round(r.risk_score, 3) AS risk_score, r.risk_tier, ST_AsGeoJSON(t.geom) AS geojson
                        FROM {GOLD}.transformers t LEFT JOIN {GOLD}.transformer_risk r USING (transformer_id)
                        WHERE {{where_t}}""",
    "zones": f"""SELECT z.zone_id, z.device_type, z.line_id, z.ced_code, z.n_customers, z.interruptions_3m,
                        z.interruptions_6m, z.chi_6m, zr.risk_tier, zr.risk_score, ST_AsGeoJSON(z.zone_geom) AS geojson
                 FROM {GOLD}.protection_zones z LEFT JOIN {GOLD}.zone_risk zr USING (zone_id)
                 WHERE z.device_type = 'COUPE-CIRCUIT' AND {{where_z}}""",
    "chi_hex": f"""SELECT h3_cell_8, ced_code, chi_6m, zones_impacted, ST_AsGeoJSON(geom) AS geojson
                   FROM {GOLD}.chi_h3_hex WHERE {{where_ced}}""",
    "outages_active": f"""SELECT outage_id, zone_id, line_id, ced_code, cause, customers_interrupted,
                                 CAST(start_ts AS STRING) AS start_ts, round(duration_h, 1) AS elapsed_h,
                                 ST_AsGeoJSON(fault_geom) AS geojson
                          FROM {GOLD}.outages WHERE is_ongoing AND {{where}}""",
    "devices": f"""SELECT device_id, device_type, line_id, ced_code, customers_downstream, ST_AsGeoJSON(geom) AS geojson
                   FROM {GOLD}.protection_devices WHERE device_type = 'COUPE-CIRCUIT' AND {{where}}""",
}


def _filters(alias: str = "", ced: str | None = None, line_id: str | None = None, line_ok: bool = True):
    p = f"{alias}." if alias else ""
    clauses, params = ["1=1"], {}
    if ced:
        clauses.append(f"{p}ced_code = :ced")
        params["ced"] = ced
    if line_id and line_ok:
        clauses.append(f"{p}line_id = :line_id")
        params["line_id"] = line_id
    return " AND ".join(clauses), params


@app.get("/api/layers/{name}")
def layer(name: str, ced: str | None = Query(None), line_id: str | None = Query(None)):
    if name not in LAYERS:
        raise HTTPException(404, f"Unknown layer {name}")
    line_ok = name not in ("substations", "chi_hex")
    where, params = _filters("", ced, line_id, line_ok)
    where_t, _ = _filters("t", ced, line_id)
    where_z, _ = _filters("z", ced, line_id)
    where_ced, _ = _filters("", ced, None)
    query = LAYERS[name].format(where=where, where_t=where_t, where_z=where_z, where_ced=where_ced)
    return cached(f"{name}|{ced}|{line_id}", lambda: feature_collection(run_sql(query, params)))


@app.get("/api/meta")
def meta():
    def load():
        lines = run_sql(f"""SELECT line_id, substation_code, substation_name, ced_code, region_name, length_km,
                                   n_customers FROM {GOLD}.lines ORDER BY line_id""")
        ceds = run_sql(f"SELECT ced_code, ced_name, region_name FROM {GOLD}.operating_centers ORDER BY ced_code")
        bounds = run_sql(f"""SELECT ced_code, ST_XMin(geom) AS xmin, ST_YMin(geom) AS ymin, ST_XMax(geom) AS xmax,
                                    ST_YMax(geom) AS ymax FROM {GOLD}.operating_centers""")
        line_bounds = run_sql(f"""SELECT line_id, ST_XMin(geom) AS xmin, ST_YMin(geom) AS ymin, ST_XMax(geom) AS xmax,
                                         ST_YMax(geom) AS ymax FROM {GOLD}.lines""")
        return {"ceds": ceds, "lines": lines,
                "ced_bounds": {b["ced_code"]: [b["xmin"], b["ymin"], b["xmax"], b["ymax"]] for b in bounds},
                "line_bounds": {b["line_id"]: [b["xmin"], b["ymin"], b["xmax"], b["ymax"]] for b in line_bounds},
                "genie_enabled": bool(GENIE_SPACE_ID)}
    return cached("meta", load)


# ---------------------------------------------------------------------------------------------
# Spatial selection
# ---------------------------------------------------------------------------------------------

class Selection(BaseModel):
    geometry: dict = Field(..., description="GeoJSON Polygon in EPSG:4326")


@app.post("/api/select")
def select(sel: Selection):
    g = json.dumps(sel.geometry)
    q = f"""
    WITH sel AS (SELECT ST_GeomFromGeoJSON(:g) AS g)
    SELECT
      (SELECT count(*) FROM {GOLD}.poles p, sel WHERE ST_Intersects(sel.g, p.geom)) AS poles,
      (SELECT count(*) FROM {GOLD}.poles p, sel WHERE ST_Intersects(sel.g, p.geom) AND p.install_year < 1985) AS poles_pre_1985,
      (SELECT count(*) FROM {GOLD}.transformers t, sel WHERE ST_Intersects(sel.g, t.geom)) AS transformers,
      (SELECT count(*) FROM {GOLD}.transformers t, sel WHERE ST_Intersects(sel.g, t.geom)
                                                       AND t.max_overload_pct_hiver_2026 > 150) AS transformers_over_150,
      (SELECT count(*) FROM {GOLD}.customers c, sel WHERE ST_Intersects(sel.g, c.geom)) AS customers,
      (SELECT round(sum(ST_Length(ST_Transform(ST_Intersection(sel.g, s.geom), 32198))) / 1000, 2)
         FROM {GOLD}.spans s, sel WHERE ST_Intersects(sel.g, s.geom)) AS conductor_km,
      (SELECT round(ST_Area(ST_Transform(sel.g, 32198)) / 1e6, 3) FROM sel) AS area_km2
    """
    stats = run_sql(q, {"g": g})[0]
    top = run_sql(f"""
        WITH sel AS (SELECT ST_GeomFromGeoJSON(:g) AS g)
        SELECT t.transformer_id, t.line_id, t.kva, t.max_overload_pct_hiver_2026, round(r.risk_score, 3) AS risk_score,
               r.risk_tier
        FROM {GOLD}.transformers t JOIN {GOLD}.transformer_risk r USING (transformer_id), sel
        WHERE ST_Intersects(sel.g, t.geom) ORDER BY r.risk_score DESC LIMIT 10""", {"g": g})
    return {"stats": {k: _jsonable(v) for k, v in stats.items()},
            "top_transformers": [{k: _jsonable(v) for k, v in r.items()} for r in top]}


# ---------------------------------------------------------------------------------------------
# Operational panels — Lakebase
# ---------------------------------------------------------------------------------------------

def _pg(query: str, params=None):
    try:
        rows, ms = run_pg(query, params)
    except Exception as e:  # noqa: BLE001
        log.exception("Lakebase query failed")
        raise HTTPException(503, f"Lakebase unavailable: {e}") from e
    return {"source": "lakebase", "latency_ms": ms, "rows": [{k: _jsonable(v) for k, v in r.items()} for r in rows]}


@app.get("/api/ops/summary")
def ops_summary(ced: str | None = None):
    return _pg(f"""SELECT ced_code, count(*) AS lines, sum(n_customers) AS customers,
                          sum(active_outages) AS active_outages, sum(customers_out) AS customers_out,
                          sum(high_risk_transformers) AS high_risk_transformers, round(sum(chi_6m)::numeric, 0) AS chi_6m
                   FROM {PG_SCHEMA}.ops_line_status WHERE (%(ced)s::text IS NULL OR ced_code = %(ced)s)
                   GROUP BY ced_code ORDER BY ced_code""", {"ced": ced})


@app.get("/api/ops/active_outages")
def ops_active_outages(ced: str | None = None):
    return _pg(f"""SELECT outage_id, zone_id, line_id, ced_code, cause, customers_interrupted,
                          round(elapsed_h::numeric, 1) AS elapsed_h, round(chi_so_far::numeric, 0) AS chi_so_far, lon, lat
                   FROM {PG_SCHEMA}.ops_active_outages WHERE (%(ced)s::text IS NULL OR ced_code = %(ced)s)
                   ORDER BY customers_interrupted DESC""", {"ced": ced})


@app.get("/api/ops/zone_risk")
def ops_zone_risk(ced: str | None = None, limit: int = 15):
    return _pg(f"""SELECT zone_id, line_id, ced_code, n_customers, interruptions_6m, round(chi_6m::numeric, 0) AS chi_6m,
                          chi_trend_pct, avg_pole_age_years, high_risk_transformers, risk_score, risk_tier,
                          recommendation_fr, recommendation_en, lon, lat
                   FROM {PG_SCHEMA}.ops_zone_risk WHERE (%(ced)s::text IS NULL OR ced_code = %(ced)s)
                   ORDER BY risk_score DESC LIMIT %(limit)s""", {"ced": ced, "limit": min(limit, 100)})


@app.get("/api/ops/transformer_risk")
def ops_transformer_risk(ced: str | None = None, line_id: str | None = None, limit: int = 15):
    return _pg(f"""SELECT transformer_id, line_id, ced_code, kva, n_customers, max_overload_pct_last_winter,
                          risk_score_next_winter, risk_tier, lon, lat
                   FROM {PG_SCHEMA}.ops_transformer_status
                   WHERE (%(ced)s::text IS NULL OR ced_code = %(ced)s) AND (%(line)s::text IS NULL OR line_id = %(line)s)
                   ORDER BY risk_score_next_winter DESC NULLS LAST LIMIT %(limit)s""",
               {"ced": ced, "line": line_id, "limit": min(limit, 100)})


# ---------------------------------------------------------------------------------------------
# Genie assistant
# ---------------------------------------------------------------------------------------------

class Ask(BaseModel):
    question: str = Field(..., min_length=2, max_length=2000)
    conversation_id: str | None = None


def _genie(method: str, path: str, body: dict | None = None):
    if not GENIE_SPACE_ID:
        raise HTTPException(503, "Genie space not configured")
    return w.api_client.do(method, f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}{path}", body=body)


@app.post("/api/genie/ask")
def genie_ask(req: Ask):
    if req.conversation_id:
        r = _genie("POST", f"/conversations/{req.conversation_id}/messages", {"content": req.question})
        return {"conversation_id": req.conversation_id, "message_id": r.get("message_id") or r.get("id")}
    r = _genie("POST", "/start-conversation", {"content": req.question})
    return {"conversation_id": r["conversation_id"], "message_id": r["message_id"]}


@app.get("/api/genie/messages/{conversation_id}/{message_id}")
def genie_poll(conversation_id: str, message_id: str):
    m = _genie("GET", f"/conversations/{conversation_id}/messages/{message_id}")
    status = m.get("status")
    out: dict[str, Any] = {"status": status, "text": None, "sql": None, "description": None,
                           "columns": [], "rows": [], "row_count": 0, "suggested_questions": []}
    if status in ("FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
        out["text"] = (m.get("error") or {}).get("error") or "Genie could not answer this question."
        return out
    if status != "COMPLETED":
        return out
    for att in m.get("attachments") or []:
        if att.get("text"):
            out["text"] = att["text"].get("content")
        if att.get("suggested_questions"):
            out["suggested_questions"] = att["suggested_questions"].get("questions", [])
        if att.get("query"):
            out["sql"] = att["query"].get("query")
            out["description"] = att["query"].get("description")
            res = _genie("GET", f"/conversations/{conversation_id}/messages/{message_id}"
                                f"/attachments/{att['attachment_id']}/query-result")
            stmt = res.get("statement_response", {})
            cols = [c["name"] for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = (stmt.get("result") or {}).get("data_array") or []
            out["columns"], out["rows"] = cols, rows[:1000]
            out["row_count"] = stmt.get("manifest", {}).get("total_row_count", len(rows))
    return out


# ---------------------------------------------------------------------------------------------
# Static front-end
# ---------------------------------------------------------------------------------------------

@app.get("/api/health")
def health():
    return {"ok": True, "catalog": CATALOG, "genie": bool(GENIE_SPACE_ID)}


app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))
