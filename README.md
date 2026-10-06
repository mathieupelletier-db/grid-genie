# ⚡ Grid Genie

A working prototype for a Québec distribution utility: distribution asset management, winter-peak overload
risk and spatial outage response. Geometry is stored natively in Delta (`GEOMETRY(4326)`), the conversational
interface is bilingual (FR/EN, Genie), and everything is rendered on a MapLibre GL map inside a Databricks App.

```
Lakeflow (Auto Loader → bronze → silver GEOMETRY) → UC gold + governance (ST_*, H3, masks, row filters)
  → ML (overload >150 % next winter) + ai_query → Lakebase (synced ops tables) → Genie (FR/EN) → App (MapLibre)
```

| Path | Content |
|---|---|
| `src/00_generate_raw.py` | Synthetic network and history, landed as raw GIS / OT / AMI extracts in a UC Volume |
| `src/pipeline/01_ingest_lakeflow.py` | Lakeflow Spark Declarative Pipeline: bronze Auto Loader → silver with native `GEOMETRY` |
| `src/02_governance_spatial.sql` | Gold spatial tables (`ST_Transform`/`ST_Distance`/`ST_Length`/`ST_Buffer`/`ST_Area`/`ST_ConvexHull`/H3), masks, row filters, comments |
| `src/03_intelligence.py` | Winter overload model (MLflow → UC), zone risk classification, `ai_query` recommendations, `ops_*` tables |
| `src/04_lakebase_sync.py` | Lakebase Autoscaling project + synced tables |
| `src/05_genie_space.py` | Bilingual Genie space: instructions, synonyms, join specs, 10 benchmark questions |
| `app/` | Databricks App: FastAPI (`app.py`) + MapLibre GL (`static/`), `app.yaml`, `requirements.txt`, own bundle |
| `docs/ARCHITECTURE.md` | Blueprint (diagram, design decisions, data model) |
| `docs/DEPLOYMENT.md` | 4–8 h deployment & execution guide |
| `scripts/deploy.sh` | One-shot deploy |

Quick start: `./scripts/deploy.sh <PROFILE>` — see `docs/DEPLOYMENT.md`.

The 10 reference questions (in `questions.md`) are encoded as Genie example SQL and benchmarks (FR + EN) in
`src/05_genie_space.py`. Each returns a `geojson` column, so the app draws the answer on the map.

All data is synthetic.
