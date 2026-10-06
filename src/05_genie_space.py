#
# # 05 · Genie space — « Grid Genie · Réseau de distribution »
#
# Creates or updates (idempotent, matched by title) a bilingual FR/EN Genie space over the gold tables,
# with domain instructions, spatial column descriptions, synonyms, and example SQL + benchmarks that use
# native `ST_` functions for the 10 reference operational questions.

# ---------------------------------------------------------------------------------------------

import hashlib
import json
import sys

from databricks.sdk import WorkspaceClient


def _arg(name, default):
    if f"--{name}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{name}") + 1]
    return default


CATALOG = _arg("catalog", "grid_genie")
WAREHOUSE_ID = _arg("warehouse_id", "")
TITLE = "Grid Genie · Réseau de distribution"
G = f"{CATALOG}.gold"


def sid(*parts) -> str:
    """Stable 32-hex id so re-runs update in place instead of duplicating."""
    return hashlib.md5("|".join(parts).encode()).hexdigest()

# ---------------------------------------------------------------------------------------------

### Reference questions (FR, with EN variants) and their canonical SQL

# ---------------------------------------------------------------------------------------------

GEO_RULE = "-- geojson column lets the map display the result"

QUESTIONS = [
    (["Où sont les transformateurs installés avant 1985 en Beauce ?",
      "Where are the transformers installed before 1985 in Beauce?"],
     f"""SELECT t.transformer_id, t.line_id, t.substation_code, t.install_year, t.kva, t.n_customers,
       t.lon, t.lat, ST_AsGeoJSON(t.geom) AS geojson {GEO_RULE}
FROM {G}.transformers t
JOIN {G}.operating_centers c ON ST_Contains(c.geom, t.geom)
WHERE c.region_name = 'Beauce' AND t.install_year < 1985
ORDER BY t.install_year, t.transformer_id"""),

    (["Combien de clients vont être affectés si le coupe-circuit « LAV_Y3Z4G » lâche ?",
      "How many customers will be affected if fuse cutout LAV_Y3Z4G fails?"],
     f"""SELECT d.device_id, d.device_type, d.line_id, d.customers_downstream AS clients_affectes,
       d.transformers_downstream AS transformateurs_affectes, ST_AsGeoJSON(z.zone_geom) AS geojson
FROM {G}.protection_devices d
JOIN {G}.protection_zones z ON z.zone_id = d.device_id
WHERE d.device_id = 'LAV_Y3Z4G'"""),

    (["Quelle est la distance maximale au poste pour la ligne « LAV_SVR_242 » ?",
      "What is the maximum distance to the substation for line LAV_SVR_242?"],
     f"""SELECT p.line_id,
       round(max(p.distance_to_substation_m)) AS distance_max_vol_oiseau_m,
       round(max(p.network_distance_m))       AS distance_max_le_long_de_la_ligne_m
FROM {G}.poles p
WHERE p.line_id = 'LAV_SVR_242'
GROUP BY p.line_id"""),

    (["Quelles sont les zones de protection ayant eu le plus d'interruptions au cours des 3 derniers mois ?",
      "Which protection zones had the most interruptions in the last 3 months?"],
     f"""SELECT o.zone_id, z.device_type, z.line_id, z.region_name, z.n_customers,
       count(*) AS nb_interruptions, round(sum(o.chi), 1) AS chi,
       ST_AsGeoJSON(z.zone_geom) AS geojson
FROM {G}.outages o
JOIN {G}.protection_zones z ON z.zone_id = o.zone_id
WHERE o.start_ts >= add_months(current_date(), -3)
GROUP BY o.zone_id, z.device_type, z.line_id, z.region_name, z.n_customers, z.zone_geom
ORDER BY nb_interruptions DESC, chi DESC
LIMIT 10"""),

    (["Quelles sont les zones de protection ayant eu le plus de clients-heures interrompus (CHI) au cours des 6 derniers mois ?",
      "Which protection zones had the highest customer-hours interrupted (CHI) over the last 6 months?"],
     f"""SELECT o.zone_id, z.device_type, z.line_id, z.region_name, z.n_customers,
       round(sum(o.chi), 1) AS chi_6_mois, count(*) AS nb_interruptions,
       ST_AsGeoJSON(z.zone_geom) AS geojson
FROM {G}.outages o
JOIN {G}.protection_zones z ON z.zone_id = o.zone_id
WHERE o.start_ts >= add_months(current_date(), -6)
GROUP BY o.zone_id, z.device_type, z.line_id, z.region_name, z.n_customers, z.zone_geom
ORDER BY chi_6_mois DESC
LIMIT 10"""),

    (["Afficher l'emprise (surface) de chaque ligne autour du poste « LAV SVR »",
      "Show the right-of-way area of each line around substation LAV SVR"],
     f"""SELECT l.line_id, l.substation_name, l.length_km,
       round(ST_Area(ST_Transform(l.right_of_way_geom, 32198))) AS emprise_m2,
       round(ST_Area(ST_Transform(l.right_of_way_geom, 32198)) / 10000, 2) AS emprise_ha,
       ST_AsGeoJSON(l.right_of_way_geom) AS geojson
FROM {G}.lines l
WHERE l.substation_name = 'LAV SVR'
ORDER BY l.line_id"""),

    (["Affiche le 1% des clients ayant la plus longue distance au poste de distribution pour le CED MAT (Matapédia)",
      "Show the 1% of customers farthest from their distribution substation in CED MAT (Matapédia)"],
     f"""WITH ranked AS (
  SELECT c.customer_id, c.line_id, c.substation_code, c.municipality,
         c.distance_to_substation_m, c.network_distance_m, c.geom,
         percent_rank() OVER (ORDER BY c.distance_to_substation_m DESC) AS pct_rank
  FROM {G}.customers c
  WHERE c.ced_code = 'MAT')
SELECT customer_id, line_id, substation_code, municipality,
       round(distance_to_substation_m) AS distance_vol_oiseau_m,
       round(network_distance_m) AS distance_reseau_m,
       ST_AsGeoJSON(geom) AS geojson
FROM ranked
WHERE pct_rank <= 0.01
ORDER BY distance_to_substation_m DESC"""),

    (["Sur la ligne « LAV_SVR_242 », quels sont les transformateurs ayant eu une surcharge de plus de 150% durant l'hiver 2025 ?",
      "On line LAV_SVR_242, which transformers were overloaded above 150% during winter 2025?"],
     f"""SELECT t.transformer_id, t.kva, t.n_customers,
       max(ld.overload_pct) AS surcharge_max_pct, count(*) AS jours_sup_150,
       min(ld.reading_date) AS premier_jour, max(ld.reading_date) AS dernier_jour,
       ST_AsGeoJSON(t.geom) AS geojson
FROM {G}.transformer_load_daily ld
JOIN {G}.transformers t ON t.transformer_id = ld.transformer_id
WHERE ld.line_id = 'LAV_SVR_242' AND ld.winter_label = 'Hiver 2025' AND ld.overload_pct > 150
GROUP BY t.transformer_id, t.kva, t.n_customers, t.geom
ORDER BY surcharge_max_pct DESC"""),

    (["Quels sont les poteaux les plus âgés de la ligne « LAV_SVR_242 » ?",
      "What are the oldest poles on line LAV_SVR_242?"],
     f"""SELECT p.pole_id, p.install_year, p.age_years, p.material, p.branch_type,
       round(p.network_distance_m) AS distance_reseau_m, ST_AsGeoJSON(p.geom) AS geojson
FROM {G}.poles p
WHERE p.line_id = 'LAV_SVR_242'
ORDER BY p.install_year, p.pole_id
LIMIT 20"""),

    (["Où sont les portées les plus longues ?", "Where are the longest spans?"],
     f"""SELECT s.span_id, s.line_id, s.region_name, s.from_node_id, s.to_pole_id,
       round(ST_Length(ST_Transform(s.geom, 32198)), 1) AS longueur_m,
       ST_AsGeoJSON(s.geom) AS geojson
FROM {G}.spans s
ORDER BY longueur_m DESC
LIMIT 20"""),
]

EXTRA_EXAMPLES = [
    (["Quels transformateurs sont les plus à risque de surcharge l'hiver prochain ?",
      "Which transformers are most at risk of overload next winter?"],
     f"""SELECT r.transformer_id, r.line_id, r.ced_code, round(100 * r.risk_score, 1) AS risque_pct, r.risk_tier,
       r.max_overload_pct_last_winter, ST_AsGeoJSON(t.geom) AS geojson
FROM {G}.transformer_risk r JOIN {G}.transformers t ON t.transformer_id = r.transformer_id
ORDER BY r.risk_score DESC LIMIT 20"""),
    (["Quelles zones de protection sont classées à risque élevé ?", "Which protection zones are high risk?"],
     f"""SELECT z.zone_id, z.line_id, z.region_name, z.risk_score, z.chi_6m, z.chi_trend_pct, z.avg_pole_age_years,
       z.high_risk_transformers, ST_AsGeoJSON(pz.zone_geom) AS geojson
FROM {G}.zone_risk z JOIN {G}.protection_zones pz ON pz.zone_id = z.zone_id
WHERE z.risk_tier = 'ÉLEVÉ' ORDER BY z.risk_score DESC"""),
    (["Quelles interruptions sont en cours ?", "Which outages are ongoing right now?"],
     f"""SELECT o.outage_id, o.zone_id, o.line_id, o.region_name, o.start_ts, o.cause, o.customers_interrupted,
       round(o.duration_h, 1) AS duree_h, ST_AsGeoJSON(o.fault_geom) AS geojson
FROM {G}.outages o WHERE o.is_ongoing ORDER BY o.customers_interrupted DESC"""),
    (["Combien de clients sont à moins de 500 m du poste « BCE STG » ?",
      "How many customers are within 500 m of substation BCE STG?"],
     f"""SELECT count(*) AS nb_clients
FROM {G}.customers c JOIN {G}.substations s ON s.substation_name = 'BCE STG'
WHERE ST_DWithin(ST_Transform(c.geom, 32198), ST_Transform(s.geom, 32198), 500)"""),
]

# ---------------------------------------------------------------------------------------------

### Instructions, data sources & column configuration

# ---------------------------------------------------------------------------------------------

INSTRUCTIONS = [
    "Tu es l'assistant des ingénieurs d'exploitation, des gestionnaires d'actifs et des répartiteurs pannes d'un distributeur "
    "d'électricité au Québec. Réponds dans la langue de la question (français ou anglais). "
    "You assist distribution operations engineers, asset managers and outage dispatchers. Answer in the language of the question.",
    "GLOSSAIRE / GLOSSARY: poste = substation (table substations, label substation_name e.g. 'LAV SVR', code substation_code e.g. 'LAV_SVR'); "
    "ligne = feeder/line (line_id e.g. 'LAV_SVR_242'); poteau = pole; portée = span (segment between two poles); "
    "transformateur = transformer; coupe-circuit = fuse cutout and disjoncteur = breaker (protection_devices.device_type); "
    "zone de protection = protection zone = everything downstream of one protection device (zone_id = device_id); "
    "emprise = right-of-way (lines.right_of_way_geom, 15 m each side of the conductor); "
    "CHI = clients-heures interrompus = customer-hours interrupted (outages.chi); "
    "CED = centre d'exploitation de distribution = operating centre: LAV = Laval, BCE = Beauce, MAT = Matapédia.",
    "RÉGIONS: 'en Beauce' → operating_centers.region_name = 'Beauce' (ced_code 'BCE'); 'Matapédia' or 'CED MAT' → ced_code 'MAT'; "
    "'Laval' → 'LAV'. For 'where/où' questions on assets inside a region, prefer the spatial predicate "
    "ST_Contains(operating_centers.geom, asset.geom).",
    "HIVER / WINTER: 'hiver YYYY' / 'winter YYYY' = December of YYYY-1 through March of YYYY, stored in "
    "transformer_load_daily.winter_label as 'Hiver YYYY' (e.g. 'Hiver 2025' = 2024-12-01 → 2025-03-31). "
    "Surcharge / overload % = transformer_load_daily.overload_pct (daily peak kVA / nameplate kVA × 100). "
    "'Next winter' / 'l'hiver prochain' = Hiver 2027 predictions in transformer_risk.",
    "DISTANCE AU POSTE: by default use the straight-line distance (distance_to_substation_m, computed with ST_Distance in "
    "EPSG:32198) and also show network_distance_m (distance along the line). For 'distance maximale … pour la ligne X' use poles of that line.",
    "CLASSEMENTS / RANKINGS: for 'le plus de …', 'les plus …', 'top', 'most', 'highest', 'oldest', 'longest' questions, "
    "return the top 10 rows unless the user gives another number: ORDER BY the metric DESC (then a secondary metric to break "
    "ties, e.g. CHI for interruptions, interruptions for CHI, pole_id for poles) and LIMIT 10. Never use RANK()/DENSE_RANK() "
    "filters that return every tied row. Zones de protection: aggregate outages by zone_id and join protection_zones.",
    "PÉRIODES: 'les 3 derniers mois' → outages.start_ts >= add_months(current_date(), -3); '6 derniers mois' → -6. "
    "Ongoing outages: outages.is_ongoing = true.",
    "SPATIAL SQL: geometry columns are native GEOMETRY(4326) (lon/lat). Always compute lengths, areas, distances and buffers "
    "in metres after ST_Transform(geom, 32198) (NAD83 / Québec Lambert): ST_Length, ST_Area, ST_Distance, ST_DWithin, ST_Buffer. "
    "Use ST_Contains / ST_Intersects for spatial filters.",
    "MAP OUTPUT: whenever the answer lists located objects (assets, zones, lines, customers, outages), ALWAYS add a column "
    "ST_AsGeoJSON(<geometry column>) AS geojson so the application can draw the result on the map. Do not return raw geometry columns.",
    "PII: customers.customer_name, address and geom are masked for most users; never try to unmask them. Report counts and distances.",
    "Units: distances in metres (m) or kilometres (km), areas in m² and hectares (ha), loads in kVA, overload in %.",
]

TABLES = {
    "operating_centers": [("region_name", "Région (Beauce, Laval, Matapédia)", ["région", "region", "territoire"], True),
                          ("ced_code", "Code CED", ["CED", "centre d'exploitation"], True)],
    "substations": [("substation_name", "Nom opérationnel du poste, ex. 'LAV SVR'", ["poste", "substation", "poste de distribution"], True),
                    ("substation_code", "Code du poste, ex. 'LAV_SVR'", ["code poste"], True)],
    "lines": [("line_id", "Identifiant de ligne, ex. 'LAV_SVR_242'", ["ligne", "feeder", "circuit", "artère"], True),
              ("substation_name", "Poste d'alimentation", ["poste"], True),
              ("right_of_way_area_m2", "Surface de l'emprise (m²)", ["emprise", "surface d'emprise", "right-of-way area"], False),
              ("right_of_way_geom", "Polygone d'emprise (GEOMETRY)", ["emprise", "corridor", "buffer"], False)],
    "poles": [("install_year", "Année d'installation du poteau", ["année", "âge", "plus vieux", "oldest"], False),
              ("line_id", "Ligne", ["ligne"], True)],
    "spans": [("length_m", "Longueur de portée en mètres", ["longueur", "portée longue", "span length"], False)],
    "transformers": [("install_year", "Année d'installation", ["installé", "installed", "année"], False),
                     ("kva", "Puissance nominale kVA", ["capacité", "rating", "kVA"], False)],
    "transformer_load_daily": [("overload_pct", "Taux de charge / surcharge %", ["surcharge", "overload", "charge"], False),
                               ("winter_label", "Saison d'hiver 'Hiver YYYY'", ["hiver", "winter"], True)],
    "protection_devices": [("device_id", "Identifiant de l'appareil, ex. 'LAV_Y3Z4G'", ["coupe-circuit", "disjoncteur", "fusible", "cutout", "breaker"], True),
                           ("device_type", "DISJONCTEUR ou COUPE-CIRCUIT", ["type d'appareil"], True),
                           ("customers_downstream", "Clients en aval (affectés si l'appareil opère)", ["clients affectés", "affected customers"], False)],
    "protection_zones": [("zone_id", "Zone de protection (= device_id)", ["zone", "zone de protection"], True)],
    "customers": [("distance_to_substation_m", "Distance au poste (m)", ["distance au poste", "éloignement"], False),
                  ("ced_code", "CED", ["CED"], True)],
    "outages": [("chi", "Clients-heures interrompus", ["CHI", "clients-heures", "customer hours interrupted"], False),
                ("cause", "Cause de l'interruption", ["cause"], True),
                ("zone_id", "Zone de protection", ["zone"], True)],
    "transformer_risk": [("risk_score", "Probabilité de surcharge >150% l'hiver prochain", ["risque", "risk", "probabilité"], False)],
    "zone_risk": [("risk_tier", "Niveau de risque ÉLEVÉ / MOYEN / FAIBLE", ["niveau de risque", "risk level"], True)],
}


def column_configs(cols):
    out = []
    for name, desc, syn, entity in cols:
        cfg = {"column_name": name, "description": [desc], "synonyms": syn}
        if entity:
            cfg.update({"enable_entity_matching": True, "enable_format_assistance": True})
        out.append(cfg)
    return sorted(out, key=lambda c: c["column_name"])


space = {
    "version": 2,
    "config": {"sample_questions": sorted(
        [{"id": sid("sample", q[0]), "question": [q[0]]} for q, _ in QUESTIONS]
        + [{"id": sid("sample", q[1]), "question": [q[1]]} for q, _ in QUESTIONS[:3]],
        key=lambda x: x["id"])},
    "data_sources": {"tables": sorted(
        [{"identifier": f"{G}.{t}", "column_configs": column_configs(c)} for t, c in TABLES.items()],
        key=lambda x: x["identifier"])},
    "instructions": {
        "text_instructions": [{"id": sid("instructions"), "content": [s + "\n" for s in INSTRUCTIONS]}],
        "example_question_sqls": sorted(
            [{"id": sid("example", q[0]), "question": [q[0]], "sql": [sql]} for q, sql in QUESTIONS + EXTRA_EXAMPLES],
            key=lambda x: x["id"]),
        "join_specs": sorted([
            {"id": sid("join", "tx_load"),
             "left": {"identifier": f"{G}.transformers", "alias": "transformers"},
             "right": {"identifier": f"{G}.transformer_load_daily", "alias": "transformer_load_daily"},
             "sql": ["`transformers`.`transformer_id` = `transformer_load_daily`.`transformer_id`",
                     "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"]},
            {"id": sid("join", "zone_outages"),
             "left": {"identifier": f"{G}.protection_zones", "alias": "protection_zones"},
             "right": {"identifier": f"{G}.outages", "alias": "outages"},
             "sql": ["`protection_zones`.`zone_id` = `outages`.`zone_id`", "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"]},
            {"id": sid("join", "line_poles"),
             "left": {"identifier": f"{G}.lines", "alias": "lines"},
             "right": {"identifier": f"{G}.poles", "alias": "poles"},
             "sql": ["`lines`.`line_id` = `poles`.`line_id`", "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--"]},
            {"id": sid("join", "tx_risk"),
             "left": {"identifier": f"{G}.transformers", "alias": "transformers"},
             "right": {"identifier": f"{G}.transformer_risk", "alias": "transformer_risk"},
             "sql": ["`transformers`.`transformer_id` = `transformer_risk`.`transformer_id`",
                     "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--"]},
        ], key=lambda x: x["id"]),
    },
    "benchmarks": {"questions": sorted(
        [{"id": sid("bench", lang, q[i]), "question": [q[i]], "answer": [{"format": "SQL", "content": [sql]}]}
         for q, sql in QUESTIONS for i, lang in ((0, "fr"), (1, "en"))],
        key=lambda x: x["id"])},
}

# ---------------------------------------------------------------------------------------------

### Create or update the space

# ---------------------------------------------------------------------------------------------

w = WorkspaceClient()
if not WAREHOUSE_ID:
    WAREHOUSE_ID = next(wh.id for wh in w.warehouses.list() if wh.enable_serverless_compute)

body = {
    "title": TITLE,
    "description": "Assistant bilingue (FR/EN) — actifs de distribution, surcharges hivernales, interruptions et CHI, "
                   "analyses spatiales natives (GEOMETRY, ST_). Bilingual distribution network assistant.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(space, ensure_ascii=False),
}

existing, token = None, None
while True:
    resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_size": 100, **({"page_token": token} if token else {})})
    existing = next((s for s in resp.get("spaces", []) if s.get("title") == TITLE), None)
    token = resp.get("next_page_token")
    if existing or not token:
        break

if existing:
    space_id = existing["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    print(f"Updated Genie space {space_id}")
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    print(f"Created Genie space {space_id}")

try:
    from databricks.sdk.runtime import dbutils  # available inside jobs
    dbutils.jobs.taskValues.set(key="genie_space_id", value=space_id)
except Exception:  # noqa: BLE001
    pass
print(f"GENIE_SPACE_ID={space_id}")
