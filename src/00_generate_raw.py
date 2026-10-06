#
# # 00 · Synthetic distribution-network generator
#
# Generates a realistic, fully synthetic distribution network for three operating centres
# (CED Laval, CED Beauce, CED Matapédia) and lands it as raw GIS/OT exports in a UC Volume:
#
# | File | Format | Content |
# |---|---|---|
# | `ced/` | JSON | Operating-centre territories (boundary as WKT, as exported by the GIS) |
# | `substations/` | JSON | Distribution substations (postes) — lon/lat |
# | `lines/` | JSON | Feeder metadata (lignes) |
# | `poles/` | CSV | Poles with parent pole (network topology) — lon/lat |
# | `transformers/` | CSV | Pole-mounted transformers |
# | `protection_devices/` | CSV | Breakers (disjoncteurs) and fuse cutouts (coupe-circuits) |
# | `customers/` | CSV | Customer connection points (PII) |
# | `outages/` | JSON | Interruption events (incl. ongoing) |
# | `weather/` | CSV | Daily temperature per CED |
# | `transformer_load/` | Parquet | Daily peak load per transformer (AMI aggregation) |
#
# No real customer or network data is used.

# ---------------------------------------------------------------------------------------------

import json
import math
import os
import random
import string
import sys
import zlib
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd


def _arg(name, default):
    """Read `--name value` from job parameters, falling back to a default."""
    if f"--{name}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{name}") + 1]
    return default


CATALOG = _arg("catalog", "grid_genie")
VOLUME_ROOT = _arg("volume_root", f"/Volumes/{CATALOG}/bronze/landing")
AS_OF = date.fromisoformat(_arg("as_of", "2026-10-05"))
HISTORY_START = date(2024, 10, 1)
SEED = 20260105

if VOLUME_ROOT.startswith("/Volumes/"):
    # Schemas + landing volume (the catalog itself is created by scripts/deploy.sh)
    from pyspark.sql import SparkSession

    _spark = SparkSession.builder.getOrCreate()
    for _schema, _comment in [("bronze", "Bronze — raw extracts as landed"),
                              ("silver", "Silver — curated, typed, native GEOMETRY"),
                              ("gold", "Gold — curated network, outage & risk data products"),
                              ("governance", "Access-control mappings and policy functions")]:
        _spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{_schema} COMMENT '{_comment}'")
    _spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing COMMENT 'Landing zone for GIS / OT / AMI extracts'")

rng = np.random.default_rng(SEED)
random.seed(SEED)

# ---------------------------------------------------------------------------------------------

### Reference geography

# ---------------------------------------------------------------------------------------------

CEDS = {
    "LAV": {"name": "CED Laval", "region": "Laval", "urban": True},
    "BCE": {"name": "CED Beauce", "region": "Beauce", "urban": False},
    "MAT": {"name": "CED Matapédia", "region": "Matapédia", "urban": False},
}

# code, display name, CED, lon, lat, line numbers
SUBSTATIONS = [
    ("LAV_SVR", "LAV SVR", "LAV", -73.7350, 45.6050, [241, 242, 243, 244, 245]),
    ("LAV_DUV", "LAV DUV", "LAV", -73.6900, 45.5650, [311, 312, 313, 314]),
    ("BCE_STG", "BCE STG", "BCE", -70.6650, 46.1200, [221, 222, 223, 224]),
    ("BCE_SJB", "BCE SJB", "BCE", -70.8800, 46.3050, [131, 132, 133]),
    ("MAT_AMQ", "MAT AMQ", "MAT", -67.4300, 48.4650, [251, 252, 253, 254]),
    ("MAT_CAU", "MAT CAU", "MAT", -67.2300, 48.3600, [161, 162, 163]),
]

MUNICIPALITIES = {
    "LAV": ["Laval", "Sainte-Rose", "Auteuil", "Vimont", "Duvernay", "Saint-François"],
    "BCE": ["Saint-Georges", "Saint-Joseph-de-Beauce", "Beauceville", "Saint-Côme-Linière", "Notre-Dame-des-Pins"],
    "MAT": ["Amqui", "Causapscal", "Lac-au-Saumon", "Sayabec", "Val-Brillant"],
}
FIRST = ["Marie", "Jean", "Sophie", "Luc", "Isabelle", "Marc", "Julie", "Pierre", "Nathalie", "François",
         "Catherine", "Michel", "Geneviève", "Éric", "Chantal", "Martin", "Valérie", "Alexandre", "Mélanie", "Simon"]
LAST = ["Tremblay", "Gagnon", "Roy", "Côté", "Bouchard", "Gauthier", "Morin", "Lavoie", "Fortin", "Gagné",
        "Ouellet", "Pelletier", "Bélanger", "Lévesque", "Bergeron", "Leblanc", "Paquette", "Girard", "Simard", "Boucher"]
STREETS = ["rue Principale", "boulevard des Laurentides", "rang Saint-Joseph", "chemin du Lac", "rue de l'Église",
           "avenue du Parc", "route 132", "rue Notre-Dame", "rang des Érables", "boulevard Lacroix", "rue Saint-Pierre"]


def to_lonlat(lon0, lat0, x, y):
    """Local metric offsets (m) → lon/lat (small-area equirectangular approximation)."""
    return lon0 + x / (111_320 * math.cos(math.radians(lat0))), lat0 + y / 110_540


def rand_code(n=5):
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))

# ---------------------------------------------------------------------------------------------

### Network topology: poles → spans → lines, transformers, protection, customers

# ---------------------------------------------------------------------------------------------

poles, transformers, devices, customers, lines = [], [], [], [], []
used_device_codes = {"LAV_Y3Z4G"}
tx_seq, cust_seq = 0, 0
STD_KVA = np.array([25, 37.5, 50, 75, 100, 167])

for sub_code, sub_name, ced, lon0, lat0, line_nums in SUBSTATIONS:
    urban = CEDS[ced]["urban"]
    n_lines = len(line_nums)
    for li, num in enumerate(line_nums):
        line_id = f"{sub_code}_{num}"
        build_year = int(rng.integers(1962, 1996)) if not urban else int(rng.integers(1958, 1993))
        if line_id == "LAV_SVR_242":
            build_year = 1964  # storyline: one of the oldest feeders out of LAV SVR
        lines.append({
            "line_id": line_id, "substation_code": sub_code, "ced_code": ced,
            "voltage_kv": 25 if urban else 25 if rng.random() < 0.7 else 12,
            "commissioning_year": build_year,
            "conductor": random.choice(["ACSR 336", "ACSR 4/0", "AAAC 2/0"]),
        })

        # Trunk: meandering walk away from the substation in this feeder's sector
        heading = (2 * math.pi * li / n_lines) + rng.normal(0, 0.25)
        n_trunk = int(rng.integers(55, 80)) if urban else int(rng.integers(90, 150))
        x = y = 0.0
        branch_specs = [("T", None, n_trunk, heading)]
        # Laterals tapped from trunk poles
        n_lat = int(rng.integers(4, 7))
        tap_positions = sorted(rng.choice(np.arange(5, n_trunk - 3), size=n_lat, replace=False).tolist())
        trunk_ids, trunk_poles = [], []

        def span_len():
            base = rng.uniform(35, 75) if urban else rng.uniform(55, 130)
            if rng.random() < (0.01 if urban else 0.03):  # river / road crossings
                base = rng.uniform(220, 480)
            return base

        for k in range(1, n_trunk + 1):
            heading += rng.normal(0, 0.12)
            d = span_len()
            x += d * math.cos(heading)
            y += d * math.sin(heading)
            lon, lat = to_lonlat(lon0, lat0, x, y)
            pid = f"P-{line_id}-T{k:03d}"
            replaced = rng.random() < 0.45
            year = int(rng.integers(build_year + 8, 2025)) if replaced else build_year + int(rng.integers(0, 3))
            poles.append({
                "pole_id": pid, "line_id": line_id, "branch_id": f"{line_id}-T", "branch_seq": k,
                "parent_pole_id": trunk_ids[-1] if trunk_ids else None,
                "lon": round(lon, 7), "lat": round(lat, 7), "install_year": min(year, 2024),
                "material": "BOIS" if rng.random() < 0.88 else "BÉTON",
                "height_m": float(random.choice([10.7, 12.2, 13.7])),
                "_x": x, "_y": y, "_heading": heading,
            })
            trunk_ids.append(pid)
            trunk_poles.append(poles[-1])

        # Breaker at the substation protects the whole feeder
        devices.append({
            "device_id": f"{line_id}_DJ", "device_type": "DISJONCTEUR", "line_id": line_id,
            "protected_branch_id": f"{line_id}-T", "pole_id": None,
            "lon": lon0, "lat": lat0, "rating_a": 600, "install_year": build_year,
        })

        for j, tap in enumerate(tap_positions, start=1):
            tap_pole = trunk_poles[tap - 1]
            lx, ly = tap_pole["_x"], tap_pole["_y"]
            lh = tap_pole["_heading"] + random.choice([-1, 1]) * (math.pi / 2 + rng.normal(0, 0.3))
            n_l = int(rng.integers(10, 28))
            if line_id == "LAV_SVR_242" and j == 2:
                n_l = 26  # the lateral protected by LAV_Y3Z4G
            branch_id = f"{line_id}-L{j:02d}"
            prev = tap_pole["pole_id"]
            first_lat_pole = None
            for k in range(1, n_l + 1):
                lh += rng.normal(0, 0.15)
                d = span_len()
                lx += d * math.cos(lh)
                ly += d * math.sin(lh)
                lon, lat = to_lonlat(lon0, lat0, lx, ly)
                pid = f"P-{line_id}-L{j:02d}-{k:03d}"
                replaced = rng.random() < 0.4
                year = int(rng.integers(build_year + 10, 2025)) if replaced else build_year + int(rng.integers(2, 12))
                poles.append({
                    "pole_id": pid, "line_id": line_id, "branch_id": branch_id, "branch_seq": k,
                    "parent_pole_id": prev, "lon": round(lon, 7), "lat": round(lat, 7),
                    "install_year": min(year, 2024),
                    "material": "BOIS" if rng.random() < 0.93 else "BÉTON",
                    "height_m": float(random.choice([10.7, 12.2])),
                    "_x": lx, "_y": ly, "_heading": lh,
                })
                prev = pid
                first_lat_pole = first_lat_pole or pid
            if line_id == "LAV_SVR_242" and j == 2:
                dev_code = "LAV_Y3Z4G"
            else:
                dev_code = f"{ced}_{rand_code()}"
                while dev_code in used_device_codes:
                    dev_code = f"{ced}_{rand_code()}"
                used_device_codes.add(dev_code)
            devices.append({
                "device_id": dev_code, "device_type": "COUPE-CIRCUIT", "line_id": line_id,
                "protected_branch_id": branch_id, "pole_id": tap_pole["pole_id"],
                "lon": tap_pole["lon"], "lat": tap_pole["lat"],
                "rating_a": int(random.choice([40, 65, 100])), "install_year": int(rng.integers(build_year, 2025)),
            })

poles_df = pd.DataFrame(poles)
print(f"lines={len(lines)} poles={len(poles_df)} devices={len(devices)}")

# ---------------------------------------------------------------------------------------------

# Transformers on ~30% of poles, customers around each transformer
line_meta = {l["line_id"]: l for l in lines}
forced_hot = set()
for p in poles:
    ced = line_meta[p["line_id"]]["ced_code"]
    urban = CEDS[ced]["urban"]
    if rng.random() > (0.32 if urban else 0.27):
        continue
    tx_seq += 1
    tid = f"T-{tx_seq:06d}"
    n_c = int(rng.integers(6, 16)) if urban else int(rng.integers(2, 9))
    heat_share = rng.uniform(0.75, 0.95)
    # design ratio (peak at -25 °C / nameplate). >1.35 → winter overload candidates
    u = float(rng.lognormal(math.log(0.80), 0.26))
    if p["line_id"] == "LAV_SVR_242" and len(forced_hot) < 6 and rng.random() < 0.35:
        u = float(rng.uniform(1.45, 1.8))
        forced_hot.add(tid)
    design_kw = n_c * (1.2 + heat_share * 0.20 * 40) * 0.6
    kva = float(STD_KVA[np.argmin(np.abs(STD_KVA - design_kw / u))])
    pole_year = p["install_year"]
    tx_year = int(min(2024, max(1958, pole_year + rng.integers(-4, 15)))) if rng.random() < 0.7 \
        else int(rng.integers(1960, 2025))
    transformers.append({
        "transformer_id": tid, "pole_id": p["pole_id"], "kva": kva, "install_year": tx_year,
        "phase": "MONO" if rng.random() < 0.9 else "TRI",
        "manufacturer": random.choice(["Moloney", "Carte", "Hammond", "ABB", "Pioneer"]),
        "_n_cust": n_c, "_heat_share": heat_share,
        # electrification pace differs by neighbourhood (heat pumps, EVs, new connections)
        "_growth": float(np.clip(rng.normal(0.035, 0.06), -0.03, 0.25)),
    })
    for _ in range(n_c):
        cust_seq += 1
        ang, dist = rng.uniform(0, 2 * math.pi), rng.uniform(15, 70 if urban else 160)
        clon, clat = to_lonlat(p["lon"], p["lat"], dist * math.cos(ang), dist * math.sin(ang))
        muni = random.choice(MUNICIPALITIES[ced])
        customers.append({
            "customer_id": f"C-{cust_seq:07d}", "transformer_id": tid,
            "customer_name": f"{random.choice(FIRST)} {random.choice(LAST)}",
            "address": f"{int(rng.integers(10, 9999))} {random.choice(STREETS)}, {muni}",
            "municipality": muni, "lon": round(clon, 7), "lat": round(clat, 7),
            "customer_type": "RÉSIDENTIEL" if rng.random() < (0.86 if urban else 0.8) else
                             random.choice(["COMMERCIAL", "AGRICOLE" if not urban else "COMMERCIAL", "INSTITUTIONNEL"]),
            "electric_heating": bool(rng.random() < heat_share),
            "contract_kw": float(random.choice([10, 15, 20, 25, 40])),
        })

tx_df = pd.DataFrame(transformers)
cust_df = pd.DataFrame(customers)
print(f"transformers={len(tx_df)} customers={len(cust_df)} forced_hot_242={len(forced_hot)}")

# ---------------------------------------------------------------------------------------------

### Weather, transformer daily peak load and outages

# ---------------------------------------------------------------------------------------------

days = pd.date_range(HISTORY_START, AS_OF - timedelta(days=1), freq="D")
weather_rows = []
JAN_TMIN = {"LAV": -15.0, "BCE": -19.0, "MAT": -20.5}
SUMMER_TMIN = {"LAV": 15.0, "BCE": 11.5, "MAT": 10.0}
temps = {}
for ced in CEDS:
    noise, series = 0.0, []
    for d in days:
        doy = d.dayofyear
        season = (1 + math.cos(2 * math.pi * (doy - 20) / 365.25)) / 2  # 1 in late Jan, 0 in late Jul
        mean = SUMMER_TMIN[ced] + (JAN_TMIN[ced] - SUMMER_TMIN[ced]) * season
        noise = 0.75 * noise + rng.normal(0, 3.6)
        # Two named cold snaps used in the storyline (Jan 2025, Feb 2026)
        snap = -9.0 if date(2025, 1, 20) <= d.date() <= date(2025, 1, 24) else 0.0
        snap += -7.0 if date(2026, 2, 2) <= d.date() <= date(2026, 2, 5) else 0.0
        tmin = round(mean + noise + snap, 1)
        series.append(tmin)
        weather_rows.append({"date": d.date().isoformat(), "ced_code": ced, "temp_min_c": tmin,
                             "temp_max_c": round(tmin + rng.uniform(5, 11), 1)})
    temps[ced] = np.array(series)
weather_df = pd.DataFrame(weather_rows)

# Daily peak load per transformer (vectorised per transformer)
pole_line = poles_df.set_index("pole_id")["line_id"].to_dict()
years_since = np.array([(d.date() - HISTORY_START).days / 365.25 for d in days])
load_frames = []
for t in transformers:
    ced = line_meta[pole_line[t["pole_id"]]]["ced_code"]
    tmin = temps[ced]
    heat_kw = t["_heat_share"] * 0.20 * np.maximum(0, 15 - tmin)
    base_kw = 1.2 + 0.25 * (np.array([d.dayofweek for d in days]) < 5)
    growth = 1 + t["_growth"] * years_since
    noise = rng.lognormal(0, 0.07, size=len(days))
    peak_kva = t["_n_cust"] * (base_kw + heat_kw) * 0.6 * growth * noise / 0.95  # pf ≈ 0.95
    load_frames.append(pd.DataFrame({
        "transformer_id": t["transformer_id"], "reading_date": [d.date() for d in days],
        "peak_kva": np.round(peak_kva, 2), "energy_kwh": np.round(peak_kva * 0.95 * rng.uniform(11, 15, len(days)), 1),
    }))
load_df = pd.concat(load_frames, ignore_index=True)
print(f"load rows={len(load_df):,}")

# ---------------------------------------------------------------------------------------------

# Outages per protection zone (Poisson; older / rural / longer zones fail more, winter and summer storm peaks)
poles_df["age"] = AS_OF.year - poles_df["install_year"]
branch_stats = poles_df.groupby("branch_id").agg(n_poles=("pole_id", "count"), avg_age=("age", "mean"))
tx_branch = tx_df.merge(poles_df[["pole_id", "branch_id", "line_id"]], on="pole_id")
cust_branch = cust_df.merge(tx_branch[["transformer_id", "branch_id", "line_id"]], on="transformer_id")
cust_per_branch = cust_branch.groupby("branch_id").size().to_dict()
cust_per_line = cust_branch.groupby("line_id").size().to_dict()

CAUSES = [("VÉGÉTATION", 0.32), ("BRIS ÉQUIPEMENT", 0.22), ("INTEMPÉRIES", 0.24), ("ANIMAL", 0.08),
          ("ACCIDENT VÉHICULE", 0.05), ("INCONNUE", 0.09)]
storm_days = {ced: set(rng.choice(len(days), size=8, replace=False).tolist()) for ced in CEDS}
outages, out_seq = [], 0
pole_by_branch = {b: g for b, g in poles_df.groupby("branch_id")}
for dvc in devices:
    ced = line_meta[dvc["line_id"]]["ced_code"]
    urban = CEDS[ced]["urban"]
    st = branch_stats.loc[dvc["protected_branch_id"]]
    lam_year = (0.4 + 0.035 * st.avg_age + 0.012 * st.n_poles) * (0.7 if urban else 1.25)
    if dvc["device_type"] == "DISJONCTEUR":
        lam_year *= 0.45
    # Hot spots for the storyline: a few rural zones deteriorated recently
    recent_boost = 2.6 if zlib.crc32(dvc["device_id"].encode()) % 7 == 0 else 1.0
    for i, d in enumerate(days):
        doy = d.dayofyear
        seasonal = 1 + 0.6 * math.cos(2 * math.pi * (doy - 25) / 365.25) ** 2
        lam = lam_year / 365.25 * seasonal
        if (AS_OF - d.date()).days < 180:
            lam *= recent_boost
        if i in storm_days[ced]:
            lam *= 25
        for _ in range(rng.poisson(lam)):
            out_seq += 1
            start = datetime.combine(d.date(), datetime.min.time()) + timedelta(minutes=int(rng.integers(0, 1440)))
            dur_h = float(rng.lognormal(math.log(1.8 if urban else 3.2), 0.8))
            n_aff = cust_per_line.get(dvc["line_id"], 0) if dvc["device_type"] == "DISJONCTEUR" \
                else cust_per_branch.get(dvc["protected_branch_id"], 0)
            if n_aff == 0:
                continue
            fault = pole_by_branch[dvc["protected_branch_id"]].sample(1, random_state=int(rng.integers(1e9))).iloc[0]
            cause = rng.choice([c for c, _ in CAUSES], p=[w for _, w in CAUSES])
            if i in storm_days[ced]:
                cause = "INTEMPÉRIES"
            outages.append({
                "outage_id": f"INT-{out_seq:07d}", "device_id": dvc["device_id"],
                "start_ts": start.isoformat(), "end_ts": (start + timedelta(hours=dur_h)).isoformat(timespec="seconds"),
                "cause": cause, "customers_interrupted": int(n_aff),
                "fault_lon": float(fault["lon"]), "fault_lat": float(fault["lat"]),
            })

# A handful of interruptions still ongoing at AS_OF (dispatch view)
for dvc in random.sample([d for d in devices if d["device_type"] == "COUPE-CIRCUIT"], 5):
    out_seq += 1
    start = datetime.combine(AS_OF, datetime.min.time()) + timedelta(hours=int(rng.integers(5, 10)),
                                                                     minutes=int(rng.integers(0, 60)))
    fault = pole_by_branch[dvc["protected_branch_id"]].sample(1, random_state=out_seq).iloc[0]
    outages.append({
        "outage_id": f"INT-{out_seq:07d}", "device_id": dvc["device_id"], "start_ts": start.isoformat(),
        "end_ts": None, "cause": random.choice(["VÉGÉTATION", "BRIS ÉQUIPEMENT", "INCONNUE"]),
        "customers_interrupted": int(cust_per_branch.get(dvc["protected_branch_id"], 1)),
        "fault_lon": float(fault["lon"]), "fault_lat": float(fault["lat"]),
    })
print(f"outages={len(outages):,}")

# ---------------------------------------------------------------------------------------------

### Operating-centre territories (WKT polygons)

# ---------------------------------------------------------------------------------------------

ced_rows = []
for ced, meta in CEDS.items():
    subs = [s for s in SUBSTATIONS if s[2] == ced]
    clon = float(np.mean([s[3] for s in subs]))
    clat = float(np.mean([s[4] for s in subs]))
    ced_poles = poles_df[poles_df["line_id"].map(lambda l: line_meta[l]["ced_code"]) == ced]
    kx = 111_320 * math.cos(math.radians(clat))
    r = float(np.max(np.hypot((ced_poles["lon"] - clon) * kx, (ced_poles["lat"] - clat) * 110_540))) + 4_000
    ring = []
    for a in np.linspace(0, 2 * math.pi, 25)[:-1]:
        rr = r * (1 + 0.08 * math.sin(3 * a))
        ring.append(to_lonlat(clon, clat, rr * math.cos(a), rr * math.sin(a)))
    ring.append(ring[0])
    wkt = "POLYGON((" + ", ".join(f"{lo:.6f} {la:.6f}" for lo, la in ring) + "))"
    ced_rows.append({"ced_code": ced, "ced_name": meta["name"], "region_name": meta["region"],
                     "is_urban": meta["urban"], "boundary_wkt": wkt})

# ---------------------------------------------------------------------------------------------

### Land raw files in the Volume

# ---------------------------------------------------------------------------------------------

def write_json_lines(rows, folder, name):
    os.makedirs(f"{VOLUME_ROOT}/{folder}", exist_ok=True)
    with open(f"{VOLUME_ROOT}/{folder}/{name}.json", "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_csv(df, folder, name):
    os.makedirs(f"{VOLUME_ROOT}/{folder}", exist_ok=True)
    df.to_csv(f"{VOLUME_ROOT}/{folder}/{name}.csv", index=False)


# Each run lands a complete snapshot: clear previous extracts so stale records never linger
# (the pipeline task runs with full_refresh accordingly).
import shutil

for _folder in ["ced", "substations", "lines", "poles", "transformers", "protection_devices", "customers",
                "outages", "weather", "transformer_load"]:
    shutil.rmtree(f"{VOLUME_ROOT}/{_folder}", ignore_errors=True)

batch = f"extract_{AS_OF.isoformat()}"
write_json_lines(ced_rows, "ced", batch)
write_json_lines([{"substation_code": s[0], "substation_name": s[1], "ced_code": s[2], "lon": s[3], "lat": s[4],
                   "capacity_mva": float(random.choice([28, 47, 56, 70]))} for s in SUBSTATIONS], "substations", batch)
write_json_lines(lines, "lines", batch)
write_csv(poles_df.drop(columns=["_x", "_y", "_heading", "age"]), "poles", batch)
write_csv(tx_df.drop(columns=["_n_cust", "_heat_share", "_growth"]), "transformers", batch)
write_csv(pd.DataFrame(devices), "protection_devices", batch)
write_csv(cust_df, "customers", batch)
write_json_lines(outages, "outages", batch)
write_csv(weather_df, "weather", batch)
os.makedirs(f"{VOLUME_ROOT}/transformer_load", exist_ok=True)
load_df.to_parquet(f"{VOLUME_ROOT}/transformer_load/{batch}.parquet", index=False)

print(f"Landed raw extracts under {VOLUME_ROOT}")
