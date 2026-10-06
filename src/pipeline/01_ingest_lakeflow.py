# Lakeflow Spark Declarative Pipeline — Grid Genie ingestion
#
# bronze : Auto Loader streaming tables over the raw GIS / OT / AMI extracts (as-landed, lon/lat & WKT)
# silver : materialized views with native GEOMETRY(4326) columns built with ST_ functions,
#          network topology (spans), H3 indexes, data-quality expectations.
#
# Distances / lengths / areas are always computed in EPSG:32198 (NAD83 / Québec Lambert, metres).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = f"/Volumes/{CATALOG}/bronze/landing"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
QC_LAMBERT = 32198

# Québec bounding box used for coordinate sanity checks
VALID_COORDS = "lon BETWEEN -80 AND -57 AND lat BETWEEN 44 AND 63"
VALID_GEOM = "ST_X(geom) BETWEEN -80 AND -57 AND ST_Y(geom) BETWEEN 44 AND 63"


# ---------------------------------------------------------------------------------------------
# Bronze — Auto Loader
# ---------------------------------------------------------------------------------------------

def _autoload(folder: str, fmt: str, hints: str | None = None):
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    if hints:
        reader = reader.option("cloudFiles.schemaHints", hints)
    return (
        reader.load(f"{LANDING}/{folder}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


BRONZE_SOURCES = {
    # table               folder                 format     schema hints
    "ced_raw":                ("ced",                "json",    None),
    "substations_raw":        ("substations",        "json",    "lon DOUBLE, lat DOUBLE"),
    "lines_raw":              ("lines",              "json",    None),
    "poles_raw":              ("poles",              "csv",     "lon DOUBLE, lat DOUBLE, install_year INT, branch_seq INT"),
    "transformers_raw":       ("transformers",       "csv",     "kva DOUBLE, install_year INT"),
    "protection_devices_raw": ("protection_devices", "csv",     "lon DOUBLE, lat DOUBLE, pole_id STRING"),
    "customers_raw":          ("customers",          "csv",     "lon DOUBLE, lat DOUBLE, electric_heating BOOLEAN"),
    "outages_raw":            ("outages",            "json",    "start_ts STRING, end_ts STRING"),
    "weather_raw":            ("weather",            "csv",     "date DATE, temp_min_c DOUBLE, temp_max_c DOUBLE"),
    "transformer_load_raw":   ("transformer_load",   "parquet", None),
}


def _register_bronze(table: str, folder: str, fmt: str, hints: str | None):
    @dp.table(
        name=f"{BRONZE}.{table}",
        comment=f"Raw `{folder}` extract ingested as-is by Auto Loader ({fmt}).",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return _autoload(folder, fmt, hints)


for _t, (_folder, _fmt, _hints) in BRONZE_SOURCES.items():
    _register_bronze(_t, _folder, _fmt, _hints)


# ---------------------------------------------------------------------------------------------
# Silver — curated, typed, native GEOMETRY
# ---------------------------------------------------------------------------------------------

def _latest(table: str, key: str):
    """Latest record per business key from a bronze table (extracts can be re-landed)."""
    return spark.sql(f"""
        SELECT * EXCEPT (_rn) FROM (
          SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY _ingested_at DESC, _source_file DESC) AS _rn
          FROM {BRONZE}.{table}) WHERE _rn = 1""")


@dp.materialized_view(name=f"{SILVER}.operating_centers",
                      comment="Operating centres (CED) with territory polygon as native GEOMETRY(4326).")
@dp.expect_or_fail("valid_polygon", "ST_IsValid(geom)")
def operating_centers():
    return _latest("ced_raw", "ced_code").selectExpr(
        "ced_code", "ced_name", "region_name", "CAST(is_urban AS BOOLEAN) AS is_urban",
        "ST_GeomFromWKT(boundary_wkt, 4326) AS geom")


@dp.materialized_view(name=f"{SILVER}.substations",
                      comment="Distribution substations (postes) as GEOMETRY(4326) points.")
@dp.expect_or_drop("valid_coords", VALID_COORDS)
def substations():
    return _latest("substations_raw", "substation_code").selectExpr(
        "substation_code", "substation_name", "ced_code", "CAST(capacity_mva AS DOUBLE) AS capacity_mva",
        "lon", "lat", "ST_Point(lon, lat, 4326) AS geom")


@dp.materialized_view(name=f"{SILVER}.lines", comment="Feeder (ligne) reference data.")
def lines():
    return _latest("lines_raw", "line_id").selectExpr(
        "line_id", "substation_code", "ced_code", "CAST(voltage_kv AS INT) AS voltage_kv",
        "CAST(commissioning_year AS INT) AS commissioning_year", "conductor")


@dp.materialized_view(name=f"{SILVER}.poles",
                      comment="Poles (poteaux) with GEOMETRY(4326) location, topology (parent pole) and H3 res-9 cell.",
                      cluster_by=["line_id"])
@dp.expect_or_drop("valid_coords", VALID_COORDS)
@dp.expect("plausible_install_year", "install_year BETWEEN 1940 AND year(current_date())")
def poles():
    return _latest("poles_raw", "pole_id").selectExpr(
        "pole_id", "line_id", "branch_id", "branch_seq", "parent_pole_id",
        "install_year", "year(current_date()) - install_year AS age_years", "material",
        "CAST(height_m AS DOUBLE) AS height_m", "lon", "lat",
        "ST_Point(lon, lat, 4326) AS geom", "h3_longlatash3(lon, lat, 9) AS h3_cell_9")


@dp.materialized_view(name=f"{SILVER}.spans",
                      comment="Spans (portées): conductor segment between a pole and its upstream pole "
                              "(or the substation for the first trunk pole). Length via ST_Length in EPSG:32198.",
                      cluster_by=["line_id"])
@dp.expect_or_drop("positive_length", "length_m > 0")
def spans():
    return spark.sql(f"""
        WITH p AS (SELECT * FROM {SILVER}.poles),
        upstream AS (
          SELECT c.pole_id AS to_pole_id, c.line_id, c.branch_id, c.branch_seq,
                 coalesce(par.pole_id, s.substation_code) AS from_node_id,
                 coalesce(par.geom, s.geom) AS from_geom, c.geom AS to_geom
          FROM p c
          LEFT JOIN p par ON c.parent_pole_id = par.pole_id
          LEFT JOIN {SILVER}.lines l ON c.line_id = l.line_id
          LEFT JOIN {SILVER}.substations s ON l.substation_code = s.substation_code AND c.parent_pole_id IS NULL)
        SELECT concat('S-', to_pole_id) AS span_id, line_id, branch_id, branch_seq,
               from_node_id, to_pole_id,
               ST_MakeLine(array(from_geom, to_geom)) AS geom,
               ST_Length(ST_Transform(ST_MakeLine(array(from_geom, to_geom)), {QC_LAMBERT})) AS length_m
        FROM upstream""")


@dp.materialized_view(name=f"{SILVER}.transformers",
                      comment="Pole-mounted distribution transformers, located at their pole (GEOMETRY(4326)).")
@dp.expect_or_drop("has_pole", "pole_id IS NOT NULL")
@dp.expect("valid_kva", "kva > 0")
def transformers():
    t = _latest("transformers_raw", "transformer_id").alias("t")
    p = spark.read.table(f"{SILVER}.poles").alias("p")
    return t.join(p, "pole_id", "left").selectExpr(
        "t.transformer_id", "t.pole_id", "p.line_id", "p.branch_id", "t.kva", "t.install_year",
        "year(current_date()) - t.install_year AS age_years", "t.phase", "t.manufacturer", "p.geom", "p.h3_cell_9")


@dp.materialized_view(name=f"{SILVER}.protection_devices",
                      comment="Protection devices: DISJONCTEUR (feeder breaker at the substation) and "
                              "COUPE-CIRCUIT (fuse cutout at a lateral tap). Each protects one branch.")
@dp.expect_or_drop("valid_coords", VALID_GEOM)
def protection_devices():
    return _latest("protection_devices_raw", "device_id").selectExpr(
        "device_id", "device_type", "line_id", "protected_branch_id", "pole_id",
        "CAST(rating_a AS INT) AS rating_a", "CAST(install_year AS INT) AS install_year",
        "ST_Point(lon, lat, 4326) AS geom")


@dp.materialized_view(name=f"{SILVER}.customers",
                      comment="Customer connection points (points de raccordement). Contains PII (name, address, location).",
                      cluster_by=["transformer_id"])
@dp.expect_or_drop("valid_coords", VALID_GEOM)
@dp.expect_or_drop("has_transformer", "transformer_id IS NOT NULL")
def customers():
    return _latest("customers_raw", "customer_id").selectExpr(
        "customer_id", "transformer_id", "customer_name", "address", "municipality", "customer_type",
        "electric_heating", "CAST(contract_kw AS DOUBLE) AS contract_kw",
        "ST_Point(lon, lat, 4326) AS geom", "h3_longlatash3(lon, lat, 8) AS h3_cell_8")


@dp.materialized_view(name=f"{SILVER}.outages",
                      comment="Interruptions with duration, Customer-Hours Interrupted (CHI) and fault location. "
                              "Ongoing interruptions have end_ts NULL; their CHI is accrued up to refresh time.")
@dp.expect_or_drop("valid_customers", "customers_interrupted > 0")
@dp.expect("end_after_start", "end_ts IS NULL OR end_ts >= start_ts")
def outages():
    return _latest("outages_raw", "outage_id").selectExpr(
        "outage_id", "device_id", "CAST(start_ts AS TIMESTAMP) AS start_ts", "CAST(end_ts AS TIMESTAMP) AS end_ts",
        "end_ts IS NULL AS is_ongoing", "cause", "CAST(customers_interrupted AS INT) AS customers_interrupted",
        "(unix_timestamp(coalesce(CAST(end_ts AS TIMESTAMP), current_timestamp())) "
        " - unix_timestamp(CAST(start_ts AS TIMESTAMP))) / 3600.0 AS duration_h",
        "ST_Point(fault_lon, fault_lat, 4326) AS fault_geom")


@dp.materialized_view(name=f"{SILVER}.weather_daily", comment="Daily min/max temperature per CED.")
def weather_daily():
    return _latest("weather_raw", "date, ced_code").selectExpr(
        "date AS weather_date", "ced_code", "temp_min_c", "temp_max_c")


@dp.materialized_view(name=f"{SILVER}.transformer_load_daily",
                      comment="Daily peak load per transformer with overload % (peak kVA / nameplate kVA) and winter label. "
                              "Winter 'Hiver YYYY' = December of YYYY-1 through March of YYYY.",
                      cluster_by=["transformer_id", "reading_date"])
@dp.expect_or_drop("positive_peak", "peak_kva >= 0")
def transformer_load_daily():
    ld = spark.read.table(f"{BRONZE}.transformer_load_raw").alias("ld")
    t = spark.read.table(f"{SILVER}.transformers").select("transformer_id", "kva", "line_id").alias("t")
    return ld.join(t, "transformer_id").selectExpr(
        "transformer_id", "line_id", "CAST(reading_date AS DATE) AS reading_date", "peak_kva", "energy_kwh",
        "round(100 * peak_kva / kva, 1) AS overload_pct",
        "CASE WHEN month(reading_date) = 12 THEN concat('Hiver ', year(reading_date) + 1) "
        "     WHEN month(reading_date) <= 3 THEN concat('Hiver ', year(reading_date)) END AS winter_label")
