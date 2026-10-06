-- =============================================================================================
-- 02 · Governance & spatial curation (Unity Catalog, Databricks SQL)
--
-- Builds the gold layer from silver with native GEOMETRY(4326) columns and ST_ functions:
--   ST_Transform → EPSG:32198 (NAD83 / Québec Lambert) for every metric measure,
--   ST_Distance / ST_Length / ST_Buffer / ST_Area / ST_Contains / ST_ConvexHull / ST_Union_Agg,
--   H3 for heat-map aggregation.
-- Then applies governance: column masks (customer PII), row filters (operating-centre access),
-- bilingual comments (FR/EN) for Genie, and Change Data Feed on the tables synced to Lakebase.
--
-- Runs as a SQL task on a SQL warehouse. Idempotent.
-- =============================================================================================

USE CATALOG grid_genie;
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Gold — curated network, outage & risk data products (Grid Genie)';
CREATE SCHEMA IF NOT EXISTS governance COMMENT 'Access-control mappings and policy functions';
USE SCHEMA gold;

-- ---------------------------------------------------------------------------------------------
-- 1. Governance objects (mapping table + policy functions) — created first so tables can bind them
-- ---------------------------------------------------------------------------------------------

-- Who may see which operating centre ('*' = all). Principal = user e-mail, SP application id, or group name.
CREATE TABLE IF NOT EXISTS governance.ced_access (
  principal STRING NOT NULL COMMENT 'User e-mail, service principal application id, or account group name',
  ced_code  STRING NOT NULL COMMENT 'CED code (LAV, BCE, MAT) or * for all')
COMMENT 'Row-level access mapping: principal → operating centre (CED).';

MERGE INTO governance.ced_access t
USING (SELECT current_user() AS principal, '*' AS ced_code
       UNION ALL SELECT 'grid_genie_all_ced', '*'
       UNION ALL SELECT 'ced_matapedia_ops', 'MAT'
       UNION ALL SELECT 'ced_beauce_ops', 'BCE'
       UNION ALL SELECT 'ced_laval_ops', 'LAV') s
ON t.principal = s.principal AND t.ced_code = s.ced_code
WHEN NOT MATCHED THEN INSERT *;

CREATE OR REPLACE FUNCTION governance.ced_row_filter(ced STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: caller sees a row when mapped to its CED (directly or through an account group), or to *.'
RETURN is_account_group_member('admins')
    OR EXISTS (SELECT 1 FROM governance.ced_access a
               WHERE (a.principal = current_user() OR is_account_group_member(a.principal))
                 AND (a.ced_code = '*' OR a.ced_code = ced));

CREATE OR REPLACE FUNCTION governance.mask_pii_text(val STRING)
RETURNS STRING
COMMENT 'Column mask: customer name/address visible only to members of grid_genie_pii_readers.'
RETURN CASE WHEN is_account_group_member('grid_genie_pii_readers') THEN val ELSE '*** masqué / masked ***' END;

CREATE OR REPLACE FUNCTION governance.mask_pii_geom(g GEOMETRY(4326))
RETURNS GEOMETRY(4326)
COMMENT 'Location generalisation (used by view gold.customers): exact customer location visible only to grid_genie_pii_readers; others get the centre of the H3 res-8 cell (~0.7 km²).'
RETURN CASE WHEN is_account_group_member('grid_genie_pii_readers') THEN g
            ELSE ST_GeomFromWKB(h3_centeraswkb(h3_longlatash3(ST_X(g), ST_Y(g), 8)), 4326) END;

-- ---------------------------------------------------------------------------------------------
-- 2. Reference & network assets
-- ---------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE operating_centers
COMMENT 'Centres d''exploitation de distribution (CED) / distribution operating centres, with territory polygon. Beauce = BCE, Matapédia = MAT, Laval = LAV.'
AS SELECT ced_code, ced_name, region_name, is_urban,
          ST_Area(ST_Transform(geom, 32198)) / 1e6 AS area_km2,
          geom
   FROM grid_genie.silver.operating_centers;

CREATE OR REPLACE TABLE substations
COMMENT 'Postes de distribution / distribution substations. substation_name is the operational label (e.g. "LAV SVR").'
AS SELECT s.substation_code, s.substation_name, s.ced_code, c.region_name, s.capacity_mva, s.geom
   FROM grid_genie.silver.substations s JOIN grid_genie.silver.operating_centers c USING (ced_code);

-- Poles with straight-line ("à vol d'oiseau") and network (along the conductor) distance to the substation.
CREATE OR REPLACE TABLE poles CLUSTER BY (line_id)
COMMENT 'Poteaux / poles. distance_to_substation_m = straight-line distance (ST_Distance, EPSG:32198); network_distance_m = distance along the conductor from the substation (sum of span lengths).'
AS
WITH span_cum AS (
  SELECT sp.to_pole_id AS pole_id, sp.line_id, sp.branch_id, sp.branch_seq, sp.length_m,
         SUM(sp.length_m) OVER (PARTITION BY sp.branch_id ORDER BY sp.branch_seq) AS cum_in_branch_m
  FROM grid_genie.silver.spans sp),
trunk AS (SELECT pole_id, cum_in_branch_m AS network_distance_m FROM span_cum WHERE branch_id LIKE '%-T'),
lateral_offset AS (  -- network distance of the trunk pole each lateral is tapped from
  SELECT p.branch_id, t.network_distance_m AS offset_m
  FROM grid_genie.silver.poles p JOIN trunk t ON p.parent_pole_id = t.pole_id
  WHERE p.branch_seq = 1 AND p.branch_id NOT LIKE '%-T')
SELECT p.pole_id, p.line_id, l.substation_code, s.substation_name, l.ced_code, c.region_name,
       p.branch_id, CASE WHEN p.branch_id LIKE '%-T' THEN 'TRONC' ELSE 'DÉRIVATION' END AS branch_type,
       p.branch_seq, p.parent_pole_id, p.install_year, p.age_years, p.material, p.height_m,
       ST_Distance(ST_Transform(p.geom, 32198), ST_Transform(s.geom, 32198)) AS distance_to_substation_m,
       sc.cum_in_branch_m + coalesce(lo.offset_m, 0) AS network_distance_m,
       p.h3_cell_9, p.geom
FROM grid_genie.silver.poles p
JOIN grid_genie.silver.lines l USING (line_id)
JOIN grid_genie.silver.substations s ON l.substation_code = s.substation_code
JOIN grid_genie.silver.operating_centers c ON l.ced_code = c.ced_code
JOIN span_cum sc ON sc.pole_id = p.pole_id
LEFT JOIN lateral_offset lo ON lo.branch_id = p.branch_id;

CREATE OR REPLACE TABLE spans CLUSTER BY (line_id)
COMMENT 'Portées / spans: conductor segment between two consecutive poles (LINESTRING). length_m from ST_Length in EPSG:32198.'
AS SELECT sp.span_id, sp.line_id, l.substation_code, l.ced_code, c.region_name, sp.branch_id,
          sp.from_node_id, sp.to_pole_id, round(sp.length_m, 1) AS length_m, sp.geom
   FROM grid_genie.silver.spans sp
   JOIN grid_genie.silver.lines l USING (line_id)
   JOIN grid_genie.silver.operating_centers c ON l.ced_code = c.ced_code;

-- Protection devices and the population downstream of each (what is lost if the device operates / fails).
CREATE OR REPLACE TABLE protection_devices
COMMENT 'Appareils de protection / protection devices. DISJONCTEUR = feeder breaker (protects the whole line); COUPE-CIRCUIT = fuse cutout protecting one lateral. customers_downstream = customers interrupted if the device operates.'
AS
WITH cust_branch AS (
  SELECT t.branch_id, t.line_id, count(*) AS n
  FROM grid_genie.silver.customers cu JOIN grid_genie.silver.transformers t USING (transformer_id)
  GROUP BY ALL),
tx_branch AS (SELECT branch_id, line_id, count(*) AS n FROM grid_genie.silver.transformers GROUP BY ALL)
SELECT d.device_id, d.device_type, d.line_id, l.substation_code, l.ced_code, c.region_name,
       d.protected_branch_id, d.pole_id, d.rating_a, d.install_year,
       CASE WHEN d.device_type = 'DISJONCTEUR'
            THEN (SELECT sum(n) FROM cust_branch cb WHERE cb.line_id = d.line_id)
            ELSE (SELECT sum(n) FROM cust_branch cb WHERE cb.branch_id = d.protected_branch_id) END AS customers_downstream,
       CASE WHEN d.device_type = 'DISJONCTEUR'
            THEN (SELECT sum(n) FROM tx_branch tb WHERE tb.line_id = d.line_id)
            ELSE (SELECT sum(n) FROM tx_branch tb WHERE tb.branch_id = d.protected_branch_id) END AS transformers_downstream,
       d.geom
FROM grid_genie.silver.protection_devices d
JOIN grid_genie.silver.lines l USING (line_id)
JOIN grid_genie.silver.operating_centers c ON l.ced_code = c.ced_code;

-- Customers (PII) — base table with exact locations; masks & row filter applied below.
-- UC column masks do not support GEOMETRY, so consumers use the dynamic view `customers` (geometry generalised
-- to the H3 res-8 cell centre unless the caller is in grid_genie_pii_readers).
CREATE OR REPLACE TABLE customers_pii CLUSTER BY (line_id)
COMMENT 'RESTRICTED base table — customer connection points with exact location. Use the view gold.customers.'
AS
SELECT cu.customer_id, cu.customer_name, cu.address, cu.municipality, cu.customer_type, cu.electric_heating,
       cu.contract_kw, cu.transformer_id, p.pole_id, p.line_id, p.substation_code, p.substation_name,
       p.ced_code, p.region_name, d.device_id AS zone_id,
       ST_Distance(ST_Transform(cu.geom, 32198), ST_Transform(s.geom, 32198)) AS distance_to_substation_m,
       p.network_distance_m + ST_Distance(ST_Transform(cu.geom, 32198), ST_Transform(p.geom, 32198)) AS network_distance_m,
       cu.h3_cell_8, cu.geom
FROM grid_genie.silver.customers cu
JOIN grid_genie.silver.transformers t USING (transformer_id)
JOIN poles p ON t.pole_id = p.pole_id
JOIN grid_genie.silver.substations s ON p.substation_code = s.substation_code
JOIN grid_genie.silver.protection_devices d ON d.protected_branch_id = t.branch_id;

CREATE OR REPLACE VIEW customers
COMMENT 'Clients / customer connection points. zone_id = protection zone (immediate upstream protection device). distance_to_substation_m = straight-line distance to the feeding substation (EPSG:32198). customer_name, address and geom are PII: masked / generalised to the H3 res-8 cell centre unless the caller is in grid_genie_pii_readers.'
AS SELECT * EXCEPT (geom), governance.mask_pii_geom(geom) AS geom FROM customers_pii;

-- Lines: geometry assembled from spans, right-of-way (emprise) buffer and its surface.
CREATE OR REPLACE TABLE lines
COMMENT 'Lignes / distribution feeders. geom = MULTILINESTRING of all spans (ST_Union_Agg). right_of_way_geom = emprise: 15 m buffer each side of the conductor (ST_Buffer in EPSG:32198). right_of_way_area_m2 = ST_Area of the emprise.'
AS
WITH g AS (
  SELECT line_id, ST_Union_Agg(geom) AS geom, sum(length_m) AS length_m, count(*) AS n_spans
  FROM grid_genie.silver.spans GROUP BY line_id),
stats AS (
  SELECT line_id, count(*) AS n_poles, max(distance_to_substation_m) AS max_distance_to_substation_m,
         max(network_distance_m) AS max_network_distance_m, avg(age_years) AS avg_pole_age_years
  FROM poles GROUP BY line_id),
cust AS (SELECT t.line_id, count(*) AS n_customers
         FROM grid_genie.silver.customers cu JOIN grid_genie.silver.transformers t USING (transformer_id) GROUP BY t.line_id),
tx AS (SELECT line_id, count(*) AS n_transformers FROM grid_genie.silver.transformers GROUP BY line_id)
SELECT l.line_id, l.substation_code, s.substation_name, l.ced_code, c.region_name, l.voltage_kv,
       l.commissioning_year, l.conductor,
       round(g.length_m / 1000, 2) AS length_km, g.n_spans, stats.n_poles, tx.n_transformers, cust.n_customers,
       round(stats.max_distance_to_substation_m, 0) AS max_distance_to_substation_m,
       round(stats.max_network_distance_m, 0) AS max_network_distance_m,
       round(stats.avg_pole_age_years, 1) AS avg_pole_age_years,
       15.0 AS right_of_way_half_width_m,
       round(ST_Area(ST_Buffer(ST_Transform(g.geom, 32198), 15.0)), 0) AS right_of_way_area_m2,
       g.geom,
       ST_Transform(ST_Buffer(ST_Transform(g.geom, 32198), 15.0), 4326) AS right_of_way_geom
FROM grid_genie.silver.lines l
JOIN g USING (line_id) JOIN stats USING (line_id)
LEFT JOIN cust USING (line_id) LEFT JOIN tx USING (line_id)
JOIN grid_genie.silver.substations s ON l.substation_code = s.substation_code
JOIN grid_genie.silver.operating_centers c ON l.ced_code = c.ced_code;

-- Transformers with winter peak overload summary.
CREATE OR REPLACE TABLE transformers CLUSTER BY (line_id)
COMMENT 'Transformateurs / distribution transformers. overload % = daily peak kVA / nameplate kVA × 100. Winter "Hiver YYYY" = Dec YYYY-1 → Mar YYYY. max_overload_pct_hiver_2025 / _2026 = worst day of that winter.'
AS
WITH w AS (
  SELECT transformer_id,
         max(CASE WHEN winter_label = 'Hiver 2025' THEN overload_pct END) AS max_overload_pct_hiver_2025,
         max(CASE WHEN winter_label = 'Hiver 2026' THEN overload_pct END) AS max_overload_pct_hiver_2026,
         count_if(winter_label = 'Hiver 2025' AND overload_pct > 150) AS days_over_150_hiver_2025,
         count_if(winter_label = 'Hiver 2026' AND overload_pct > 150) AS days_over_150_hiver_2026
  FROM grid_genie.silver.transformer_load_daily GROUP BY transformer_id),
cust AS (SELECT transformer_id, count(*) AS n_customers, avg(CAST(electric_heating AS INT)) AS electric_heating_share
         FROM grid_genie.silver.customers GROUP BY transformer_id)
SELECT t.transformer_id, t.pole_id, t.line_id, p.substation_code, p.ced_code, p.region_name, d.device_id AS zone_id,
       t.kva, t.install_year, t.age_years, t.phase, t.manufacturer,
       coalesce(cust.n_customers, 0) AS n_customers, round(cust.electric_heating_share, 2) AS electric_heating_share,
       w.max_overload_pct_hiver_2025, w.max_overload_pct_hiver_2026,
       w.days_over_150_hiver_2025, w.days_over_150_hiver_2026,
       p.distance_to_substation_m, t.h3_cell_9, ST_X(t.geom) AS lon, ST_Y(t.geom) AS lat, t.geom
FROM grid_genie.silver.transformers t
JOIN poles p ON t.pole_id = p.pole_id
JOIN grid_genie.silver.protection_devices d ON d.protected_branch_id = t.branch_id
LEFT JOIN w USING (transformer_id) LEFT JOIN cust USING (transformer_id);

CREATE OR REPLACE TABLE transformer_load_daily CLUSTER BY (transformer_id, reading_date)
COMMENT 'Charge quotidienne / daily peak load per transformer with overload % and winter label (Hiver YYYY = Dec YYYY-1 → Mar YYYY).'
AS SELECT ld.transformer_id, ld.line_id, t.ced_code, ld.reading_date, ld.winter_label, ld.peak_kva, t.kva,
          ld.overload_pct, ld.energy_kwh, w.temp_min_c
   FROM grid_genie.silver.transformer_load_daily ld
   JOIN transformers t USING (transformer_id)
   LEFT JOIN grid_genie.silver.weather_daily w ON w.ced_code = t.ced_code AND w.weather_date = ld.reading_date;

-- ---------------------------------------------------------------------------------------------
-- 3. Outages & protection zones
-- ---------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE outages CLUSTER BY (start_ts)
COMMENT 'Interruptions / outage events. zone_id = protection zone (device that operated). chi = customers_interrupted × duration_h (clients-heures interrompus). Ongoing outages: is_ongoing = true, end_ts NULL.'
AS SELECT o.outage_id, o.device_id AS zone_id, d.device_type, d.line_id, d.substation_code, d.ced_code, d.region_name,
          o.start_ts, o.end_ts, o.is_ongoing, o.cause, o.customers_interrupted,
          round(o.duration_h, 2) AS duration_h, round(o.customers_interrupted * o.duration_h, 1) AS chi,
          ST_X(o.fault_geom) AS fault_lon, ST_Y(o.fault_geom) AS fault_lat, o.fault_geom
   FROM grid_genie.silver.outages o JOIN protection_devices d ON o.device_id = d.device_id;

-- A protection zone = area served downstream of a protection device: convex hull of its assets + 30 m.
CREATE OR REPLACE TABLE protection_zones
COMMENT 'Zones de protection / protection zones (one per protection device). zone_geom = ST_ConvexHull of downstream poles buffered 30 m. Includes 3- and 6-month interruption and CHI KPIs relative to current_date() at build time.'
AS
WITH zone_poles AS (
  SELECT d.device_id AS zone_id, p.geom, p.age_years
  FROM protection_devices d
  JOIN poles p ON (d.device_type = 'COUPE-CIRCUIT' AND p.branch_id = d.protected_branch_id)
               OR (d.device_type = 'DISJONCTEUR'   AND p.line_id = d.line_id)),
shape AS (
  SELECT zone_id, ST_Transform(ST_Buffer(ST_ConvexHull(ST_Transform(ST_Union_Agg(geom), 32198)), 30.0), 4326) AS zone_geom,
         count(*) AS n_poles, avg(age_years) AS avg_pole_age_years
  FROM zone_poles GROUP BY zone_id),
kpi AS (
  SELECT zone_id,
         count_if(start_ts >= add_months(current_date(), -3)) AS interruptions_3m,
         count_if(start_ts >= add_months(current_date(), -6)) AS interruptions_6m,
         sum(CASE WHEN start_ts >= add_months(current_date(), -6) THEN chi ELSE 0 END) AS chi_6m,
         sum(CASE WHEN start_ts >= add_months(current_date(), -3) THEN chi ELSE 0 END) AS chi_3m,
         sum(CASE WHEN start_ts <  add_months(current_date(), -3) AND start_ts >= add_months(current_date(), -6) THEN chi ELSE 0 END) AS chi_prev_3m,
         sum(chi) AS chi_24m
  FROM outages GROUP BY zone_id)
SELECT d.device_id AS zone_id, d.device_type, d.line_id, d.substation_code, d.ced_code, d.region_name,
       d.customers_downstream AS n_customers, shape.n_poles, round(shape.avg_pole_age_years, 1) AS avg_pole_age_years,
       coalesce(kpi.interruptions_3m, 0) AS interruptions_3m, coalesce(kpi.interruptions_6m, 0) AS interruptions_6m,
       round(coalesce(kpi.chi_3m, 0), 1) AS chi_3m, round(coalesce(kpi.chi_prev_3m, 0), 1) AS chi_prev_3m,
       round(coalesce(kpi.chi_6m, 0), 1) AS chi_6m, round(coalesce(kpi.chi_24m, 0), 1) AS chi_24m,
       round(ST_Area(ST_Transform(shape.zone_geom, 32198)) / 1e6, 3) AS zone_area_km2,
       ST_X(ST_Centroid(shape.zone_geom)) AS centroid_lon, ST_Y(ST_Centroid(shape.zone_geom)) AS centroid_lat,
       shape.zone_geom
FROM protection_devices d JOIN shape ON shape.zone_id = d.device_id LEFT JOIN kpi ON kpi.zone_id = d.device_id;

-- CHI heat map on H3 res-8 hexagons (customer-weighted share of each outage's CHI over the last 6 months).
CREATE OR REPLACE TABLE chi_h3_hex
COMMENT 'Carte de chaleur CHI / CHI heat map: customer-hours interrupted over the last 6 months aggregated on H3 resolution 8 cells. geom = hexagon polygon.'
AS
WITH o6 AS (SELECT zone_id, chi, customers_interrupted FROM outages WHERE start_ts >= add_months(current_date(), -6)),
zone_cust AS (
  SELECT c.zone_id, c.h3_cell_8, c.ced_code, count(*) AS n FROM customers c GROUP BY ALL
  UNION ALL  -- breaker trips interrupt every customer of the line
  SELECT d.device_id, c.h3_cell_8, c.ced_code, count(*) FROM customers c
  JOIN protection_devices d ON d.device_type = 'DISJONCTEUR' AND d.line_id = c.line_id AND c.zone_id <> d.device_id
  GROUP BY ALL),
zone_tot AS (SELECT zone_id, sum(n) AS n_tot FROM zone_cust GROUP BY zone_id)
SELECT zc.h3_cell_8, zc.ced_code, round(sum(o6.chi * zc.n / zt.n_tot), 1) AS chi_6m, count(DISTINCT o6.zone_id) AS zones_impacted,
       ST_GeomFromGeoJSON(h3_boundaryasgeojson(zc.h3_cell_8)) AS geom
FROM o6 JOIN zone_cust zc USING (zone_id) JOIN zone_tot zt USING (zone_id)
GROUP BY zc.h3_cell_8, zc.ced_code;

-- ---------------------------------------------------------------------------------------------
-- 4. Fine-grained access control
-- ---------------------------------------------------------------------------------------------

ALTER TABLE customers_pii ALTER COLUMN customer_name SET MASK governance.mask_pii_text;
ALTER TABLE customers_pii ALTER COLUMN address       SET MASK governance.mask_pii_text;

ALTER TABLE customers_pii SET ROW FILTER governance.ced_row_filter ON (ced_code);
ALTER TABLE outages    SET ROW FILTER governance.ced_row_filter ON (ced_code);

-- ---------------------------------------------------------------------------------------------
-- 5. Bilingual column documentation (used by Genie and Catalog Explorer)
-- ---------------------------------------------------------------------------------------------

COMMENT ON COLUMN poles.distance_to_substation_m IS 'Distance à vol d''oiseau au poste (m) / straight-line distance to substation (m), ST_Distance in EPSG:32198';
COMMENT ON COLUMN poles.network_distance_m        IS 'Distance électrique le long de la ligne depuis le poste (m) / distance along the conductor from the substation (m)';
COMMENT ON COLUMN poles.install_year              IS 'Année d''installation du poteau / pole installation year (oldest = smallest)';
COMMENT ON COLUMN poles.age_years                 IS 'Âge du poteau en années / pole age in years';
COMMENT ON COLUMN poles.geom                      IS 'Localisation du poteau, GEOMETRY(4326) POINT';
COMMENT ON COLUMN spans.length_m                  IS 'Longueur de la portée (m) / span length in metres';
COMMENT ON COLUMN spans.geom                      IS 'Tracé de la portée, GEOMETRY(4326) LINESTRING';
COMMENT ON COLUMN lines.right_of_way_area_m2      IS 'Surface de l''emprise (m²) / right-of-way area in m² (15 m each side)';
COMMENT ON COLUMN lines.right_of_way_geom         IS 'Emprise de la ligne (polygone) / right-of-way polygon, GEOMETRY(4326)';
COMMENT ON COLUMN lines.max_distance_to_substation_m IS 'Distance maximale au poste à vol d''oiseau (m) / max straight-line distance to substation';
COMMENT ON COLUMN lines.max_network_distance_m    IS 'Distance maximale au poste le long de la ligne (m) / max distance along the line';
COMMENT ON COLUMN transformers.install_year       IS 'Année d''installation du transformateur / transformer installation year';
COMMENT ON COLUMN transformers.kva                IS 'Puissance nominale (kVA) / nameplate rating';
COMMENT ON COLUMN transformers.zone_id            IS 'Zone de protection (appareil de protection en amont) / protection zone';
COMMENT ON COLUMN transformers.geom               IS 'Localisation du transformateur, GEOMETRY(4326) POINT';
COMMENT ON COLUMN transformer_load_daily.overload_pct IS 'Taux de charge / surcharge (%) = pointe kVA / kVA nominal × 100; >100 = surcharge';
COMMENT ON COLUMN transformer_load_daily.winter_label IS 'Hiver YYYY = décembre YYYY-1 à mars YYYY / winter season label';
COMMENT ON COLUMN protection_devices.customers_downstream IS 'Clients affectés si l''appareil opère / customers interrupted if the device operates or fails';
COMMENT ON COLUMN protection_devices.device_type  IS 'DISJONCTEUR (breaker) ou COUPE-CIRCUIT (fuse cutout)';
COMMENT ON COLUMN customers_pii.distance_to_substation_m IS 'Distance à vol d''oiseau du client au poste (m) / straight-line distance to substation';
COMMENT ON COLUMN customers_pii.network_distance_m    IS 'Distance électrique client-poste (m) / network distance customer→substation';
COMMENT ON COLUMN customers_pii.geom              IS 'Localisation exacte du client (PII) / exact customer location (PII) — exposed generalised via view gold.customers';
COMMENT ON COLUMN outages.chi                     IS 'Clients-heures interrompus (CHI) / customer-hours interrupted';
COMMENT ON COLUMN outages.zone_id                 IS 'Zone de protection / protection zone (device that operated)';
COMMENT ON COLUMN protection_zones.zone_geom      IS 'Polygone de la zone de protection, GEOMETRY(4326)';
COMMENT ON COLUMN protection_zones.chi_6m         IS 'CHI des 6 derniers mois / CHI over the last 6 months';
COMMENT ON COLUMN protection_zones.interruptions_3m IS 'Nombre d''interruptions des 3 derniers mois / interruptions in the last 3 months';

-- ---------------------------------------------------------------------------------------------
-- 6. Grants (demo groups; ignore if the groups do not exist yet)
-- ---------------------------------------------------------------------------------------------
-- GRANT USE CATALOG ON CATALOG grid_genie TO `grid_genie_users`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA grid_genie.gold TO `grid_genie_users`;
-- GRANT EXECUTE ON SCHEMA grid_genie.governance TO `grid_genie_users`;
