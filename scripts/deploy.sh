#!/usr/bin/env bash
# Grid Genie — end-to-end deployment.
#   ./scripts/deploy.sh <profile> [warehouse_id] [managed_location]
#
# 1. Creates the UC catalog (optionally with a managed location)
# 2. Deploys + runs the platform bundle (Lakeflow → gold/governance → ML/AI → Lakebase → Genie)
# 3. Deploys + starts the Databricks App (MapLibre) wired to the Genie space and Lakebase
# 4. Grants the app service principal access to gold (UC), the row-filter mapping, and the Lakebase ops schema
set -euo pipefail

PROFILE="${1:?usage: deploy.sh <profile> [warehouse_id] [managed_location]}"
WAREHOUSE_ID="${2:-}"
MANAGED_LOCATION="${3:-}"
CATALOG="grid_genie"
PROJECT="grid-genie"
APP_NAME="grid-genie-map"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB=(databricks --profile "$PROFILE")

sqlq() { "${DB[@]}" experimental aitools tools query "$1" 2>/dev/null; }

if [[ -z "$WAREHOUSE_ID" ]]; then
  WAREHOUSE_ID=$("${DB[@]}" warehouses list -o json | jq -r '[.[] | select(.enable_serverless_compute)][0].id')
fi
echo "▶ Warehouse: $WAREHOUSE_ID"

echo "▶ Catalog $CATALOG"
if [[ -n "$MANAGED_LOCATION" ]]; then
  sqlq "CREATE CATALOG IF NOT EXISTS $CATALOG MANAGED LOCATION '$MANAGED_LOCATION'"
else
  sqlq "CREATE CATALOG IF NOT EXISTS $CATALOG"
fi

echo "▶ Platform bundle: deploy + run (≈ 20–30 min)"
cd "$ROOT"
"${DB[@]}" bundle deploy -t dev --var "warehouse_id=$WAREHOUSE_ID"
"${DB[@]}" bundle run grid_genie_setup -t dev --var "warehouse_id=$WAREHOUSE_ID"

SPACE_ID=$("${DB[@]}" api get /api/2.0/genie/spaces -o json \
  | jq -r '.spaces[] | select(.title=="Grid Genie · Réseau de distribution") | .space_id' | head -1)
DB_ID=$("${DB[@]}" postgres list-databases "projects/$PROJECT/branches/production" -o json \
  | jq -r '.[] | select(.status.postgres_database=="databricks_postgres") | .name | split("/") | last')
echo "▶ Genie space: $SPACE_ID · Lakebase database: $DB_ID"

echo "▶ App bundle: deploy + start"
cd "$ROOT/app"
VARS=(--var "warehouse_id=$WAREHOUSE_ID" --var "genie_space_id=$SPACE_ID" --var "lakebase_database_id=$DB_ID")
"${DB[@]}" bundle deploy -t dev "${VARS[@]}"
"${DB[@]}" bundle run grid_genie_map -t dev "${VARS[@]}"

SP=$("${DB[@]}" apps get "$APP_NAME" -o json | jq -r '.service_principal_client_id')
echo "▶ Grants for app service principal $SP"
sqlq "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$SP\`"
sqlq "GRANT USE SCHEMA, SELECT ON SCHEMA $CATALOG.gold TO \`$SP\`"
sqlq "GRANT USE SCHEMA, EXECUTE ON SCHEMA $CATALOG.governance TO \`$SP\`"
sqlq "GRANT SELECT ON TABLE $CATALOG.governance.ced_access TO \`$SP\`"
sqlq "INSERT INTO $CATALOG.governance.ced_access SELECT '$SP', '*' WHERE NOT EXISTS (SELECT 1 FROM $CATALOG.governance.ced_access WHERE principal = '$SP')"

EP="projects/$PROJECT/branches/production/endpoints/primary"
HOST=$("${DB[@]}" postgres get-endpoint "$EP" -o json | jq -r '.status.hosts.host')
TOKEN=$("${DB[@]}" postgres generate-database-credential "$EP" -o json | jq -r '.token')
ME=$("${DB[@]}" current-user me -o json | jq -r '.userName')
PGPASSWORD="$TOKEN" psql "host=$HOST user=$ME dbname=databricks_postgres sslmode=require" -v ON_ERROR_STOP=1 <<SQL
GRANT USAGE ON SCHEMA ops TO "$SP";
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO "$SP";
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO "$SP";
SQL

echo "✅ $("${DB[@]}" apps get "$APP_NAME" -o json | jq -r '.url')"
