#!/bin/bash
# Runs once, on first start: the app role the jobs use. It is not a superuser.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v pw="$ETL_PASSWORD" <<'SQL'
CREATE ROLE etl LOGIN PASSWORD :'pw';
GRANT CREATE ON DATABASE warehouse TO etl;
SQL
