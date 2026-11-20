#!/bin/bash
# Generate random credentials into .env (never committed) and link it where docker compose
# looks for it. Run once on the lab box before starting anything.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .env ]; then
  r() { openssl rand -hex 12; }
  etl=$(r)
  cat > .env <<ENV
POSTGRES_PASSWORD=$(r)
ETL_PASSWORD=$etl
DATABASE_URL=postgresql://etl:$etl@127.0.0.1:5432/warehouse
DATABASE_URL_DOCKER=postgresql://etl:$etl@warehouse:5432/warehouse
DATABASE_URL_DOCKER_B64=$(printf '%s' "postgresql://etl:$etl@warehouse:5432/warehouse" | base64 -w0)
KESTRA_USER=admin@example.com
KESTRA_PASSWORD=Lab-$(r)-A1
KESTRA_DB_PASSWORD=$(r)
PREFECT_DB_PASSWORD=$(r)
PREFECT_AUTH_STRING=admin:$(r)
ENV
  chmod 600 .env
fi
ln -sf ../.env services/.env
ln -sf ../../.env setups/4-kestra/.env
ln -sf ../../.env setups/5-prefect/.env
echo ".env ready"
