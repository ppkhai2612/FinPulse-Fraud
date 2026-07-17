#!/bin/bash
# Resiter the transactions_scored hybrid table with the Pinot controller

set -euo pipefail

CONTROLLER="http://localhost:9100"
PINOT_CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../pinot_conf" && pwd)"

echo "==> Registering transactions_scored schema"
curl -fsS -X POST "${CONTROLLER}/schemas" \
    -H "Content-Type: application/json" \
    -d @"${PINOT_CONFIG_DIR}/transactions_scored-schema.json"
echo

echo "==> Registering REALTIME transactions_scored table"
curl -X POST "${CONTROLLER}/tables" \
    -H "Content-Type: application/json" \
    -d @"${PINOT_CONFIG_DIR}/transactions_scored_realtime_table_config.json"
echo

echo "==> Registering OFFLINE transactions_scored table"
curl -fsS -X POST "${CONTROLLER}/tables" \
    -H "Content-Type: application/json" \
    -d @"${PINOT_CONFIG_DIR}/transactions_scored_offline_table_config.json"
echo

echo "==> Tables now registered"
curl -fsS "${CONTROLLER}/tables"
echo