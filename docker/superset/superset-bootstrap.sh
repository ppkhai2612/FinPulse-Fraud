#!/bin/bash

set -euo pipefail

REQS=/app/docker/requirements-local.txt
if [ -f "$REQS" ]; then
  echo "[superset-bootstrap] pip install -r $REQS"
  pip install --no-cache-dir -r "$REQS"
fi

echo "[superset-bootstrap] exec run-server.sh"
exec /usr/bin/run-server.sh