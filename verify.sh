#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m compileall -q src tests
python3 -m unittest discover -s tests -v
docker compose -p t4h-event-worker-mcp-fresh config -q
docker compose -p t4h-event-worker-mcp-fresh ps
