#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker compose -p t4h-event-worker-mcp-fresh down
echo "Fresh event-worker MCP stopped. Its named data volume was preserved."
echo "Existing MCP runtime was not changed."
