#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
docker compose version >/dev/null || { echo "docker compose is required" >&2; exit 1; }
[[ -f .env ]] || cp .env.example .env
docker compose -p t4h-event-worker-mcp-fresh up -d --build
docker compose -p t4h-event-worker-mcp-fresh ps
echo "Fresh MCP endpoint: http://127.0.0.1:8081/mcp"
echo "Existing MCP runtime was not changed."
