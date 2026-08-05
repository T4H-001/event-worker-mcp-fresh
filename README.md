# T4H Fresh Event-Worker MCP

This is a new standalone MCP server. It does not import, call, replace, stop, or reconfigure the existing `mcp.tech4humanity.com.au` runtime.

It follows the current stable MCP Python SDK v2 and serves Streamable HTTP at `http://127.0.0.1:8081/mcp`.

## Native tools

- `event_worker.dispatch` — durable idempotent event submission
- `event_worker.status` — task result/readback
- `event_worker.health` — heartbeat, WAL and queue telemetry
- `event_worker.replay` — non-destructive replay as a new task
- `event_worker.dlq_list` / `event_worker.dlq_retry` — exhausted-failure recovery
- `event_worker.approvals` / `event_worker.approval_decide` — durable approval gates
- `event_worker.audit` — SHA-256-backed receipt readback
- `event_worker.metrics` — operational and budget telemetry

This release implements Phase 1 reliability and the minimum governance hooks from the ultimate control-plane specification. Retries use timed exponential backoff and end in a durable DLQ. Dispatch can require approval and is limited by an hourly task budget.

External cron, file, email, GitHub, Slack and database adapters; external MCP tool registry; credential vault; and OpenTelemetry backends are not active in this release. They require source-specific configuration and credentials and must not be described as live.

## Start

```bash
unzip event-worker-mcp-fresh.zip
cd event-worker-mcp-fresh
./enable.sh
```

The port is bound to loopback only. Put an authenticated HTTPS reverse proxy in front of it before exposing it publicly. Do not reuse the existing MCP hostname.

Suggested new hostname: `event-worker.tech4humanity.com.au`.

## Verify and observe

```bash
./verify.sh
./verify-live.sh
docker compose -p t4h-event-worker-mcp-fresh logs -f
```

Use MCP Inspector or your MCP host to connect to `http://127.0.0.1:8081/mcp`, then call `event_worker.health`, dispatch `ping`, and read the returned task with `event_worker.status`.

## Stop / rollback

```bash
./rollback.sh
```

Rollback stops only the fresh service and preserves its named SQLite data volume. The old MCP service remains unaffected.

## Data and recovery

SQLite uses WAL and `synchronous=FULL`. Accepted, completed, failed and recovery events receive append-only receipt rows with SHA-256 body hashes. Tasks found in `running` state after restart are returned to `queued`. Idempotency keys are unique and original tasks are never overwritten by replay.

## Security boundary

The initial server binds to `127.0.0.1` and has no public authentication layer. Production exposure requires a separate hostname, TLS, OAuth/bearer validation at the reverse proxy or MCP authorization layer, and restricted firewall ingress. Do not bind port 8081 publicly without those controls.
