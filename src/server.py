import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from mcp.server import MCPServer
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .adapters import FileWatcher, next_cron_time, normalize_github_event, verify_github_signature
from .engine import EventEngine
from .store import EventStore

store = EventStore(os.getenv("EVENT_DB", "/data/event-worker.sqlite3"))
engine = EventEngine(store)
file_watcher = FileWatcher(
    store,
    os.getenv("FILE_WATCH_ROOT", "/watch"),
    os.getenv("FILE_WATCH_PREFIX", "file"),
    float(os.getenv("FILE_WATCH_SECONDS", "2")),
)
mcp = MCPServer(
    "T4H Fresh Event Worker",
    version="2.0.0",
    instructions="Independent durable event worker with governed event sources. This server does not call or modify any other MCP runtime.",
)


@mcp.tool(name="event_worker.dispatch")
def dispatch(event_type: str, payload: dict, idempotency_key: str | None = None, approval_required: bool = False, max_attempts: int | None = None) -> dict:
    """Queue an event exactly once and return its durable task and receipt identifiers."""
    if not event_type.strip():
        raise ValueError("event_type is required")
    hourly_limit = int(os.getenv("EVENT_MAX_TASKS_PER_HOUR", "100"))
    if store.tasks_last_hour() >= hourly_limit:
        raise RuntimeError(f"hourly task budget exceeded ({hourly_limit})")
    attempts = max_attempts or int(os.getenv("EVENT_MAX_ATTEMPTS", "3"))
    if attempts < 1 or attempts > 20:
        raise ValueError("max_attempts must be between 1 and 20")
    return store.dispatch(event_type.strip(), payload, idempotency_key, approval_required, attempts)


@mcp.tool(name="event_worker.status")
def status(task_id: str) -> dict:
    """Return the current durable status and result for a task."""
    task = store.get_task(task_id)
    if not task:
        raise ValueError("task not found")
    return task


@mcp.tool(name="event_worker.health")
def health() -> dict:
    """Return worker, scheduler, watcher, WAL and queue telemetry."""
    return {**store.health(), "server": "t4h-event-worker-mcp-fresh", "enabled": engine.enabled, "scheduler_enabled": engine.scheduler_enabled, "file_watcher": file_watcher.status(), "independent": True}


@mcp.tool(name="event_worker.replay")
def replay(task_id: str) -> dict:
    """Create a new queued task from a prior task without altering the original ledger entry."""
    return store.replay(task_id)


@mcp.tool(name="event_worker.dlq_list")
def dlq_list(limit: int = 100) -> list[dict]:
    """List exhausted tasks in the durable dead-letter queue."""
    return store.list_dlq(limit)


@mcp.tool(name="event_worker.dlq_retry")
def dlq_retry(task_id: str) -> dict:
    """Retry one dead-lettered task as a new task while preserving the original."""
    return store.retry_dlq(task_id)


@mcp.tool(name="event_worker.approvals")
def approvals(status: str = "pending", limit: int = 100) -> list[dict]:
    """List durable approval records by status."""
    return store.list_approvals(status, limit)


@mcp.tool(name="event_worker.approval_decide")
def approval_decide(approval_id: str, decision: str, decided_by: str, reason: str | None = None) -> dict:
    """Approve or reject a pending task and append an immutable decision receipt."""
    return store.decide_approval(approval_id, decision, decided_by, reason)


@mcp.tool(name="event_worker.audit")
def audit(task_id: str | None = None, limit: int = 100) -> list[dict]:
    """Read immutable, SHA-256-backed runtime receipts."""
    return store.audit(task_id, limit)


@mcp.tool(name="event_worker.metrics")
def metrics() -> dict:
    """Return queue, retry, DLQ, approval, schedule, receipt and budget telemetry."""
    return store.metrics()


@mcp.tool(name="event_worker.schedule_create")
def schedule_create(name: str, cron_expression: str, event_type: str, payload: dict) -> dict:
    """Create a durable UTC five-field cron schedule that only emits events."""
    if not name.strip() or not event_type.strip():
        raise ValueError("name and event_type are required")
    return store.create_schedule(name.strip(), cron_expression, event_type.strip(), payload, next_cron_time(cron_expression))


@mcp.tool(name="event_worker.schedule_list")
def schedule_list() -> list[dict]:
    """List durable schedules and their next UTC run times."""
    return store.list_schedules()


@mcp.tool(name="event_worker.schedule_enable")
def schedule_enable(schedule_id: str, enabled: bool) -> dict:
    """Enable or pause a durable schedule."""
    return store.set_schedule_enabled(schedule_id, enabled)


@mcp.tool(name="event_worker.schedule_delete")
def schedule_delete(schedule_id: str) -> dict:
    """Delete one schedule while retaining its audit receipts."""
    return store.delete_schedule(schedule_id)


@mcp.tool(name="event_worker.file_watch_status")
def file_watch_status() -> dict:
    """Return the bounded file watch root and live watcher state."""
    return file_watcher.status()


async def healthz(request: Request):
    return JSONResponse({"ok": True, **store.health()})


async def github_webhook(request: Request):
    body = await request.body()
    secret = os.getenv("GITHUB_WEBHOOK_SECRET", "")
    if not secret:
        return JSONResponse({"error": "github_webhook_not_configured"}, status_code=503)
    if not verify_github_signature(secret, body, request.headers.get("x-hub-signature-256")):
        return JSONResponse({"error": "invalid_signature"}, status_code=401)
    delivery_id = request.headers.get("x-github-delivery")
    event_name = request.headers.get("x-github-event")
    if not delivery_id or not event_name:
        return JSONResponse({"error": "missing_github_headers"}, status_code=400)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    event_type, normalized = normalize_github_event(event_name, delivery_id, payload)
    result = store.dispatch(event_type, normalized, f"github:{delivery_id}")
    return JSONResponse(result, status_code=202 if not result["duplicate"] else 200)


mcp_app = mcp.streamable_http_app(stateless_http=True, json_response=True)


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    engine.start()
    if os.getenv("FILE_WATCH_ENABLED", "false").lower() == "true":
        file_watcher.start()
    try:
        async with mcp.session_manager.run():
            yield
    finally:
        file_watcher.stop()
        engine.stop()


app = Starlette(
    routes=[
        Route("/healthz", healthz, methods=["GET"]),
        Route("/webhooks/github", github_webhook, methods=["POST"]),
        Mount("/", app=mcp_app),
    ],
    lifespan=lifespan,
)


def main():
    uvicorn.run(app, host=os.getenv("MCP_HOST", "0.0.0.0"), port=int(os.getenv("MCP_PORT", "8081")))


if __name__ == "__main__":
    main()
