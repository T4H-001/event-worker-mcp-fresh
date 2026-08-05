import os

from mcp.server import MCPServer

from .engine import EventEngine
from .store import EventStore

store = EventStore(os.getenv("EVENT_DB", "/data/event-worker.sqlite3"))
engine = EventEngine(store)
mcp = MCPServer(
    "T4H Fresh Event Worker",
    version="1.0.0",
    instructions="Independent durable event worker. This server does not call or modify any other MCP runtime.",
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
    """Return worker heartbeat, WAL state, queue counts, and isolation identity."""
    return {**store.health(), "server": "t4h-event-worker-mcp-fresh", "enabled": engine.enabled, "independent": True}


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
    """Return queue, retry, DLQ, approval, receipt and hourly budget telemetry."""
    return store.metrics()


def main():
    engine.start()
    try:
        mcp.run(
            transport="streamable-http",
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=int(os.getenv("MCP_PORT", "8081")),
            streamable_http_path=os.getenv("MCP_PATH", "/mcp"),
            stateless_http=True,
            json_response=True,
        )
    finally:
        engine.stop()


if __name__ == "__main__":
    main()
