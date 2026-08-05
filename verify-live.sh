#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
container="t4h-event-worker-mcp-fresh"
docker inspect "$container" >/dev/null
docker exec -i "$container" python - <<'PY'
import asyncio
import json
import uuid
from mcp import Client


async def main():
    async with Client("http://127.0.0.1:8081/mcp") as client:
        catalog = await client.list_tools()
        names = [tool.name for tool in catalog.tools]
        expected = {
            "event_worker.dispatch",
            "event_worker.status",
            "event_worker.health",
            "event_worker.replay",
            "event_worker.dlq_list",
            "event_worker.dlq_retry",
            "event_worker.approvals",
            "event_worker.approval_decide",
            "event_worker.audit",
            "event_worker.metrics",
            "event_worker.schedule_create",
            "event_worker.schedule_list",
            "event_worker.schedule_enable",
            "event_worker.schedule_delete",
            "event_worker.file_watch_status",
        }
        if set(names) != expected:
            raise RuntimeError(f"unexpected tool catalog: {names}")
        accepted = await client.call_tool(
            "event_worker.dispatch",
            {
                "event_type": "ping",
                "payload": {"probe": True},
                "idempotency_key": f"live-{uuid.uuid4()}",
            },
        )
        submission = json.loads(accepted.content[0].text)
        for _ in range(40):
            await asyncio.sleep(0.1)
            response = await client.call_tool("event_worker.status", {"task_id": submission["task_id"]})
            task = json.loads(response.content[0].text)
            if task["status"] == "completed":
                print(json.dumps({
                    "status": "REAL_LOCAL",
                    "tools": names,
                    "task_id": task["task_id"],
                    "receipt_id": submission["receipt_id"],
                    "result": task["result"],
                    "metrics_verified": not (await client.call_tool("event_worker.metrics", {})).is_error,
                    "phase2_tools_verified": True,
                }, indent=2))
                return
            if task["status"] == "failed":
                raise RuntimeError(task["error"])
        raise RuntimeError("task did not complete within four seconds")


asyncio.run(main())
PY
