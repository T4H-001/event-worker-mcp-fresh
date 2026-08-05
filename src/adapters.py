import hashlib
import hmac
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _field_matches(value: int, expression: str, minimum: int, maximum: int) -> bool:
    for part in expression.split(","):
        if part == "*":
            return True
        if part.startswith("*/"):
            step = int(part[2:])
            return step > 0 and (value - minimum) % step == 0
        if "-" in part:
            start, end = (int(item) for item in part.split("-", 1))
            if start <= value <= end:
                return True
        elif int(part) == value:
            return True
    return False


def next_cron_time(expression: str, after: float | None = None) -> float:
    """Return next UTC epoch for a bounded five-field cron expression."""
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError("cron expression must contain five fields")
    minute, hour, day, month, weekday = fields
    cursor = datetime.fromtimestamp(after or time.time(), timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(527040):
        cron_weekday = (cursor.weekday() + 1) % 7
        if (
            _field_matches(cursor.minute, minute, 0, 59)
            and _field_matches(cursor.hour, hour, 0, 23)
            and _field_matches(cursor.day, day, 1, 31)
            and _field_matches(cursor.month, month, 1, 12)
            and _field_matches(cron_weekday, weekday, 0, 6)
        ):
            return cursor.timestamp()
        cursor += timedelta(minutes=1)
    raise ValueError("cron expression produced no time within one year")


def verify_github_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not secret or not signature or not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def normalize_github_event(event_name: str, delivery_id: str, payload: dict) -> tuple[str, dict]:
    action = payload.get("action")
    event_type = f"github.{event_name}" + (f".{action}" if action else "")
    repository = payload.get("repository") or {}
    sender = payload.get("sender") or {}
    issue = payload.get("issue") or {}
    pull_request = payload.get("pull_request") or {}
    data = {
        "delivery_id": delivery_id,
        "repository": repository.get("full_name"),
        "sender": sender.get("login"),
        "action": action,
        "issue_number": issue.get("number"),
        "pull_request_number": pull_request.get("number"),
        "ref": payload.get("ref"),
        "head_commit": (payload.get("head_commit") or {}).get("id"),
    }
    return event_type, {key: value for key, value in data.items() if value is not None}


class FileWatcher:
    def __init__(self, store, root: str, prefix: str = "file", interval: float = 2):
        self.store = store
        self.root = Path(root).resolve()
        self.prefix = prefix
        self.interval = interval
        self.snapshot = {}
        self.stop_event = threading.Event()
        self.thread = None

    def _scan(self):
        if not self.root.exists() or not self.root.is_dir():
            return {}
        result = {}
        for path in self.root.rglob("*"):
            if path.is_file() and self.root in path.resolve().parents:
                stat = path.stat()
                result[str(path.relative_to(self.root))] = (stat.st_mtime_ns, stat.st_size)
        return result

    def scan_once(self, emit: bool = True):
        current = self._scan()
        changes = []
        for relative, metadata in current.items():
            if relative not in self.snapshot:
                changes.append(("created", relative, metadata[1]))
            elif self.snapshot[relative] != metadata:
                changes.append(("modified", relative, metadata[1]))
        for relative in self.snapshot.keys() - current.keys():
            changes.append(("deleted", relative, None))
        self.snapshot = current
        if emit:
            for change, relative, size in changes:
                payload = {"root": str(self.root), "path": relative, "change": change}
                if size is not None:
                    payload["size"] = size
                digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
                self.store.dispatch(f"{self.prefix}.{change}", payload, f"file:{digest}:{time.time_ns()}")
        return changes

    def loop(self):
        self.scan_once(emit=False)
        while not self.stop_event.wait(self.interval):
            self.scan_once(emit=True)

    def start(self):
        self.thread = threading.Thread(target=self.loop, name="file-watcher", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)

    def status(self):
        return {"root": str(self.root), "prefix": self.prefix, "files_known": len(self.snapshot), "running": bool(self.thread and self.thread.is_alive())}
