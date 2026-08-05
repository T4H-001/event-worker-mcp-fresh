import os
import threading
import time

from .store import EventStore


class EventEngine:
    def __init__(self, store: EventStore):
        self.store = store
        self.enabled = os.getenv("EVENT_WORKER_ENABLED", "true").lower() == "true"
        self.poll_seconds = float(os.getenv("EVENT_POLL_SECONDS", "0.5"))
        self.max_attempts = int(os.getenv("EVENT_MAX_ATTEMPTS", "3"))
        self.retry_base = int(os.getenv("EVENT_RETRY_BASE_SECONDS", "10"))
        self.retry_max = int(os.getenv("EVENT_RETRY_MAX_SECONDS", "1800"))
        self.stop_event = threading.Event()
        self.thread = None

    def execute(self, task: dict):
        event_type = task["event_type"]
        payload = task["payload"]
        if event_type == "ping":
            return {"pong": True, "echo": payload}
        if event_type == "echo":
            return {"echo": payload}
        if event_type == "fail.test":
            raise RuntimeError("intentional failure test")
        return {"handled": True, "event_type": event_type, "payload": payload}

    def loop(self):
        self.store.recover()
        while not self.stop_event.is_set():
            self.store.heartbeat()
            if not self.enabled:
                self.stop_event.wait(self.poll_seconds)
                continue
            task = self.store.claim_next()
            if not task:
                self.stop_event.wait(self.poll_seconds)
                continue
            try:
                self.store.complete(task["task_id"], self.execute(task))
            except Exception as exc:
                self.store.fail(task["task_id"], str(exc), retry=True, base_delay=self.retry_base, max_delay=self.retry_max)

    def start(self):
        self.thread = threading.Thread(target=self.loop, name="event-worker", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
