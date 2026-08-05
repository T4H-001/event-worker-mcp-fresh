import hashlib
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path


class EventStore:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._initialize()

    def connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            self._local.connection = connection
        return connection

    def _initialize(self):
        db = self.connection()
        db.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
          task_id TEXT PRIMARY KEY,
          event_type TEXT NOT NULL,
          idempotency_key TEXT NOT NULL UNIQUE,
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed','cancelled')),
          attempt INTEGER NOT NULL DEFAULT 0,
          result_json TEXT,
          error TEXT,
          parent_task_id TEXT,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS receipts (
          receipt_id TEXT PRIMARY KEY,
          task_id TEXT,
          event_type TEXT NOT NULL,
          body_json TEXT NOT NULL,
          body_sha256 TEXT NOT NULL,
          created_at REAL NOT NULL,
          FOREIGN KEY(task_id) REFERENCES tasks(task_id)
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_receipts_task_created ON receipts(task_id, created_at);
        CREATE TABLE IF NOT EXISTS runtime (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dead_letters (
          dlq_id TEXT PRIMARY KEY,
          task_id TEXT NOT NULL UNIQUE,
          error TEXT NOT NULL,
          attempts INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          failed_at REAL NOT NULL,
          retried_at REAL,
          FOREIGN KEY(task_id) REFERENCES tasks(task_id)
        );
        CREATE TABLE IF NOT EXISTS approvals (
          approval_id TEXT PRIMARY KEY,
          task_id TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL CHECK(status IN ('pending','approved','rejected')),
          reason TEXT,
          decided_by TEXT,
          created_at REAL NOT NULL,
          decided_at REAL,
          FOREIGN KEY(task_id) REFERENCES tasks(task_id)
        );
        CREATE TABLE IF NOT EXISTS schedules (
          schedule_id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          cron_expression TEXT NOT NULL,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          next_run_at REAL NOT NULL,
          last_run_at REAL,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );
        """)
        columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
        if "next_retry_at" not in columns:
            db.execute("ALTER TABLE tasks ADD COLUMN next_retry_at REAL")
        if "max_attempts" not in columns:
            db.execute("ALTER TABLE tasks ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3")

    @staticmethod
    def _json(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    def receipt(self, event_type: str, body: dict, task_id: str | None = None):
        body_json = self._json(body)
        receipt_id = f"rcpt-{uuid.uuid4()}"
        self.connection().execute(
            "INSERT INTO receipts VALUES(?,?,?,?,?,?)",
            (receipt_id, task_id, event_type, body_json, hashlib.sha256(body_json.encode()).hexdigest(), time.time()),
        )
        return receipt_id

    def dispatch(self, event_type: str, payload: dict, idempotency_key: str | None = None, approval_required: bool = False, max_attempts: int = 3):
        now = time.time()
        key = idempotency_key or f"evt-{uuid.uuid4()}"
        task_id = f"task-{uuid.uuid4()}"
        db = self.connection()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO tasks(task_id,event_type,idempotency_key,payload_json,status,created_at,updated_at,max_attempts) VALUES(?,?,?,?,?,?,?,?)",
                (task_id, event_type, key, self._json(payload), "queued", now, now, max_attempts),
            )
            approval_id = None
            if approval_required:
                approval_id = f"approval-{uuid.uuid4()}"
                db.execute(
                    "INSERT INTO approvals(approval_id,task_id,status,created_at) VALUES(?,?,'pending',?)",
                    (approval_id, task_id, now),
                )
            receipt_id = self.receipt("task.accepted", {"task_id": task_id, "event_type": event_type, "idempotency_key": key}, task_id)
            db.execute("COMMIT")
            return {"status": "awaiting_approval" if approval_id else "accepted", "duplicate": False, "task_id": task_id, "receipt_id": receipt_id, "approval_id": approval_id}
        except sqlite3.IntegrityError:
            db.execute("ROLLBACK")
            row = db.execute("SELECT task_id,status FROM tasks WHERE idempotency_key=?", (key,)).fetchone()
            return {"status": row["status"], "duplicate": True, "task_id": row["task_id"], "receipt_id": None}
        except Exception:
            db.execute("ROLLBACK")
            raise

    def claim_next(self):
        db = self.connection()
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute(
                """SELECT t.* FROM tasks t
                WHERE t.status='queued'
                  AND (t.next_retry_at IS NULL OR t.next_retry_at<=?)
                  AND NOT EXISTS (SELECT 1 FROM approvals a WHERE a.task_id=t.task_id AND a.status='pending')
                ORDER BY t.created_at LIMIT 1""",
                (time.time(),),
            ).fetchone()
            if not row:
                db.execute("COMMIT")
                return None
            now = time.time()
            db.execute("UPDATE tasks SET status='running',attempt=attempt+1,next_retry_at=NULL,updated_at=? WHERE task_id=?", (now, row["task_id"]))
            db.execute("COMMIT")
            return self.get_task(row["task_id"])
        except Exception:
            db.execute("ROLLBACK")
            raise

    def complete(self, task_id: str, result: dict):
        now = time.time()
        self.connection().execute("UPDATE tasks SET status='completed',result_json=?,error=NULL,updated_at=? WHERE task_id=?", (self._json(result), now, task_id))
        return self.receipt("task.completed", {"task_id": task_id, "result": result}, task_id)

    def fail(self, task_id: str, error: str, retry: bool, base_delay: int, max_delay: int):
        db = self.connection()
        row = db.execute("SELECT attempt,max_attempts,payload_json FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        can_retry = retry and row and row["attempt"] < row["max_attempts"]
        if can_retry:
            delay = min(max_delay, base_delay * (2 ** max(0, row["attempt"] - 1)))
            next_retry_at = time.time() + delay
            db.execute("UPDATE tasks SET status='queued',error=?,next_retry_at=?,updated_at=? WHERE task_id=?", (error, next_retry_at, time.time(), task_id))
            event = "task.retry_scheduled"
            body = {"task_id": task_id, "error": error, "attempt": row["attempt"], "next_retry_at": next_retry_at}
        else:
            db.execute("UPDATE tasks SET status='failed',error=?,updated_at=? WHERE task_id=?", (error, time.time(), task_id))
            dlq_id = f"dlq-{uuid.uuid4()}"
            db.execute(
                "INSERT OR IGNORE INTO dead_letters(dlq_id,task_id,error,attempts,payload_json,failed_at) VALUES(?,?,?,?,?,?)",
                (dlq_id, task_id, error, row["attempt"] if row else 0, row["payload_json"] if row else "{}", time.time()),
            )
            event = "task.dead_lettered"
            body = {"task_id": task_id, "dlq_id": dlq_id, "error": error, "attempts": row["attempt"] if row else None}
        return self.receipt(event, body, task_id)

    def recover(self):
        cursor = self.connection().execute("UPDATE tasks SET status='queued',updated_at=? WHERE status='running'", (time.time(),))
        count = cursor.rowcount
        self.receipt("runtime.recovered", {"requeued_tasks": count})
        return count

    def get_task(self, task_id: str):
        row = self.connection().execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            return None
        value = dict(row)
        value["payload"] = json.loads(value.pop("payload_json"))
        value["result"] = json.loads(value.pop("result_json")) if value["result_json"] else None
        return value

    def replay(self, task_id: str):
        original = self.get_task(task_id)
        if not original:
            raise ValueError("task not found")
        result = self.dispatch(original["event_type"], original["payload"], f"replay:{task_id}:{uuid.uuid4()}")
        self.connection().execute("UPDATE tasks SET parent_task_id=? WHERE task_id=?", (task_id, result["task_id"]))
        return result

    def list_dlq(self, limit: int = 100):
        rows = self.connection().execute("SELECT * FROM dead_letters WHERE retried_at IS NULL ORDER BY failed_at DESC LIMIT ?", (min(max(limit, 1), 500),)).fetchall()
        return [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows]

    def retry_dlq(self, task_id: str):
        db = self.connection()
        row = db.execute("SELECT * FROM dead_letters WHERE task_id=? AND retried_at IS NULL", (task_id,)).fetchone()
        if not row:
            raise ValueError("active DLQ task not found")
        result = self.replay(task_id)
        db.execute("UPDATE dead_letters SET retried_at=? WHERE task_id=?", (time.time(), task_id))
        self.receipt("dlq.retried", {"original_task_id": task_id, "new_task_id": result["task_id"]}, result["task_id"])
        return result

    def list_approvals(self, status: str = "pending", limit: int = 100):
        if status not in {"pending", "approved", "rejected"}:
            raise ValueError("invalid approval status")
        rows = self.connection().execute(
            "SELECT a.*,t.event_type,t.payload_json FROM approvals a JOIN tasks t ON t.task_id=a.task_id WHERE a.status=? ORDER BY a.created_at LIMIT ?",
            (status, min(max(limit, 1), 500)),
        ).fetchall()
        return [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows]

    def decide_approval(self, approval_id: str, decision: str, decided_by: str, reason: str | None = None):
        if decision not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        db = self.connection()
        row = db.execute("SELECT * FROM approvals WHERE approval_id=?", (approval_id,)).fetchone()
        if not row or row["status"] != "pending":
            raise ValueError("pending approval not found")
        now = time.time()
        db.execute("UPDATE approvals SET status=?,reason=?,decided_by=?,decided_at=? WHERE approval_id=?", (decision, reason, decided_by, now, approval_id))
        if decision == "rejected":
            db.execute("UPDATE tasks SET status='cancelled',error=?,updated_at=? WHERE task_id=?", (reason or "approval rejected", now, row["task_id"]))
        receipt_id = self.receipt(f"approval.{decision}", {"approval_id": approval_id, "task_id": row["task_id"], "decided_by": decided_by, "reason": reason}, row["task_id"])
        return {"approval_id": approval_id, "task_id": row["task_id"], "status": decision, "receipt_id": receipt_id}

    def audit(self, task_id: str | None = None, limit: int = 100):
        sql = "SELECT * FROM receipts"
        params = []
        if task_id:
            sql += " WHERE task_id=?"
            params.append(task_id)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(min(max(limit, 1), 500))
        return [dict(row) | {"body": json.loads(row["body_json"])} for row in self.connection().execute(sql, params)]

    def tasks_last_hour(self):
        row = self.connection().execute("SELECT COUNT(*) FROM tasks WHERE created_at>=?", (time.time() - 3600,)).fetchone()
        return row[0]

    def metrics(self):
        db = self.connection()
        task_counts = {row["status"]: row["count"] for row in db.execute("SELECT status,COUNT(*) count FROM tasks GROUP BY status")}
        return {
            "tasks": task_counts,
            "dlq_size": db.execute("SELECT COUNT(*) FROM dead_letters WHERE retried_at IS NULL").fetchone()[0],
            "approvals_pending": db.execute("SELECT COUNT(*) FROM approvals WHERE status='pending'").fetchone()[0],
            "receipts_total": db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0],
            "tasks_last_hour": self.tasks_last_hour(),
            "schedules_enabled": db.execute("SELECT COUNT(*) FROM schedules WHERE enabled=1").fetchone()[0],
        }

    def create_schedule(self, name: str, cron_expression: str, event_type: str, payload: dict, next_run_at: float):
        now = time.time()
        schedule_id = f"schedule-{uuid.uuid4()}"
        self.connection().execute(
            "INSERT INTO schedules(schedule_id,name,cron_expression,event_type,payload_json,next_run_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (schedule_id, name, cron_expression, event_type, self._json(payload), next_run_at, now, now),
        )
        receipt_id = self.receipt("schedule.created", {"schedule_id": schedule_id, "name": name, "next_run_at": next_run_at})
        return {"schedule_id": schedule_id, "name": name, "next_run_at": next_run_at, "receipt_id": receipt_id}

    def list_schedules(self):
        rows = self.connection().execute("SELECT * FROM schedules ORDER BY name").fetchall()
        return [dict(row) | {"payload": json.loads(row["payload_json"]), "enabled": bool(row["enabled"])} for row in rows]

    def set_schedule_enabled(self, schedule_id: str, enabled: bool):
        cursor = self.connection().execute("UPDATE schedules SET enabled=?,updated_at=? WHERE schedule_id=?", (int(enabled), time.time(), schedule_id))
        if cursor.rowcount != 1:
            raise ValueError("schedule not found")
        receipt_id = self.receipt("schedule.enabled" if enabled else "schedule.paused", {"schedule_id": schedule_id})
        return {"schedule_id": schedule_id, "enabled": enabled, "receipt_id": receipt_id}

    def delete_schedule(self, schedule_id: str):
        row = self.connection().execute("SELECT name FROM schedules WHERE schedule_id=?", (schedule_id,)).fetchone()
        if not row:
            raise ValueError("schedule not found")
        self.connection().execute("DELETE FROM schedules WHERE schedule_id=?", (schedule_id,))
        receipt_id = self.receipt("schedule.deleted", {"schedule_id": schedule_id, "name": row["name"]})
        return {"schedule_id": schedule_id, "deleted": True, "receipt_id": receipt_id}

    def claim_due_schedule(self):
        db = self.connection()
        db.execute("BEGIN IMMEDIATE")
        try:
            row = db.execute("SELECT * FROM schedules WHERE enabled=1 AND next_run_at<=? ORDER BY next_run_at LIMIT 1", (time.time(),)).fetchone()
            if not row:
                db.execute("COMMIT")
                return None
            db.execute("UPDATE schedules SET enabled=0,updated_at=? WHERE schedule_id=?", (time.time(), row["schedule_id"]))
            db.execute("COMMIT")
            value = dict(row)
            value["payload"] = json.loads(value.pop("payload_json"))
            return value
        except Exception:
            db.execute("ROLLBACK")
            raise

    def finish_schedule(self, schedule_id: str, last_run_at: float, next_run_at: float):
        self.connection().execute("UPDATE schedules SET enabled=1,last_run_at=?,next_run_at=?,updated_at=? WHERE schedule_id=?", (last_run_at, next_run_at, time.time(), schedule_id))
        return self.receipt("schedule.emitted", {"schedule_id": schedule_id, "last_run_at": last_run_at, "next_run_at": next_run_at})

    def heartbeat(self):
        now = time.time()
        self.connection().execute(
            "INSERT INTO runtime(key,value,updated_at) VALUES('worker_heartbeat','alive',?) ON CONFLICT(key) DO UPDATE SET value='alive',updated_at=excluded.updated_at",
            (now,),
        )

    def health(self):
        db = self.connection()
        heartbeat = db.execute("SELECT updated_at FROM runtime WHERE key='worker_heartbeat'").fetchone()
        counts = {row["status"]: row["count"] for row in db.execute("SELECT status,COUNT(*) count FROM tasks GROUP BY status")}
        return {"database": "ok", "wal": db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal", "heartbeat_at": heartbeat[0] if heartbeat else None, "tasks": counts}
