import tempfile
import time
import unittest
import hashlib
import hmac
from pathlib import Path

from src.engine import EventEngine
from src.store import EventStore
from src.adapters import FileWatcher, next_cron_time, normalize_github_event, verify_github_signature


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = EventStore(f"{self.temp.name}/test.sqlite3")

    def tearDown(self):
        self.temp.cleanup()

    def test_idempotency(self):
        first = self.store.dispatch("ping", {"a": 1}, "same-key")
        second = self.store.dispatch("ping", {"a": 2}, "same-key")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["task_id"], second["task_id"])

    def test_execution_and_receipt(self):
        accepted = self.store.dispatch("ping", {"hello": "worker"}, "ping-1")
        task = self.store.claim_next()
        engine = EventEngine(self.store)
        receipt = self.store.complete(task["task_id"], engine.execute(task))
        final = self.store.get_task(accepted["task_id"])
        self.assertEqual("completed", final["status"])
        self.assertTrue(final["result"]["pong"])
        self.assertTrue(receipt.startswith("rcpt-"))

    def test_recovery(self):
        accepted = self.store.dispatch("echo", {}, "recover-1")
        self.store.claim_next()
        self.assertEqual(1, self.store.recover())
        self.assertEqual("queued", self.store.get_task(accepted["task_id"])["status"])

    def test_replay_is_new_task(self):
        accepted = self.store.dispatch("echo", {"x": 1}, "original")
        replayed = self.store.replay(accepted["task_id"])
        self.assertNotEqual(accepted["task_id"], replayed["task_id"])

    def test_exhausted_failure_goes_to_dlq(self):
        accepted = self.store.dispatch("fail.test", {}, "fail-once", max_attempts=1)
        task = self.store.claim_next()
        self.store.fail(task["task_id"], "boom", retry=True, base_delay=1, max_delay=10)
        self.assertEqual("failed", self.store.get_task(accepted["task_id"])["status"])
        self.assertEqual(1, len(self.store.list_dlq()))

    def test_approval_blocks_until_approved(self):
        accepted = self.store.dispatch("ping", {}, "approval-1", approval_required=True)
        self.assertIsNone(self.store.claim_next())
        decision = self.store.decide_approval(accepted["approval_id"], "approved", "tester")
        self.assertEqual("approved", decision["status"])
        self.assertEqual(accepted["task_id"], self.store.claim_next()["task_id"])

    def test_metrics_and_audit(self):
        accepted = self.store.dispatch("echo", {}, "audit-1")
        self.assertGreaterEqual(self.store.metrics()["receipts_total"], 1)
        self.assertEqual(accepted["task_id"], self.store.audit(accepted["task_id"])[0]["task_id"])

    def test_cron_schedule_storage(self):
        next_run = next_cron_time("*/5 * * * *", 1785928200)
        created = self.store.create_schedule("five-minute", "*/5 * * * *", "cron.test", {"x": 1}, next_run)
        schedules = self.store.list_schedules()
        self.assertEqual(created["schedule_id"], schedules[0]["schedule_id"])
        self.assertGreater(next_run, 1785928200)

    def test_file_watcher_detects_bounded_change(self):
        root = Path(self.temp.name) / "watch"
        root.mkdir()
        watcher = FileWatcher(self.store, str(root))
        watcher.scan_once(emit=False)
        (root / "new.txt").write_text("hello")
        changes = watcher.scan_once(emit=True)
        self.assertEqual([("created", "new.txt", 5)], changes)
        task = self.store.claim_next()
        self.assertEqual("file.created", task["event_type"])

    def test_github_signature_and_normalization(self):
        body = b'{"action":"opened"}'
        signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
        self.assertTrue(verify_github_signature("secret", body, signature))
        self.assertFalse(verify_github_signature("secret", body, "sha256=bad"))
        event_type, data = normalize_github_event("issues", "delivery-1", {"action": "opened", "repository": {"full_name": "T4H001/repo"}, "issue": {"number": 7}})
        self.assertEqual("github.issues.opened", event_type)
        self.assertEqual(7, data["issue_number"])


if __name__ == "__main__":
    unittest.main()
