import tempfile
import time
import unittest

from src.engine import EventEngine
from src.store import EventStore


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


if __name__ == "__main__":
    unittest.main()
