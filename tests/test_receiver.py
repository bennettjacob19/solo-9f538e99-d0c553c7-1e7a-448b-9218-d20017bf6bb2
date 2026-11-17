"""接收端幂等性单元测试：python3 -m unittest tests.test_receiver -v

验证：
1. 同 seq 重放只入队一次、duplicate_count 累加，不产生第二次重建安排；
2. 同编号多笔通知在待重建队列中只占一行（reasons 记录全部来源 seq）；
3. drain 后进入重建历史，队列清空，再收到新通知可重新入队；
4. fail-mode 钩子只影响响应码，落盘处理仍然只发生一次。
"""
from __future__ import annotations

import os
import tempfile
import unittest

from app.receiver.store import ReceiverStore


def payload(seq: int, xid: int, no: int, op: str = "insert") -> dict:
    return {
        "notification_seq": seq,
        "xid": xid,
        "commit_lsn": f"0/{1000 + seq}",
        "commit_ts": None,
        "changes": [{
            "artifact_no": no, "op": op,
            "service_name": "svc", "version": "1", "sha256": "a" * 64,
        }],
    }


class ReceiverStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.store = ReceiverStore(os.path.join(self.tmp, "receiver.db"))

    def tearDown(self) -> None:
        self.store.close()

    def test_first_notification_enqueues_once(self):
        r1 = self.store.accept_notification(payload(1, 101, 1001))
        self.assertFalse(r1["duplicate"])
        self.assertEqual(r1["newly_enqueued"], [1001])
        pending = {p["artifact_no"]: p for p in self.store.pending()}
        self.assertIn(1001, pending)
        self.assertEqual(pending[1001]["reasons"], [1])

    def test_redelivery_is_idempotent(self):
        self.store.accept_notification(payload(1, 101, 1001))
        r2 = self.store.accept_notification(payload(1, 101, 1001))
        r3 = self.store.accept_notification(payload(1, 101, 1001))
        self.assertTrue(r2["duplicate"])
        self.assertTrue(r3["duplicate"])
        self.assertEqual(len(self.store.pending()), 1)  # 无重复入队
        ledger = {p["notification_seq"]: p for p in self.store.processed()}
        self.assertEqual(ledger[1]["duplicate_count"], 2)
        self.assertEqual(self.store.stats()["duplicate_deliveries"], 2)

    def test_same_artifact_in_multiple_txns_queued_once(self):
        self.store.accept_notification(payload(1, 101, 1001))
        self.store.accept_notification(payload(2, 102, 1001, op="update"))
        self.store.accept_notification(payload(3, 103, 1002))
        pending = {p["artifact_no"]: p for p in self.store.pending()}
        self.assertEqual(set(pending), {1001, 1002})
        self.assertEqual(pending[1001]["reasons"], [1, 2])
        self.assertEqual(pending[1002]["reasons"], [3])

    def test_drain_moves_to_history_and_allows_reenqueue(self):
        self.store.accept_notification(payload(1, 101, 1001))
        rebuilt = self.store.drain_pending()
        self.assertEqual(rebuilt, [1001])
        self.assertEqual(self.store.pending(), [])
        self.assertEqual(len(self.store.history()), 1)
        # 重建后该编号再次变更可重新入队
        self.store.accept_notification(payload(2, 102, 1001, op="update"))
        self.assertEqual([p["artifact_no"] for p in self.store.pending()], [1001])

    def test_fail_mode_does_not_change_idempotency(self):
        self.store.set_fail_mode(500)
        self.assertEqual(self.store.get_fail_status(), 500)
        # 处理落盘发生在返回错误之前；重发时已是 duplicate
        r1 = self.store.accept_notification(payload(1, 101, 1001))
        self.assertFalse(r1["duplicate"])
        r2 = self.store.accept_notification(payload(1, 101, 1001))
        self.assertTrue(r2["duplicate"])
        self.assertEqual(len(self.store.pending()), 1)
        self.store.set_fail_mode(None)
        self.assertIsNone(self.store.get_fail_status())


if __name__ == "__main__":
    unittest.main()
