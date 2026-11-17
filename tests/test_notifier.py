"""通知发件箱与投递线程单元测试：python3 -m unittest tests.test_notifier -v

用临时 SQLite + 内存假接收端验证：
1. 事务落盘与通知同事务、重放不造第二笔通知；
2. 严格 head-of-line 顺序，前一笔未确认后续不越过；
3. 接收端已处理但响应丢失 → 重发幂等（假接收端只处理一次）；
4. 手动重试可中断退避立即续投；
5. 通知故障路径不影响历史查询接口数据。
"""
from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest

from app.notifier import NotifierWorker
from app.storage import (
    NOTIF_FAILED,
    NOTIF_PENDING,
    NOTIF_SENT,
    HistoryStore,
)


class FakeSender:
    """假接收端：按 seq 幂等，可配置前 N 次调用丢响应。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.attempts: list[int] = []          # 所有投递尝试（含失败）
        self.delivered: list[int] = []         # 仅成功送达的顺序
        self.processed_seq: set[int] = set()
        # seq -> 在成功响应前先失败多少次（模拟响应丢失/不可用）
        self.fail_times: dict[int, int] = {}
        self.offline = False

    def post(self, payload: dict):
        with self.lock:
            seq = payload["notification_seq"]
            self.attempts.append(seq)
            if self.offline or self.fail_times.get(seq, 0) > 0:
                self.fail_times[seq] = self.fail_times.get(seq, 0) - 1
                raise ConnectionError(f"simulated failure for seq={seq}")
            duplicate = seq in self.processed_seq
            self.processed_seq.add(seq)
            self.delivered.append(seq)
            return 200, '{"duplicate": %s}' % ("true" if duplicate else "false")


def make_store() -> tuple[HistoryStore, str]:
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "history.db")
    return HistoryStore(path), path


def changes(no: int) -> list[dict]:
    return [{
        "kind": "CHANGE", "change_type": "INSERT", "artifact_no": no,
        "service_name": "svc", "version": "1", "sha256": "a" * 64,
    }]


class OutboxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store, _ = make_store()

    def tearDown(self) -> None:
        self.store.close()

    def test_notification_created_in_same_transaction(self):
        self.store.persist_transaction(
            101, "0/1000", None, changes(1001)
        )
        notifs = self.store.list_notifications()
        self.assertEqual(len(notifs), 1)
        self.assertEqual(notifs[0]["status"], NOTIF_PENDING)
        self.assertEqual(notifs[0]["seq"], 1)
        self.assertEqual(
            notifs[0]["payload"]["changes"][0]["artifact_no"], 1001
        )

    def test_replay_does_not_create_second_notification(self):
        self.store.persist_transaction(101, "0/1000", None, changes(1001))
        written = self.store.persist_transaction(
            101, "0/1000", None, changes(1001)
        )
        self.assertFalse(written)
        self.assertEqual(len(self.store.list_notifications()), 1)

    def test_seq_follows_commit_order_and_hol_blocks(self):
        for xid, no in ((101, 1), (102, 2), (103, 3)):
            self.store.persist_transaction(
                xid, f"0/{1000 + xid}", None, changes(no)
            )
        # 队首永远是 seq=1
        first = self.store.fetch_next_pending_notification()
        self.assertEqual(first["seq"], 1)
        self.store.mark_notification_sent(first["id"])
        second = self.store.fetch_next_pending_notification()
        self.assertEqual(second["seq"], 2)

    def test_empty_change_transaction_has_no_notification(self):
        written = self.store.persist_transaction(200, "0/2000", None, [])
        self.assertTrue(written)
        self.assertEqual(self.store.notification_stats()["total"], 0)


class NotifierWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store, _ = make_store()
        self.sender = FakeSender()
        self.worker = NotifierWorker.__new__(NotifierWorker)
        NotifierWorker.__init__(
            self.worker, self.store,
            _Settings(backoff=0.05, max_backoff=0.2, poll=0.05),
            sender=self.sender,
        )
        self.worker.start()

    def tearDown(self) -> None:
        self.worker.stop(timeout=5)
        self.store.close()

    def _wait_status(self, seq: int, status: str, timeout: float = 8.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for n in self.store.list_notifications():
                if n["seq"] == seq and n["status"] == status:
                    return n
            time.sleep(0.05)
        self.fail(f"seq={seq} did not reach {status}")

    def test_happy_path_all_acked_in_order(self):
        for xid, no in ((101, 1), (102, 2)):
            self.store.persist_transaction(
                xid, f"0/{1000 + xid}", None, changes(no)
            )
            self.worker.notify_new_data()
        self._wait_status(2, NOTIF_SENT)
        self.assertEqual(self.sender.delivered, [1, 2])
        self.assertEqual(self.store.notification_stats()[NOTIF_SENT], 2)

    def test_lost_response_then_redelivery_idempotent(self):
        # seq=1 前两次调用模拟失败（接收端处理后响应丢失/不可用）
        self.sender.fail_times[1] = 2
        self.store.persist_transaction(101, "0/1000", None, changes(1))
        self.worker.notify_new_data()
        n1 = self._wait_status(1, NOTIF_SENT, timeout=10)
        self.assertGreaterEqual(n1["attempts"], 2)
        # 实际投递次数 = 失败次数 + 最终成功；seq=1 只被接收端处理一次
        self.assertIn(1, self.sender.processed_seq)
        self.assertEqual(
            self.sender.attempts.count(1), 3
        )

    def test_head_of_line_never_bypassed(self):
        # seq=1 持续离线，seq=2 即使已落盘也不得先投递
        self.sender.offline = True
        self.store.persist_transaction(101, "0/1000", None, changes(1))
        self.store.persist_transaction(102, "0/1001", None, changes(2))
        self.worker.notify_new_data()
        time.sleep(1.0)
        self.assertEqual(set(self.sender.delivered), set())  # 离线期均未送达
        self.sender.offline = False
        # 手动重试立即清退避
        first = self.store.fetch_next_pending_notification()
        self.store.rearm_notification(first["id"])
        self.worker.kick()
        self._wait_status(1, NOTIF_SENT, timeout=8)
        self._wait_status(2, NOTIF_SENT, timeout=8)
        # 严格顺序：seq=1 的首次成功投递早于 seq=2 的任何投递
        self.assertEqual(
            self.sender.delivered[: self.sender.delivered.index(1) + 1]
            .count(2), 0
        )
        self.assertEqual(
            sorted(set(self.sender.delivered)), [1, 2]
        )

    def test_failure_does_not_corrupt_history_reads(self):
        self.sender.offline = True
        self.store.persist_transaction(101, "0/1000", None, changes(77))
        self.worker.notify_new_data()
        time.sleep(0.6)
        # 通知一直失败，但历史查询照常返回数据
        hist = self.store.get_history_of(77)
        self.assertEqual(len(hist), 1)
        points = self.store.get_points()
        self.assertEqual(len(points), 1)
        stats = self.store.notification_stats()
        self.assertEqual(stats[NOTIF_FAILED], 1)
        self.sender.offline = False
        self._wait_status(1, NOTIF_SENT, timeout=8)


class _Settings:
    def __init__(self, backoff: float, max_backoff: float, poll: float):
        self.notifier_url = "http://local/notify"
        self.notifier_timeout_s = 2.0
        self.notifier_poll_s = poll
        self.notifier_backoff_s = backoff
        self.notifier_max_backoff_s = max_backoff


if __name__ == "__main__":
    unittest.main()
