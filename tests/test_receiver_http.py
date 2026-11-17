"""接收端 HTTP 层冒烟测试：python3 -m unittest tests.test_receiver_http -v

在临时端口启动真实的 stdlib HTTP 服务，验证 /notify 幂等响应、
fail-mode 钩子、pending/drain/history 全链路与非法请求 4xx。
各用例使用独立编号/seq，互不依赖执行顺序。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.request
from urllib.error import HTTPError

from app.receiver.server import build_server
from app.receiver.store import ReceiverStore


def post(url: str, obj: dict) -> tuple[int, dict]:
    data = json.dumps(obj).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


class ReceiverHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.mkdtemp()
        cls.store = ReceiverStore(os.path.join(cls.tmp, "recv.db"))
        cls.httpd = build_server("127.0.0.1", 0, cls.store)
        cls.port = cls.httpd.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(
            target=cls.httpd.serve_forever, daemon=True
        )
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.store.close()

    def _payload(self, seq: int, no: int) -> dict:
        return {
            "notification_seq": seq, "xid": 9000 + seq,
            "commit_lsn": f"0/{seq}", "commit_ts": None,
            "changes": [{
                "artifact_no": no, "op": "insert",
                "service_name": "s", "version": "1", "sha256": "a" * 64,
            }],
        }

    def _pending_nos(self) -> set[int]:
        return {p["artifact_no"] for p in
                get(self.base + "/receiver/pending")["pending"]}

    def test_health_and_notify_idempotency(self):
        self.assertEqual(get(self.base + "/healthz")["status"], "ok")
        s1, b1 = post(self.base + "/notify", self._payload(11, 7101))
        self.assertEqual(s1, 200)
        self.assertFalse(b1["duplicate"])
        s2, b2 = post(self.base + "/notify", self._payload(11, 7101))
        self.assertEqual(s2, 200)
        self.assertTrue(b2["duplicate"])  # 重发不产生第二次重建安排
        self.assertIn(7101, self._pending_nos())

    def test_fail_mode_returns_error_after_processing(self):
        post(self.base + "/receiver/fail-mode", {"status": 503})
        status, _ = post(self.base + "/notify", self._payload(12, 7102))
        self.assertEqual(status, 503)
        # 处理已落盘：解除 fail-mode 后重发是 duplicate，不重复入队
        post(self.base + "/receiver/fail-mode", {"status": 0})
        status2, body2 = post(self.base + "/notify", self._payload(12, 7102))
        self.assertEqual(status2, 200)
        self.assertTrue(body2["duplicate"])
        self.assertIn(7102, self._pending_nos())

    def test_drain_moves_current_queue_to_history(self):
        post(self.base + "/notify", self._payload(13, 7103))
        self.assertIn(7103, self._pending_nos())
        resp = post(self.base + "/receiver/drain", {})[1]
        self.assertIn(7103, resp["rebuilt"])
        self.assertEqual(resp["remaining"], 0)
        self.assertEqual(self._pending_nos(), set())
        history_nos = {h["artifact_no"] for h in
                       get(self.base + "/receiver/history")["history"]}
        self.assertIn(7103, history_nos)

    def test_invalid_payload_rejected(self):
        status, _ = post(self.base + "/notify", {"notification_seq": "x"})
        self.assertEqual(status, 422)
        status, _ = post(self.base + "/notify", {
            "notification_seq": 14, "xid": 1,
            "changes": [{"artifact_no": 1, "op": "truncate"}],
        })
        self.assertEqual(status, 422)
        status, _ = post(self.base + "/notify", {"changes": []})
        self.assertEqual(status, 422)

    def test_unknown_route_404(self):
        try:
            urllib.request.urlopen(self.base + "/nope", timeout=5)
            self.fail("expected 404")
        except HTTPError as exc:
            self.assertEqual(exc.code, 404)


if __name__ == "__main__":
    unittest.main()
