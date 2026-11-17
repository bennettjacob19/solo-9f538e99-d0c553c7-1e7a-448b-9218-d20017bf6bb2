"""通知相关 HTTP API 单元测试：python3 -m unittest tests.test_api_notify -v

不依赖 PostgreSQL：直接构造 HistoryStore + 假发送端，挂载到 FastAPI 路由，
验证发件箱查询、手动重试的 404/409 语义，以及接收端代理的 503 隔离。
"""
from __future__ import annotations

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from app import main as web
from app.storage import HistoryStore


class NotifApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.mkdtemp()
        cls.store = HistoryStore(os.path.join(cls._tmp, "history.db"))
        # 替换全局组件，避免启动真实捕获/通知线程与 PostgreSQL 连接
        web.store = cls.store
        web.notifier._stop.set()  # 阻止后台线程实际投递
        cls.client = TestClient(web.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.store.close()

    def _seed(self, xid: int, no: int) -> int:
        self.store.persist_transaction(
            xid, f"0/{1000 + xid}", None,
            [{
                "change_type": "INSERT", "artifact_no": no,
                "service_name": "s", "version": "1", "sha256": "a" * 64,
            }],
        )
        return self.store.list_notifications()[0]["id"]

    def test_list_notifications_and_stats(self):
        nid = self._seed(301, 6001)
        r = self.client.get("/api/notifications")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertIn("stats", body)
        self.assertEqual(body["stats"]["total"], 1)
        self.assertEqual(body["notifications"][0]["id"], nid)
        self.assertEqual(
            body["notifications"][0]["payload"]["changes"][0]["artifact_no"],
            6001,
        )

    def test_retry_missing_and_already_sent(self):
        self.assertEqual(
            self.client.post("/api/notifications/99999/retry").status_code, 404
        )
        nid = self._seed(302, 6002)
        # 未确认 → 可重试
        r = self.client.post(f"/api/notifications/{nid}/retry")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["rearmed"])
        # 标记已确认后再试 → 409
        self.store.mark_notification_sent(nid)
        self.assertEqual(
            self.client.post(f"/api/notifications/{nid}/retry").status_code, 409
        )

    def test_receiver_proxy_unavailable_returns_503(self):
        # NOTIFIER_URL 指向一个必然拒绝连接的端口
        web.settings.__dict__  # frozen dataclass：改用 monkeypatch 式替换
        original = web.settings
        from dataclasses import replace

        web.settings = replace(
            original, notifier_url="http://127.0.0.1:1/notify",
            notifier_timeout_s=1.0,
        )
        try:
            r = self.client.get("/api/receiver/pending")
            self.assertEqual(r.status_code, 503)
            self.assertIn("接收端暂不可用", r.json()["detail"])
        finally:
            web.settings = original


if __name__ == "__main__":
    unittest.main()
