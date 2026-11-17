"""接收端 HTTP 服务（stdlib http.server，线程化，无第三方依赖）。

路由
----
GET  /healthz              存活探针
POST /notify               接收一笔事务通知（幂等）
GET  /receiver/pending     待重建制品编号
POST /receiver/drain       执行重建：待重建编号转入历史并清空队列
GET  /receiver/history     重建历史
GET  /receiver/processed   已处理通知账本（含重复投递计数）
POST /receiver/fail-mode   测试钩子：body {"status":500} 模拟响应丢失；{"status":0} 解除
"""
from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .store import ReceiverStore

log = logging.getLogger("receiver")

MAX_BODY = 1 << 20  # 1 MiB 足够单笔事务通知


class ReceiverHandler(BaseHTTPRequestHandler):
    server_version = "ArtifactReceiver/1.0"

    # ------------------------------------------------------------ 基础工具
    def _send_json(self, status: int, obj: Any) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any] | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_BODY:
            self._send_json(400, {"error": "invalid Content-Length"})
            return None
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"error": "invalid JSON body"})
            return None
        if not isinstance(data, dict):
            self._send_json(400, {"error": "body must be a JSON object"})
            return None
        return data

    def log_message(self, fmt: str, *args: Any) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    # ------------------------------------------------------------------ GET
    def do_GET(self) -> None:  # noqa: N802 (stdlib 命名)
        path = self.path.split("?", 1)[0]
        store: ReceiverStore = self.server.receiver_store  # type: ignore[attr-defined]
        if path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif path == "/receiver/pending":
            self._send_json(200, {"pending": store.pending()})
        elif path == "/receiver/history":
            self._send_json(200, {"history": store.history()})
        elif path == "/receiver/processed":
            self._send_json(200, {"processed": store.processed()})
        elif path == "/receiver/stats":
            self._send_json(200, store.stats())
        else:
            self._send_json(404, {"error": "not found"})

    # ----------------------------------------------------------------- POST
    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        store: ReceiverStore = self.server.receiver_store  # type: ignore[attr-defined]
        if path == "/notify":
            self._handle_notify(store)
        elif path == "/receiver/drain":
            rebuilt = store.drain_pending()
            self._send_json(200, {"rebuilt": rebuilt, "remaining": 0})
        elif path == "/receiver/fail-mode":
            data = self._read_json()
            if data is None:
                return
            status = int(data.get("status") or 0)
            if status and not (400 <= status <= 599):
                self._send_json(400, {"error": "status must be 4xx/5xx or 0"})
                return
            store.set_fail_mode(status or None)
            self._send_json(200, {"fail_status": status or None})
        else:
            self._send_json(404, {"error": "not found"})

    def _handle_notify(self, store: ReceiverStore) -> None:
        data = self._read_json()
        if data is None:
            return
        # 固定结构校验：seq/xid/changes 必须存在，changes 为列表
        if not isinstance(data.get("notification_seq"), int) or \
                not isinstance(data.get("xid"), int) or \
                not isinstance(data.get("changes"), list):
            self._send_json(
                422,
                {"error": "need integer notification_seq, xid and changes[]"},
            )
            return
        for c in data["changes"]:
            if not isinstance(c, dict) or not isinstance(
                c.get("artifact_no"), int
            ) or c.get("op") not in ("insert", "update", "delete"):
                self._send_json(
                    422,
                    {"error": "each change needs integer artifact_no and op"
                              " in insert/update/delete"},
                )
                return

        # 先完成幂等处理并落盘（账本 + 队列同一事务）
        result = store.accept_notification(data)
        log.info(
            "notification seq=%s xid=%s duplicate=%s pending=%s",
            result["notification_seq"], result["xid"],
            result["duplicate"], result["queued_total"],
        )

        # 测试钩子：处理已落盘后返回错误，模拟“已处理但响应丢失”。
        # 发送端重发时走 duplicate 分支，不会再安排任何重建。
        fail_status = store.get_fail_status()
        if fail_status:
            self._send_json(
                fail_status,
                {"error": f"simulated lost response (status {fail_status})",
                 "notification_seq": result["notification_seq"]},
            )
            return
        self._send_json(200, result)


def build_server(host: str, port: int, store: ReceiverStore) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), ReceiverHandler)
    httpd.receiver_store = store  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    import os
    import signal
    import threading

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    host = os.environ.get("RECEIVER_HOST", "0.0.0.0")
    port = int(os.environ.get("RECEIVER_PORT", "8090"))
    db_path = os.environ.get("RECEIVER_DB", "/data/receiver.db")
    store = ReceiverStore(db_path)
    httpd = build_server(host, port, store)

    # 容器 stop 发 SIGTERM：优雅退出。注意 serve_forever() 与 shutdown()
    # 不能在同一线程调用（会死锁），信号处理器里另起线程触发 shutdown。
    def _graceful(*_args: object) -> None:
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _graceful)
    log.info("receiver listening on http://%s:%s (db=%s)", host, port, db_path)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        store.close()
        log.info("receiver stopped")


if __name__ == "__main__":
    main()
