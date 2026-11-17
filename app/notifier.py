"""逐事务可靠通知线程：事务性发件箱 → 固定接收端，严格按提交顺序投递。

可靠性设计
----------
1. 通知行与源事务数据在**同一个 SQLite 事务**落盘（见 storage
   .persist_transaction）。只有已完整落入 SQLite 的事务才会有通知；
   捕获重放按 xid 幂等跳过，重复重放不会造出第二笔通知。
2. 本线程只取 seq 最小的未确认通知（head-of-line）：前一笔没有收到
   接收端确认（HTTP 2xx）之前，后续事务一律等待，绝不越过。
3. 接收端“已处理但响应丢失”：本地仍为未确认，会原样重发整笔通知；
   接收端按 notification_seq 幂等去重，只安排一次重建并再次回 2xx。
4. 接收端暂不可用 / 本服务重启：通知状态持久化在 SQLite，重启后线程
   自动从最早未确认项续投，失败按指数退避，也可由网页手动立即重试。
5. 通知线程的任何故障都只写通知表的错误字段，不触碰历史库读写路径，
   因此投递故障不影响历史查询。
"""
from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import Settings
from .storage import HistoryStore

log = logging.getLogger("notifier")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class NotificationSender:
    """HTTP 投递封装（stdlib urllib，无额外依赖），便于测试替换。"""

    def __init__(self, url: str, timeout_s: float) -> None:
        self._url = url
        self._timeout = timeout_s

    def post(self, payload: dict[str, Any]) -> tuple[int, str]:
        """同步 POST JSON，返回 (http_status, body)。网络层异常向上抛。"""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            # 接收端有响应但状态码非 2xx：读取 body 后作为失败返回，
            # 由调用方按未确认处理（不 ACK）。
            return exc.code, exc.read().decode("utf-8", "replace")


class NotifierWorker:
    def __init__(self, store: HistoryStore, settings: Settings,
                 sender: NotificationSender | None = None) -> None:
        self._store = store
        self._settings = settings
        self._sender = sender or NotificationSender(
            settings.notifier_url, settings.notifier_timeout_s
        )
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    # ----------------------------------------------------------------- life
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._safe_loop, name="notifier-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def notify_new_data(self) -> None:
        """捕获线程落盘新事务后唤醒，实现近实时投递。"""
        self._wake.set()

    def kick(self) -> None:
        """网页手动重试时唤醒（退避等待立即中断）。"""
        self._wake.set()

    def _safe_loop(self) -> None:
        try:
            self._loop()
        except Exception as exc:  # 兜底：通知故障不影响主服务
            log.exception("notifier worker crashed: %s", exc)

    # ----------------------------------------------------------------- loop
    def _loop(self) -> None:
        idle_wait = max(0.05, self._settings.notifier_poll_s)
        while not self._stop.is_set():
            self._wake.clear()
            try:
                item = self._store.fetch_next_pending_notification()
            except Exception as exc:
                log.warning("fetch outbox failed: %r", exc)
                item = None
            if item is None:
                # 无未确认通知：等待新数据唤醒，也定期醒来兜底
                self._stop.wait(idle_wait)
                continue

            wait_s = self._backoff_remaining(item)
            if wait_s > 0:
                # 退避窗口内：可被新数据/手动重试/停止提前唤醒。
                # 唤醒后回到循环顶部重新取队首、重算等待时间——手动重试会把
                # next_attempt_at 清空从而立即投递；普通唤醒则继续等到期。
                self._wake.wait(timeout=wait_s)
                if self._stop.is_set():
                    break
                continue

            self._deliver(item)

    def _backoff_remaining(self, item: dict[str, Any]) -> float:
        due = item.get("next_attempt_at")
        if not due:
            return 0.0
        try:
            then = datetime.strptime(due, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return 0.0
        return max(0.0, (then - _utcnow()).total_seconds())

    def _deliver(self, item: dict[str, Any]) -> None:
        nid = item["id"]
        try:
            payload = json.loads(item["payload"])
        except (TypeError, ValueError) as exc:
            # 持久化内容损坏属于不可重试错误：记录后仍保持未确认，
            # 等待人工介入（不跳过，避免静默丢通知）。
            log.error("notification %s payload unreadable: %s", nid, exc)
            self._store.mark_notification_failed(
                nid, f"unreadable payload: {exc}",
                _iso(_utcnow() + timedelta(
                    seconds=self._settings.notifier_max_backoff_s)),
            )
            return

        log.info(
            "delivering notification seq=%s xid=%s (attempt %s)",
            item["seq"], item["xid"], item["attempts"] + 1,
        )
        try:
            status, body = self._sender.post(payload)
        except Exception as exc:
            self._schedule_retry(nid, f"connection error: {exc!r}")
            return

        if 200 <= status < 300:
            self._store.mark_notification_sent(nid)
            log.info(
                "notification seq=%s xid=%s ACKed by receiver (status=%s)",
                item["seq"], item["xid"], status,
            )
            self._wake.set()  # 立即尝试下一笔，缩短队首切换延迟
            return

        self._schedule_retry(
            nid, f"receiver returned HTTP {status}: {body[:200]}"
        )

    def _schedule_retry(self, notification_id: int, error: str) -> None:
        item = self._store.get_notification(notification_id)
        attempts = (item["attempts"] if item else 0) + 1
        base = max(0.01, self._settings.notifier_backoff_s)
        cap = max(base, self._settings.notifier_max_backoff_s)
        delay = min(cap, base * (2 ** min(attempts - 1, 10)))
        next_at = _iso(_utcnow() + timedelta(seconds=delay))
        log.warning(
            "notification %s not ACKed: %s; retry #%s in %.0fs",
            notification_id, error, attempts, delay,
        )
        self._store.mark_notification_failed(notification_id, error, next_at)
