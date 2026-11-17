"""接收端持久化：幂等账本 + 待重建队列 + 重建历史，全部在 SQLite。"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS processed_notifications (
    notification_seq INTEGER PRIMARY KEY,   -- 发送端每笔通知的单调序号
    xid              INTEGER NOT NULL,
    commit_lsn       TEXT NOT NULL,
    payload          TEXT NOT NULL,
    duplicate_count  INTEGER NOT NULL DEFAULT 0,
    first_seen_at    TEXT NOT NULL,
    last_seen_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rebuild_queue (
    artifact_no INTEGER PRIMARY KEY,        -- 待重建编号天然去重
    enqueued_at TEXT NOT NULL,
    reasons     TEXT NOT NULL               -- 触发该编号的通知 seq 列表
);

CREATE TABLE IF NOT EXISTS rebuild_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no   INTEGER NOT NULL,
    trigger_seqs  TEXT NOT NULL,
    rebuilt_at    TEXT NOT NULL
);

-- 测试钩子：非空时 /notify 在“已处理并落盘之后”返回该 HTTP 状态码，
-- 模拟“接收端已处理但响应丢失/错误”，发送端只能靠重发收敛。
CREATE TABLE IF NOT EXISTS receiver_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class ReceiverStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- 测试钩子
    def set_fail_mode(self, status: int | None) -> None:
        with self._lock:
            if status:
                self._conn.execute(
                    "INSERT INTO receiver_state(key, value) VALUES('fail_status', ?)"
                    " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(status),),
                )
            else:
                self._conn.execute(
                    "DELETE FROM receiver_state WHERE key='fail_status'"
                )

    def get_fail_status(self) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM receiver_state WHERE key='fail_status'"
            ).fetchone()
            return int(row["value"]) if row else None

    # ------------------------------------------------------------- 通知处理
    def accept_notification(self, payload: dict[str, Any]) -> dict[str, Any]:
        """幂等处理一笔通知。返回包含 duplicate 标志的结果。

        首次到达：登记账本 + 把涉及的制品编号去重加入待重建队列，
        全部在同一个 SQLite 事务里完成；重发（同 seq）只累加重复计数，
        绝不重复入队/重复安排重建。
        """
        seq = int(payload["notification_seq"])
        xid = int(payload["xid"])
        commit_lsn = str(payload.get("commit_lsn", ""))
        changes = payload.get("changes") or []
        artifact_nos = sorted({int(c["artifact_no"]) for c in changes})
        now = utc_now_iso()

        with self._lock:
            existing = self._conn.execute(
                "SELECT notification_seq FROM processed_notifications"
                " WHERE notification_seq=?",
                (seq,),
            ).fetchone()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if existing:
                    self._conn.execute(
                        "UPDATE processed_notifications"
                        " SET duplicate_count=duplicate_count+1, last_seen_at=?"
                        " WHERE notification_seq=?",
                        (now, seq),
                    )
                    duplicate = True
                    enqueued: list[int] = []
                    already_queued: list[int] = []
                else:
                    self._conn.execute(
                        "INSERT INTO processed_notifications("
                        "notification_seq, xid, commit_lsn, payload,"
                        " duplicate_count, first_seen_at, last_seen_at)"
                        " VALUES (?, ?, ?, ?, 0, ?, ?)",
                        (
                            seq, xid, commit_lsn,
                            json.dumps(payload, ensure_ascii=False), now, now,
                        ),
                    )
                    duplicate = False
                    enqueued, already_queued = [], []
                    for no in artifact_nos:
                        cur = self._conn.execute(
                            "INSERT OR IGNORE INTO rebuild_queue("
                            "artifact_no, enqueued_at, reasons) VALUES (?, ?, ?)",
                            (no, now, json.dumps([seq])),
                        )
                        if cur.rowcount:
                            enqueued.append(no)
                        else:
                            already_queued.append(no)
                            self._conn.execute(
                                "UPDATE rebuild_queue"
                                " SET reasons=json_insert(reasons,"
                                " '$[#]', ?) WHERE artifact_no=?",
                                (seq, no),
                            )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

            return {
                "notification_seq": seq,
                "xid": xid,
                "duplicate": duplicate,
                "artifact_nos": artifact_nos,
                "newly_enqueued": enqueued,
                "already_queued": already_queued,
                "queued_total": self._queue_count_locked(),
            }

    def _queue_count_locked(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) AS c FROM rebuild_queue"
        ).fetchone()["c"]

    # ------------------------------------------------------------------ 查询
    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT artifact_no, enqueued_at, reasons"
                " FROM rebuild_queue ORDER BY artifact_no"
            ).fetchall()
            result = []
            for r in rows:
                item = dict(r)
                try:
                    item["reasons"] = json.loads(item["reasons"])
                except (TypeError, ValueError):
                    item["reasons"] = []
                result.append(item)
            return result

    def drain_pending(self) -> list[int]:
        """演示/运维操作：把当前待重建编号视为“已重建”移入历史并清空队列。"""
        now = utc_now_iso()
        with self._lock:
            rows = self._conn.execute(
                "SELECT artifact_no, reasons FROM rebuild_queue"
            ).fetchall()
            nos = [r["artifact_no"] for r in rows]
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for r in rows:
                    self._conn.execute(
                        "INSERT INTO rebuild_history(artifact_no, trigger_seqs,"
                        " rebuilt_at) VALUES (?, ?, ?)",
                        (r["artifact_no"], r["reasons"], now),
                    )
                self._conn.execute("DELETE FROM rebuild_queue")
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return nos

    def processed(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT notification_seq, xid, commit_lsn, duplicate_count,"
                " first_seen_at, last_seen_at"
                " FROM processed_notifications"
                " ORDER BY notification_seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def history(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, artifact_no, trigger_seqs, rebuilt_at"
                " FROM rebuild_history ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            result = []
            for r in rows:
                item = dict(r)
                try:
                    item["trigger_seqs"] = json.loads(item["trigger_seqs"])
                except (TypeError, ValueError):
                    item["trigger_seqs"] = []
                result.append(item)
            return result

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "processed_notifications": self._conn.execute(
                    "SELECT COUNT(*) AS c FROM processed_notifications"
                ).fetchone()["c"],
                "duplicate_deliveries": self._conn.execute(
                    "SELECT COALESCE(SUM(duplicate_count), 0) AS c"
                    " FROM processed_notifications"
                ).fetchone()["c"],
                "pending_rebuilds": self._queue_count_locked(),
                "rebuild_history": self._conn.execute(
                    "SELECT COUNT(*) AS c FROM rebuild_history"
                ).fetchone()["c"],
            }
