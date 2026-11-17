"""SQLite 历史库：只增不改的变更日志 + 捕获状态。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    captured_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);

-- 逐事务可靠通知的发件箱（outbox）：
-- * 与该事务的历史行/提交点在同一个 SQLite 事务里写入，因此“已完整落盘”
--   与“存在待发通知”是原子的，捕获重放按 xid 去重，永远不会造出第二笔；
-- * seq 单调递增即源事务提交顺序，通知线程严格按 seq 投递，前一笔未确认
--   时后续事务不得越过（head-of-line）；
-- * status/attempts/last_error/next_attempt_at 记录投递状态，ACK 只更新状态，
--   通知本身保留（仅增），网页可逐笔查看。
CREATE TABLE IF NOT EXISTS notifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    seq             INTEGER NOT NULL UNIQUE,
    xid             INTEGER NOT NULL UNIQUE,
    commit_lsn      TEXT NOT NULL,
    commit_lsn_num  INTEGER NOT NULL,
    commit_ts       TEXT,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING / SENT / FAILED
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    next_attempt_at TEXT,
    sent_at         TEXT
);

CREATE INDEX IF NOT EXISTS idx_notif_status ON notifications(status, seq);
"""

NOTIF_PENDING = "PENDING"
NOTIF_SENT = "SENT"
NOTIF_FAILED = "FAILED"

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
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
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        return self.get_phase() == PHASE_STREAMING

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线与基线提交点在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        now,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。

        新事务同时在同一个 SQLite 事务里写入一条 outbox 通知（整笔变更），
        因此通知只可能为“已完整落盘的事务”而建；重放的重复事务直接返回，
        不会制造第二笔通知。
        """
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
                    " VALUES (?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            now,
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘
                self._set_many(
                    {
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                    }
                )
                # outbox 通知与事务数据同事务落盘；只对含目标表变更的事务建通知
                if changes:
                    seq_row = self._conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq"
                        " FROM notifications"
                    ).fetchone()
                    next_seq = seq_row["next_seq"]
                    payload = {
                        "notification_seq": next_seq,
                        "xid": xid,
                        "commit_lsn": commit_lsn,
                        "commit_ts": commit_ts,
                        "changes": [
                            {
                                "artifact_no": c["artifact_no"],
                                "op": c["change_type"].lower(),
                                "service_name": c.get("service_name"),
                                "version": c.get("version"),
                                "sha256": c.get("sha256"),
                            }
                            for c in changes
                        ],
                    }
                    self._conn.execute(
                        """
                        INSERT INTO notifications(
                            seq, xid, commit_lsn, commit_lsn_num, commit_ts,
                            payload, status, attempts, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                        """,
                        (
                            next_seq, xid, commit_lsn, lsn_num, commit_ts,
                            json.dumps(payload, ensure_ascii=False),
                            NOTIF_PENDING, now, now,
                        ),
                    )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------- 通知发件箱
    def fetch_next_pending_notification(self) -> dict[str, Any] | None:
        """取最早一笔未确认通知（head-of-line）；无则返回 None。

        只按 seq 取最小的未确认项：前一笔未 ACK 时，后面的事务永远不会被
        先投递，保证“后续事务不得越过它”。
        """
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, seq, xid, commit_lsn, commit_lsn_num, commit_ts,
                       payload, status, attempts, last_error, created_at,
                       updated_at, next_attempt_at, sent_at
                FROM notifications
                WHERE status != ?
                ORDER BY seq ASC LIMIT 1
                """,
                (NOTIF_SENT,),
            ).fetchone()
            return dict(row) if row else None

    def list_notifications(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, seq, xid, commit_lsn, commit_ts, status, attempts,
                       last_error, created_at, updated_at, next_attempt_at,
                       sent_at, payload
                FROM notifications ORDER BY seq DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            result = []
            for r in rows:
                item = dict(r)
                try:
                    item["payload"] = json.loads(item["payload"])
                except (TypeError, ValueError):
                    pass
                result.append(item)
            return result

    def get_notification(self, notification_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, seq, xid, commit_lsn, status, attempts, last_error,"
                " next_attempt_at, sent_at, payload, created_at, updated_at"
                " FROM notifications WHERE id=?",
                (notification_id,),
            ).fetchone()
            return dict(row) if row else None

    def mark_notification_sent(self, notification_id: int) -> None:
        """接收端确认（2xx）后标记 SENT，保留全部历史行。"""
        now = utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE notifications
                SET status=?, last_error=NULL, next_attempt_at=NULL,
                    sent_at=?, updated_at=?
                WHERE id=?
                """,
                (NOTIF_SENT, now, now, notification_id),
            )

    def mark_notification_failed(
        self, notification_id: int, error: str, next_attempt_at: str
    ) -> None:
        """投递失败：记录错误与下次可投时间，状态保持未确认（PENDING/FAILED）。"""
        now = utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE notifications
                SET status=?, attempts=attempts+1, last_error=?,
                    next_attempt_at=?, updated_at=?
                WHERE id=?
                """,
                (NOTIF_FAILED, error[:500], next_attempt_at, now,
                 notification_id),
            )

    def rearm_notification(self, notification_id: int) -> bool:
        """网页手动重试：清空退避时间立即可投；返回是否找到该未确认通知。"""
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE notifications
                SET next_attempt_at=NULL, updated_at=?
                WHERE id=? AND status != ?
                """,
                (now, notification_id, NOTIF_SENT),
            )
            return cur.rowcount > 0

    def notification_stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS c FROM notifications GROUP BY status"
            ).fetchall()
            stats = {NOTIF_PENDING: 0, NOTIF_FAILED: 0, NOTIF_SENT: 0}
            for r in rows:
                stats[r["status"]] = r["c"]
            stats["total"] = sum(stats.values())
            return stats

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count
                FROM txns t ORDER BY t.commit_lsn_num, t.xid
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                    }
                )
            return points

    def get_directory_at(self, lsn_num: int) -> list[dict[str, Any]]:
        """查看某个提交点（含）为止的整目录：每个编号取最后一条变更。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at
                FROM artifact_history WHERE artifact_no=?
                ORDER BY id
                """,
                (artifact_no,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            notif_stats = self.notification_stats()
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "notifications": notif_stats,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
            }
