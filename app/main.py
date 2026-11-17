"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度。

初始化未完成（phase != STREAMING）时，所有历史查询接口返回 503，
不对外开放尚未完整的历史；源目录修改走源库自身事务，不受此限制。
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker
from .config import settings
from .notifier import NotifierWorker
from .source import SourceDB, SourceError
from .storage import HistoryStore, lsn_to_num

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    import re

    if not re.match(SHA256_RE, v):
        raise ValueError("sha256 必须是 64 位十六进制")
    return v


class _Columns(BaseModel):
    """增/改共用的四列固定字段，严格拒绝额外列。"""

    artifact_no: int = Field(gt=0)
    service_name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _check_sha(v)


class InsertOp(_Columns):
    op: Literal["insert"]


class UpdateOp(_Columns):
    op: Literal["update"]


class DeleteOp(BaseModel):
    op: Literal["delete"]
    artifact_no: int = Field(gt=0)


AnyOp = Annotated[
    Union[InsertOp, UpdateOp, DeleteOp], Field(discriminator="op")
]


class BatchRequest(BaseModel):
    operations: list[AnyOp] = Field(min_length=1)


store = HistoryStore(settings.sqlite_path)
source = SourceDB(settings)
notifier = NotifierWorker(store, settings)
worker = CaptureWorker(store, source, settings, notifier=notifier)


@asynccontextmanager
async def lifespan(app: FastAPI):
    notifier.start()
    worker.start()
    try:
        yield
    finally:
        worker.stop()
        notifier.stop()
        store.close()


app = FastAPI(title="制品目录历史查询", lifespan=lifespan)


def _require_ready() -> None:
    if not store.is_ready():
        progress = store.get_progress()
        raise HTTPException(
            status_code=503,
            detail={
                "message": "捕获初始化尚未完成，历史暂不开放",
                "phase": progress.get("phase"),
                "last_error": progress.get("last_error"),
            },
        )


# --------------------------------------------------------------------- 页面
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )


# --------------------------------------------------------------------- 进度
@app.get("/api/progress")
def api_progress() -> dict:
    return store.get_progress()


# --------------------------------------------------------------- 源目录读写
@app.get("/api/source/artifacts")
def api_source_list() -> dict:
    return {"artifacts": source.list_artifacts()}


@app.post("/api/source/operations")
def api_batch_write(req: BatchRequest) -> dict:
    """成组修改源目录；整组在一个源事务内提交，失败整体回滚。"""
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# ---------------------------------------------------------------- 历史查询
@app.get("/api/points")
def api_points() -> dict:
    _require_ready()
    return {"points": store.get_points()}


@app.get("/api/directory")
def api_directory(point: str) -> JSONResponse:
    """按提交点（LSN，如 0/16B3748）查看该时刻的整张目录。"""
    _require_ready()
    try:
        lsn_num = lsn_to_num(point)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point 必须是 LSN，形如 0/16B3748")
    points = store.get_points()
    valid_nums = {p["lsn_num"] for p in points}
    if lsn_num not in valid_nums:
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
        )
    return JSONResponse(
        {
            "point": point,
            "point_lsn_num": lsn_num,
            "artifacts": store.get_directory_at(lsn_num),
        }
    )


@app.get("/api/history/{artifact_no}")
def api_artifact_history(artifact_no: int) -> dict:
    """单个制品编号的完整变更链；删除后重用编号，旧历史也全部保留。"""
    _require_ready()
    return {
        "artifact_no": artifact_no,
        "history": store.get_history_of(artifact_no),
    }


# ------------------------------------------------------------- 通知发件箱
@app.get("/api/notifications")
def api_notifications() -> dict:
    """逐笔投递状态（初始化期也可查看，发件箱不依赖历史开放状态）。"""
    return {
        "stats": store.notification_stats(),
        "notifications": store.list_notifications(),
        "receiver_url": settings.notifier_url,
    }


@app.post("/api/notifications/{notification_id}/retry")
def api_retry_notification(notification_id: int) -> dict:
    """手动重试一笔未确认通知：清空退避并立即唤醒通知线程。"""
    item = store.get_notification(notification_id)
    if item is None:
        raise HTTPException(status_code=404, detail="通知不存在")
    if item["status"] == "SENT":
        raise HTTPException(status_code=409, detail="该通知已确认，无需重试")
    ok = store.rearm_notification(notification_id)
    notifier.kick()
    return {"rearmed": ok, "notification": store.get_notification(notification_id)}


# ------------------------------------------------- 接收端待重建编号（代理）
def _receiver_base_url() -> str:
    # NOTIFIER_URL 形如 http://receiver:8090/notify，去掉尾部 /notify
    return settings.notifier_url.rsplit("/", 1)[0]


@app.get("/api/receiver/pending")
def api_receiver_pending() -> JSONResponse:
    """接收端当前待重建的制品编号。"""
    return _receiver_get("/receiver/pending")


@app.get("/api/receiver/history")
def api_receiver_history() -> JSONResponse:
    return _receiver_get("/receiver/history")


@app.get("/api/receiver/processed")
def api_receiver_processed() -> JSONResponse:
    return _receiver_get("/receiver/processed")


@app.post("/api/receiver/drain")
def api_receiver_drain() -> JSONResponse:
    """演示操作：接收端执行一次重建，待重建编号转入历史。"""
    return _receiver_post("/receiver/drain", b"")


def _receiver_get(path: str) -> JSONResponse:
    import urllib.error
    import urllib.request

    url = _receiver_base_url() + path
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, method="GET"),
            timeout=settings.notifier_timeout_s,
        ) as resp:
            return JSONResponse(json.loads(resp.read().decode("utf-8")))
    except urllib.error.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"接收端返回 {exc.code}: {exc.read().decode('utf-8', 'replace')}",
        )
    except Exception as exc:
        # 接收端暂不可用不影响本服务的历史查询，只在该接口上报
        raise HTTPException(status_code=503, detail=f"接收端暂不可用: {exc}")


def _receiver_post(path: str, body: bytes) -> JSONResponse:
    import urllib.error
    import urllib.request

    url = _receiver_base_url() + path
    try:
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(
            req, timeout=settings.notifier_timeout_s
        ) as resp:
            return JSONResponse(json.loads(resp.read().decode("utf-8")))
    except urllib.error.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"接收端返回 {exc.code}: {exc.read().decode('utf-8', 'replace')}",
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"接收端暂不可用: {exc}")
