"""在线改表（Online DDL）服务

为指定 PostgreSQL 表在线添加「可空 text 列」，语义如下：

* 锁忙重试：每次尝试在数据库锁队列中最多等待 100ms（lock_timeout），
  超时即回滚、退出锁队列，稍作停顿后重试；两次尝试之间不持有任何锁，
  普通查询可以正常通过。
* 总时限：从受理时刻起，累计等待超过「等锁总时限」仍未拿到锁，
  出具「未执行」回执，且该单号此后不可再用于加列。
* 原子提交：加列（ALTER TABLE）与成功回执在同一事务中提交，
  要么同时生效，要么同时回滚。
* 幂等回执：回执持久化在 PostgreSQL 中。服务重启后，
  同单号同列名返回原回执；已受理的单号换列名一律拒收。
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import re
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field, field_validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("online-ddl")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://app:apppass@localhost:5432/appdb"
)
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")
TARGET_TABLE = os.environ.get("TARGET_TABLE", "orders")

# 每次尝试在数据库锁队列中的最大等待时间（毫秒）
LOCK_QUEUE_TIMEOUT_MS = 100
# 两次尝试之间的停顿区间（秒）：不持锁，让普通查询从锁队列中通过
RETRY_PAUSE_RANGE_S = (0.04, 0.12)

IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
if not IDENT_RE.fullmatch(TARGET_TABLE) or not IDENT_RE.fullmatch(DB_SCHEMA):
    raise RuntimeError(f"非法的表名/模式名配置: {DB_SCHEMA}.{TARGET_TABLE}")

QUALIFIED_TABLE = f'"{DB_SCHEMA}"."{TARGET_TABLE}"'

CREATE_RECEIPTS_SQL = """
CREATE TABLE IF NOT EXISTS schema_change_receipts (
    ticket_no   TEXT PRIMARY KEY,
    column_name TEXT NOT NULL,
    status      TEXT NOT NULL
                CHECK (status IN ('processing', 'success', 'not_executed')),
    receipt     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

pool: ConnectionPool


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def db_conn():
    """借出一条连接：正常退出则提交，异常则回滚，始终归还连接池。"""
    with pool.connection() as conn:
        try:
            yield conn
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except Exception:
                pass
            raise


def create_pool_with_retry(retries: int = 30, delay: float = 1.0) -> ConnectionPool:
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            p = ConnectionPool(
                DATABASE_URL,
                min_size=1,
                max_size=10,
                kwargs={"row_factory": dict_row, "autocommit": False},
            )
            with p.connection() as conn:
                conn.execute("SELECT 1")
            log.info("数据库连接池已就绪")
            return p
        except Exception as exc:  # noqa: BLE001
            last = exc
            log.info("等待数据库就绪 (%d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"数据库不可用: {last}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = create_pool_with_retry()
    with db_conn() as conn:
        conn.execute(CREATE_RECEIPTS_SQL)
        # 上次进程退出时仍停留在 processing 的单据：其事务已随连接断开而回滚，
        # 列并未真正加上，删除占位记录，允许该单号重新受理。
        cur = conn.execute(
            "DELETE FROM schema_change_receipts WHERE status = 'processing'"
        )
        if cur.rowcount:
            log.warning("清理了 %d 条上次中断的处理中记录", cur.rowcount)
    yield
    pool.close()


app = FastAPI(
    title="在线改表服务（Online DDL）",
    description=__doc__,
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------- models


class AddColumnRequest(BaseModel):
    ticket_no: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="变更单号（全局唯一，作为幂等键）",
        examples=["CHG-20261123-001"],
    )
    column_name: str = Field(
        ...,
        pattern=r"^[a-z_][a-z0-9_]{0,62}$",
        description="要新增的列名（小写字母/数字/下划线；可空 text 列）",
        examples=["deploy_note"],
    )
    wait_timeout_ms: int = Field(
        ...,
        ge=0,
        le=3_600_000,
        description="等锁总时限（毫秒）：从受理时刻起，累计等待表锁的上限",
        examples=[5000],
    )

    @field_validator("ticket_no")
    @classmethod
    def _strip_ticket(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("变更单号不能为空")
        return v


class Receipt(BaseModel):
    receipt_no: str = Field(description="回执编号")
    ticket_no: str = Field(description="变更单号")
    table_name: str = Field(description="目标表")
    column_name: str = Field(description="列名")
    column_type: str = Field(description="列类型")
    nullable: bool = Field(description="是否可空")
    status: Literal["success", "not_executed"] = Field(description="执行结果")
    message: str = Field(description="结果说明")
    attempts: int = Field(description="抢锁尝试次数")
    accepted_at: str = Field(description="受理时间（UTC）")
    finished_at: str = Field(description="办结时间（UTC）")


# ---------------------------------------------------------------- db helpers


def get_ticket(ticket_no: str) -> dict | None:
    with db_conn() as conn:
        return conn.execute(
            "SELECT ticket_no, column_name, status, receipt "
            "FROM schema_change_receipts WHERE ticket_no = %s",
            (ticket_no,),
        ).fetchone()


def try_claim(ticket_no: str, column_name: str) -> bool:
    """插入 processing 占位记录抢占单号；已存在则返回 False。"""
    with db_conn() as conn:
        row = conn.execute(
            """
            INSERT INTO schema_change_receipts (ticket_no, column_name, status)
            VALUES (%s, %s, 'processing')
            ON CONFLICT (ticket_no) DO NOTHING
            RETURNING ticket_no
            """,
            (ticket_no, column_name),
        ).fetchone()
    return row is not None


def release_claim(ticket_no: str) -> None:
    """加列未发生（如列已存在等校验失败）时释放单号占位。"""
    try:
        with db_conn() as conn:
            conn.execute(
                "DELETE FROM schema_change_receipts "
                "WHERE ticket_no = %s AND status = 'processing'",
                (ticket_no,),
            )
    except Exception:  # noqa: BLE001
        log.exception("释放单号占位失败: %s", ticket_no)


def wait_for_final(ticket_no: str, timeout_s: float) -> dict | None:
    """等待同一单号的并发请求产出最终回执。"""
    deadline = time.monotonic() + timeout_s
    while True:
        row = get_ticket(ticket_no)
        if row is None or row["status"] != "processing":
            return row
        if time.monotonic() >= deadline:
            return row
        time.sleep(0.1)


def build_receipt(
    ticket_no: str,
    column_name: str,
    status: str,
    message: str,
    attempts: int,
    accepted_at: datetime,
    finished_at: datetime,
) -> dict:
    return {
        "receipt_no": uuid.uuid4().hex,
        "ticket_no": ticket_no,
        "table_name": f"{DB_SCHEMA}.{TARGET_TABLE}",
        "column_name": column_name,
        "column_type": "text",
        "nullable": True,
        "status": status,
        "message": message,
        "attempts": attempts,
        "accepted_at": accepted_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }


def replay(row: dict, column_name: str) -> Receipt:
    """已受理的单号：同列名返回原回执，换列名一律拒收。"""
    if row["column_name"] != column_name:
        raise HTTPException(
            status_code=409,
            detail=(
                f"变更单 {row['ticket_no']} 已受理（列 "
                f"{row['column_name']}，状态 {row['status']}），"
                "同一单号不允许更换列名"
            ),
        )
    return Receipt(**json.loads(row["receipt"]))


# ---------------------------------------------------------------- core logic


def run_change(req: AddColumnRequest, accepted_at: datetime, deadline: float) -> Receipt:
    """抢锁重试循环：成功则加列与回执同事务提交；超时则出具未执行回执。"""
    ticket_no, column_name = req.ticket_no, req.column_name
    attempts = 0

    while True:
        remaining_ms = (deadline - time.monotonic()) * 1000
        if remaining_ms <= 0:
            break
        # 每次尝试在数据库锁队列中最多等 100ms（且不超过剩余总时限）
        lock_ms = max(1, min(LOCK_QUEUE_TIMEOUT_MS, math.ceil(remaining_ms)))
        attempts += 1
        try:
            with db_conn() as conn:
                # set_config(..., is_local=true)：仅本事务生效，连接归还池后不残留
                conn.execute(
                    "SELECT set_config('lock_timeout', %s, true)", (f"{lock_ms}ms",)
                )
                conn.execute(
                    f'ALTER TABLE {QUALIFIED_TABLE} ADD COLUMN "{column_name}" text'
                )
                receipt = build_receipt(
                    ticket_no,
                    column_name,
                    "success",
                    "已执行：可空 text 列已添加，加列与回执在同一事务中提交",
                    attempts,
                    accepted_at,
                    utcnow(),
                )
                # 加列与成功回执在同一事务中提交
                conn.execute(
                    "UPDATE schema_change_receipts "
                    "SET status = 'success', receipt = %s, updated_at = now() "
                    "WHERE ticket_no = %s",
                    (json.dumps(receipt, ensure_ascii=False), ticket_no),
                )
            log.info(
                "单号 %s：列 %s 添加成功（第 %d 次尝试）",
                ticket_no, column_name, attempts,
            )
            return Receipt(**receipt)
        except errors.LockNotAvailable:
            # 本次尝试已回滚，不持有任何锁；停顿片刻让普通查询通过后再重试
            remaining_ms = (deadline - time.monotonic()) * 1000
            if remaining_ms <= 0:
                break
            log.info(
                "单号 %s：第 %d 次尝试未拿到锁（%dms 内），稍后重试",
                ticket_no, attempts, lock_ms,
            )
            time.sleep(min(random.uniform(*RETRY_PAUSE_RANGE_S), remaining_ms / 1000))
        except errors.DuplicateColumn:
            release_claim(ticket_no)
            raise HTTPException(
                status_code=409,
                detail=f"列 {column_name} 已存在于表 {DB_SCHEMA}.{TARGET_TABLE}",
            )
        except errors.UndefinedTable:
            release_claim(ticket_no)
            raise HTTPException(
                status_code=404,
                detail=f"目标表 {DB_SCHEMA}.{TARGET_TABLE} 不存在",
            )
        except Exception:
            release_claim(ticket_no)
            raise

    # 从受理起到时仍未拿到锁：出具「未执行」回执，此后该单号不可再加列
    receipt = build_receipt(
        ticket_no,
        column_name,
        "not_executed",
        "未执行：从受理起在等锁总时限内未获得表锁；该单号此后不可再用于加列",
        attempts,
        accepted_at,
        utcnow(),
    )
    with db_conn() as conn:
        conn.execute(
            "UPDATE schema_change_receipts "
            "SET status = 'not_executed', receipt = %s, updated_at = now() "
            "WHERE ticket_no = %s AND status = 'processing'",
            (json.dumps(receipt, ensure_ascii=False), ticket_no),
        )
    log.warning(
        "单号 %s：等锁总时限已到（尝试 %d 次），未执行", ticket_no, attempts
    )
    return Receipt(**receipt)


# ---------------------------------------------------------------- endpoints


@app.post(
    "/schema-changes/add-column",
    response_model=Receipt,
    summary="为样例表添加可空 text 列（在线改表）",
    tags=["在线改表"],
    responses={
        409: {"description": "单号冲突：该单号已受理但列名不同 / 列已存在 / 正在处理中"},
        404: {"description": "目标表不存在"},
    },
)
def add_column(req: AddColumnRequest) -> Receipt:
    accepted_at = utcnow()
    deadline = time.monotonic() + req.wait_timeout_ms / 1000.0

    row = get_ticket(req.ticket_no)
    if row is None:
        if try_claim(req.ticket_no, req.column_name):
            return run_change(req, accepted_at, deadline)
        row = get_ticket(req.ticket_no)  # 被并发请求抢先受理
    if row is not None and row["status"] == "processing":
        # 另一个并发请求正在处理同一单号：等它产出最终回执
        row = wait_for_final(req.ticket_no, req.wait_timeout_ms / 1000.0 + 10)
    if row is None or row["status"] == "processing":
        raise HTTPException(
            status_code=409,
            detail="该变更单正在处理中，请稍后通过 GET /receipts/{ticket_no} 查询回执",
        )
    return replay(row, req.column_name)


@app.get(
    "/receipts/{ticket_no}",
    response_model=Receipt,
    summary="按变更单号查询回执",
    tags=["在线改表"],
)
def get_receipt(ticket_no: str) -> Receipt:
    row = get_ticket(ticket_no)
    if row is None:
        raise HTTPException(status_code=404, detail="变更单不存在")
    if row["status"] == "processing":
        raise HTTPException(status_code=409, detail="变更单处理中，请稍后重试")
    return Receipt(**json.loads(row["receipt"]))


@app.get("/tables/columns", summary="查看目标表现有列", tags=["在线改表"])
def list_columns() -> dict:
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (DB_SCHEMA, TARGET_TABLE),
        ).fetchall()
    return {"table": f"{DB_SCHEMA}.{TARGET_TABLE}", "columns": rows}


@app.get("/", include_in_schema=False)
def index() -> RedirectResponse:
    return RedirectResponse("/docs")


@app.get("/health", include_in_schema=False)
def health() -> dict:
    with db_conn() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}
