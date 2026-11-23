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
* 等锁采样：服务在重试过程中自动采样，记录每次尝试「谁挡住了这次 ALTER」
  与「谁被这次 ALTER 挡住」。采样最多额外占用一条数据库连接，
  单次采样查询自身限时 50ms，不影响「每次等锁不超过 100ms」。
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Literal

import psycopg
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

# 采样：一次等锁样本里，最多隔多久回头确认一次「谁挡住了 ALTER」。
# 只做轻量快照，不在采样连接上排队等锁。
SAMPLE_PROBE_INTERVAL_S = 0.02
# 单次采样查询的最长耗时（毫秒），防止采样自身拖住等锁尝试
SAMPLE_QUERY_TIMEOUT_MS = 50

# 每次尝试都在事务内给连接打上标记，采样据此把样本归到「实际等锁的那次尝试」。
# 连接归还连接池、再被别的单据借用时，标记不会残留（set_config ... is_local）。
ATTEMPT_TAG_PREFIX = "online-ddl-attempt/"

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

# 等锁样本：每次抢锁尝试最多落一行。
# is_current=false 表示该尝试已结束（重试间隙 / 单号办结），保留为历史。
CREATE_WAIT_SAMPLES_SQL = """
CREATE TABLE IF NOT EXISTS schema_change_wait_samples (
    id              BIGSERIAL PRIMARY KEY,
    ticket_no       TEXT NOT NULL,
    column_name     TEXT NOT NULL,
    attempt_no      INTEGER NOT NULL,
    attempt_token   TEXT NOT NULL,
    sampled_at      TIMESTAMPTZ NOT NULL,
    alter_pid       INTEGER,
    blocked_by      INTEGER[] NOT NULL DEFAULT '{}',
    blocked_pids    INTEGER[] NOT NULL DEFAULT '{}',
    blocker_details JSONB NOT NULL DEFAULT '[]'::jsonb,
    blocked_details JSONB NOT NULL DEFAULT '[]'::jsonb,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

CREATE_WAIT_SAMPLES_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS schema_change_wait_samples_ticket_idx
    ON schema_change_wait_samples (ticket_no, sampled_at DESC)
"""

CREATE_WAIT_SAMPLES_TOKEN_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS schema_change_wait_samples_token_key
    ON schema_change_wait_samples (attempt_token)
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
        conn.execute(CREATE_WAIT_SAMPLES_SQL)
        conn.execute(CREATE_WAIT_SAMPLES_INDEX_SQL)
        conn.execute(CREATE_WAIT_SAMPLES_TOKEN_INDEX_SQL)
        # 上个进程残留的「当前」样本：对应的尝试已随进程退出结束，统一标为历史
        cur = conn.execute(
            "UPDATE schema_change_wait_samples SET is_current = FALSE, updated_at = now() "
            "WHERE is_current"
        )
        if cur.rowcount:
            log.warning("将 %d 份上次中断的等锁样本标为历史", cur.rowcount)
        # 上次进程退出时仍停留在 processing 的单据：其事务已随连接断开而回滚，
        # 列并未真正加上，删除占位记录，允许该单号重新受理。
        cur = conn.execute(
            "DELETE FROM schema_change_receipts WHERE status = 'processing'"
        )
        if cur.rowcount:
            log.warning("清理了 %d 条上次中断的处理中记录", cur.rowcount)
    yield
    close_sampler_conn()
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


class WaitSample(BaseModel):
    """一次等锁尝试的采样结果。"""

    attempt_no: int = Field(description="尝试序号（从 1 起，按受理后每次抢锁递增）")
    sampled_at: str = Field(description="采样时间（UTC）")
    alter_pid: int = Field(description="本次 ALTER 会话 PID（直接挡住它的会话见下）")
    direct_blocker_pids: list[int] = Field(
        description="直接挡住本次 ALTER 的会话 PID（ALTER 正排在它们后面）"
    )
    blocked_select_pids: list[int] = Field(
        description="直接被本次 ALTER 挡住的 SELECT 会话 PID（排在 ALTER 后面）"
    )
    is_history: bool = Field(
        description="是否为历史样本（该次尝试已结束：重试间隙或单号已办结）"
    )


class WaitSampleReport(BaseModel):
    """按变更单号查询最近一份等锁样本。"""

    ticket_no: str
    column_name: str | None = Field(description="该单号要加的列")
    status: str | None = Field(
        default=None,
        description="该单号当前状态：processing / success / not_executed；不存在则为 null",
    )
    has_sample: bool = Field(description="是否采到过等锁样本")
    message: str = Field(description="结果说明")
    latest: WaitSample | None = Field(
        default=None, description="最近一份样本；从未采到则为 null"
    )


class WaitSampleDetail(WaitSample):
    """最近一份样本的展开详情（含阻挡/被挡住的会话明细）。"""

    ticket_no: str
    column_name: str
    direct_blocker_details: list[dict] = Field(
        default_factory=list, description="直接挡住 ALTER 的会话明细"
    )
    blocked_select_details: list[dict] = Field(
        default_factory=list, description="直接被 ALTER 挡住的 SELECT 会话明细"
    )


class WaitSampleHoldingLocks(BaseModel):
    """现场核对：样本里出现过的会话，此刻是否还在（仍持有/等待锁）。"""

    ticket_no: str
    column_name: str
    attempt_no: int = Field(description="样本对应的尝试序号")
    sampled_at: str = Field(description="样本采样时间（UTC）")
    blocker_details: list[dict] = Field(
        default_factory=list, description="持续挡住 ALTER 的会话（读事务等）"
    )
    blocked_select_details: list[dict] = Field(
        default_factory=list, description="反复排到 ALTER 后面的 SELECT（报表等）"
    )
    missing_pids: list[int] = Field(
        default_factory=list, description="采样后已断开、现场查不到的 PID"
    )
    note: str


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


# ---------------------------------------------------------------- lock sampler

# 采样专用连接：整个进程最多额外用这一条，所有采样查询串行共用。
_sampler_conn: psycopg.Connection | None = None
_sampler_conn_lock = threading.Lock()

# 一次快照：先定位「本次尝试」的 ALTER 会话（PID + 一次性标记双重校验），
# 再取它的直接阻塞者，以及直接排在它后面的会话。
_SAMPLE_SQL = """
WITH ab AS (
    SELECT pid, state, wait_event_type, wait_event,
           left(query, 500) AS query, usename::text AS usename,
           application_name, xact_start, query_start
      FROM pg_stat_activity
     WHERE pid = %(pid)s
       AND application_name = %(token)s
       AND datname = current_database()
),
blockers AS (
    SELECT a.pid, a.state, a.wait_event_type, a.wait_event,
           left(a.query, 500) AS query, a.usename::text AS usename,
           a.application_name, a.xact_start, a.query_start
      FROM pg_stat_activity a
     WHERE a.datname = current_database()
       AND a.pid IN (SELECT unnest(pg_blocking_pids((SELECT pid FROM ab))))
),
blocked AS (
    SELECT a.pid, a.state, a.wait_event_type, a.wait_event,
           left(a.query, 500) AS query, lower(left(a.query, 500)) AS query_lower,
           a.usename::text AS usename,
           a.application_name, a.xact_start, a.query_start
      FROM pg_stat_activity a
     WHERE a.datname = current_database()
       AND a.pid <> (SELECT pid FROM ab)
       AND (SELECT pid FROM ab) = ANY(pg_blocking_pids(a.pid))
)
SELECT
    (SELECT pid FROM ab)                       AS alter_pid,
    (SELECT state FROM ab)                     AS alter_state,
    (SELECT wait_event_type FROM ab)           AS alter_wait_event_type,
    (SELECT wait_event FROM ab)                AS alter_wait_event,
    (SELECT coalesce(jsonb_agg(to_jsonb(b) ORDER BY b.pid), '[]'::jsonb)
       FROM blockers b)                        AS blockers,
    (SELECT coalesce(jsonb_agg(to_jsonb(b) ORDER BY b.pid), '[]'::jsonb)
       FROM blocked b)                         AS blocked
"""


def get_sampler_conn() -> psycopg.Connection:
    """取出采样连接；断了就重连。调用方必须持有 _sampler_conn_lock。"""
    global _sampler_conn
    if (
        _sampler_conn is None
        or _sampler_conn.closed
        or _sampler_conn.broken
    ):
        if _sampler_conn is not None:
            try:
                _sampler_conn.close()
            except Exception:  # noqa: BLE001
                pass
        _sampler_conn = psycopg.connect(
            DATABASE_URL,
            autocommit=True,
            row_factory=dict_row,
            options=(
                f"-c lock_timeout={SAMPLE_QUERY_TIMEOUT_MS}ms"
                f" -c statement_timeout={SAMPLE_QUERY_TIMEOUT_MS}ms"
            ),
        )
    return _sampler_conn


def close_sampler_conn() -> None:
    global _sampler_conn
    with _sampler_conn_lock:
        if _sampler_conn is not None:
            try:
                _sampler_conn.close()
            except Exception:  # noqa: BLE001
                pass
            _sampler_conn = None


def _json_rows(rows: list[dict]) -> str:
    return json.dumps(rows, ensure_ascii=False, default=str)


def _sample_row_to_model(row: dict) -> WaitSample:
    return WaitSample(
        attempt_no=row["attempt_no"],
        sampled_at=row["sampled_at"].isoformat(),
        alter_pid=row["alter_pid"],
        direct_blocker_pids=list(row["blocked_by"] or []),
        blocked_select_pids=list(row["blocked_pids"] or []),
        is_history=not row["is_current"],
    )


class AttemptSampler:
    """对「一次抢锁尝试」自动采样。

    采样线程用一次性标记（application_name）找到实际执行本次 ALTER 的会话，
    因此连接归还连接池、随后被别的单据借用时，样本仍归属本次尝试。
    一次尝试最多落一行；同一尝试内累积观察到的两类会话。
    """

    def __init__(
        self,
        ticket_no: str,
        column_name: str,
        attempt_no: int,
        token: str,
        alter_pid: int,
    ) -> None:
        self.ticket_no = ticket_no
        self.column_name = column_name
        self.attempt_no = attempt_no
        self.token = token
        self.alter_pid = alter_pid
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, name=f"lock-sampler-{token}", daemon=True
        )
        self._blocked_by: dict[int, dict] = {}
        self._blocked: dict[int, dict] = {}
        self._sampled_at: datetime | None = None
        self._persisted = False

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """尝试结束（成功、超时或异常）时调用：停线程并把样本标为历史。"""
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=0.5)
        with self._lock:
            if self._persisted:
                self._write(is_current=False)

    def _run(self) -> None:
        while not self._stop.is_set():
            # 先立即探测一次（短等锁也可能被采到），再按固定间隔重采样
            snap = self._snapshot()
            if snap is not None:
                with self._lock:
                    if self._stop.is_set():
                        break
                    self._merge(snap)
                    self._write(is_current=True)
            if self._stop.wait(SAMPLE_PROBE_INTERVAL_S):
                break

    def _snapshot(self) -> dict | None:
        """读取一次当前等锁状态；仅当 ALTER 确实在等锁时返回。"""
        try:
            with _sampler_conn_lock:
                conn = get_sampler_conn()
                row = conn.execute(
                    _SAMPLE_SQL, {"pid": self.alter_pid, "token": self.token}
                ).fetchone()
        except Exception as exc:  # noqa: BLE001
            log.debug("等锁采样失败（单号 %s 第 %d 次尝试）：%s",
                      self.ticket_no, self.attempt_no, exc)
            return None
        if row is None or row["alter_pid"] is None:
            return None
        if row["alter_wait_event_type"] != "Lock" or row["alter_state"] != "active":
            return None
        return row

    def _merge(self, snap: dict) -> None:
        self._sampled_at = utcnow()
        for item in snap["blockers"]:
            self._blocked_by[int(item["pid"])] = item
        for item in snap["blocked"]:
            # 只统计排在 ALTER 后面的 SELECT（报表），并发改表的 ALTER 不算
            if item["query_lower"].lstrip().startswith(("select", "with")):
                self._blocked[int(item["pid"])] = {
                    k: v for k, v in item.items() if k != "query_lower"
                }

    def _write(self, is_current: bool) -> None:
        upsert_sql = (
            "INSERT INTO schema_change_wait_samples "
            "(ticket_no, column_name, attempt_no, attempt_token, sampled_at, "
            " alter_pid, blocked_by, blocked_pids, blocker_details, "
            " blocked_details, is_current, updated_at) "
            "VALUES (%(ticket_no)s, %(column_name)s, %(attempt_no)s, "
            "        %(attempt_token)s, %(sampled_at)s, %(alter_pid)s, "
            "        %(blocked_by)s, %(blocked_pids)s, %(blocker_details)s::jsonb, "
            "        %(blocked_details)s::jsonb, %(is_current)s, now()) "
            "ON CONFLICT (attempt_token) DO UPDATE SET "
            "    sampled_at = EXCLUDED.sampled_at, "
            "    alter_pid = EXCLUDED.alter_pid, "
            "    blocked_by = EXCLUDED.blocked_by, "
            "    blocked_pids = EXCLUDED.blocked_pids, "
            "    blocker_details = EXCLUDED.blocker_details, "
            "    blocked_details = EXCLUDED.blocked_details, "
            "    is_current = EXCLUDED.is_current, "
            "    updated_at = now()"
        )
        params = {
            "ticket_no": self.ticket_no,
            "column_name": self.column_name,
            "attempt_no": self.attempt_no,
            "attempt_token": self.token,
            "sampled_at": self._sampled_at or utcnow(),
            "alter_pid": self.alter_pid,
            "blocked_by": list(self._blocked_by),
            "blocked_pids": list(self._blocked),
            "blocker_details": _json_rows(list(self._blocked_by.values())),
            "blocked_details": _json_rows(list(self._blocked.values())),
            "is_current": is_current,
        }
        try:
            with _sampler_conn_lock:
                conn = get_sampler_conn()
                conn.execute(upsert_sql, params)
            self._persisted = True
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "写等锁样本失败（单号 %s 第 %d 次尝试）：%s",
                self.ticket_no, self.attempt_no, exc,
            )


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
    """抢锁重试循环：成功则加列与回执同事务提交；超时则出具未执行回执。

    每次尝试都会自动采样：采样线程凭本次尝试的一次性标记找到真正执行
    ALTER 的会话，记录「谁挡住 ALTER」与「谁被 ALTER 挡住」。
    """
    ticket_no, column_name = req.ticket_no, req.column_name
    attempts = 0

    while True:
        remaining_ms = (deadline - time.monotonic()) * 1000
        if remaining_ms <= 0:
            break
        # 每次尝试在数据库锁队列中最多等 100ms（且不超过剩余总时限）
        lock_ms = max(1, min(LOCK_QUEUE_TIMEOUT_MS, math.ceil(remaining_ms)))
        attempts += 1
        attempt_token = f"{ATTEMPT_TAG_PREFIX}{uuid.uuid4().hex}"
        sampler: AttemptSampler | None = None
        try:
            with db_conn() as conn:
                # set_config(..., is_local=true)：仅本事务生效，连接归还池后不残留
                conn.execute(
                    "SELECT set_config('lock_timeout', %s, true)", (f"{lock_ms}ms",)
                )
                # 给本次尝试打上一次性标记，采样据此把样本归到实际等锁的这次尝试
                conn.execute(
                    "SELECT set_config('application_name', %s, true)",
                    (attempt_token,),
                )
                alter_pid = conn.execute(
                    "SELECT pg_backend_pid() AS pid"
                ).fetchone()["pid"]
                sampler = AttemptSampler(
                    ticket_no, column_name, attempts, attempt_token, alter_pid
                )
                sampler.start()
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
        finally:
            # 本次尝试结束（成功 / 超时回滚 / 出错）：停采样，最后一份样本标为历史
            if sampler is not None:
                sampler.stop()

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


def fetch_latest_sample(ticket_no: str) -> dict | None:
    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM schema_change_wait_samples "
            "WHERE ticket_no = %s "
            "ORDER BY sampled_at DESC, id DESC LIMIT 1",
            (ticket_no,),
        ).fetchone()


@app.get(
    "/schema-changes/{ticket_no}/wait-sample",
    response_model=WaitSampleReport,
    summary="按变更单号查询最近一份等锁样本",
    tags=["等锁采样"],
)
def get_wait_sample(ticket_no: str) -> WaitSampleReport:
    """返回该单号最近一份等锁样本。

    * `attempt_no`：尝试序号（受理后第几次抢锁）
    * `sampled_at`：采样时间
    * `direct_blocker_pids`：直接挡住这次 ALTER 的会话 PID
    * `blocked_select_pids`：直接被这次 ALTER 挡住的 SELECT 会话 PID
    * `is_history`：该次尝试已结束（重试间隙 / 单号已办结）时最后一份样本为历史
    * `has_sample=false`：该单号从未采到等锁样本（并非默认有样本）

    持续有读事务挡住加列、或报表反复排到 ALTER 后面时，这两个 PID 列表
    就是值班员要追查的会话。`/wait-sample/latest` 还会给出这些会话的明细。
    """
    row = fetch_latest_sample(ticket_no)
    ticket = get_ticket(ticket_no)
    if row is None:
        message = describe_no_sample(ticket_no, ticket)
        return WaitSampleReport(
            ticket_no=ticket_no,
            column_name=ticket["column_name"] if ticket else None,
            status=ticket["status"] if ticket else None,
            has_sample=False,
            message=message,
            latest=None,
        )
    if row["is_current"]:
        message = f"变更单 {ticket_no} 第 {row['attempt_no']} 次尝试正在等锁，样本为当前采样"
    else:
        message = (
            f"变更单 {ticket_no} 最后一次等锁样本（第 {row['attempt_no']} 次尝试）"
            "已结束，标记为历史"
        )
    return WaitSampleReport(
        ticket_no=ticket_no,
        column_name=row["column_name"],
        status=ticket["status"] if ticket else None,
        has_sample=True,
        message=message,
        latest=_sample_row_to_model(row),
    )


def describe_no_sample(ticket_no: str, ticket: dict | None) -> str:
    if ticket is None:
        return f"变更单 {ticket_no} 尚不存在：从未采到等锁样本"
    if ticket["status"] == "processing":
        return (
            f"变更单 {ticket_no} 正在处理中，尚未采到等锁样本"
            "（也可能是一次即拿到锁，从未发生等锁）"
        )
    if ticket["status"] == "success":
        return (
            f"变更单 {ticket_no} 已办结且从未发生过等锁，"
            "因此从未采到等锁样本（加列一次即拿到锁）"
        )
    return (
        f"变更单 {ticket_no} 已办结但从未采到等锁样本"
        "（未观测到可采样的等锁状态）"
    )


@app.get(
    "/schema-changes/{ticket_no}/wait-sample/latest",
    response_model=WaitSampleDetail,
    summary="最近一份等锁样本的会话明细",
    tags=["等锁采样"],
)
def get_wait_sample_detail(ticket_no: str) -> WaitSampleDetail:
    """在最近一份样本基础上展开两类会话明细（状态、等待事件、语句、事务开始时间）。"""
    row = fetch_latest_sample(ticket_no)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=describe_no_sample(ticket_no, get_ticket(ticket_no)),
        )
    base = _sample_row_to_model(row)
    return WaitSampleDetail(
        **base.model_dump(),
        ticket_no=ticket_no,
        column_name=row["column_name"],
        direct_blocker_details=row["blocker_details"] or [],
        blocked_select_details=row["blocked_details"] or [],
    )


@app.get(
    "/schema-changes/{ticket_no}/wait-sample/history",
    response_model=list[WaitSample],
    summary="该单号的全部等锁样本（历史在前，最新在后）",
    tags=["等锁采样"],
)
def list_wait_samples(ticket_no: str, limit: int = 200) -> list[WaitSample]:
    """按尝试列出全部样本，便于看清哪些尝试被同一批读事务/报表反复挡住。"""
    limit = max(1, min(limit, 2000))
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM schema_change_wait_samples "
            "WHERE ticket_no = %s ORDER BY attempt_no, sampled_at LIMIT %s",
            (ticket_no, limit),
        ).fetchall()
    return [_sample_row_to_model(r) for r in rows]


@app.get(
    "/schema-changes/{ticket_no}/wait-sample/live",
    response_model=WaitSampleHoldingLocks,
    summary="按单号现场核对：常挡加列的读事务与反复排队的报表",
    tags=["等锁采样"],
)
def get_wait_sample_live(ticket_no: str) -> WaitSampleHoldingLocks:
    """现场读取最近一份样本里出现过的会话，给出它们此刻是否仍持有锁。

    用于「持续有读事务挡住加列、报表反复排到 ALTER 后面」时确认这些会话
    是否还在：`blocker_details` 是持续挡住 ALTER 的读事务，
    `blocked_select_details` 是反复排队的报表。
    """
    row = fetch_latest_sample(ticket_no)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=describe_no_sample(ticket_no, get_ticket(ticket_no)),
        )
    blocker_pids = list(row["blocked_by"] or [])
    blocked_pids = list(row["blocked_pids"] or [])
    pids = sorted(set(blocker_pids) | set(blocked_pids))
    details: list[dict] = []
    if pids:
        try:
            with _sampler_conn_lock:
                conn = get_sampler_conn()
                details = conn.execute(
                    "SELECT pid, state, wait_event_type, wait_event, "
                    "left(query, 500) AS query, usename::text AS usename, "
                    "application_name, xact_start, query_start, "
                    "coalesce(pg_blocking_pids(pid), '{}') AS blocking_pids "
                    "FROM pg_stat_activity "
                    "WHERE datname = current_database() AND pid = ANY(%s) "
                    "ORDER BY pid",
                    (pids,),
                ).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.warning("现场核对等锁会话失败（单号 %s）：%s", ticket_no, exc)
    details = [dict(d) for d in details]
    by_pid = {d["pid"]: d for d in details}
    return WaitSampleHoldingLocks(
        ticket_no=ticket_no,
        column_name=row["column_name"],
        attempt_no=row["attempt_no"],
        sampled_at=row["sampled_at"].isoformat(),
        blocker_details=[by_pid[p] for p in blocker_pids if p in by_pid],
        blocked_select_details=[by_pid[p] for p in blocked_pids if p in by_pid],
        missing_pids=[p for p in pids if p not in by_pid],
        note=(
            "以上会话来自最近一份等锁样本；missing_pids 表示采样后已断开。"
            "持续挡住加列的读事务通常处于 idle in transaction 并持有表锁，"
            "反复排队的报表则会持续出现在 blocked_select_details 中。"
        ),
    )


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
