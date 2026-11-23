"""等锁采样端到端测试。

需要一个可用的 PostgreSQL，通过 TEST_DATABASE_URL 指定（默认指向本机 appdb）。
测试使用独立的临时表，结束后删除，不会动 orders 样例数据。

    TEST_DATABASE_URL='postgresql://app:apppass@localhost:5432/appdb' \
        python3 -m pytest tests -q
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.rows import dict_row

TEST_DSN = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://app:apppass@localhost:5432/appdb"
)
TEST_TABLE = "wait_sample_test_orders"

# app.main 在导入时读取这些环境变量：必须在导入前设置
os.environ.setdefault("DATABASE_URL", TEST_DSN)
os.environ.setdefault("DB_SCHEMA", "public")
os.environ["TARGET_TABLE"] = TEST_TABLE

from app import main as app_main  # noqa: E402

# 每次运行使用不同单号，避免与历史回执/样本相互影响
RUN = uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def client():
    with TestClient(app_main.app) as tc:
        yield tc


@pytest.fixture(scope="module", autouse=True)
def test_table(client):
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {app_main.QUALIFIED_TABLE} "
            "(id BIGSERIAL PRIMARY KEY, payload TEXT)"
        )
        conn.execute(
            f"INSERT INTO {app_main.QUALIFIED_TABLE} (payload) VALUES ('a'), ('b')"
        )
        conn.execute(app_main.CREATE_WAIT_SAMPLES_SQL)
        conn.execute(app_main.CREATE_WAIT_SAMPLES_INDEX_SQL)
        conn.execute(app_main.CREATE_WAIT_SAMPLES_TOKEN_INDEX_SQL)
    yield
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {app_main.QUALIFIED_TABLE}")


@pytest.fixture()
def long_reader():
    """持有一个长读事务，压住 ALTER 需要的 ACCESS EXCLUSIVE 锁。"""
    conn = psycopg.connect(TEST_DSN, autocommit=False)
    pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
    conn.execute(f"SELECT count(*) FROM {app_main.QUALIFIED_TABLE}")
    try:
        yield conn, pid
    finally:
        try:
            conn.rollback()
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def _ticket(name: str) -> str:
    return f"T-{RUN}-{name}"


def _sample_rows(ticket_no: str) -> list[dict]:
    with psycopg.connect(TEST_DSN, autocommit=True, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM schema_change_wait_samples WHERE ticket_no = %s "
            "ORDER BY attempt_no",
            (ticket_no,),
        ).fetchall()


def test_never_sampled_is_explicit(client):
    body = client.get("/schema-changes/T-NOBODY/wait-sample").json()
    assert body["has_sample"] is False
    assert body["latest"] is None
    assert "从未" in body["message"]


def test_no_wait_means_no_sample(client):
    ticket = _ticket("fast")
    resp = client.post(
        "/schema-changes/add-column",
        json={"ticket_no": ticket, "column_name": "fast_unit_col",
              "wait_timeout_ms": 2000},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"

    body = client.get(f"/schema-changes/{ticket}/wait-sample").json()
    assert body["has_sample"] is False
    assert body["latest"] is None
    assert "从未" in body["message"]


def test_blocker_and_history_recorded(client, long_reader):
    _, reader_pid = long_reader
    ticket = _ticket("block")
    resp = client.post(
        "/schema-changes/add-column",
        json={"ticket_no": ticket, "column_name": "block_unit_col",
              "wait_timeout_ms": 1500},
    )
    assert resp.status_code == 200

    rows = _sample_rows(ticket)
    assert rows, "应至少采到一份等锁样本"
    assert any(reader_pid in (r["blocked_by"] or []) for r in rows)
    # 尝试结束后全部标为历史
    assert all(r["is_current"] is False for r in rows)

    body = client.get(f"/schema-changes/{ticket}/wait-sample").json()
    assert body["has_sample"] is True
    latest = body["latest"]
    assert latest["is_history"] is True
    assert reader_pid in latest["direct_blocker_pids"]
    assert latest["attempt_no"] >= 1
    assert latest["sampled_at"]

    detail = client.get(f"/schema-changes/{ticket}/wait-sample/latest").json()
    assert any(d["pid"] == reader_pid for d in detail["direct_blocker_details"])

    live = client.get(f"/schema-changes/{ticket}/wait-sample/live").json()
    assert any(d["pid"] == reader_pid for d in live["blocker_details"])


def test_queued_selects_are_captured(client, long_reader):
    ticket = _ticket("select")
    collected: dict[str, object] = {}

    def run_call():
        collected["resp"] = client.post(
            "/schema-changes/add-column",
            json={"ticket_no": ticket, "column_name": "sel_unit_col",
                  "wait_timeout_ms": 2000},
        )

    t = threading.Thread(target=run_call)
    t.start()
    time.sleep(0.1)

    pids: list[int] = []
    stop_at = time.monotonic() + 1.6

    def report():
        conn = psycopg.connect(TEST_DSN, autocommit=True)
        try:
            pids.append(conn.execute("SELECT pg_backend_pid()").fetchone()[0])
            while time.monotonic() < stop_at:
                try:
                    conn.execute(f"SELECT count(*) FROM {app_main.QUALIFIED_TABLE}")
                except Exception:  # noqa: BLE001
                    pass
        finally:
            conn.close()

    reporters = [threading.Thread(target=report) for _ in range(2)]
    for r in reporters:
        r.start()
    for r in reporters:
        r.join()
    t.join()

    assert collected["resp"].status_code == 200
    rows = _sample_rows(ticket)
    seen = {pid for row in rows for pid in (row["blocked_pids"] or [])}
    assert set(pids) <= seen, f"报表 SELECT {pids} 应出现在样本中，实际 {seen}"


def test_only_selects_count_as_blocked_reports(client, long_reader):
    """并发改表的 ALTER 也会被挡住，但不属于「报表」，不计入 SELECT 列表。"""
    ticket = _ticket("onlysel")
    collected: dict[str, object] = {}

    def run_call():
        collected["resp"] = client.post(
            "/schema-changes/add-column",
            json={"ticket_no": ticket, "column_name": "onlysel_col",
                  "wait_timeout_ms": 1600},
        )

    t = threading.Thread(target=run_call)
    t.start()
    time.sleep(0.1)

    writer_pids: list[int] = []
    stop_at = time.monotonic() + 1.2

    def writer():
        conn = psycopg.connect(TEST_DSN, autocommit=True)
        try:
            writer_pids.append(conn.execute("SELECT pg_backend_pid()").fetchone()[0])
            while time.monotonic() < stop_at:
                try:
                    conn.execute(
                        f"UPDATE {app_main.QUALIFIED_TABLE} "
                        "SET payload = payload WHERE id = 1"
                    )
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.02)
        finally:
            conn.close()

    w = threading.Thread(target=writer)
    w.start()
    w.join()
    t.join()

    assert collected["resp"].status_code == 200
    rows = _sample_rows(ticket)
    seen = {pid for row in rows for pid in (row["blocked_pids"] or [])}
    assert not (set(writer_pids) & seen)


def test_sample_is_bound_to_real_attempt(client, long_reader):
    """连接被别的单据复用后，样本仍归到实际等锁的那次尝试。"""
    tickets = [(f"{name}", f"reuse_{name}_col")
               for name in ("a", "b", "c")]
    results: dict[str, object] = {}

    def call(name: str, col: str):
        ticket = _ticket(name)
        results[ticket] = client.post(
            "/schema-changes/add-column",
            json={"ticket_no": ticket, "column_name": col,
                  "wait_timeout_ms": 900},
        )

    threads = [
        threading.Thread(target=call, args=(name, col)) for name, col in tickets
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        for name, _ in tickets:
            ticket = _ticket(name)
            assert results[ticket].status_code == 200
            rows = _sample_rows(ticket)
            assert rows
            tokens = {r["attempt_token"] for r in rows}
            for tok in tokens:
                owners = conn.execute(
                    "SELECT DISTINCT ticket_no FROM schema_change_wait_samples "
                    "WHERE attempt_token = %s",
                    (tok,),
                ).fetchall()
                assert [o[0] for o in owners] == [ticket]


def test_sampler_uses_at_most_one_extra_connection(client, long_reader):
    """采样最多额外一条连接：并发多单共用同一个采样连接对象。"""
    tickets = [_ticket(f"budget{i}") for i in range(4)]
    seen_conn_ids: set[int] = set()

    def one(ticket: str, i: int):
        resp = client.post(
            "/schema-changes/add-column",
            json={"ticket_no": ticket, "column_name": f"budget_col_{i}",
                  "wait_timeout_ms": 900},
        )
        assert resp.status_code == 200

    threads = [threading.Thread(target=one, args=(tk, i))
               for i, tk in enumerate(tickets)]
    for t in threads:
        t.start()
    for t in threads:
        # 采样连接在第一次采样时惰性创建，此后全程复用同一个对象
        if app_main._sampler_conn is not None:
            seen_conn_ids.add(id(app_main._sampler_conn))
        time.sleep(0.05)
    for t in threads:
        t.join()
    if app_main._sampler_conn is not None:
        seen_conn_ids.add(id(app_main._sampler_conn))
    # 整个模块只应存在一个采样连接实例
    assert len(seen_conn_ids) <= 1


def test_each_lock_wait_is_short(client, long_reader):
    """持续被挡时，多次短尝试（每次最多 100ms 锁队列）而非一次长等。"""
    ticket = _ticket("attempts")
    # 总时限 1s：若每次都在锁队列里等满总时限，只会尝试 1 次；
    # 实际每次最多 100ms + 40~120ms 停顿，应至少尝试 5 次。
    resp = client.post(
        "/schema-changes/add-column",
        json={"ticket_no": ticket, "column_name": "attempts_col",
              "wait_timeout_ms": 1000},
    )
    assert resp.status_code == 200
    assert resp.json()["attempts"] >= 5


def test_history_lists_every_attempt(client, long_reader):
    ticket = _ticket("hist")
    resp = client.post(
        "/schema-changes/add-column",
        json={"ticket_no": ticket, "column_name": "hist_col",
              "wait_timeout_ms": 900},
    )
    assert resp.status_code == 200
    hist = client.get(f"/schema-changes/{ticket}/wait-sample/history").json()
    rows = _sample_rows(ticket)
    assert len(hist) == len(rows)
    assert [h["attempt_no"] for h in hist] == sorted(h["attempt_no"] for h in hist)
    assert all(h["is_history"] for h in hist)
