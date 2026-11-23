# 在线改表服务（Online DDL）

FastAPI + PostgreSQL 实现的在线加列 API：为样例表 `orders` 添加**可空 text 列**，
专门解决「长读事务持有表锁时，`ALTER TABLE` 排队会堵住后续普通查询」的问题。

## 核心语义

| 需求 | 实现 |
| --- | --- |
| 锁忙重试 | 每次尝试用 `lock_timeout = 100ms` 在数据库锁队列中最多等 **100 毫秒**，超时即回滚重试 |
| 尝试之间普通查询可通过 | 两次尝试之间连接**不持有任何锁**（事务已回滚），并停顿 40~120ms |
| 等锁总时限 | 从**受理时刻**起算，超时仍未拿到锁 → 出具「未执行」回执，该单号**此后不可再加列** |
| 加列与回执一起提交 | `ALTER TABLE` 与成功回执的写入在**同一事务**中提交（PG 事务性 DDL） |
| 重启后同单同列返回原回执 | 回执持久化在 `schema_change_receipts` 表，重启后原样返回（同一回执编号/时间戳） |
| 已受理单号换列拒收 | 同单号不同列名 → `409 Conflict` |

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger UI）：http://localhost:8000/docs
- 在 `/docs` 中展开 `POST /schema-changes/add-column`，填三个字段即可试调用：
  - `ticket_no`：变更单号（幂等键）
  - `column_name`：新列名（小写字母/数字/下划线）
  - `wait_timeout_ms`：等锁总时限（毫秒）

也可以用 curl：

```bash
curl -s -X POST http://localhost:8000/schema-changes/add-column \
  -H 'Content-Type: application/json' \
  -d '{"ticket_no":"CHG-20261123-001","column_name":"deploy_note","wait_timeout_ms":5000}'
```

成功回执示例：

```json
{
  "receipt_no": "9f2c…",
  "ticket_no": "CHG-20261123-001",
  "table_name": "public.orders",
  "column_name": "deploy_note",
  "column_type": "text",
  "nullable": true,
  "status": "success",
  "message": "已执行：可空 text 列已添加，加列与回执在同一事务中提交",
  "attempts": 1,
  "accepted_at": "2026-11-23T08:00:00.001234+00:00",
  "finished_at": "2026-11-23T08:00:00.010123+00:00"
}
```

## 验证「锁忙重试 / 超时不执行」

1. 开一个长读事务，持有 `orders` 的 ACCESS SHARE 锁（模拟长读事务查部署记录）：

   ```bash
   docker compose exec db psql -U app -d appdb
   ```

   ```sql
   BEGIN;
   SELECT count(*) FROM orders;   -- 事务不提交，锁一直持有
   ```

2. 另开终端调用 API（总时限 3 秒）：

   ```bash
   curl -s -X POST http://localhost:8000/schema-changes/add-column \
     -H 'Content-Type: application/json' \
     -d '{"ticket_no":"CHG-002","column_name":"remark","wait_timeout_ms":3000}'
   ```

   约 3 秒后返回 `"status": "not_executed"` 的回执；期间每次抢锁最多在锁队列里
   停 100ms，其余时间普通 `SELECT` 照常通过（可在第三个终端反复查询验证）。

3. 在 psql 里 `ROLLBACK;` 释放锁。此时列**并未**加上（未执行），且单号 `CHG-002`
   已作废：同单号同列名只会返回原来的「未执行」回执，换列名则 `409` 拒收。

4. 换个新单号再调用即可成功加列。

## 验证幂等与重启

```bash
# 同一单号同一列名：返回与首次完全相同的回执（同一 receipt_no）
curl -s -X POST http://localhost:8000/schema-changes/add-column \
  -H 'Content-Type: application/json' \
  -d '{"ticket_no":"CHG-20261123-001","column_name":"deploy_note","wait_timeout_ms":5000}'

# 重启服务后再次调用，仍返回原回执
docker compose restart app

# 已受理的单号换列名 → 409
curl -s -X POST http://localhost:8000/schema-changes/add-column \
  -H 'Content-Type: application/json' \
  -d '{"ticket_no":"CHG-20261123-001","column_name":"other_col","wait_timeout_ms":5000}'
```

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/schema-changes/add-column` | 受理加列变更单（字段见上） |
| `GET` | `/receipts/{ticket_no}` | 按单号查询回执 |
| `GET` | `/tables/columns` | 查看目标表现有列 |
| `GET` | `/health` | 健康检查 |

## 设计要点

- **每次最多在锁队列等 100ms**：`SET LOCAL lock_timeout = '100ms'`（仅本事务生效，
  连接归还连接池后不残留）。PostgreSQL 的锁队列是排他的——排队的 `ALTER TABLE`
  会堵住后来的 `SELECT`，所以必须把单次排队时间压短。
- **两次尝试之间不持锁**：超时后事务回滚、停顿 40~120ms 再试，普通查询得以通过。
- **原子性**：`ALTER TABLE` 与回执 `UPDATE` 同事务提交；进程崩溃时事务整体回滚，
  不会出现「列加了但回执丢了」或反之。
- **幂等**：`ticket_no` 是回执表主键。并发同单号只有一方能 `INSERT … ON CONFLICT
  DO NOTHING` 抢占成功，另一方等待并返回同一回执。
- **重启恢复**：启动时清理残留的 `processing` 占位记录（其事务已随连接断开回滚，
  列并未加上，允许重新受理）；已办结的回执原样保留。
- 单号一旦出具「未执行」回执即作废，防止「超时后又被重放加列」造成账实不符。

## 目录结构

```
├── app/main.py            # FastAPI 应用（全部业务逻辑）
├── db/init/01_init.sql    # 样例表 orders 及示例数据（首次启动自动执行）
├── Dockerfile
├── docker-compose.yml     # app + postgres:16 一键启动
└── requirements.txt
```
