-- 样例表：部署记录/订单表，用于演示在线加列
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    order_no   TEXT NOT NULL UNIQUE,
    customer   TEXT NOT NULL,
    amount     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO orders (order_no, customer, amount) VALUES
    ('SO-2026-0001', 'Alice', 199.00),
    ('SO-2026-0002', 'Bob',    58.50),
    ('SO-2026-0003', 'Carol', 1024.75)
ON CONFLICT (order_no) DO NOTHING;
