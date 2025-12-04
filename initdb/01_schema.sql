-- 1. 账户表 (快照表)
CREATE TABLE IF NOT EXISTS accounts (
    user_id BIGINT,
    currency VARCHAR(10),
    balance DECIMAL(36, 18) NOT NULL DEFAULT 0,
    updated_at BIGINT, -- 使用时间戳整数，处理更快
    PRIMARY KEY (user_id, currency)
);

-- 2. 持仓表 (快照表)
CREATE TABLE IF NOT EXISTS positions (
    user_id BIGINT,
    symbol VARCHAR(20),
    size DECIMAL(36, 18) NOT NULL DEFAULT 0,
    entry_value DECIMAL(36, 18) NOT NULL DEFAULT 0,
    updated_at BIGINT,
    PRIMARY KEY (user_id, symbol)
);

-- 3. 流水表 (日志表 - Append Only)
-- 生产环境通常按月分表 (ledger_202501, ledger_202502...)
CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    tx_id VARCHAR(64),
    user_id BIGINT,
    currency VARCHAR(10),
    amount DECIMAL(36, 18),
    type VARCHAR(32), -- FEE, PNL, DEPOSIT...
    related_id BIGINT,
    created_at BIGINT
);

-- 4. 幂等表 (可选，内存去重性能更好，但DB留底更安全)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key_id VARCHAR(64) PRIMARY KEY,
    created_at BIGINT
);

-- 索引优化
CREATE INDEX IF NOT EXISTS idx_ledger_user ON ledger_entries(user_id);