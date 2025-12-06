-- 清理旧表
DROP TABLE IF EXISTS accounts, positions, ledger_entries, system_state, idempotency_keys;

-- 1. 账户表
CREATE TABLE accounts (
                          user_id BIGINT,
                          currency VARCHAR(10),
                          balance DECIMAL(36, 18) NOT NULL DEFAULT 0,
                          updated_at BIGINT,
                          PRIMARY KEY (user_id, currency)
);

-- 2. 持仓表
CREATE TABLE positions (
                           user_id BIGINT,
                           symbol VARCHAR(20),
                           size DECIMAL(36, 18) NOT NULL DEFAULT 0,
                           entry_value DECIMAL(36, 18) NOT NULL DEFAULT 0,
                           updated_at BIGINT,
                           PRIMARY KEY (user_id, symbol)
);

-- 3. 流水表 (关键：添加唯一索引以支持幂等重放)
CREATE TABLE ledger_entries (
                                id BIGSERIAL PRIMARY KEY,
                                tx_id VARCHAR(64) NOT NULL,
                                user_id BIGINT NOT NULL,
                                currency VARCHAR(10),
                                amount DECIMAL(36, 18),
                                type VARCHAR(32) NOT NULL, -- FEE, PNL...
                                related_id BIGINT,
                                created_at BIGINT
);
-- 【核心】防止重放时产生重复流水
CREATE UNIQUE INDEX idx_ledger_unique ON ledger_entries(tx_id, type);

-- 4. 系统状态表 (记录 Kafka Offset)
CREATE TABLE system_state (
                              topic_partition VARCHAR(64) PRIMARY KEY, -- 例如 "engine.trades-0"
                              last_offset BIGINT NOT NULL
);