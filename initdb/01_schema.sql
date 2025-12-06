DROP TABLE IF EXISTS accounts, positions, ledger_entries, system_state;

-- 1. 账户表 (资金池)
CREATE TABLE accounts (
                          user_id BIGINT NOT NULL,
                          currency VARCHAR(16) NOT NULL,
                          balance DECIMAL(36, 18) NOT NULL DEFAULT 0,
                          updated_at BIGINT,
                          PRIMARY KEY (user_id, currency)
);

-- 2. 持仓表 (增加逐仓字段)
CREATE TABLE positions (
                           user_id BIGINT NOT NULL,
                           symbol VARCHAR(32) NOT NULL,
                           size DECIMAL(36, 18) NOT NULL DEFAULT 0,
                           entry_value DECIMAL(36, 18) NOT NULL DEFAULT 0,
    -- 新增字段
                           margin_mode INT NOT NULL DEFAULT 0, -- 0: Cross(全仓), 1: Isolated(逐仓)
                           isolated_margin DECIMAL(36, 18) NOT NULL DEFAULT 0, -- 仅逐仓有效

                           updated_at BIGINT,
                           PRIMARY KEY (user_id, symbol)
);

-- 3. 流水表
CREATE TABLE ledger_entries (
                                id BIGSERIAL PRIMARY KEY,
                                tx_id VARCHAR(64) NOT NULL,
                                user_id BIGINT NOT NULL,
                                currency VARCHAR(16) NOT NULL,
                                amount DECIMAL(36, 18) NOT NULL,
                                type VARCHAR(32) NOT NULL,
                                related_id BIGINT,
                                created_at BIGINT
);
CREATE UNIQUE INDEX idx_ledger_dedup ON ledger_entries(tx_id, type, user_id);

-- 4. 系统状态
CREATE TABLE system_state (
                              topic_partition VARCHAR(64) PRIMARY KEY,
                              last_offset BIGINT NOT NULL
);

INSERT INTO accounts (user_id, currency, balance) VALUES (0, 'USDT', 0) ON CONFLICT DO NOTHING;