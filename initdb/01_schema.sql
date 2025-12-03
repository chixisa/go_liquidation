-- ========================================================
-- 清算系统数据库初始化脚本 (PostgreSQL)
-- 适用业务：永续合约 (Perpetual Futures) & 现货 (Spot)
-- ========================================================

-- 1. 幂等性控制表 (Idempotency Keys)
-- 用于防止重复消费 Kafka 消息，key_id 通常是 "MATCH-{MatchID}"
CREATE TABLE IF NOT EXISTS idempotency_keys
(
    key_id
    VARCHAR
(
    64
) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

-- 2. 资金账户表 (Accounts)
-- 存储用户的资金余额 (如 USDT, USDC, BTC)
-- 对应 Go 代码中的 ON CONFLICT (user_id, currency)
CREATE TABLE IF NOT EXISTS accounts
(
    user_id
    BIGINT
    NOT
    NULL,
    currency
    VARCHAR
(
    10
) NOT NULL, -- e.g., 'USDT'
    balance NUMERIC
(
    32,
    18
) NOT NULL DEFAULT 0, -- 高精度余额
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- 联合主键，确保每个用户每种币种只有一条记录
    PRIMARY KEY
(
    user_id,
    currency
)
    );

-- 3. 持仓表 (Positions)
-- 存储用户的合约持仓 (永续合约核心)
-- 对应 Go 代码中的 ON CONFLICT (user_id, symbol)
CREATE TABLE IF NOT EXISTS positions
(
    user_id
    BIGINT
    NOT
    NULL,
    symbol
    VARCHAR
(
    20
) NOT NULL, -- e.g., 'BTC-USDT'
    size NUMERIC
(
    32,
    18
) NOT NULL DEFAULT 0, -- 持仓数量：正数=多头，负数=空头
    entry_value NUMERIC
(
    32,
    18
) NOT NULL DEFAULT 0, -- 开仓名义价值 (用于计算平均持仓价和 PnL)
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- 联合主键，确保每个用户每个交易对只有一条持仓记录
    PRIMARY KEY
(
    user_id,
    symbol
)
    );

-- 4. 资金流水表 (Ledger Entries)
-- 不可变日志，记录每一笔资金变动的历史，用于对账和审计
CREATE TABLE IF NOT EXISTS ledger_entries
(
    id
    BIGSERIAL
    PRIMARY
    KEY, -- 自增流水号
    tx_id
    VARCHAR
(
    64
) NOT NULL, -- 关联的幂等键 (Transaction ID)
    user_id BIGINT NOT NULL,
    currency VARCHAR
(
    10
) NOT NULL,
    amount NUMERIC
(
    32,
    18
) NOT NULL, -- 变动金额 (+/-)
    type VARCHAR
(
    32
) NOT NULL, -- 类型: TRADE_SELL, TRADE_BUY, FEE_PAY, FEE_INCOME
    related_id BIGINT NOT NULL, -- 关联业务ID (如 MatchID)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

-- ========================================================
-- 索引优化 (Indexes for Performance)
-- ========================================================

-- 加速查询某个用户的流水 (对账用)
CREATE INDEX IF NOT EXISTS idx_ledger_user_currency ON ledger_entries(user_id, currency);

-- 加速查询某笔业务的流水 (审计用)
CREATE INDEX IF NOT EXISTS idx_ledger_tx_id ON ledger_entries(tx_id);

-- 加速查询某笔 Match 的所有相关流水
CREATE INDEX IF NOT EXISTS idx_ledger_related_id ON ledger_entries(related_id);

-- ========================================================
-- 注释 (Comments for Maintenance)
-- ========================================================
COMMENT
ON TABLE idempotency_keys IS '防止重复处理交易的幂等键存储';
COMMENT
ON TABLE accounts IS '用户资金账户余额快照';
COMMENT
ON TABLE positions IS '用户永续合约持仓快照';
COMMENT
ON TABLE ledger_entries IS '资金变动不可变流水日志 (复式记账源数据)';