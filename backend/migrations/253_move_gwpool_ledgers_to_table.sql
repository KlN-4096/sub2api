-- 网关池运行态大账本迁出 accounts.extra：history/contacts/usage/outbox 按（账号, 原 extra 键名）一行存，
-- 每次读写只碰需要的那本账。peer 发现键（*_tag）、rest_state、usage_blocked_at 仍在 extra。
-- 非对象值（坏数据）不搬，读者本来就当空账本。extra 里还有这些键只可能是回滚后旧版本新写的，
-- 以 extra 为准覆盖；重放无副作用。软删行一并搬，读路径都按 deleted_at 过滤。
CREATE TABLE IF NOT EXISTS gateway_pool_ledgers (
    account_id BIGINT      NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind       TEXT        NOT NULL,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, kind)
);

INSERT INTO gateway_pool_ledgers (account_id, kind, value)
SELECT a.id, k.kind, a.extra -> k.kind
FROM accounts a
CROSS JOIN (VALUES ('openai_gwpool_gateways'), ('openai_gwpool_contacts'),
                   ('openai_gwpool_usage_rounds'), ('openai_gwpool_feedback_outbox')) AS k(kind)
WHERE jsonb_typeof(a.extra -> k.kind) = 'object'
ON CONFLICT (account_id, kind) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();

UPDATE accounts
SET extra = extra - ARRAY['openai_gwpool_gateways', 'openai_gwpool_contacts', 'openai_gwpool_usage_rounds', 'openai_gwpool_feedback_outbox']
WHERE extra ?| ARRAY['openai_gwpool_gateways', 'openai_gwpool_contacts', 'openai_gwpool_usage_rounds', 'openai_gwpool_feedback_outbox'];
