-- 使用记录保存 Codex 回合状态：
--   1. turn_state：上游本次响应头里新铸的 x-codex-turn-state（不透明 Fernet 密文）
--   2. turn_state_overridden：该账号当时是否配置了 turn-state 覆写
--      （extra.openai_turn_state_override 非空且该账号类型落到 ChatGPT Codex 后端）
--      注意是「配置了」而非「本次出站确实生效」：判定只看账号类型，不看本次打的是
--      哪个端点，所以 OAuth/CPR 账号走 /embeddings、/chat/completions 等端点时同样
--      记 true（头确实被强塞了，只是在那些端点没有意义）。
--
-- 两列都可空：非 Codex 上游、WS 模式拿不到上游响应头时保持 NULL。
-- 不建索引：只做逐条排查用，不参与筛选或聚合。

-- ADD COLUMN 取 ACCESS EXCLUSIVE：热表上可能排在长 SELECT 后面并阻塞后续全部访问。
-- 与 033/038/079/080/081 同型，宁可迁移失败重试也不要把网关卡死。
SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS turn_state TEXT;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS turn_state_overridden BOOLEAN;

COMMENT ON COLUMN usage_logs.turn_state IS
    'Codex x-codex-turn-state minted by upstream for this request (opaque Fernet blob)';

COMMENT ON COLUMN usage_logs.turn_state_overridden IS
    'Whether the account had a turn-state override configured at request time (by account type, not by endpoint)';
