-- 区分本次 turn-state 覆写的来源：手填 / 自动接管。
--
-- 取值：
--   NULL        没带覆写
--   manual      账号 extra.openai_turn_state_override（手填）
--   auto        自动接管注入的候选
--   auto_stale  自动接管注入，且该候选铸造已超过保鲜期（默认 60 分钟）
--
-- auto 与 auto_stale 刻意分开：候选过了保鲜期仍然照用不删（292 太稀缺），
-- 这两个值的后续铸造结果分布，就是「1 小时到底会不会过期」的直接答案。
-- 确认会过期之后，再让保鲜期真正淘汰候选。

SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS turn_state_source TEXT;

COMMENT ON COLUMN usage_logs.turn_state_source IS
    'Source of the turn-state override on this request: manual | auto | auto_stale (NULL = none)';

-- turn_state_overridden 的语义在本次改动里收紧了：239 上线时是「该账号当时配了手填
-- 覆写」（按账号类型推导，不看本次请求），现在是「本次请求真的注入了覆写值」。
-- 239 之后、240 之前写入的行按旧语义理解，之后的按新语义；两段无法 backfill 区分，
-- 做统计时用 turn_state_source IS NOT NULL 更可靠。
COMMENT ON COLUMN usage_logs.turn_state_overridden IS
    'Whether this request actually injected a turn-state override (rows written before migration 240 mean "the account had one configured")';
