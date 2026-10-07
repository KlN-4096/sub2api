-- 本次出站实际带的 x-codex-turn-state。
--
-- 与 turn_state 不是一回事：
--   turn_state       上游本次响应头里**新铸**的 blob
--   turn_state_sent  我们**发出去**的 blob（客户端回带的，或覆写/自动接管注入的）
--
-- 为什么必须分开记：实测 4262 条 /responses，请求带了 turn-state 时只有 8.0% 会拿到
-- 新铸 blob。自动接管一开，注入请求里九成以上 turn_state 是 NULL——正好在最想看
-- 「到底发了什么」的时候什么都看不到。

SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS turn_state_sent TEXT;

COMMENT ON COLUMN usage_logs.turn_state_sent IS
    'The x-codex-turn-state actually sent upstream on this request (echoed by the client or injected); turn_state is the one the upstream newly minted';
