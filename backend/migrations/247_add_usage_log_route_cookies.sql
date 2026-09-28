-- 使用记录保存 Codex 推理面的路由对读数（openai_codex_route_cookies.go）：
--   1. route_gateway  从 __oailb 的 JWT 载荷 host 里解出的后端网关段，形如 unified-101
--   2. route_pair     这一发实际生效的 "__cflb=...; __oailb=..."，上游新下发就是新的，
--                     否则是罐里当前回放的那一组
--
-- 2026-09-27 直连解包实测：__oailb 的载荷只有 host / iss / aud / exp，**没有账号身份，也没有
-- 任何能力声明**——它钉的是"这一发走哪台后端网关"。落库纯为观测：按账号 × 网关看表现能不能
-- 对上，以及票（x-codex-turn-state）与路由对是不是同进同退。判据一概不建在这两列上。
--
-- 两列都可空：非 Codex 上游、API Key 账号、cpr 原样中继、罐里还没有路由对时保持 NULL。
-- route_pair 存整串是为了能把某一发的路由对原样取出来复现（__oailb 是签名值，按网关名重建不出来）。
-- 不建索引：只做逐条排查与按账号聚合。
--
-- ADD COLUMN 取 ACCESS EXCLUSIVE：与 239/240/246 同型，宁可迁移失败重试也不要把网关卡死。
SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS route_gateway TEXT;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS route_pair TEXT;

COMMENT ON COLUMN usage_logs.route_gateway IS
    'Backend gateway parsed from the __oailb JWT host for this request (NULL = no route cookie / not a Codex upstream)';

COMMENT ON COLUMN usage_logs.route_pair IS
    'Route cookie pair (__cflb / __oailb) in effect for this request (NULL = none)';
