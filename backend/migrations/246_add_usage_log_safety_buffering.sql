-- 使用记录保存 Codex 上游响应头里的 safety-buffering 读数：
--   1. safety_buffering_enabled       x-codex-safety-buffering-enabled（true / false）
--   2. safety_buffering_faster_model  x-codex-safety-buffering-faster-model：客户端
--      "Retry with a faster model" 选项会换到的模型（SSE safety_buffering 事件缺 retry_model
--      时的兜底值），不是实际服务模型，也不证明请求被换过模型。
--
-- 2026-09-23 起上游的 openai-model / response.created.model 不再反映实际服务模型，
-- 这对头（codex-api/src/safety_buffering.rs）是仅剩的被动读数：真实客户端直连时每条
-- /responses 都能看到，网关此前既不落库也不放行给下游。健康账号同样带这对头
-- （gpt-6-sol → gpt-6-luna），落库是为了按账号 × 请求模型对比 faster_model 的取值
-- （2026-09-25 直连实测：降智账号请求 gpt-6-astra 得到 gpt-5.6-luna）。
--
-- 两列都可空：非 Codex 上游、上游没带该头、OAuth WS 轮次（暂不取事件里的头）保持 NULL。
-- 不建索引：只做逐条排查与按账号聚合。
--
-- ADD COLUMN 取 ACCESS EXCLUSIVE：与 239/240 同型，宁可迁移失败重试也不要把网关卡死。
SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS safety_buffering_enabled BOOLEAN;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS safety_buffering_faster_model TEXT;

COMMENT ON COLUMN usage_logs.safety_buffering_enabled IS
    'Upstream x-codex-safety-buffering-enabled for this request (NULL = header absent / not a Codex upstream)';

COMMENT ON COLUMN usage_logs.safety_buffering_faster_model IS
    'Upstream x-codex-safety-buffering-faster-model for this request (NULL = header absent)';
