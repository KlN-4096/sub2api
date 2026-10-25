-- 使用记录 TPS 只算答案：(output_tokens − reasoning_tokens) ÷ (duration_ms − first_answer_ms)。
--
--   reasoning_tokens  上游 usage 的 output_tokens_details.reasoning_tokens（Chat 口径为
--                     completion_tokens_details.reasoning_tokens；OpenAI 口径已含在 output_tokens 里）；
--                     上游没报（缺失或 null）时为 NULL
--   first_answer_ms   首个答案 delta（推理与推理摘要以外的非空 delta：正文、拒答、工具参数等）到达的毫秒数；
--                     只有 HTTP 流式路径记录，安全缓冲回合与客户端工具映射的请求不记
--
-- 推理正文通常不下发（下发的推理 delta 与摘要也不算答案），首个答案之前可能已生成大量推理 token：只扣首字会把它们算进分子（虚高），
-- 按总耗时又把排队、预填充与推理时间算进分母（偏低）。两列都有才按答案算，否则前端退回总耗时口径。

SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS reasoning_tokens INTEGER,
    ADD COLUMN IF NOT EXISTS first_answer_ms INTEGER;

COMMENT ON COLUMN usage_logs.reasoning_tokens IS
    'Reasoning tokens from usage output_tokens_details / completion_tokens_details (included in output_tokens for OpenAI); NULL when not reported';
COMMENT ON COLUMN usage_logs.first_answer_ms IS
    'Milliseconds until the first non-reasoning delta (output text, refusal, tool arguments); HTTP streaming only, NULL for safety-buffered or client-tool-mapped requests';
