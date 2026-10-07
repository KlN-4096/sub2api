-- state-echo 降智判据的丢弃行记账：usage_logs.request_type 新增 7（gwpool_degraded）。
--
-- 这种行是「发出去了、响应头到手、被判成降智后整发丢掉」的那次上游尝试
-- （backend/internal/service/openai_gwpool_state_echo.go）。它真的到了上游、上游真的跑了模型，
-- 所以必须留一条可审计的记录；但判定点在响应体一个字节都没读的时刻，而输入与输出 token **都**
-- 来自上游 response.completed 事件里的 usage ⇒ 两边都观测不到，这种行恒为 0 token / 0 金额。
-- 刻意不做本地估算：编出来的数字进了计费表，事后没人分得清哪条是真的。
--
-- 与 244 同型，只放宽 CHECK 上界。
ALTER TABLE usage_logs
    DROP CONSTRAINT IF EXISTS usage_logs_request_type_check;

ALTER TABLE usage_logs
    ADD CONSTRAINT usage_logs_request_type_check
    CHECK (request_type >= 0 AND request_type <= 7);
