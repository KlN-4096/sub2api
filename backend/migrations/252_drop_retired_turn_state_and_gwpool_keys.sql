-- turn-state 猎手 / 自动接管 / 恢复探测 / 降智暂停 / 手填覆写已删除，网关池的 metrics 与 prepare_retries
-- 也已退役：
-- 清掉它们留在 extra 里的配置与运行态。降智暂停借 model_rate_limits 存的条目一并删掉——没有猎手放行，
-- 未到期的条目会按普通模型限流把该模型挡到到期（最长 24 小时）。只动对象值，重放无副作用。
UPDATE accounts
SET extra = extra - ARRAY[
    'openai_turn_state_hunter', 'openai_turn_state_hunt',
    'openai_turn_state_auto', 'openai_turn_state_pool', 'openai_turn_state_pool_size', 'openai_turn_state_fail_threshold',
    'openai_turn_state_recovery', 'openai_turn_state_recovery_state',
    'openai_turn_state_override', 'openai_turn_state_stale_after_minutes',
    'openai_gwpool_metrics', 'openai_gwpool_prepare_retries']
WHERE extra ?| ARRAY[
    'openai_turn_state_hunter', 'openai_turn_state_hunt',
    'openai_turn_state_auto', 'openai_turn_state_pool', 'openai_turn_state_pool_size', 'openai_turn_state_fail_threshold',
    'openai_turn_state_recovery', 'openai_turn_state_recovery_state',
    'openai_turn_state_override', 'openai_turn_state_stale_after_minutes',
    'openai_gwpool_metrics', 'openai_gwpool_prepare_retries'];

UPDATE accounts
SET extra = jsonb_set(extra, '{model_rate_limits}', (
    SELECT coalesce(jsonb_object_agg(e.key, e.value), '{}'::jsonb)
    FROM jsonb_each(extra->'model_rate_limits') AS e
    WHERE e.value->>'reason' IS DISTINCT FROM 'turn_state_hold'))
WHERE jsonb_typeof(extra->'model_rate_limits') = 'object'
  -- WHERE 内各条件的求值顺序没有保证：用不会报错的 lax jsonpath 判命中，jsonb_each 只在 SET 里跑。
  AND jsonb_path_exists(extra, '$.model_rate_limits.* ? (@.reason == "turn_state_hold")');
