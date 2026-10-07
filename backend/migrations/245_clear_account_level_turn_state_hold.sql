-- 降智暂停从账号级（temp_unschedulable，klno.13）改为模型级（model_rate_limits，klno.14）：
-- 旧格式的停调度已经没有代码会放回，升级时一次性清掉，否则要等 24 小时到期或人工恢复。
UPDATE accounts
SET temp_unschedulable_until = NULL,
    temp_unschedulable_reason = NULL,
    updated_at = NOW()
WHERE temp_unschedulable_reason LIKE 'turn_state_hold:%'
  AND deleted_at IS NULL;
