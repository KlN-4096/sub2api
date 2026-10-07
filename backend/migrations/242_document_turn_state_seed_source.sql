-- turn_state_source 新增取值 seed：本次注入的是冷启动引子。
--
-- 引子的由来：候选池只能靠「自然铸出的 292」起步。账号一旦全面降智（所有 session
-- 都落 312），池子永远填不满，自动接管一直空转。管理员手填一条健康 292 当引子
-- （extra.openai_turn_state_seed），系统用它换回一条上游新铸的 292 入池，然后把引子
-- 置空，之后靠自己铸的票续下去。
--
-- 它与 auto 分开记，是为了能回答「这个账号的接管是靠人喂起来的还是自举起来的」。
--
-- 该列是无约束 TEXT，本迁移只更新注释，不改结构。

SET LOCAL lock_timeout = '5s';

COMMENT ON COLUMN usage_logs.turn_state_source IS
    'Source of the turn-state override on this request: manual | auto | auto_stale | seed (NULL = none)';
