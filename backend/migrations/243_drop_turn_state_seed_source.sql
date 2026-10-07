-- 撤回 242 引入的 turn_state_source='seed'：冷启动引子已下线。
--
-- 引子的假设是「带一条健康 292 去换，大概率换回健康 292」。实测把它推翻了：新铸
-- blob 的块数由账号当时的权重决定，与请求里带的那张票无关（带 11 块照样铸 10 块
-- n=11，带 10 块铸 11 块 n=0）。既然换回什么与引子无关，引子就只是白烧一条票。
--
-- 该取值从未在生产写出过（自动接管在任何账号上都没开启过），所以不需要回填数据。
--
-- 该列是无约束 TEXT，本迁移只更新注释，不改结构。

SET LOCAL lock_timeout = '5s';

COMMENT ON COLUMN usage_logs.turn_state_source IS
    'Source of the turn-state override on this request: manual | auto | auto_stale (NULL = none)';
