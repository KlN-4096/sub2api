-- 回滚迁移 253_move_gwpool_ledgers_to_table.sql：把 gateway_pool_ledgers 里的网关池运行态账本
-- 写回 accounts.extra 的原键，删表，并删掉 253 的迁移记录（之后再升级，253 会重新搬一次）。
--
-- 只在换回 253 之前的版本时用，顺序：
--   1. 停掉所有应用实例；
--   2. 确认有可用的数据库备份或恢复点；
--   3. psql -v ON_ERROR_STOP=1 -f gwpool-ledger-rollback.sql
--   4. 启动旧版本。
-- 若旧版本已先跑过、在 extra 写了同名键，以表里的账本为准（旧版本那段时间新写的会被覆盖）；
-- 这种情况下再升级前也先跑本脚本，否则 253 不会重跑，旧版写进 extra 的残留会一直留着。
-- 每行只聚合自己那至多 4 本账，不在内存里攒整张表。

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

UPDATE accounts a
SET extra = COALESCE(a.extra, '{}'::jsonb) || (
    SELECT jsonb_object_agg(l.kind, l.value) FROM gateway_pool_ledgers l WHERE l.account_id = a.id)
WHERE EXISTS (SELECT 1 FROM gateway_pool_ledgers l WHERE l.account_id = a.id);

DROP TABLE gateway_pool_ledgers;

DELETE FROM schema_migrations WHERE filename = '253_move_gwpool_ledgers_to_table.sql';

COMMIT;
