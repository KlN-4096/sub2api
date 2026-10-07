-- 使用记录标记「这一发的路由对是网关池下发的」（openai_gwpool.go）。
--
-- 247 落了 route_gateway / route_pair 两列，但它们对「这组 cookie 从哪来」是哑的：罐回放
-- （上游上次下发的那组）和网关池接管（顶掉罐、改派落点）记出来一模一样。排查「这一发到底有没有
-- 被覆写」只能去翻日志，而 pair 的供给是个位数张/小时，这正是最需要看的那一列。
--
-- 第二列 route_pair_pool_gateway 是**池子交付时说的那个网关**，用来和实际落点
-- （route_gateway，247）比对：
--   一致   ⇒ 注入的 pair 生效了
--   不一致 ⇒ 上游下发了新的 __oailb，把这一发改派走了 = 注入被拒
-- 判据只比**落点**，不看「上游有没有下发 Set-Cookie」：没下发（两件齐发时的常态）与下发了但落点
-- 相同（续期那一发只送 __cflb，上游必然补发一套，见 openai_gwpool.go 的 gatewayPoolRenew）都是
-- 注入生效。只有 route_pair_overridden
-- 一列的话，「注入 142 实际落 126」这种被拒在页面上看不出来——而那正是这个功能唯一要回答的问题。
--
-- 第三列 route_pair_pool_version 是池子给**这一张具体的票**的身份（cookie_version），只为和池子
-- 侧的交付/验证日志对上账：页面说「被改派」时，拿它去池子那边看这张票的交付时刻与验证结论。
-- 它不是 cookie 本体（本体仍只在 route_pair 列，页面也只在复制按钮里给）。
--
-- 四列都可空：非 Codex 上游、API Key 账号保持 NULL。**cpr 原样中继落 FALSE 不是 NULL**：
-- 判据用的是 TargetsChatGPTCodexUpstream()（= IsOpenAIOAuthLike() || IsCPR()，见 account.go），
-- cpr 的上游也是 chatgpt.com，只是我们不给它注入 ⇒ FALSE 才是对的读数。接管着但这一发没覆写（没有活
-- pair、或走的是非推理面端点）落 FALSE + NULL 网关，与 NULL 区分开。
-- 不建索引：与 247 同口径，只做逐条排查与按账号聚合。
--
-- ADD COLUMN 取 ACCESS EXCLUSIVE：与 239/240/246/247 同型，宁可迁移失败重试也不要把网关卡死。
SET LOCAL lock_timeout = '5s';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS route_pair_overridden BOOLEAN;

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS route_pair_pool_gateway TEXT;

COMMENT ON COLUMN usage_logs.route_pair_overridden IS
    'TRUE when the outbound route cookie pair came from the gateway pool instead of the account cookie jar (NULL = not a Codex upstream)';

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS route_pair_pool_version TEXT;

COMMENT ON COLUMN usage_logs.route_pair_pool_version IS
    'Opaque pool-side identity (cookie_version) of the delivered pair, for reconciling this request against the pool own delivery log (NULL = pair did not come from the pool)';

COMMENT ON COLUMN usage_logs.route_pair_pool_gateway IS
    'Gateway the pool promised when it handed out the pair; differs from route_gateway when the upstream re-dispatched the request (NULL = pair did not come from the pool)';
