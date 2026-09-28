# Basis Points 直通（bps.openai.com）

2026-09-25 做完后因风险过大搁置，2026-09-29 用户重启（`gpt-6-astra` 被降智改派到 luna，工程侧其它手段全灭）。
账号级独立开关，默认关；这条**路由**不开就不走。只对 **OpenAI oauth 账号**、`extra.openai_basispoints: true`（编辑弹窗「Basis Points 直通（实验）」，不勾就删键）。cpr / setup-token / Agent Identity 账号不生效（`Account.UsesOpenAIBasisPoints`）。

> 原来这里写的是「不开就一个字节都不改」，**那句是假的**：同一分支还把 HTTP `/responses` 上的 `OpenAI-Beta` 从「只剥 `responses=experimental`」收紧成整头 `Del`（`openai_gateway_forward.go` / `openai_gateway_passthrough.go`，条件是 `UsesOpenAICodexProtocol()`，**与本开关无关**），因为真 Codex 客户端在 HTTP 上确实不发这个头。净效果对 images / messages 两条路逐字相同（它们各自 `Set` 回需要的值），变的是「客户端送了非 legacy 值时不再跟着出站」。

## 走向
- HTTP `/v1/responses`（`openai_gateway_forward.go` 四个清理调用之后、`prepareCodexAccountIdentitySource` 之前分派；`/v1/responses/<子路径>` 一律不走）。WS 客户端在 `openai_ws_forwarder_ingress.go` 被强制走 HTTP 桥，`proxyOpenAIWSHTTPBridgeTurn` 每一轮在透传之前先 `beginOpenAIBasisPoints`。`/v1/messages`、chat-completions 桥、count_tokens、/models 不受影响。**legacy compact（`/v1/responses/compact`）在开着开关的账号上判死**，见下文。
- 上游 `https://bps.openai.com/basispoints/api/responses`，同一份 ChatGPT access token + `chatgpt-account-id`（JWT 里有 `chatgpt_account_user_id` 就再带 `X-OpenAI-Account-User-ID`）；头是 Excel 加载项形态（`openAIBasisPointsClientProfile`），**不发** Codex 头、不走插件传输、不压缩，直接 `s.httpUpstream.Do` 经账号代理；绑了代理但取不到 URL 按 `requireOpenAIProxyBinding` 直接失败（fail-closed，不落回、不直连）。上游请求用 `detachUpstreamContext`：客户端断开也读完，usage 才记得上。
- 用量记录：`Model` 是客户端的名字、`BillingModel` / `UpstreamModel` 走同一套模型映射，`UpstreamEndpoint=/basispoints/api/responses`，`ReasoningEffort` 客户端没给就是 nil（与原路径同口径），给了记 BPS 归一后的档位。

## 请求改写（`openai_basispoints_protocol.go`）
- **BPS 的请求 schema 是封闭的**，2026-09-29 直连 `bps.openai.com` 逐字段实测出的拒绝模型（比「封闭」精确，以后照这个读错误）：
  - **422 `"Invalid request body."`（不带 param）** = 这个键是 Responses API 的合法参数，但**不在 BPS 的白名单里**。实测命中：`include`、`max_output_tokens`、`service_tier`。
  - **400（带精确 `param` 和 `code`）** = 正常校验失败。实测命中：`Unknown parameter: 'input[1].zzz_unknown_field'`（**item 级也封闭**）、`Invalid 'context_management': empty array`（`empty_array`）。
  - 实测 200 的：完全不发 `context_management`、`function_call_output` 上带 `name` / `namespace`（这两个是该 item 的合法字段，不必删）、`reasoning_effort=low` + astra、input 里两条 developer 消息。

  所以出站体是**严格白名单**，加字段前必须先直连实测：model、model_selection=explicit、stream=true、store=false、reasoning_effort、context_management、prompt_cache_key、metadata{task_id, turn_id(uuid v5), agent_iteration}、input。
- tools / text / instructions / include / max_output_tokens 一律不发。`instructions` 会被 BPS 换成它自己那套 Excel 人格，只能降级成首条 developer 消息；客户端一条都没给就补 `defaultCodexSynthInstructions`，与原路径对齐 —— **但 `input.0.type == "additional_tools"`（真实 Codex Lite 的签名）时不补**：它的基础提示已经在 input 的 developer 消息里，原路径为此刻意只记进 `liteFallbackInstructions` 而不写进体，补一份就是两份系统提示叠加。
- 封闭 schema 换来的两处已知代价（上游不给口子，本层修不了）：
  - 发不了 `include=["reasoning.encrypted_content"]` ⇒ `store=false` 下拿不回推理密文，`translateHistory` 里回放 `encrypted_content` 的分支在这条路上恒不命中，多轮工具会话每轮从零推理。
  - 发不了 `max_output_tokens` ⇒ BPS 侧按它自己的上限生成，客户端设的输出上限不生效，也等不到 `incomplete(max_output_tokens)`。
- **BPS 解不开别处铸的推理密文：400 `invalid_encrypted_content`。** `translateHistory` 会原样回放客户端历史里带 `encrypted_content` 的 reasoning 项（跨过开关、或开关打开前这个会话跑过 Codex 路径，就会带上一批 BPS 铸不出来的 blob），而客户端**每一轮都原样回放同一批**，所以硬报错口径下不记账就等于「这个会话从此每轮都失败」。收口方式：命中这个错误码就把摘要记进**本层自己的 `bps:` 会话 lineage**，下一轮进 `beginOpenAIBasisPoints` 时既有的剥离段把它们摘掉 —— **一次失败之后自愈**。
  - **判据全部收在 `openAIBasisPointsRejectedInvalidEncryptedContent` 里，取码是两个读法的并集，它们交叉、谁都不包含谁，少一个就是对应那半边零覆盖。** ①`openAIBasisPointsErrorDetail` 读 `error.code` / `response.error.code` / `response.status_details.error.code` / 裸 `code`（**peek 抓到的 `response.failed` 帧把码放在 `response.error.code`，只有它读得到**）；②单独读一次 `error.message` 里的 JSON 信封（网关包一层时码在那儿）。**这两条都不能用 `extractUpstreamErrorCode` 代替**：它在 `error.code` 非空时就早返回、压根不解信封，而 `{"error":{"code":"invalid_request_error","message":"{…invalid_encrypted_content…}"}}` 恰好是这个形状；反过来它比 ② 多的那点（截到最后一个 `}` 再解一次）是**死重** —— 实测 gjson 本来就穿过尾部垃圾（`{"error":{"code":"X"}} trailing {garbage` 直接取到 X），那次调用已删。**「只用 `extractUpstreamErrorCode` 替换掉原判据」这个改法被实测证伪过**：`response.failed` 那半直接变成零覆盖，而且当时的用例因为消息里带了关键词、被消息兜底替 code 路径兜住，照旧全绿 —— 现在那条用例的消息刻意写成中性文案。三个分支各有用例（`TestOpenAIBasisPointsRejectedInvalidEncryptedContentReadings`）。不记就不打日志，现场没有任何读数看得出来。
  - **收摘要时跳过 `compaction` 项**（`openAIBasisPointsSuspectEncryptedDigests`）。这条路上两种密文来源互斥可判：`reasoning.encrypted_content` 只可能是 Codex 铸的（出站体发不了 `include=[reasoning.encrypted_content]`，BPS 从不回推理密文），`compaction.encrypted_content` 只可能是 BPS 自己铸的（09-29 实测它解得开自己那颗）。所以同一轮里两种都在时元凶一定是前者；不区分的后果是**静默的** —— 一次 Codex blob 被拒会连带把有效的压缩摘要拉黑，下一轮 `sanitizeEncryptedReasoningInputItem` 把 compaction 项整项删除，模型丢掉压缩前的全部历史，客户端侧零信号。`compaction_summary` 不跳（那是 Codex 侧的同胞类型，里面的密文本来就是 Codex 铸的）。
  - **有码但都不是这一条 → 立刻判否，绝不再看消息。** `classifyOpenAIWSErrorEventFromRaw` 自己**不成立**这条性质：`openai_ws_forwarder_support.go:748` 的 `switch code` 在 code 非空且不匹配时会穿到 `strings.Contains(msg, "invalid_encrypted_content")` 那两行。消息是上游可控的自由文本，让它决定「剥掉这个会话的推理密文」等于把一个静默降质开关交给对端，而被剥的 blob 在 Codex 后端本来有效 —— 所以本层在中间加了这道闸。消息兜底只在**两个取码读法都空**时生效（有些形态确实只有一句话）。反面用例：`TestOpenAIBasisPoints_UnrelatedErrorCodeIgnoresTheMessageKeywords`（码 = `rate_limit_exceeded`、消息里带关键词，下一轮出站体里 blob 必须还在）。参考实现只有「优先 code、code 缺失退到消息前缀」两级（`openai_excel_bps_encrypted.go:12-25`），没有这道闸。
  - 判据窄口还剩一处**既存**的（不在本轮范围）：WS 桥的 Codex 侧 HTTP 400 分支（`openai_ws_http_bridge.go:617`）仍是裸 `extractUpstreamErrorCode`，既无消息兜底也读不到 `response.error.code`。
  - 会话键走 `openAIWSLineageSessionHashFromContext` 而不是裸 `GenerateSessionHash`：WS 入口把会话哈希写在 ctx 里，读写两侧只要有一侧按体派生就永远对不上，记了也白记。摘要按**剥离前**的体收（与原路径的 `lineageEntryBody` 同口径）。`HasAnySessionInvalidEncryptedContent` 那道全局探测必须排在算哈希之前：`GenerateSessionHash` 不是纯函数（`attachOpenAILegacySessionHashToGin` 会替换 `c.Request` 的 context），而这里用的体是策略处理过的，与 handler 早先按客户端原体算的那次可以不同。
  - **键加 `bps:` 前缀，不与 Codex 路径共享。**「BPS 解不开」≠「这个 blob 失效了」—— 它在 Codex 后端仍然有效。分组里混着开/关开关的账号是用户允许的形态，共享一把键会让下一轮落到没开开关的账号时，Codex 路径把本来能用的推理密文也剥掉：整个会话在 sticky TTL 内每轮从零推理，且没有任何客户端可见信号 —— 正好是「宁可失败也不要静默降质」的反面。反方向仍然共享（读侧取两把键的并集）：Codex 拒过的 blob 对 BPS 确实也是死的。
  - **记的地方有三个，但覆盖不完备，缺口是刻意留的**：HTTP 非 2xx（`rejectOpenAIBasisPointsResponse`）、peek 抓到的首帧失败（`beginOpenAIBasisPoints`）、WS 桥上首输出**之后**才到的失败帧（`openai_ws_http_bridge.go`）。桥上那次刻意放在 `eventType == "error"` 判断**之外**：`response.failed` 把码放在 `response.error.code`，只在 error 分支里调就漏掉 BPS 上更常见的那一半（第八轮 blocker 点名的形态）。用例 `TestOpenAIBasisPoints_WSHTTPBridgeRemembersInvalidEncryptedContent` 咬住这一点。
  - **缺的那一格：HTTP 侧首输出之后才到的失败帧。** peek 有 4 MiB 上限，吃满之后就放行，后面到的失败帧在 HTTP 上没有任何人记 lineage —— 与桥上那一半同构、覆盖却不同构。命中后果与桥上一致：客户端每轮原样回放同一批 blob，硬报错口径下这个会话在这个账号上每轮都失败且**永不自愈**。**刻意不补**：那一族回到 `openai_gateway_forward.go` 时只剩一个 `*UpstreamFailoverError{StatusCode}`，原始失败报文被共用处理器（`openai_gateway_response_handling.go`）吃掉了，要拿到它得改那个 Codex 也在走的处理器的返回形态 —— 为一个尾部形态动共用路径，风险大于收益。真要补就从那里把 payload 带出来，别在 BPS 这层硬猜。
  - **刻意不做同路重试**。参考实现（[ranxi2001/sub2api](https://github.com/ranxi2001/sub2api) 的 `openai_excel_bps_encrypted.go`）当场剥掉不透明 reasoning 重发一次，对客户端零可见失败；这里先不发那第二次上游请求 —— BPS 通道并发打多了会被封（见 `sub2api-basispoints-route` 的现场结论），拿「少一次可见错误」换「每条失败请求都翻倍上游流量」不值。要改成重试就在这里加，判据和摘要收集都已经在手。
  - 另一类是 message / 工具结果的 `content[].type == "encrypted_content"` 部件（Codex 多智能体 v2 历史）：**替换成一句明文说明，保留部件位置**（`openAIBasisPointsEncryptedContentNotice`），不再判死。用户 2026-09-29 拍板「让模型知道少了一段」。
    原来是 `bpsNative("input_content")`，而这条**没有自愈路径**：失败发生在 `bridge.prepare`、出站之前，上面那套 lineage 压根不会被触发 —— 客户端每轮回放同一批，这个会话在开着开关的账号上**永久** `basispoints_input_content`，只能让用户关开关。取舍是「让模型知道少了一段」还是「整个会话永久报错」，那段密文在 BPS 上无论如何都递送不出去（出站体是严格白名单，这个部件类型本身不在其中）。
    落地三处，缺一处替换就是死代码：①事前闸门 `bpsContentRoute` 要放它过（否则请求在进 bridge 之前就被判死）；②`rewriteContent` 里换成 `{type: <文本 kind>, text: <说明>}`；③文本 kind 按角色选 —— assistant 用 `output_text`、其余 `input_text`，**工具结果位置用 `input_text`**（那个位置是给模型的**输入**，官方 schema 在 `function_call_output.output` 上的联合类型是 `input_text|input_image|input_file`；这里原来写的是 `output_text`，代码也真的那么发了 —— 那是 `textKind` 从「只给这句说明用」扩到全部文本部件时的意外副作用，把本来原样转发的 `input_text` 换掉了。选错等于自己造一个上游没见过的形态，而这个文件的规矩是「换掉类型本身就是赌」）。

- **事前闸门只扫真会出站的那几类项、且按项分字段**：`message` / `""` 只扫 `content`，`function_call_output` / `custom_tool_call_output` 只扫 `output`（item 级白名单也只送这两个字段，反过来扫就是同一个缺陷降到字段级 —— 那个字段一个字节都不会发，却能把整条请求判死）。`reasoning` / `compaction` / `compaction_summary` 是整项重建、`compaction_trigger` / `additional_tools` 是挪位或丢弃 —— 它们的 `content` 里有什么根本不会出站，扫它就是假阳性。原来对每个 item 无条件扫，于是一个带 `content:[{type:"reasoning_text",…}]` 的 `reasoning` 项（本仓库 apicompat 的 fixture 就是这个形状，`/v1/responses` 客户端回放历史时的常规形态）会撞上部件白名单、被判成 `input_content` 硬 502 —— **同一类的第三个实例**（前两个是 `encrypted_content` 部件与 `compaction` 项）。
- **部件级字段也是白名单**（不只 item 级）。文本三类（`input_text` / `output_text` / `text`）重建成 `{type, text}`、`refusal` 重建成 `{type, refusal}`，`"text"` 顺带归一成按位置算出的那个 kind。依据：上游对部件上多出来的字段同样是拒 —— `{"type":"input_image","file_id":…,"detail":"auto"}` 稳定 422、去掉 `detail` 就 200，而 `detail` 是完全合法的 Responses 字段。而 `output_text` 部件天生带 `annotations`（新版还带 `logprobs`），把 `response.output` 的 assistant 消息原样回放进下一轮 `input` 是官方的多轮写法；顶层 `messages` 那条 legacy 入站路径还会产出带 `prompt_cache_breakpoint` 的 `input_text`。都是常规形态，而后果同上：上游 400 + 每轮回放 ⇒ 永久失败。
  - **重建的前提是正文真的是字符串。** `bpsText` 对非字符串返回 `""`，于是 `text` 是对象/数组时模型看到一个空文本部件、正文一个字节都不出站，客户端侧零信号、日志里零读数 —— 整个 switch 里只有这一格会静默吞（其它每个分支都是 `bpsNative` 判死）。最现实的生产者是 **Assistants API v2 的消息形状** `{"type":"text","text":{"value":…,"annotations":[]}}`，而 `text` 这个类型本来就是为「确实有客户端这么发」才放过的。所以事前闸门 `bpsContentRoute` 上加了类型检查：**有正文但不是字符串 ⇒ `input_content` 判死**（`refusal` 同理）；**缺字段不判死**（那种部件本来就没有正文可丢）。刻意**不**去读 `text.value` 兜底 —— 那是替客户端猜格式。用例 `…NonStringTextIsAHardErrorNotAnEmptyPart`。

- **这一类缺陷的通用形状，改代码前先按它自检**：客户端每一轮都原样回放同一批历史，所以任何「在出站**之前**判死某个形态」的分支 = 那个会话在开着开关的账号上每轮都硬报错，且 `bps:` lineage 自愈**压根不触发**（它只对上游拒绝生效）。第九、十两轮一共抓到三个实例，第十一轮第四个（见下面 compaction 那条：**不是判死，而是自愈路径断了** —— 同一个后果，入口不同）。新增任何 `bpsNative(...)` 出口都要先问：这个形态会不会出现在**历史**里？出口之外还要问第二句：这个形态被**上游**拒了之后，下一轮靠什么不再发它？**密文值一个字节都不出站**：它对 BPS 是垃圾 token，本层也无从判断里面是什么。用例 `TestOpenAIBasisPoints_EncryptedContentPartBecomesAPlainNotice` / `…NoticeUsesOutputTextForAssistant`。
- 档位：max/ultra→xhigh，none/minimal→low，未知→medium。**不按模型钳档**：原来对 gpt-6-astra 把 low 钳到 medium（引「ghcp_proxy 实测」），2026-09-29 直连实测 astra@low 是 200，四家参考实现也都不钳 —— 钳掉等于白吃掉客户端更快更便宜的低档，已移除。
- `parallel_tool_calls` 回写的是**这一轮实际发生了什么**（`b.parallelToolCalls || tools > 1`），不是客户端要求了什么。**刻意不因此报错**：这个字段不在出站白名单里，BPS 只从 developer 提示里看到一句软约束，拿一个从没转发出去的约束去判上游「协议违规」并杀掉整轮，结果是这轮零产出，而客户端对「声明 false 却来了两个调用」绝大多数照样逐个执行。翻译出的 function_call 带 `encrypted_function_args: []`（Codex 的协作工具靠「显式空列表 vs 字段缺失」区分明文与密文，缺了它客户端会把明文当 `encrypted_content` 塞给子 agent）；直接目录调用时原生项自带的该字段原样透传。
- custom 工具的 `summary` 标记容忍近失（前后空白、大小写、标记与名字之间多一个空格）。**只放宽到能精确识别的近失，不猜**：summary 是别的内容时仍按 FUNCTION 信封解 `code`，解不开就报错 —— 目录里只有一个 custom 工具就拿原文兜底那种做法会把「模型写错了 FUNCTION 信封」也吞成 custom 调用。
- 默认 `context_management` = `compaction / compact_threshold: 872000` = **BPS 实测上限 918,000 的 95%**（excel-codex-bridge 的 `_compaction_limits` 口径）。上限是它的原话：「Measured on the real backend for gpt-5.6-sol, gpt-6-sol, gpt-6-luna and gpt-6-astra alike: 918,843 input tokens accepted, ~921,375 refused with context_length_exceeded」。它的 `DEFAULT_CONTEXT_WINDOW=500_000` 只是给 Codex CLI 的保守别名（同源码：`-1m` 别名才「runs at the longest input the Excel backend accepts」），**不是后端窗口** —— 所以按 500k 算出的 475000 和原来那个 200000 一样是拍的。本仓库独立地也有 `configuredCodexGPT56MaxContext = 872_000`。
  - **这个值是一笔花钱的决定，用户 2026-09-29 拍板就按 872000（要满上下文，不要为省钱提前摘要）。** 下面这些后果是已知并接受的，别当成待修项：872000 意味着单发 input 上限也是 872K，而本站没有任何一层事前闸门拦得住：`max_input_tokens` 全仓只是展示字段、没有执行点；中间件只有请求速率限制，没有额度中间件；余额检查是 `CheckBalanceAfterDeduction`（扣费之后）；利润门只按价格比排除不赚钱的账号。叠上三点放大：①长上下文阶梯的判定把 `CacheReadTokens` 算进总数，而 BPS 每发额外带约 22.5K 全命中缓存的 Excel 提示词，等于提前约 11% 触达阈值；②超阈值后倍率作用于**整发**（input + cache_read + **output**），不是只作用于超出部分；③这条路 `store=false` 且发不了 `include=[reasoning.encrypted_content]`，每轮从零推理、output 本来就翻倍，而 output 也吃这个倍率。**原来的 200000 压在所有相关阈值下方**（gpt-6-astra/sol/luna 的 fallback 卡片是 `LongContextInputThreshold=272_000`，`billing_service.go:539/555/569；200000` 那批全是 Grok），改成 872000 之后长会话的整条尾部会稳定落在贵档。要省钱就把客户端自己的 `context_management` 调低（本层以客户端为准）。
    **前提是这道阶梯开着**：`contextTierPricingEnabled`（`billing_service.go:1500`）来自分组的长上下文定价开关或账号 `extra.openai_long_context_billing_enabled`（默认 false），两者都没开时 `pricingContext` 被压成 1、走基础价，上面整段代价不成立。
  - 客户端给的非空数组**不原样转发**：只接 `{"type":"compaction","compact_threshold":<number>}` 一项、阈值钳进上限，其它形态判死（`bpsNative("context_management")`）。出站体每一个字段都是白名单，赌上游收不收只会换来一条 422，而 422 在硬报错口径下就是客户端可见的失败。
  - 那边的 `-1m-excel` 模型别名**不改上游模型名**（`gpt-6-sol-1m-excel` → `gpt-6-sol`），只是给 Codex CLI 声明 918k 窗口、让它别在 500k 就本地压缩。本层不暴露客户端模型名，这条不适用。
- 客户端画像头 16 个 = bridge 的 `_DEFAULT_CLIENT_HEADERS`（它抓不到真实 Excel 会话时的兜底集合），不是真实会话全集。真实全集（`_ALLOWED_CAPTURED_HEADERS`）另有 `x-openai-internal-basispoints-browser-{name,ua-brands,ua-mobile,ua-platform}` 和 `x-stainless-runtime-version`，**刻意不补**：没有任何参考实现公布过真实取值，`ua-brands` 这种结构化串猜错了比缺失更显眼。`x-openai-account-user-id` 在真实全集里，留着。
- 客户端工具目录写成 developer 消息；模型只能调原生 `run_officejs`，其 `code` 是 JSON 信封 `{name, arguments}`（也认 `const x = {...};` 外壳，只取 JSON 不执行），翻成客户端的 function_call；客户端的 function_call_output 在下一轮翻回原生项（回放缓存作用域 `accountID|apiKeyID`，`bpsLRU` 1024 条 / 16 MB，丢了就按客户端历史重建）。输出项 id 一律 `fc_<call_id>`；对应原生 `update_plan` 的输出改成 `{"status":"ok"}`（Codex 的 "Plan updated" 会让模型重新规划）。
- 用户消息里的 `data:` 图片先传 `/basispoints/api/attachments`（multipart `file` → `openai_file_id`），按账号 + sha256 缓存 256 条、30 分钟；工具输出里的图片原样内联（Excel 加载项自己就是内嵌发的）；单张上限 20 MB。
  - **类型按字节定，不按客户端声明的串定，也绝不默认 png**（`bpsSniffImageMediaType` 走 `http.DetectContentType`，只认 png / jpeg / gif / webp）。BPS 只接受 `.jpeg/.jpg/.png/.gif/.webp`，别的格式或扩展名对不上字节（`.jfif`、无后缀）会让**整单** 400 `Expected image type to be a supported format … but got none`，而历史里一旦带上这张图，这个会话**每一轮回放都失败**（[cpa-plugin-oai-basispoints#15](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints/issues/15) 的现场，维护者 v0.2.4 也改成了按字节识别）。猜成 png 的代价一样，而且客户端拿不到定位信息 —— 所以识别不出就判死成 `image_input`。
  - 声明段仍过一遍 `mime.FormatMediaType`（畸形串含 CR/LF 一律拒）。类型按字节定之后，出站 multipart 的 Content-Type 与 `filename=` 全是本层字面量，**部件头注入面从根上没了**，原来那段「从客户端子类型推扩展名再消毒」连带删掉。
  - **`input_image` 带 `file_id` 时只发 `{type, file_id}`，一个 `detail` 都不许多。** 同一张已上传的图、同一请求体，`{"type":"input_image","file_id":"…","detail":"auto"}` 稳定 422 `Invalid request body.`，去掉 `detail` 就 200（[#17](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints/issues/17) 的 A/B 对照，v0.2.4 已按同样口径修）。`file_id` 与 `image_url` 同时出现判死，不替客户端挑一个。
- **BPS 偶发只把正文放在终态项里，一条 `response.output_text.delta` 都不发** —— 只拼 delta 的客户端会拿到一条 HTTP 200 的**空回答**（[#12](https://github.com/JaxsonWang/cpa-plugin-oai-basispoints/issues/12)，issue 里点名了「把本插件响应转给下游的 sub2api」）。「200 但正文为空」不在硬报错口径的覆盖范围里，报错收口一条都拦不住，所以转写器在 `response.output_item.done` 之前按 `item.content[].output_text` 补发。**只在这一项一条 delta 都没发过时补**（`sawTextDelta`，上限 1024 条）：两边都发会让同时读 delta 和终态项的客户端看到双份正文。空文本 / refusal / reasoning 不补，`content_index` 按真实下标。

## 落回与错误

**2026-09-29 用户改口径：开着开关就不回落 Codex。** 同一个 API key 上混着满血（BPS）和降智（Codex）的回答比直接失败更糟，所以所有承载不了的情况都做成客户端可见的终态错误。**只剩一个例外**：HTTP 路径上的 `client_restriction`（WS 桥不放行）——它是本站自己的拒绝，原路径会写自己那条规范的拒绝文案，在这里拦下来只会把真实原因换成一条 BPS 错误码。

**原生 v2 压缩回合的豁免已删除（2026-09-29）。** 它原来是唯一还会真的发降智请求的地方。直连 `bps.openai.com` 实测（`bpstest-pro1`，3 发 + 1 发事件 dump）：带 `compaction_trigger` 的体 **HTTP 200**，事件序列与 Codex 后端一致 —— `response.created` → `in_progress` → `output_item.added(item.type=compaction)` → `response.compaction.compacting` → `output_item.done(compaction)` → `completed(output=[compaction])`，无正文 delta（压缩回合本来就不产正文）；对照组（同历史不带 trigger）13 种事件 + 有正文。`x-codex-beta-features: remote_compaction_v2` 加不加结果一样，**BPS 不需要那个头**。
原先豁免的三条理由实测都不成立：带 prologue 的体照样 200；trigger 挪到末尾对真 Codex 是 no-op（真客户端本来就放末尾 —— `normalizeOpenAIResponsesCompactRequest` 把「单个 trigger 在末尾」当已规范直接 `return body, false`）；那个头不需要。
**legacy compact 回合（`/responses/compact`）判死，reason `legacy_compact_path` → 502**（第十二轮 blocker）。它与上面那条删掉的豁免**逐字同一个后果**：那一轮摘要由降智的 Codex 生成，然后成为此后每一轮 BPS 请求的历史 ⇒ 同一个会话一半满血一半降智，而客户端拿到的是 HTTP 200 + 一份正常摘要，**没有任何可见信号**（唯一读数是事后 `usage_logs.upstream_endpoint`）。**判据用仓库自己的 `isOpenAIResponsesCompactPath`，不要在这里写字符串比较**：`/responses/compact/<子路径>` 在本仓是一等公民（路由 `/responses/*subpath`、`endpoint.go` 明文把四种嵌套形态都归到 `EndpointResponsesCompact`、handler 用例名就叫 `nested_compact`），第一版写的精确相等 `== "/compact"` 漏掉它 ⇒ 那一格照旧静默走 Codex（第十三轮 blocker，与本条自己批评上一版文案的理由逐字同一条）。该谓词两个分支都蕴含 suffix != `""`，只会扩大判死范围、抢不走 BPS 分派。
命中路径两条：客户端直接打 `/v1/responses/compact`（含嵌套子路径），或者裸 `/responses` 带 `compaction_trigger` 但 `stream` 不是 bool `true` —— 后者被 handler 的 body-signal 提升改写 `c.Request.URL.Path`（`codex.remote_compact.detected_body_signal`），所以路径后缀判空**挡得住它、但挡的方式是让它走 Codex**，这才是漏点。
**刻意判死而不是归一成原生 trigger 形态走 BPS**：那要新写一层 `/compact` → trigger 的协议翻译，而 BPS 收不收 legacy compact 的体没有实测。判死是可见的一次性失败，符合「宁可硬错也不掺杂」；代价是走 legacy compact 的老客户端在开着开关的账号上压缩不了（真 Codex 用的是原生 v2，走上面那条已经进 BPS 的路）。**要改成走 BPS 得先直连实测。**

本层对 compaction **项**是原样透传：`compaction` 项不满足 `bpsIsTool`，既不扣留也不翻译；`response.compaction.compacting` 是未知事件类型、走 `emitLocked` 原样转发；终态快照里的 compaction 项走 `backfillText` 但 `type != "message"` 立刻返回。**但压缩回合的 `response.completed` 信封不是逐字节透传**：`translateResponse` 照常往里注入 `reasoning` / `parallel_tool_calls`（与普通回合同一套，不为压缩回合开分支）。

**压缩回合的产物必须能回放。** 上游产出的 `compaction` 项客户端会存进历史、此后每一轮都放在 `input` 里 —— `translateHistory` 原来把它落到 `default` 判死成 `input_content`，那意味着会话从压缩那一刻起每轮 502，且失败在出站之前、lineage 自愈压根不触发。**2026-09-29 直连实测（bpstest-pro1，两发）：BPS 收得回去，而且密文真被解开了** —— 第一发触发压缩拿到 `{type:"compaction", id:"cmp_…", encrypted_content:<3320 字符>}`，第二发把它原样放回 `input`（不带 trigger）+ 一句「列出我让你记的水果」→ HTTP 200，答出全部四种。所以**透传**，不是替换成明文说明。字段按上游自己产出的那三个白名单；出站类型一律用它自己产的 `compaction`（`compaction_summary` 是 Codex 侧的同胞类型、BPS 没见过，而出站体的规矩是「不赌」；那种项里的密文本来也是 Codex 铸的、解不开会被拒 ⇒ lineage 记下 ⇒ 下一轮 `sanitizeEncryptedReasoningInputItem` 整项删除，一次失败后自愈）。

**BPS 自己铸的那颗被拒时也要自愈。** 记 lineage 用 `openAIBasisPointsSuspectEncryptedDigests`：它跳过 `compaction` 项，因为这条路上两种密文来源互斥可判（`reasoning.encrypted_content` 只可能是 Codex 铸的 —— 出站体发不了 `include=[reasoning.encrypted_content]`、BPS 从不回推理密文；`compaction.encrypted_content` 只可能是 BPS 自己铸的），同一轮里两种都在时元凶一定是前者。不跳的后果是**静默**的：一次 Codex blob 被拒会连带把有效的压缩摘要拉黑，下一轮整项删除 ⇒ 模型丢掉压缩前的全部历史，客户端侧零信号。**候选池必须先减掉本会话已知失效的摘要**（第十二轮 blocker）：`entryBody` 是剥离**前**的客户端原体（会话哈希两侧要对得上），而客户端每轮原样回放全部历史 ⇒ 曾被拒、已经不出站的 blob 每轮都还在里面。不减掉的话下面那条回退**永远不触发**（`filtered` 里永远有那颗幽灵），压缩密文永远进不了 lineage —— 会话每轮硬错、永不自愈。repro：先拒一颗 Codex blob 让它进 lineage，之后压缩密文失效，第 3 轮起每轮都在重复标记那颗幽灵。用例 `…StaleStrippedBlobDoesNotBlockCompactionSelfHeal`。
**过滤把候选清空时回退到不过滤**（第十一轮 blocker）：这一轮唯一的密文就是压缩摘要时它就是唯一可能的元凶（BPS 也会拒自己铸的 blob —— 过期、跨网关、或者这条会话换到了另一个同样开着开关的账号，A 号铸的密文拿 B 号的凭据去解），跳过它等于一个字节都不记 ⇒ 下一轮没东西可剥 ⇒ **会话从压缩那一刻起每轮硬错、永不自愈**。宁可剥掉压缩历史自愈一次（可见：模型丢上下文），也不要会话永久死。用例一对：`…ValidCompactionBlobSurvivesACodexBlobRejection`（混合场景不许连带丢）+ `…CompactionOnlyBlobRejectionStillSelfHeals`（只有压缩摘要时必须剥）。
顺带收口了 **compact 兜底信号**（`asOpenAICompactFallbackSignal`）：它在压缩回合进 BPS 之后才变得可达，而那套「换个 compact 模型同号重试」的循环在 Codex 分支里、BPS 路上没有消费者，原样返回等于把上游原文的裸 error 丢给 handler。现在收口成 `basispoints_compact_model_unavailable`。

- **终态错误**（`writeOpenAIBasisPointsUnavailable` / WS 桥用 `buildOpenAIBasisPointsUnavailableWSEvent`）：`{"error":{"type":"basispoints_unavailable","code":"basispoints_<reason>"}}`，WS 上是一条 `response.failed` 同码。**HTTP 状态码：上游自己回的 4xx/5xx 原样透出（含 `Retry-After`），本层判定的一律 502**（`openAIBasisPointsUnavailableStatus`）。触发集就是原来的落回集：发请求前判定（previous_response_id / item_reference、tool_choice、显式联网 `external_web_access != false`、生图（含**只按模型名**请求生图，见下）、结构化输出、reasoning 配置、不支持的内容块、图片上传失败、工具历史找不回、目录写不下、拿不到 account id / token、非 compaction 的 `context_management`、真 Fast 档）、传输错误、**任何非 2xx**（含 401 / 403 / 429 / `basispoints_model_access_changed`）、HTTP 200 里**首个事件**就是 error / response.failed / response.cancelled（`peekOpenAIBasisPointsFirstOutput`，最多等 `openAIBasisPointsPeekSilence` = 3 min；这段偷看不占客户端的首输出预算，accept 时间按 peek 结束后的 `acceptedAt` 算）。
- 错误刻意具备两条性质：**不是 `UpstreamFailoverError`**（换号会换到没开开关的账号 → 又走回 Codex → 掺杂原封不动地回来），**包着 `ErrOpenAIRawRelayNotAccountFault`**（BPS 承载不了某个请求形态不是账号的错，不该拖低它的调度分）。
- **第二条性质有四个例外**（`openAIBasisPointsReasonIsAccountFault`）：`transport_error` / `image_upload_transport` / `missing_token` / `missing_account_id` 不是「承载不了这个形态」，是这个账号/代理本身发不出请求，所以照常罚分。口径与紧挨着的几行对齐 —— `resolveCredentialAccount` 失败、`GetAccessToken` 失败、`requireOpenAIProxyBinding` 失败返回的都是裸 error。豁免掉它们等于让调度器一直把请求塞给一个 100% 发不出去的账号，而硬报错口径下它又不会落回 Codex。
  - `transport_error` 另外按原路径同一口径**临时摘池** 10 分钟（`tempUnscheduleOpenAITransportError`，内存 block + 落库，到期自动恢复），判据是 `classifyUpstreamTransportError().Persistent`（只认 connection refused / no route to host / no such host / 代理鉴权失败这类**持久**原因，代理超时这种瞬时错误不摘）。
  - 附件上传那条路（`uploadOpenAIBasisPointsImage`）**三件事逐条与主请求出口对齐**：记 ops failover 事件、持久错误摘池、**持久**错误的 reason 用专用的 `image_upload_transport`（在上面那张表里 → 罚分）；**非持久（超时这类）用 `image_upload_timeout`，照样硬错、照样记 ops，但不罚分不摘池** —— 与下面 `upstream_silent` / `stream_eof` 的豁免同一条理由（可能是 BPS 通道自己超时，罚分会把流量推给没开开关的账号 = 掺杂）。原来超时也落在罚分桶里，恰好是这一段想消掉的那种「同一根因两种处置」，只是方向相反。用例 `…ImageUploadTimeoutIsNotAnAccountFault`。它排在主 `/responses` **之前**，所以带内联图片的请求上代理死亡只会经过这里；原来失败一律被包成形态类的 `image_upload`（豁免罚分、不摘池、不记 ops），等于同一个根因在两条路上两种处置。这里的 `ctx` 是**客户端 ctx**（uploader 跑在 `bridge.prepare` 里、早于主请求那次 `detachUpstreamContext`），所以 `isClientCanceledTransportError` 在这条路上是**真守卫**，不是主请求那边那种便宜保险。用例 `TestOpenAIBasisPoints_ImageUploadTransportErrorIsAnAccountFault`。
  - **不在这张表里的两条**：`upstream_silent`（peek 等满 3 min 一个事件都没到）和 `stream_eof`（连接在首事件前就断了）。它们在传输层看起来像账号侧故障，但两者都可能是 BPS 通道自己在长上下文上超时/掐断 —— 罚分会把流量推给没开开关的账号 = 掺杂，而这条路又没有 failover 兜。**刻意豁免**：真是代理死了的话 `httpUpstream.Do` 会先返回 error、走 `transport_error` 那条。
  - **摘池的代价（已知，待用户确认）**：原路径那一步是配着 failover 的（客户端那一发仍被服务），BPS 这条路没有 failover，所以代价全落在可用性上 —— 全员开开关的分组遇上代理抖动（且是持久类错误串）会有 10 分钟无可调度账号；混着开关的分组里，开开关的被摘之后同一 sticky 会话下一轮会落到没开开关的账号，变成**同一会话中途从满血切到降智**（用户已定的「分组混着开关仍然掺杂」指的是不同会话质量不同，这是另一件事）。要更保守就给摘池加「同账号连续 2 次持久传输错误」的阈值。
  - `missing_token` / `missing_account_id` **只有软权重，没有自动恢复路径**：oauth 账号走不到 `isOpenAIAPIKeyHealthBreakerAccount` 那道健康熔断（它要求 `type == apikey && IsPoolMode()`），只剩 `scheduler.ReportResult(false)`。这两条是配置问题，需要人工修。
  - **usage policy 封掉这个号的 BPS 通道 ⇒ 报错 + 停用账号** —— 用户 2026-09-29 拍板：403 有冷却，接着打没意义。落地用 `SetError`（一次做完 `status=error` + `error_message` + `schedulable=false`，带调度器 outbox 与快照同步，是全仓「自动停用带原因」的既有原语）。status 落的是 `error` 不是 `disabled`，对调度是一回事（都 != active），但管理台上能看见原因。**全仓没有任何按消息匹配的自动恢复会覆盖这条原因，所以它是永久的**，恢复是管理员的决定。落库的消息过 `maskOpenAIBasisPointsSecrets`（`sanitizeUpstreamErrorMessage` 只替 URL 里的敏感 query 参数，**不打码 bearer token 与 `chatgpt_account_id`**，而 `accounts.error_message` 会在管理台上原样显示）。
    - **判据是签名，不是 HTTP 403。** `openAIBasisPointsUsagePolicyBlock`：码含 `usage_policy`，**或**消息（先打掉这一轮客户端可控的出站字节、再容忍一段状态码前缀）以实测原话 `This request was blocked by our usage policy` **打头**。只看状态码两头都错：①**过宽而且客户端可控** —— 403 在这条通道上同时是「这个号没有这个模型」的回法（下文 09-25 的模型表：sol/luna/terra、gpt-5.5 全 403），而本层刻意不做模型白名单，于是一个分组同时开放 astra 和 luna（对 Codex 账号完全正常）时，任何持本站 key 的客户端请求一次 luna 就永久停掉一个健康账号，循环 N 次停 N 个；Cloudflare 的 WAF 403（HTML 体、解不出 message）同理。②**同时又过窄** —— 真正的封通道更常见的形态是 HTTP 200 + 流内 error / `response.failed` 帧，只看状态码恰好漏掉它。
    - 所以停号在**三个拿得到上游报文的出口**各判一次：HTTP 非 2xx、peek 首帧失败、WS 桥首输出之后。**HTTP 侧首输出之后那一格同样缺**，原因与上面那条 lineage 缺口完全一样（只剩 `StatusCode`，签名拿不到）—— 而按新判据「没有签名就不停号」，这一格的行为是安全的那一边。
    - **消息分支的收窄走了两步，第一步只关掉误触、没关掉故意。**
      - 第一步（第十轮）：从 `Contains(msg, "usage policy")` 收到整句。关掉的是**误触** —— OpenAI 对 reasoning 模型的逐请求内容审核拒绝（`code=invalid_prompt`）官方文案带 "violating our usage policy"，一句被拦下的 prompt 就能停掉一个号；404 `model_not_found` 的 "See our usage policy for details." 同理。两条都是**请求级**拒绝，跟这个号的通道被封是两件事。
      - 第二步（第十一轮，两个审查者各自独立抓到并端到端实证）：出站的 `model` 是**客户端原样控制的字节**（本层刻意不做模型白名单，分组模型白名单是 opt-in），而 OpenAI 的 `model_not_found` 文案把模型名**回显**进 `error.message` —— 于是 `{"model":"This request was blocked by our usage policy."}` 一发停一个号，循环 N 次停 N 个。修法两层：判据**锚到句首**（回显永远出现在上游自己的句子里，做不成句首），并先用 `maskOpenAIBasisPointsSecrets` 把这一轮的 `upstreamModel` / `requestedModel` 打掉（即便哪天上游改成值打头的语法也兜住）。那个 `clientEcho` 参数**只**喂判据，不参与落库那句的打码 —— ops 与 `error_message` 上要看得见真实模型名才排得了障。
      - 句首前允许一段状态码前缀：BPS 会把状态码写进消息开头（09-30 实测 `{"message":"422: Invalid request body."}`），现场那条封号原话记的也是 `403 This request was blocked by our usage policy.`。数字和冒号里塞不进客户端的字节。
      - 代价是上游改措辞就漏判真封号 —— 但**漏判是可见的反复报错，误停是不可逆的**，方向必须朝这边偏。
    - 用例：正面 `TestOpenAIBasisPoints_403DisablesTheAccount`（HTTP 403，消息刻意不带关键词 ⇒ 只咬 code 分支，并咬住落库消息不含 token/account id）、`…UsagePolicyBlockInStreamErrorAlsoDisables`（HTTP 200 流内 error，码刻意泛化 ⇒ 只咬消息分支）；反面 `…RejectWithoutUsagePolicySignatureDoesNotDisable` 六条子用例（403 + CF HTML、403 + `model_access_changed`、429、400 `invalid_prompt`、404 `model_not_found`、**404 + 客户端把封号原话塞进 model 让上游回显**）；判据本体另有 `TestOpenAIBasisPointsUsagePolicyBlockPredicate` 七条（含两种状态码前缀、回显、值打头语法）。两个正面用例**刻意各咬一个分支**：都带签名的话两个分支互相兜，各自单独改坏整套照旧全绿（第十轮变异实测）。
    - **刻意不关这个账号的 BPS 开关**（参考实现 `openai_excel_bps.go:74-93` 的 `disableExcelBPSOn403` 就是那么做的）：关了开关这个号会继续服务、但从此走 Codex = 降智，正好违背「开着开关不允许降智请求」。
    - 罚分在这里仍然是错的药（会把流量推给没开开关的账号 = 掺杂），所以 403 照旧豁免罚分 —— 停号比罚分彻底。
    - 其它非 2xx（含 429）**不停号**：429 是并发打出来的瞬时限流，停号是过度处置。
- 除上面那条 usage policy 停号外账号状态不动（摘池写的是 `temp_unschedulable_until`，不是 `status`）。
- **已知且刻意不修：WS 桥上首输出前的流内失败会丢掉已解出的 usage。** `usage` 在解 `response.failed` 时就拿到了（`openAIWSEventShouldParseUsage` 含全部终态事件），但 BPS 分支 `return nil, failOpenAIBasisPointsWSTurn(...)` ⇒ 那笔 token 永不落 `usage_logs`。HTTP 侧同一形态刻意反过来（带 usage 的 result 照样带出去）。这格在 BPS 上比 Codex 容易踩到：工具事件被扣到终态，纯工具回合下游一个语义字节都没有 ⇒ `wroteDownstream` 恒假；而 429 正是这条通道最常见的失败形态。
  **为什么不改成 `resultWithUsage()`**：`AfterTurn`（`openai_gateway_handler.go:2991`）在 `turnErr != nil` 时**直接 return**（唯一例外是 `result.ImageCount > 0`，而生图在 BPS 上判死 ⇒ 恒不成立），所以 `return resultWithUsage(), err` 带出去的 result 照样被丢掉 —— 补不上这笔账。
  > 这一段第十三轮前写的是「`AfterTurn` 把记账与报调度结果绑在同一个 `result != nil` 上，钉成 `response.failed` 会罚满血账号的调度分」。**那是错的**：`turnErr != nil` 的早返回排在调度上报之前，`SucceededForScheduling()` 压根不会被求值，描述的两害一个都不存在。真正的拦路石是那个早返回。（历史上第 9 个被证伪的「注释写的不变式」。）
  真要补就得让 `AfterTurn` 在 `turnErr != nil` 且 result 带 usage 时也记一笔 —— 那是 Codex / Grok 也在走的共用 WS 处理器，得单独一轮评审，别顺手塞进这个分支。
- **同族的第二个缺口，同样已知刻意不修：HTTP 侧 peek 出口也丢 usage。** 上一条括号里原来写「HTTP 侧同一形态刻意反过来（带 usage 的 result 照样带出去）」，那只对**很窄**的子形态成立 —— 只有 peek 吃满 4 MiB 放行、由处理器接手之后才真的带 usage 出来。peek 自己抓到失败时走 `return nil, bpsNative(reason)`，`failure` 里已经解得出的 usage 整个丢掉，`response.cancelled` 那半尤其刺眼（被取消的响应必然带着已生成 token 的 usage）。补法是让 peek 出口把 usage 挂在 native error 上、由 `forwardOpenAIBasisPoints` 造一条只带 usage 的 result；没做是因为要新加一个带 usage 的 error 类型，而这条出口上有 token 的概率不高（429/403 在生成之前就回来了）。**方向是少收钱，不是多收钱。**
- **BPS 的失败终态不许影响账号调度分**（`OpenAIForwardResult.ScheduleNeutral`）：WS 桥「首输出**之后**到的 `response.failed`」走的是 `return resultWithUsage(), nil` 这条 **nil-error** 出口，`markOpenAIBasisPointsNotAccountFault` 那套哨兵只挂在 error 上、结构上救不了它；而 `AfterTurn` 在 `turnErr == nil` 时会把 `SucceededForScheduling()`（对 `response.failed` 是 false）喂给 `ReportOpenAIAccountScheduleResult` ⇒ 罚一个满血账号的分 ⇒ 调度器更倾向挑没开开关的账号 = 掺杂。并发打出来的 429 与 usage-policy 403 恰好是这个形态，是常态不是尾部。所以桥在 BPS 轮次上按 `!SucceededForScheduling()` 置 `ScheduleNeutral`，handler 命中时**跳过上报**（不是报成功 —— 报成功会清掉模型级瞬时状态）。成功轮次照常上报：要那个延迟样本。第十三轮 blocker。
- **上游接受之后的流内失败分两族，两族都要收口**（`openai_gateway_forward.go` + `markOpenAIBasisPointsNotAccountFault`）：

  ① **`*UpstreamFailoverError`**（共用处理器里 6 处）。原样返回的话 handler 会换号 → 换到没开开关的账号就在 Codex 路径上把同一个请求重跑，掺杂从这个口子回来。所以**不分 `result` 是否为 nil，一律**转成 `basispoints_stream_incomplete` / `basispoints_status_<code>_stream`（前缀是 `writeOpenAIBasisPointsUnavailable` 加的 `basispoints_`，reason 由 `fmt.Sprintf("status_%d_stream", …)` 拼，所以顺序是 status→code→stream），而 `result` 原样带出去记账。**不能加 `result == nil` 前置条件**：`handleStreamingResponseWithReasoning` 里的 usage 是 `&OpenAIUsage{}` 恒非 nil、被 `resultWithUsage()` 原样带出，加了那个条件整段就是死代码（commit `bbba0d043` 就是为此改的）。判「有没有账可记」用 `openAIUsageHasTokens`。

  ② **裸 error**（同一个处理器还有 9 处：`upstream response failed: …`、`non-streaming openai protocol error: …`、读流中断等）。这一族**不是** failover error，所以不会换号，但因为不包 `ErrOpenAIRawRelayNotAccountFault`，handler 的 `!errors.Is(...)` 成立 → `ReportOpenAIAccountScheduleResult(..., false, ...)` 罚这个账号的调度分。**并发打出来的 429 与 usage-policy 403 恰好是「首输出之后回 `response.failed`」这个形态，正落在这一族**。后果是自我强化的：BPS 账号被降权 ⇒ 调度器更倾向挑没开开关的账号 ⇒ 掺杂延迟一步回来。收口方式是在 `forwardOpenAIBasisPoints` / `proxyOpenAIWSHTTPBridgeTurn` 的**函数边界**各 defer 一次 `markOpenAIBasisPointsNotAccountFault`，而不是在每个 err 出口各写一遍。**哨兵必须追加在尾部**（`fmt.Errorf("%w [%w]", err, sentinel)`）：handler 的 `openAIForwardErrorAlreadyCommunicated` 按 `"upstream response failed:"` / `"non-streaming openai protocol error:"` **前缀**判断响应是否已写给客户端，前缀被顶掉就会让 `ensureForwardErrorResponse` 在已写出的 SSE 尾部再追一条 502。

  **能踩到 ① 的时序**：peek 上限 4 MiB，`bpsPeekReachesClientOutput` 对工具参数增量一律判「还没到客户端」，所以一个 `apply_patch` 写大文件的 turn 会把 4 MiB 吞完后放行，此时 `clientOutputStarted` 仍为 false，CF 在终态事件前截断就命中「stream ended before a terminal event」。
- 同理，两处超时不能动 BPS 账号的状态：`openai_first_output_timeout.go` 和 `openai_gateway_response_handling.go` 的 `HandleStreamTimeout` 都加了 `!isOpenAIBasisPointsResponse(c)` 闸门。这条路上工具事件被扣到终态才补发，空闲窗口天然比 Codex 长，靠 `acceptedAt` + keepalive 只是缓解、不是闸门。
- **`client_restriction` 的落回只在 HTTP 路径成立，WS 桥上不放行。** 放行的理由是「原路径会写自己那条规范的拒绝文案」，而 `detectCodexClientRestriction` 的调用点只有 `Forward` / chat-completions / raw_relay —— 桥上没有那次检查，放行的结果不是换一条更好的文案，而是由 Codex 正常服务，正好是要禁的掺杂。
- Fast 策略（`openAIBasisPointsFastPolicyReason`）必须抢在 BPS 分派之前评估：原路径是在 `openai_gateway_passthrough.go:278` 才评估，排在分派点之后。不先评估就有两个后果 —— 管理员配的 `BetaPolicyActionBlock` 闸门被一个账号级实验开关绕过；强制 Fast 分组里的账号静默降级成标准档。两个易错点：①**策略白名单按上游 slug 配**，而分派点排在 `markPatchSet("model", upstreamModel)` 之前，所以这里要自己先 `resolveOpenAIForwardMappedModels` 一次，否则闸门照旧不命中；②**只有 `priority` / `ultrafast` 才算真 Fast 档** —— `normalizeOpenAIServiceTier` 把 `auto`/`default`/`flex`/`scale` 也当合法值留在体里（`flex` 反而更慢更便宜），按「字段存在」判会把它们一起打成硬 502，而出站白名单根本不带 `service_tier`，拦住它们保护不了任何东西。策略自己要拒时把 `OpenAIFastBlockedError` 包上 `ErrOpenAIRawRelayNotAccountFault` 并 `MarkResponseCommitted`（客户端策略拒绝不是账号的错，也不该在写完的 403 尾部再追一条 SSE）。
- 生图闸门同样要在分派点判死：原路径的分组闸门（`imageIntent && !imageGenerationAllowed`）排在分派点之后，而 `openAIBasisPointsRouteReason` 只看 `tools` / `tool_choice` 里的 `image_generation` —— 光按模型名请求生图（`gpt-image-1` 之类）会既绕开分组闸门、又把模型名带着账号主人的 bearer token 发到 bps.openai.com。所以分派前跑一次 `IsImageGenerationIntent`，命中就报 `basispoints_image_generation`。
- 顶层 `messages` / `prompt` / `commands` 这类 legacy 入站形态在分派前先跑一次 `normalizeOpenAIResponsesLegacyIngress`（结果只给 BPS 用，走回原路径时 body 保持原样），否则会被判成 `prompt_template` / `request_json` 硬报错，而原路径本来能正常服务。
- 不受影响、仍然走原路径的入口：`/v1/responses/<子路径>`（**`/compact` 除外，它判死**）、`/v1/messages`、chat-completions 桥、count_tokens、/models，以及 `/v1/alpha/search` 的 `forwardAlphaSearchViaResponsesWebSearch`（`openai_alpha_search.go`）—— **最后这条也是开着开关的账号仍会在 Codex 路径上跑真实回合的入口之一**。**原生 v2 压缩回合不在此列**（2026-09-29 起走 BPS，实测见上文「落回与错误」开头）：它是裸 `/responses` + input 末尾一个 `compaction_trigger`，`normalizeOpenAIResponsesCompactRequest` 不改路径，所以路径后缀判空挡不住，而分派条件里也**不再**有 `!HasCompactionTriggerInInput`。
- **代价（记在这里，别忘）**：`tool_history`（回放缓存丢了，比如重启后正在进行的多轮工具会话）和 **png/jpeg/gif/webp 之外的图片**现在是硬错误而不是降级继续 —— 后者不是收紧，是纠正：那些格式上游本来就会整单 400，而且会让整个会话每轮回放都失败，在这里判死至少给出了原因。分组里混着开关开 / 关的账号时，选号随机 ⇒ 照样掺杂，这一层管不了；要彻底干净得让那个分组里的 oauth 账号全开（用户明确说不加分组级校验）。
- 接受之后（`openai_basispoints_stream.go`）：文本 / 推理事件实时透传；工具事件扣到终态事件再翻译成 added/delta/done/item.done；非终态快照剔掉原生工具项；`sequence_number` 重排；**终态集用仓库那张表（`isOpenAIWSTerminalEvent`）**，带 output 快照的（completed / done / incomplete / cancelled / canceled，见 `bpsIsTerminalOutputSnapshot`）才走翻译 + 补发，`failed` 的 output 恒空不走；只有 `completed` 缺工具项才算协议违规，截断 / 取消只丢扣留记录；**`output_index` 用该 item 在 `output` 数组里的真实下标、数组不重排**（到达顺序必然和数组顺序打架，三个不变量只能保一个，保规范这条；两家参考实现同样如此）；重复 item id 判协议违规而不是静默吞掉；空闲满 keepalive（30 s，且不超过 `stream_data_interval_timeout` 的 1/3）补 `response.in_progress`，上游沉默满 `openAIBasisPointsUpstreamSilenceBudget()`（从 `stream_data_interval_timeout` 派生，见下文并发小节）合成 `response.failed` code=`basispoints_upstream_timeout`；扣住的工具项在 `response.completed` 里找不到 → 合成 `response.failed` code=`basispoints_protocol_error`（`invalid_request_error`，确定性失败，处理器不换号重跑）；整条流没有终态事件 → 返回 `io.ErrUnexpectedEOF`（截断就该长得像截断，交给原路径按传输错误处理）。流内的终态失败按原路径口径处理（首输出前 502 换号、之后原样透传），但**不改账号状态**：HTTP 路径靠 gin 键 `openai_basispoints`（`isOpenAIBasisPointsResponse`，每次 Forward 开头清掉），桥靠 `bpsAttempt != nil`。
- 所有测试离线（`openai_basispoints*_test.go`，`httpUpstreamRecorder` 桩，桥用例直接调 `proxyOpenAIWSHTTPBridgeTurn`）；上线实测由用户自己做。

## 不做
- 不自建图片托管（别人也会部署这份二开，不能硬编码域名）；不改模型名、不加模型白名单（模型不可用由落回兜住）；不给 BPS 单独的账号状态 / 熔断；不更新 codex 用量快照（BPS 响应没有 Codex 的限流头，靠 /wham/usage 轮询）。
- 不动 292 猎手 / 降智恢复探测 / 降智暂停：它们照旧运行、界面照旧显示。2026-09-25 那版顺手藏掉猎手界面，2026-09-29 重启这条路线时没有跟着带过来 —— 糖果题恢复探测仍在用，藏了是负收益。

## 2026-09-29 线上实证

克隆行 `bpstest-pro1`（整行复制生产 pro1 的凭据与代理，只加 `extra.openai_basispoints=true`），
经 sub2api 打糖果题 `gpt-6-astra@medium` 5 发：**全部 HTTP 200、答案 21（满血）**，
`usage_logs.upstream_endpoint` 全为 `/basispoints/api/responses`，`route=native_codex` 落回 0 条，耗时 15–26 s。
同一份凭据当天走 Codex 路径恒定降智。⇒ Codex OAuth token 可直接用于 BPS，且不受 (账号 × 网关) 降智影响。

两处与 09-25 判断不同：

- **推理摘要是「有时有」**，不是一律没有：第 1 发带了 90 条 `response.reasoning_summary_text.delta`，后 4 发没有。
- **保活缺口收窄但没有消失，而且剩下的那一段正好在最需要它的地方。** Forward 路径下游空闲时会写 `:\n\n`，
  首个语义输出前也生效（`openai_gateway_response_handling.go` 的 `lastDownstreamWriteAt`，测试
  `openai_passthrough_preoutput_keepalive_test.go`），BPS 走 Forward、自动享受；本层 30 s 的
  `response.in_progress` 是额外一层。**但 `peekOpenAIBasisPointsFirstOutput` 排在处理器之前**，
  peek 那段（最长 `openAIBasisPointsPeekSilence` = 3 min）下游一个字节都没有 —— 而这个预算恰恰是为
  「BPS 高 effort 思考几十秒才吐第一条」设的。
  **这是刻意的取舍，不是待修项**：peek 期间连响应头都还没写，正因为如此才能在判死时返回一条干净的
  JSON 终态错误；一旦先写了 `:\n\n`，响应就被定型成 SSE，硬报错口径承诺的「客户端可见的明确错误」
  就只能追加在一条空 SSE 流尾部。缩短 peek 预算也不是免费的：在硬报错口径下，它会把「慢但会成功」
  的请求直接变成客户端可见的失败。

建克隆行时注意：`groups.platform` 默认 `anthropic`，不改成 `openai` 会 503 `no available accounts`。

## 2026-09-29 现场风险（linux.do 2948013 / 2949128 共 143 楼）

**并发是封号的主诱因，不是请求总量。** 多份独立报告：开 5–6 并发 20–30 分钟内号就废；明确说「不开并发应该没事，我是开并发才封的」。
封号表现是 BPS 通道单独死（`403 This request was blocked by our usage policy.` 或 `429 too many`），网页端和 Codex 照常。**这里原来写的是「所以不能据此改账号状态」——  用户 2026-09-29 改了口径：命中 usage policy 签名就停号（见上文），因为 403 有冷却、接着打没意义；而按签名判之后「通道被封」与「这个号没这个模型 / WAF 拦截」不再混为一谈。**
运维口径：**开这个开关的账号把并发设成 1**，不需要为 BPS 再加一套限流配置。
生效点是**调度层的 Redis 槽** `tryAcquireAccountSlot(ctx, account.ID, account.Concurrency)`
（`/v1/responses` 走的是 `openai_gateway_scheduling.go` 那一份），选号时拿、请求结束还，排在 `Forward` 之前，BPS 自动继承。

**槽位全程持有，所以空闲上界直接等于「这个账号被锁住多久」。** 而我们补的 `response.in_progress` 会刷新
处理器的 `lastReadAt`（BPS 的 `resp.Body` 就是本层的 pipe），**网关自己那道 `stream_data_interval_timeout`
永远不会触发** —— 保活把它解除了武装。所以流内的空闲上限由 `openAIBasisPointsUpstreamSilenceBudget()`
从那个配置项派生（`2×`，下限对齐 peek 的 3 min，上限是兜底的 `openAIBasisPointsUpstreamSilence` = 15 min，
后者只在运维把配置填 0 时才生效）。翻倍是因为 BPS 高 effort 的真实沉默确实比 Codex 长，直接等于配置值会把
「慢但会成功」的请求判死。**不能写死 15 分钟**：并发设成 1 的账号，一发卡死就是整段零吞吐。
（`httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)` 的第 4 个参数**不是**请求级限流：
它只进 `resolvePoolSettings` 写连接池 sizing，且仅在 `connection_pool_isolation=account/account_proxy` 下生效，
h2 多路复用下限的也是连接数而非并发请求数。别拿它当限流的依据。）
一点该记的：附件上传和主请求**共用同一个槽**，一个带 N 张图的 turn 会在一个槽内向 bps.openai.com 发 N+1 个请求。

现有错误处理正好对：任何 `>=400` 走 `rejectOpenAIBasisPointsResponse` → `bpsNative("status_403_…")` → 硬报错，
**不换号、不罚调度分**；账号状态看签名 —— 命中 usage policy 就停号，没命中（模型没权限 / WAF）留 Active。客户端看到 `basispoints_status_403`，**HTTP 状态码按上游原样透出**
（403 永久封 / 429 瞬时限流必须可区分；全塌成 502 之后两者一样，而客户端对 5xx 的重试通常比对带
`Retry-After` 的 429 更激进，正好把封号主诱因又捶一遍）。上游的 `Retry-After` 一并透传。
**刻意不做 429 重试**（bridge 会退避到 5 分钟）：这条通道的 429 正是并发打出来的，重试等于继续捶，
而且把客户端请求挂 5 分钟比报错更糟。

其他现场读数：上下文实测 500k 以上；无 `max` 档（只有 low/medium/high/xhigh）；大任务会被 CF 超时截断；
帖里 09-25 的 403 模型表（gpt-6-sol/luna/terra、gpt-5.5 全 403，astra 与 gpt-5.6-* 全 200）与 bridge 自己的模型清单矛盾，
说明这是会漂的上游权限，**继续不做模型白名单**。
