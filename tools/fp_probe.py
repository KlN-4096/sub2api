#!/usr/bin/env python3
"""klno 出站身份验收器。

只打本机预演网关。发送前必须由预演启动器用 iptables/独立 namespace 隔离网关，
并把上游指向 echo server；本脚本不负责建立该隔离。--selftest 不发送任何请求。

前提：预演库的目标账号必须是 device + 实验收敛【双开】——线协议投影
(applyCodexDeviceWireProfile) 只在双开时生效，单开跑出来的结果不代表 pro1。

设计约束（上一版踩过的坑，逐条钉死）：
  1. 每个用例必须有对应捕获，数量和路径都要对上；没发出去 = 失败，不是跳过。
  2. curl 的退出码要检查，非 0 直接失败。
  3. 设备身份按端点声明各自的载体，不做「body 或 头，有一个就行」的兜底——
     那会掩盖正确载体缺失。
  4. 不用「存在才比较」：关系两端都在 required 里，缺任一端即失败。
  5. 契约集中在 CONTRACTS 与 WS_*_CONTRACT，升级协议时集中修改。

自检：python3 fp_probe.py --selftest —— 用构造的坏样本验证检查器确实会报错，
不连服务器。验收器本身也要被验收。
预演凭据从 SUB2API_REHEARSAL_API_KEY 读取，不写入仓库。
"""
import base64
import json
import os
import socket
import struct
import subprocess
import sys
import time

GW = "http://127.0.0.1:18080"
KEY = os.environ.get("SUB2API_REHEARSAL_API_KEY", "")
CAP = "/opt/s2a-rehearsal/echo/capture.jsonl"
UA = "codex-tui/0.153.4 (Mac OS 26.2.0; arm64) Apple_Terminal/466 (codex-tui; 0.153.4)"
INSTALL = "7f582abd-05d2-4a59-b4e5-ec1b733b4edc"
# 真实客户端把工具清单留在 body 的 client_metadata、从兼容头里剥掉
# （codex-rs core/src/responses_metadata.rs 的 compatibility_headers）。
# 探针主动塞进去，才能验证网关确实做了剥离，而不是恰好没人带。
TOOLS = ["shell", "apply_patch"]
# 真实客户端的 window_number 初值 0、每次 auto-compact 递增
# （codex-rs core/src/state/auto_compact_window.rs:51，window_id 形态
# format!("{thread_id}:{window_number}")）。探针故意发一个非初值：写死成 :0/:1 的
# 实现只有在探针恰好发同一个数时才会"通过"。
WINDOW_NUMBER = 3
# 探针发的 inference-call-id 原值；网关每次出站新铸 v4（原值直通即跨账号关联；真客户端
# 每次 attempt 新铸，rollout-trace/src/inference.rs:347-349）。
INFER_ID = "bbd9bf7b-cb3d-48e7-bdcb-1c4bba7ee0a1"
# 顶层字段序，抄自 codex-rs 16ff14c codex-api/src/common.rs（Responses :282-307 /
# CompactionInput :48-65 / ResponseCreateWsRequest :334-363 + serde tag 让 type 最前）。
RESPONSES_ORDER = ["model", "instructions", "input", "tools", "tool_choice", "parallel_tool_calls",
                   "reasoning", "store", "stream", "stream_options", "include", "service_tier",
                   "prompt_cache_key", "text", "client_metadata", "access_programs"]
COMPACT_ORDER = ["model", "input", "instructions", "tools", "parallel_tool_calls", "reasoning",
                 "service_tier", "prompt_cache_key", "text", "access_programs"]
WS_CREATE_ORDER = ["type", "model", "instructions", "previous_response_id", "input", "tools",
                   "tool_choice", "parallel_tool_calls", "reasoning", "store", "stream",
                   "stream_options", "include", "service_tier", "prompt_cache_key", "text",
                   "generate", "client_metadata", "access_programs"]
# WS turn-state：探针入站握手带 WS_TURN_STATE、首帧不带（网关按真客户端的位置补进帧内，
# core/src/client.rs:1792-1793），第二帧自带 WS_OWN_TURN_STATE（不得被覆盖）。
WS_TURN_STATE = "ts-handshake"
WS_OWN_TURN_STATE = "ts-own"
# 真客户端发送前给每个 response.create 帧盖 x-codex-ws-stream-request-start-ms（unix 毫秒的
# 十进制字符串，core/src/client.rs:2103-2112）。语义是无条件覆盖（HashMap::insert）且在重试
# 循环内（:1746 loop），每次 attempt 重盖，注释写明"发送到 socket 之前才盖"（:2099-2101）。
# 探针首帧不带（网关必须盖），第二帧自带 WS_OWN_STREAM_START（网关必须在发送边界重盖成
# 自己的时刻，出站不得仍是这个值）。
WS_OWN_STREAM_START = "1700000000123"
# 受控回放里代表"网关重盖后的值"：与 WS_OWN_STREAM_START 不同的合法十进制毫秒。
WS_RESTAMPED = "1700000000999"

# ── 端点契约 ───────────────────────────────────────────────────────────────
# device_carrier 取值：
#   header_install  独立 x-codex-installation-id 头（真实客户端只有 compact 这么发）
#   body_install    body.client_metadata["x-codex-installation-id"]
#   meta_install    出站 x-codex-turn-metadata 的 installation_id
# 列多个表示都必须存在且相等。
CONTRACTS = {
    "responses": {
        "required_headers": ["originator", "user-agent", "version", "session-id", "thread-id",
                             "x-client-request-id", "x-codex-window-id", "x-codex-turn-metadata"],
        "forbidden_headers": ["x-codex-installation-id", "session_id", "conversation_id"],
        # 真客户端默认 enable_request_compression（features/src/lib.rs:1221-1224 Stable+default_enabled）：
        # ChatGPT 登录态的每条 /responses 请求体 zstd 压缩并带 content-encoding: zstd
        # （core/src/client.rs:1534-1541、http-client/src/request.rs:192-222）；compact / 搜索 / WS 不压。
        # echo server 记录的是解压后的体，这里只看头。
        "content_encoding": "zstd",
        # rollout-trace 只在 HTTP /responses 的 attempt 上生成（core/src/client.rs:1646）；
        # 网关按账号派生，出站值必须存在且 != 探针原值。
        "namespaced_headers": {"x-codex-inference-call-id": INFER_ID},
        "required_body": ["prompt_cache_key", "client_metadata.session_id",
                          "client_metadata.thread_id", "client_metadata.x-codex-installation-id",
                          "client_metadata.x-codex-window-id",
                          "client_metadata.x-codex-turn-metadata"],
        "body_order": RESPONSES_ORDER,
        "device_carrier": ["body_install", "meta_install", "body_meta_install"],
        "relations": [
            ("h:version", "expr:ua_version"),
            ("h:session-id", "h:thread-id"),
            ("h:thread-id", "h:x-client-request-id"),
            ("h:x-codex-window-id", "expr:thread_window"),
            ("b:prompt_cache_key", "h:session-id"),
            ("b:client_metadata.session_id", "h:session-id"),
            ("b:client_metadata.thread_id", "h:thread-id"),
            ("b:client_metadata.x-codex-window-id", "h:x-codex-window-id"),
            ("m:session_id", "h:session-id"),
            ("m:thread_id", "h:thread-id"),
            ("m:window_id", "h:x-codex-window-id"),
            ("m:turn_id", "m:root_turn_id"),
            ("bm:session_id", "b:client_metadata.session_id"),
            ("bm:thread_id", "b:client_metadata.thread_id"),
            ("bm:window_id", "h:x-codex-window-id"),
            ("bm:window_number", "expr:window_number"),
            ("bm:context_window_id", "m:context_window_id"),
            ("bm:turn_id", "m:turn_id"),
            ("bm:root_turn_id", "m:root_turn_id"),
        ],
    },
    # 图片是自建 Responses body：只带设备身份，没有会话级 client_metadata，也没有缓存键。
    # 各契约里 x-codex-inference-call-id 的 forbidden 与 ("h:version","expr:ua_version") 关系在
    # 当前实现下恒真（转发白名单不含该头；version 与 UA 由同一个规范身份重建），只是回归护栏。
    "images": {
        "required_headers": ["originator", "user-agent", "version", "session-id", "thread-id",
                             "x-client-request-id", "x-codex-window-id", "x-codex-turn-metadata"],
        "forbidden_headers": ["x-codex-installation-id", "session_id", "conversation_id",
                              "x-codex-inference-call-id"],
        "content_encoding": "zstd",
        "required_body": ["client_metadata.x-codex-installation-id"],
        "body_order": RESPONSES_ORDER,
        "device_carrier": ["body_install", "meta_install"],
        "relations": [
            ("h:version", "expr:ua_version"),
            ("h:session-id", "h:thread-id"),
            ("h:thread-id", "h:x-client-request-id"),
            ("h:x-codex-window-id", "expr:thread_window"),
            ("m:session_id", "h:session-id"),
        ],
    },
    # compact 是唯一发独立安装头的端点（core/src/client.rs:646），且不发
    # x-client-request-id（构造链没有 stream_request 那一步），body 无 client_metadata、
    # 有 prompt_cache_key（codex-api/src/common.rs 的 CompactionInput）。
    "compact": {
        "required_headers": ["originator", "user-agent", "version", "session-id", "thread-id",
                             "x-codex-window-id", "x-codex-turn-metadata", "x-codex-installation-id"],
        "forbidden_headers": ["x-client-request-id", "session_id", "conversation_id",
                              "x-codex-inference-call-id"],
        "content_encoding": None,
        "required_body": ["prompt_cache_key"],
        "forbidden_body": ["client_metadata"],
        "body_order": COMPACT_ORDER,
        "device_carrier": ["header_install", "meta_install"],
        "relations": [
            ("h:version", "expr:ua_version"),
            ("h:session-id", "h:thread-id"),
            ("h:x-codex-window-id", "expr:thread_window"),
            ("b:prompt_cache_key", "h:session-id"),
            ("m:session_id", "h:session-id"),
        ],
    },
    # 搜索使用 MCP metadata 投影（16ff14c: core/src/turn_metadata.rs:234-280）：
    # 无 Responses 的设备/窗口/请求种类字段，body.id 与 session_id 同源；
    # metadata.codex_version / model 与出站 version 头 / body.model 同源。
    "search": {
        "required_headers": ["originator", "user-agent", "version", "x-codex-turn-metadata"],
        "forbidden_headers": ["session-id", "thread-id", "x-client-request-id",
                              "x-codex-window-id", "x-codex-installation-id",
                              "session_id", "conversation_id", "openai-beta",
                              "x-codex-inference-call-id"],
        "required_body": ["id"],
        "required_metadata": ["session_id", "thread_id", "turn_id", "codex_version", "model"],
        "forbidden_metadata": ["installation_id", "window_id", "window_number", "context_window_id",
                               "agent_name", "parent_turn_id", "root_turn_id", "request_kind", "compaction",
                               "history_ingest_requested", "forked_from_ordinal_exclusive", "tool_namespaces_info"],
        "check_window_number": False,
        "device_carrier": [],
        "relations": [("b:id", "m:session_id"),
                      ("h:version", "expr:ua_version"),
                      ("m:codex_version", "h:version"),
                      ("m:model", "b:model")],
    },
}

# 所有端点通用：真实客户端在任何面都不发旧的 responses=experimental；
# 兼容头的 turn-metadata 必须剥掉工具清单，body 里的必须保留。
FORBIDDEN_BETA = "responses=experimental"


def sid(n):  # v7 形态，末段可区分
    return "01a07c73-e312-76e1-9054-e4665b8ee0a%d" % n


def turn_meta(s, window_number=WINDOW_NUMBER):
    return json.dumps({"installation_id": INSTALL, "session_id": s, "thread_id": s,
                       "turn_id": "01a07c73-e3a0-7ae1-baf0-ce1c532f019c",
                       "root_turn_id": "01a07c73-e3a0-7ae1-baf0-ce1c532f019c",
                       "window_id": "%s:%d" % (s, window_number),
                       "context_window_id": "01a07c73-e312-76e1-9054-e4722b79a205",
                       "window_number": window_number, "tool_namespaces_info": TOOLS},
                      separators=(",", ":"))


def turn_meta_mcp(s):
    """搜索工具的 MCP 投影形态（core/src/turn_metadata.rs:390-444）：codex_version / model
    故意填错值，证明网关会把它们对齐到出站 version 头与 body.model。"""
    return json.dumps({"session_id": s, "thread_id": s,
                       "turn_id": "01a07c73-e3a0-7ae1-baf0-ce1c532f019c",
                       "codex_version": "0.0.1", "model": "wrong-model", "reasoning_effort": "medium"},
                      separators=(",", ":"))


def codex_headers(s, relayed=False):
    """relayed=True 模拟 31.108 中继：连字符会话头被剥掉，只剩体内 client_metadata。"""
    h = {"originator": "codex-tui", "user-agent": UA, "version": "0.153.4",
         "x-codex-installation-id": INSTALL, "x-codex-window-id": "%s:%d" % (s, WINDOW_NUMBER),
         "x-codex-turn-metadata": turn_meta(s), "openai-beta": FORBIDDEN_BETA,
         "x-codex-inference-call-id": INFER_ID}
    if not relayed:
        h.update({"session-id": s, "thread-id": s, "x-client-request-id": s})
    return h


def meta(s, window_number=WINDOW_NUMBER):
    return {"session_id": s, "thread_id": s, "x-codex-installation-id": INSTALL,
            "x-codex-window-id": "%s:%d" % (s, window_number),
            "x-codex-turn-metadata": turn_meta(s, window_number)}


def resp_body(s, stream=True):
    return {"model": "gpt-5.4", "stream": stream, "prompt_cache_key": s,
            "client_metadata": meta(s),
            "input": [{"type": "message", "role": "user", "content": "hi"}]}


def build_cases():
    s1, s2, s3, s4, s5, s6 = (sid(i) for i in range(1, 7))
    return [
        {"label": "A 直连 SSE", "path": "/v1/responses", "contract": "responses",
         "upstream": "/backend-api/codex/responses",
         "body": resp_body(s1), "headers": codex_headers(s1)},
        {"label": "B 中继剥头 SSE", "path": "/v1/responses", "contract": "responses",
         "upstream": "/backend-api/codex/responses",
         "body": resp_body(s2), "headers": codex_headers(s2, relayed=True)},
        {"label": "C 非流式", "path": "/v1/responses", "contract": "responses",
         "upstream": "/backend-api/codex/responses",
         "body": resp_body(s3, stream=False), "headers": codex_headers(s3)},
        {"label": "D compact", "path": "/v1/responses/compact", "contract": "compact",
         "upstream": "/backend-api/codex/responses/compact",
         "body": {"model": "gpt-5.4", "stream": False, "prompt_cache_key": s4,
                  "input": [{"type": "message", "role": "user", "content": "hi"}]},
         "headers": codex_headers(s4)},
        {"label": "E 图片生成", "path": "/v1/images/generations", "contract": "images",
         "upstream": "/backend-api/codex/responses",
         "body": {"model": "gpt-image-2", "prompt": "a cat", "n": 1, "size": "1024x1024"},
         "headers": codex_headers(s5)},
        {"label": "F alpha/search", "path": "/v1/alpha/search", "contract": "search",
         "upstream": "/backend-api/codex/alpha/search",
         "body": {"id": s6, "model": "gpt-5.4", "query": "x"},
         "headers": dict(codex_headers(s6), **{"x-codex-turn-metadata": turn_meta_mcp(s6)})},
    ]


# ── 检查器（纯函数，可离线自检）─────────────────────────────────────────────

def hdr(row, name):
    for k, v in row.get("headers", []):
        if k.lower() == name.lower():
            return v
    return None


def jget(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def parse_meta(raw, label, path, problems):
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    # echo server 对超长字符串截断成 '...<len N>'，截断后 JSON 解析失败会让断言全部落空。
    # 必须显式区分「抓包截断」和「网关发了坏 JSON」。
    if isinstance(raw, str) and raw.endswith(">") and "...<len " in raw:
        problems.append("%s %s 被 echo server 截断，断言无法执行（调高 trim_body 上限）" % (path, label))
        return None
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            problems.append("%s %s 必须是 JSON 对象" % (path, label))
            return None
        return parsed
    except Exception as e:
        problems.append("%s %s 解析失败：%s" % (path, label, e))
        return None


def resolve(token, ctx):
    """把关系表达式解析成 (值, 是否可用)。缺失一律视为不可用 → 由调用方报失败。"""
    kind, _, name = token.partition(":")
    if kind == "h":
        return hdr(ctx["row"], name)
    if kind == "b":
        return jget(ctx["body"], name)
    if kind == "m":
        return jget(ctx["meta"] or {}, name)
    if kind == "bm":
        return jget(ctx["body_meta"] or {}, name)
    if kind == "expr" and name == "window_number":
        return ctx.get("window_number", WINDOW_NUMBER)
    if kind == "expr" and name == "ua_version":
        # version 头是 provider 头（model-provider-info/src/lib.rs:397），值与 UA 版本段同源。
        ua = hdr(ctx["row"], "user-agent") or ""
        return ua.split("/", 1)[1].split(" ", 1)[0] if "/" in ua else None
    if kind == "expr" and name == "thread_window":
        # 契约是"<出站 thread>:<入站序号>"，不是某个固定数字。上一版把 ":1" 写死，
        # 只在探针恰好发 1 时成立，反而会保护"序号被改写"的实现。
        t = hdr(ctx["row"], "thread-id")
        return ("%s:%d" % (t, ctx.get("window_number", WINDOW_NUMBER))) if t else None
    raise AssertionError("unknown token " + token)


def check_rows(rows, cases, cross_check=True):
    """rows 与 cases 一一对应；返回 problems 列表。
    cross_check=False 时不做跨路径设备一致性判断，留给调用方合并 WS 结果后统一判。"""
    problems = []
    devices = {}

    if len(rows) != len(cases):
        problems.append("捕获数 %d != 用例数 %d（有用例没发出去或多发了）" % (len(rows), len(cases)))

    for idx, case in enumerate(cases):
        label = case["label"]
        if case.get("curl_rc") not in (None, 0):
            problems.append("%s curl 退出码 %s" % (label, case["curl_rc"]))
        if idx >= len(rows):
            problems.append("%s 没有对应的出站捕获" % label)
            continue
        row = rows[idx]
        path = row.get("path", "?")
        if path != case["upstream"]:
            problems.append("%s 出站路径 %s != 预期 %s" % (label, path, case["upstream"]))
            continue

        spec = CONTRACTS[case["contract"]]
        raw_body = row.get("body")
        body = raw_body if isinstance(raw_body, dict) else {}
        if raw_body and not isinstance(raw_body, dict):
            parsed = parse_meta(raw_body, "请求体", path, problems)
            body = parsed if isinstance(parsed, dict) else {}
        if not body:
            problems.append("%s 没抓到请求体，body 侧断言全部落空" % label)

        meta_hdr = parse_meta(hdr(row, "x-codex-turn-metadata"), "头 turn-metadata", path, problems)
        cm = body.get("client_metadata") or {}
        if not isinstance(cm, dict):
            problems.append("%s client_metadata 必须是对象" % label)
            cm = {}
        meta_body = parse_meta(cm.get("x-codex-turn-metadata"), "体 turn-metadata", path, problems)
        ctx = {"row": row, "body": body, "meta": meta_hdr, "body_meta": meta_body}

        for name in spec["required_headers"]:
            if not (hdr(row, name) or "").strip():
                problems.append("%s 缺必需头 %s" % (label, name))
        for name in spec["forbidden_headers"]:
            if hdr(row, name) is not None:
                problems.append("%s 不该发的头 %s=%r" % (label, name, hdr(row, name)))
        want_encoding = spec.get("content_encoding")
        got_encoding = (hdr(row, "content-encoding") or "").strip().lower()
        if want_encoding and got_encoding != want_encoding:
            problems.append("%s 请求体编码 %r != %r（真客户端默认 enable_request_compression）"
                            % (label, got_encoding, want_encoding))
        if not want_encoding and got_encoding:
            problems.append("%s 不该压缩的端点带了 content-encoding=%r" % (label, got_encoding))
        # echo server 记录压缩体的前 6 字节：libzstd 流式 level 3 默认帧头 = magic + FHD 00（无 FCS / 无校验和 /
        # 非 single segment）+ 窗口描述 58（2MB）。klauspost 的自选帧头（single segment / FCS / 小窗口）在这里会红。
        raw_head = row.get("body_raw_head_hex")
        if want_encoding == "zstd":
            if raw_head is None or row.get("zstd_rc") is None:
                problems.append("%s echo server 没记录 body_raw_head_hex / zstd_rc（未打 zstd 补丁），帧头断言不能静默跳过" % label)
            else:
                if raw_head.lower() != "28b52ffd0058":
                    problems.append("%s zstd 帧头 %s != 28b52ffd0058（libzstd 流式默认）" % (label, raw_head))
                if row.get("zstd_rc") != 0:
                    problems.append("%s zstd 解压失败 rc=%s" % (label, row.get("zstd_rc")))
        for field in spec["required_body"]:
            value = jget(body, field)
            if not isinstance(value, str) or not value.strip():
                problems.append("%s 缺必需 body 字段 %s" % (label, field))
        for field in spec.get("forbidden_body", []):
            if field in body:
                problems.append("%s 不该有的 body 字段 %s" % (label, field))
        if body and spec.get("body_order"):
            problems += order_problems(label, list(body.keys()), spec["body_order"])
        for name, raw in spec.get("namespaced_headers", {}).items():
            got = hdr(row, name)
            if not (got or "").strip():
                problems.append("%s 缺必需头 %s" % (label, name))
            elif got == raw:
                problems.append("%s 头 %s 原值直通（必须按账号派生）" % (label, name))
        for field in spec.get("required_metadata", []):
            value = (meta_hdr or {}).get(field)
            if not isinstance(value, str) or not value.strip():
                problems.append("%s 缺必需 metadata 字段 %s" % (label, field))
        for field in spec.get("forbidden_metadata", []):
            if field in (meta_hdr or {}):
                problems.append("%s 不该有的 metadata 字段 %s" % (label, field))

        beta = (hdr(row, "openai-beta") or "").lower()
        if FORBIDDEN_BETA in beta:
            problems.append("%s 仍在发旧的 openai-beta %s" % (label, FORBIDDEN_BETA))
        if meta_hdr is not None and "tool_namespaces_info" in meta_hdr:
            problems.append("%s 兼容头 turn-metadata 未剥掉 tool_namespaces_info" % label)
        if spec.get("check_window_number", True) and meta_hdr is not None and meta_hdr.get("window_number") != WINDOW_NUMBER:
            problems.append("%s turn-metadata 的 window_number 被改写：%r != %r"
                            % (label, meta_hdr.get("window_number"), WINDOW_NUMBER))
        if meta_body is not None and meta_body.get("tool_namespaces_info") != TOOLS:
            problems.append("%s 体内 turn-metadata 的工具清单被误删或改写（只该从头剥）" % label)

        # 设备载体：按端点声明，不做「有一个就行」的兜底
        carriers = {}
        for carrier in spec["device_carrier"]:
            if carrier == "header_install":
                carriers[carrier] = hdr(row, "x-codex-installation-id")
            elif carrier == "body_install":
                carriers[carrier] = cm.get("x-codex-installation-id")
            elif carrier == "meta_install":
                carriers[carrier] = jget(meta_hdr or {}, "installation_id")
            elif carrier == "body_meta_install":
                carriers[carrier] = jget(meta_body or {}, "installation_id")
        for carrier, value in carriers.items():
            if not isinstance(value, str) or not value.strip():
                problems.append("%s 设备载体 %s 缺失" % (label, carrier))
        present = [v for v in carriers.values() if isinstance(v, str) and v.strip()]
        if len(set(present)) > 1:
            problems.append("%s 同一请求内出现多个设备身份 %s" % (label, sorted(set(present))))
        for value in present:
            devices.setdefault(value, []).append(label)

        for left, right in spec["relations"]:
            lv, rv = resolve(left, ctx), resolve(right, ctx)
            if lv is None or rv is None:
                problems.append("%s 关系 %s == %s 无法判定（%s=%r %s=%r）" %
                                (label, left, right, left, lv, right, rv))
            elif lv != rv:
                problems.append("%s 关系不成立 %s(%r) != %s(%r)" % (label, left, lv, right, rv))

        ua = hdr(row, "user-agent") or ""
        if not ua.rstrip().endswith(")"):
            problems.append("%s UA 缺尾部客户端标识组" % label)

    if cross_check:
        problems += cross_device_problems(devices)
    return problems, devices


def order_problems(label, keys, order):
    """keys 必须全在 order 里且是它的子序列（echo server 的 json.loads 保留原始键序）。"""
    problems = []
    at = 0
    for key in keys:
        if key not in order:
            problems.append("%s 顶层字段 %s 不在字段序表里" % (label, key))
            continue
        idx = order.index(key)
        if idx < at:
            problems.append("%s 顶层字段序错乱：%s（实际 %s）" % (label, key, keys))
            return problems
        at = idx + 1
    return problems


def cross_device_problems(devices):
    if len(devices) > 1:
        return ["跨路径出现 %d 个不同设备身份：%s" %
                (len(devices), {k: sorted(set(v)) for k, v in devices.items()})]
    return []


# ── 自检：构造坏样本，验证检查器会报错 ─────────────────────────────────────

def selftest():
    good_meta = {"installation_id": "I", "session_id": "S", "thread_id": "S",
                 "turn_id": "T", "root_turn_id": "T", "window_id": "S:%d" % WINDOW_NUMBER,
                 "context_window_id": "C",
                 "window_number": WINDOW_NUMBER}
    base_headers = [["originator", "codex-tui"], ["user-agent", UA], ["version", "0.153.4"],
                    ["session-id", "S"], ["thread-id", "S"], ["x-client-request-id", "S"],
                    ["x-codex-window-id", "S:%d" % WINDOW_NUMBER],
                    ["x-codex-turn-metadata", json.dumps(good_meta)],
                    ["x-codex-inference-call-id", "DERIVED"],
                    ["content-encoding", "zstd"]]
    base_body = {"model": "gpt-5.4", "prompt_cache_key": "S",
                 "client_metadata": {"session_id": "S", "thread_id": "S",
                                     "x-codex-installation-id": "I",
                                     "x-codex-window-id": "S:%d" % WINDOW_NUMBER,
                                     "x-codex-turn-metadata": json.dumps(
                                         dict(good_meta, tool_namespaces_info=TOOLS))}}
    case = {"label": "T", "upstream": "/backend-api/codex/responses",
            "contract": "responses", "curl_rc": 0}

    def row(headers=None, body=None):
        return {"kind": "http", "path": "/backend-api/codex/responses",
                "headers": [list(h) for h in (headers if headers is not None else base_headers)],
                "body": json.loads(json.dumps(body if body is not None else base_body)),
                "body_raw_head_hex": "28b52ffd0058", "zstd_rc": 0}

    ok, _ = check_rows([row()], [case])
    if ok:
        print("[SELFTEST FAIL] 正样本被误报：%s" % ok)
        return 1

    def mutate(fn):
        h = [list(x) for x in base_headers]
        b = json.loads(json.dumps(base_body))
        fn(h, b)
        return row(h, b)

    negatives = [
        ("少发一个用例", [], [case], "捕获数"),
        ("curl 失败", [row()], [dict(case, curl_rc=7)], "curl 退出码"),
        ("路径不对", [dict(row(), path="/wrong")], [case], "出站路径"),
        ("缺必需头 session-id",
         [mutate(lambda h, b: h.remove(next(x for x in h if x[0] == "session-id")))], [case],
         "缺必需头"),
        ("缺 version 头（provider 头每条请求都带）",
         [mutate(lambda h, b: h.remove(next(x for x in h if x[0] == "version")))], [case],
         "缺必需头 version"),
        ("version 与 UA 版本段不同源",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "version"), ["version", "9.9.9"]))],
         [case], "关系不成立"),
        ("inference-call-id 原值直通",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "x-codex-inference-call-id"),
             ["x-codex-inference-call-id", INFER_ID]))],
         [case], "原值直通"),
        ("inference-call-id 丢失",
         [mutate(lambda h, b: h.remove(next(x for x in h if x[0] == "x-codex-inference-call-id")))],
         [case], "缺必需头 x-codex-inference-call-id"),
        ("/responses 请求体未压缩（真客户端默认 zstd）",
         [mutate(lambda h, b: h.remove(next(x for x in h if x[0] == "content-encoding")))],
         [case], "请求体编码"),
        ("zstd 帧头不是 libzstd 流式默认（single segment + FCS）",
         [dict(row(), body_raw_head_hex="28b52ffd605b")], [case], "zstd 帧头"),
        ("echo server 未打 zstd 补丁（没记录帧头 / 解压结果）",
         [{k: v for k, v in row().items() if k not in ("body_raw_head_hex", "zstd_rc")}], [case],
         "没记录 body_raw_head_hex"),
        ("zstd 体解压失败",
         [dict(row(), zstd_rc=1)], [case], "解压失败"),
        ("/responses 用了 gzip 而不是 zstd",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "content-encoding"), ["content-encoding", "gzip"]))],
         [case], "请求体编码"),
        ("compact 不该压缩却带了 content-encoding",
         [dict(mutate(lambda h, b: (b.pop("client_metadata"), b.pop("prompt_cache_key", None),
                                    b.__setitem__("prompt_cache_key", "S"),
                                    h.remove(next(x for x in h if x[0] == "x-client-request-id")),
                                    h.remove(next(x for x in h if x[0] == "x-codex-inference-call-id")),
                                    h.append(["x-codex-installation-id", "I"]))),
               path="/backend-api/codex/responses/compact")],
         [dict(case, upstream="/backend-api/codex/responses/compact", contract="compact")],
         "不该压缩的端点"),
        ("顶层字段序错乱（map 字典序）",
         [mutate(lambda h, b: (b.__setitem__("model", b.pop("model"))))], [case], "字段序错乱"),
        ("未知顶层字段",
         [mutate(lambda h, b: b.__setitem__("zzz_unknown", 1))], [case], "不在字段序表里"),
        ("多发独立安装头",
         [mutate(lambda h, b: h.append(["x-codex-installation-id", "I"]))], [case],
         "不该发的头"),
        ("下划线别名", [mutate(lambda h, b: h.append(["session_id", "S"]))], [case], "不该发的头"),
        ("旧 openai-beta",
         [mutate(lambda h, b: h.append(["openai-beta", FORBIDDEN_BETA]))], [case], "openai-beta"),
        ("头 metadata 没剥工具清单",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "x-codex-turn-metadata"),
             ["x-codex-turn-metadata", json.dumps(dict(good_meta, tool_namespaces_info=TOOLS))]))],
         [case], "未剥掉 tool_namespaces_info"),
        ("体 metadata 工具清单被误删",
         [mutate(lambda h, b: b["client_metadata"].__setitem__(
             "x-codex-turn-metadata", json.dumps(good_meta)))], [case], "被误删"),
        ("body 缺设备身份",
         [mutate(lambda h, b: b["client_metadata"].pop("x-codex-installation-id"))], [case],
         "设备载体 body_install 缺失"),
        ("同请求两套设备身份",
         [mutate(lambda h, b: b["client_metadata"].__setitem__(
             "x-codex-installation-id", "OTHER"))], [case], "多个设备身份"),
        ("缓存键与会话头不同源",
         [mutate(lambda h, b: b.__setitem__("prompt_cache_key", "OTHER"))], [case], "关系不成立"),
        ("窗口序号被改写",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "x-codex-window-id"),
             ["x-codex-window-id", "S:0"]))], [case], "关系不成立"),
        ("metadata 窗口序号被改写",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "x-codex-turn-metadata"),
             ["x-codex-turn-metadata", json.dumps(dict(good_meta, window_number=0))]))],
         [case], "window_number 被改写"),
        ("窗口结构被压成裸 UUID",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "x-codex-window-id"),
             ["x-codex-window-id", "S"]))], [case], "关系不成立"),
        ("body 整段丢失", [row(body={})], [case], "没抓到请求体"),
        ("UA 缺尾部组",
         [mutate(lambda h, b: h.__setitem__(
             next(i for i, x in enumerate(h) if x[0] == "user-agent"),
             ["user-agent", "codex-tui/0.153.4"]))], [case], "UA 缺尾部"),
    ]

    for field in ("installation_id", "session_id", "thread_id", "window_id",
                  "window_number", "context_window_id", "turn_id", "root_turn_id"):
        def corrupt_body_metadata(h, b, field=field):
            bm = json.loads(b["client_metadata"]["x-codex-turn-metadata"])
            bm[field] = "WRONG"
            b["client_metadata"]["x-codex-turn-metadata"] = json.dumps(bm)
        negatives.append(("体内 metadata." + field + " 错误",
                          [mutate(corrupt_body_metadata)], [case],
                          "多个设备身份" if field == "installation_id" else "关系不成立"))
    negatives.append(("体内 metadata 缺失",
                      [mutate(lambda h, b: b["client_metadata"].pop("x-codex-turn-metadata"))],
                      [case], "缺必需 body"))
    for field in ("context_window_id", "turn_id", "root_turn_id"):
        def drop_body_metadata(h, b, field=field):
            bm = json.loads(b["client_metadata"]["x-codex-turn-metadata"])
            bm.pop(field)
            b["client_metadata"]["x-codex-turn-metadata"] = json.dumps(bm)
        negatives.append(("体内 metadata." + field + " 缺失",
                          [mutate(drop_body_metadata)], [case], "无法判定"))
    failures = 0
    for name, rows, cases, expect in negatives:
        problems, _ = check_rows(rows, cases)
        if not any(expect in p for p in problems):
            print("[SELFTEST FAIL] 反例「%s」没有被 %r 捕获，实得：%s" % (name, expect, problems))
            failures += 1
        else:
            print("  [ok] 反例被拦下：%s" % name)

    # 跨路径设备不一致
    two = [row(), dict(row(), path="/backend-api/codex/responses")]
    two[1]["body"]["client_metadata"]["x-codex-installation-id"] = "J"
    two[1]["body"]["client_metadata"]["x-codex-turn-metadata"] = json.dumps(
        dict(good_meta, installation_id="J", tool_namespaces_info=TOOLS))
    two[1]["headers"] = [list(x) for x in base_headers]
    idx = next(i for i, x in enumerate(two[1]["headers"]) if x[0] == "x-codex-turn-metadata")
    two[1]["headers"][idx] = ["x-codex-turn-metadata", json.dumps(dict(good_meta, installation_id="J"))]
    problems, _ = check_rows(two, [case, dict(case, label="T2")])
    if not any("不同设备身份" in p for p in problems):
        print("[SELFTEST FAIL] 跨路径设备不一致没被拦下：%s" % problems)
        failures += 1
    else:
        print("  [ok] 反例被拦下：跨路径设备不一致")

    failures += selftest_search()
    failures += selftest_ws()
    print("\n自检结果：%s" % ("全部反例均被拦下" if failures == 0 else "%d 条未被拦下" % failures))
    return 1 if failures else 0


def selftest_search():
    case = {"label": "search", "upstream": "/backend-api/codex/alpha/search", "contract": "search"}
    good_meta = {"session_id": "S", "thread_id": "S", "turn_id": "T",
                 "codex_version": "0.153.4", "model": "gpt-5.4"}

    def row(metadata, version="0.153.4", model="gpt-5.4"):
        return {"kind": "http", "path": case["upstream"],
                "headers": [["originator", "codex-tui"], ["user-agent", UA], ["version", version],
                            ["x-codex-turn-metadata", json.dumps(metadata)]],
                "body": {"id": "S", "model": model}}

    problems, devices = check_rows([row(good_meta)], [case])
    if problems or devices:
        print("[SELFTEST FAIL] 搜索 MCP 正样本被误报/当成设备载体：%s %s" % (problems, devices))
        return 1
    failures = 0
    for name, rs, expect in [
        ("搜索 metadata.codex_version 未对齐 version 头",
         [row(dict(good_meta, codex_version="0.0.1"))], "关系不成立"),
        ("搜索 metadata.model 未对齐出站 body.model",
         [row(dict(good_meta, model="wrong-model"))], "关系不成立"),
        ("搜索缺 version 头", [dict(row(good_meta), headers=[
            ["originator", "codex-tui"], ["user-agent", UA],
            ["x-codex-turn-metadata", json.dumps(good_meta)]])], "缺必需头 version"),
    ]:
        problems, _ = check_rows(rs, [case])
        if not any(expect in p for p in problems):
            print("[SELFTEST FAIL] 反例「%s」没有被 %r 捕获，实得：%s" % (name, expect, problems))
            failures += 1
        else:
            print("  [ok] 反例被拦下：%s" % name)
    for field in ("installation_id", "window_id", "window_number", "context_window_id",
                  "agent_name", "parent_turn_id", "root_turn_id", "request_kind", "compaction",
                  "history_ingest_requested", "forked_from_ordinal_exclusive", "tool_namespaces_info"):
        problems, _ = check_rows([row(dict(good_meta, **{field: None}))], [case])
        if not any("不该有的 metadata 字段 " + field in p for p in problems):
            print("[SELFTEST FAIL] 搜索多发字段未被拦下：%s" % field)
            failures += 1
        else:
            print("  [ok] 反例被拦下：搜索 metadata." + field)
    for field in good_meta:
        metadata = dict(good_meta)
        del metadata[field]
        problems, _ = check_rows([row(metadata)], [case])
        if not any("缺必需 metadata 字段 " + field in p for p in problems):
            print("[SELFTEST FAIL] 搜索缺字段未被拦下：%s" % field)
            failures += 1
        else:
            print("  [ok] 反例被拦下：搜索缺 metadata." + field)
    return failures


def selftest_ws():
    good_meta = {"installation_id": "I", "session_id": "S", "thread_id": "S",
                 "turn_id": "T", "root_turn_id": "T", "window_id": "S:%d" % WINDOW_NUMBER,
                 "context_window_id": "C",
                 "window_number": WINDOW_NUMBER}
    hs_headers = [["originator", "codex-tui"], ["user-agent", UA], ["version", "0.153.4"],
                  ["session-id", "S"], ["thread-id", "S"], ["x-client-request-id", "S"],
                  ["x-codex-window-id", "S:%d" % WINDOW_NUMBER],
                  ["x-codex-turn-metadata", json.dumps(good_meta)],
                  ["openai-beta", "responses_websockets=2026-02-06"]] + \
                 [[k, v] for k, v in sorted(WS_CONDITIONAL.items())]
    frame_body = {"type": "response.create", "model": "gpt-5.4", "prompt_cache_key": "S",
                  "client_metadata": {"session_id": "S", "thread_id": "S",
                                      "x-codex-installation-id": "I",
                                      "x-codex-window-id": "S:%d" % WINDOW_NUMBER,
                                      "x-codex-turn-state": WS_TURN_STATE,
                                      "x-codex-turn-metadata": json.dumps(
                                          dict(good_meta, tool_namespaces_info=TOOLS))}}

    def rows(headers=None, frames=2, mutate_frame=None, drop_handshake=False):
        out = []
        if not drop_handshake:
            out.append({"kind": "ws_handshake", "path": WS_PATH,
                        "headers": [list(h) for h in (headers or hs_headers)]})
        for i in range(frames):
            b = json.loads(json.dumps(frame_body))
            number = WINDOW_NUMBER + i
            b["client_metadata"]["x-codex-window-id"] = "S:%d" % number
            b["client_metadata"]["x-codex-turn-metadata"] = json.dumps(
                dict(good_meta, window_id="S:%d" % number, window_number=number,
                     tool_namespaces_info=TOOLS))
            b["client_metadata"]["x-codex-ws-stream-request-start-ms"] = "1700000000000"
            if i == 1:
                b["client_metadata"]["x-codex-turn-state"] = WS_OWN_TURN_STATE
                b["client_metadata"]["x-codex-ws-stream-request-start-ms"] = WS_RESTAMPED
            if mutate_frame:
                mutate_frame(i, b)
            out.append({"kind": "ws_message", "opcode": 1, "body": b})
        return out

    def run(rs):
        p = []
        p += cross_device_problems(check_ws(rs, p)) or []
        return p

    ok = run(rows())
    if ok:
        print("[SELFTEST FAIL] WS 正样本被误报：%s" % ok)
        return 1

    def hs_without(name):
        return [list(x) for x in hs_headers if x[0] != name]

    def hs_with(name, value):
        h = hs_without(name)
        h.append([name, value])
        return h

    negatives = [
        ("WS 没有上游握手", rows(drop_handshake=True), "没有捕获到上游握手"),
        ("WS 只有首轮没有第二轮", rows(frames=1), "要求首轮与第二轮各一个"),
        ("WS 握手缺 session-id", rows(headers=hs_without("session-id")), "握手缺必需头"),
        ("WS 握手发了独立安装头",
         rows(headers=hs_with("x-codex-installation-id", "I")), "握手不该发的头"),
        ("WS 握手缺 WS 专用 Beta", rows(headers=hs_without("openai-beta")), "握手缺必需头"),
        ("WS 握手发的是旧 Beta",
         rows(headers=hs_with("openai-beta", FORBIDDEN_BETA)), "旧的 openai-beta"),
        ("WS 握手 Beta 值不对",
         rows(headers=hs_with("openai-beta", "responses=v1")), "不是 WS 协商值"),
        ("WS 握手 metadata 没剥工具清单",
         rows(headers=hs_with("x-codex-turn-metadata",
                              json.dumps(dict(good_meta, tool_namespaces_info=TOOLS)))),
         "握手兼容头未剥掉"),
        ("WS 握手窗口序号被改写",
         rows(headers=hs_with("x-codex-window-id", "S:0")), "握手关系不成立"),
        ("WS 握手 metadata 窗口序号被改写",
         rows(headers=hs_with("x-codex-turn-metadata",
                              json.dumps(dict(good_meta, window_number=0)))),
         "window_number 被改写"),
        ("WS 条件头没转发到上游",
         rows(headers=hs_without("x-openai-memgen-request")),
         "握手缺必需头 x-openai-memgen-request"),
        ("WS 条件头被改写",
         rows(headers=hs_with("x-responsesapi-include-timing-metrics", "false")), "被改写"),
        ("WS 第二轮帧丢了设备身份",
         rows(mutate_frame=lambda i, b: b["client_metadata"].pop("x-codex-installation-id")
              if i == 1 else None),
         "WS 第2轮帧 client_metadata 缺 x-codex-installation-id"),
        ("WS 第二轮帧换了会话",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__("session_id", "OTHER")
              if i == 1 else None),
         "WS 第2轮帧 session 与握手头不同源"),
        ("WS 帧设备身份与握手不同",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-installation-id", "J")),
         "不同设备身份"),
        ("WS 帧缓存键与握手会话不同源",
         rows(mutate_frame=lambda i, b: b.__setitem__("prompt_cache_key", "OTHER")),
         "prompt_cache_key != 握手 session-id"),
        ("WS 第二轮缺缓存键",
         rows(mutate_frame=lambda i, b: b.pop("prompt_cache_key") if i == 1 else None),
         "缺必需 body 字段 prompt_cache_key"),
        ("WS 第二轮错线程",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__("thread_id", "OTHER")
              if i == 1 else None),
         "第2轮帧关系不成立"),
        ("WS 第二轮缺嵌入 metadata",
         rows(mutate_frame=lambda i, b: b["client_metadata"].pop("x-codex-turn-metadata")
              if i == 1 else None),
         "缺必需 body 字段 client_metadata.x-codex-turn-metadata"),
        ("WS 第二轮窗口被握手旧值覆盖",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-window-id", "S:%d" % WINDOW_NUMBER) if i == 1 else None),
         "第2轮帧关系不成立"),
        ("WS 第二轮嵌入身份错误",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-turn-metadata", json.dumps(dict(good_meta, session_id="OTHER",
                                                     tool_namespaces_info=TOOLS)))
              if i == 1 else None),
         "第2轮帧关系不成立"),
        ("WS 握手带了 turn-state（真客户端握手传 None）",
         rows(headers=hs_with("x-codex-turn-state", "TS")), "握手不该发的头"),
        ("WS 握手缺 version 头（provider 头）",
         rows(headers=hs_without("version")), "握手缺必需头 version"),
        ("WS 握手 version 与 UA 版本段不同源",
         rows(headers=hs_with("version", "9.9.9")), "握手关系不成立"),
        ("WS 握手把 inference-call-id 带上了",
         rows(headers=hs_with("x-codex-inference-call-id", "X")), "握手不该发的头"),
        ("WS 首帧没有补入握手上的 turn-state",
         rows(mutate_frame=lambda i, b: b["client_metadata"].pop("x-codex-turn-state")
              if i == 0 else None),
         "缺 client_metadata.x-codex-turn-state"),
        ("WS 第二轮帧自带的 turn-state 被握手值覆盖",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-turn-state", WS_TURN_STATE) if i == 1 else None),
         "turn-state 被覆盖"),
        ("WS 帧顶层字段序错乱（type 不在最前）",
         rows(mutate_frame=lambda i, b: b.__setitem__("type", b.pop("type"))),
         "字段序错乱"),
        ("WS 首帧没有盖 stream-request-start-ms",
         rows(mutate_frame=lambda i, b: b["client_metadata"].pop("x-codex-ws-stream-request-start-ms")
              if i == 0 else None),
         "缺 client_metadata.x-codex-ws-stream-request-start-ms"),
        ("WS 第二帧沿用了客户端自带的 stream-request-start-ms（没有在发送边界重盖）",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-ws-stream-request-start-ms", WS_OWN_STREAM_START) if i == 1 else None),
         "没有在发送边界重盖"),
        ("WS 帧 stream-request-start-ms 不是十进制毫秒",
         rows(mutate_frame=lambda i, b: b["client_metadata"].__setitem__(
             "x-codex-ws-stream-request-start-ms", "abc") if i == 0 else None),
         "不是十进制毫秒"),
    ]
    for field in ("context_window_id", "turn_id", "root_turn_id"):
        for remove in (False, True):
            def corrupt_frame_metadata(i, b, field=field, remove=remove):
                if i != 1:
                    return
                bm = json.loads(b["client_metadata"]["x-codex-turn-metadata"])
                if remove:
                    bm.pop(field)
                else:
                    bm[field] = "WRONG"
                    if field == "turn_id":
                        bm["root_turn_id"] = "WRONG"
                b["client_metadata"]["x-codex-turn-metadata"] = json.dumps(bm)
            negatives.append(("WS 第二轮 %s %s" % (field, "缺失" if remove else "错误"),
                              rows(mutate_frame=corrupt_frame_metadata), "第2轮帧关系不成立"))

    failures = 0
    for name, rs, expect in negatives:
        problems = run(rs)
        if not any(expect in p for p in problems):
            print("[SELFTEST FAIL] 反例「%s」没有被 %r 捕获，实得：%s" % (name, expect, problems))
            failures += 1
        else:
            print("  [ok] 反例被拦下：%s" % name)
    return failures


# ── WS 入口：网关的 WS 只能由路由决定（handler 的 ResponsesWebSocket），
# HTTP 请求进不去，所以必须真的连一次。服务器没有 ws 库，这里用裸 socket 实现
# 客户端最小子集：握手 + 掩码文本帧 + 读帧。────────────────────────────────

WS_PATH = "/v1/responses"  # 与 HTTP 用例同前缀；WS 由方法+Upgrade 头区分（routes/gateway.go:229）
# 真实 WS 握手条件性携带这两个头（前者 build_responses_compatibility_headers，
# 后者 build_websocket_headers 直接插入 client.rs:1262）。klno.8 才把它们加进
# WS 入站转发白名单，所以探针必须主动带上并断言原样到达，否则该修复无线上证据。
WS_CONDITIONAL = {"x-openai-memgen-request": "true",
                  "x-responsesapi-include-timing-metrics": "true"}
WS_HANDSHAKE_CONTRACT = {
    # 真实 WS 握手：build_websocket_headers（core/src/client.rs:1235）——
    # 会话三件套 + window + turn-metadata + originator + WS 专用 Beta；不发独立安装头。
    "required_headers": ["originator", "user-agent", "version", "session-id", "thread-id",
                         "x-client-request-id", "x-codex-window-id", "x-codex-turn-metadata",
                         "openai-beta"] + sorted(WS_CONDITIONAL),
    # 真客户端握手显式传 turn_state=None（core/src/client.rs:1241），turn-state 只在帧内
    # client_metadata（client.rs:1792-1793，OnceLock 有值才带）；version 是 provider 头
    # （model-provider-info/src/lib.rs:397），握手经 merge_request_headers 同样带。
    "forbidden_headers": ["x-codex-turn-state", "x-codex-installation-id",
                          "session_id", "conversation_id", "x-codex-inference-call-id"],
    "device_carrier": ["meta_install"],
    "relations": [
        ("h:version", "expr:ua_version"),
        ("h:session-id", "h:thread-id"),
        ("h:thread-id", "h:x-client-request-id"),
        ("h:x-codex-window-id", "expr:thread_window"),
        ("m:session_id", "h:session-id"),
    ],
}

# 窗口属于当前帧，不能固定成握手值。这个受控探针仅递增窗口序号，
# 两帧的 turn/root/context 输入刻意相同，因此这些字段须与握手同源；
# 这不是“一切真实 WS 后续轮次都等于握手”的通用协议断言。
WS_FRAME_CONTRACT = {
    "required_body": ["type", "prompt_cache_key", "client_metadata.session_id",
                      "client_metadata.thread_id", "client_metadata.x-codex-installation-id",
                      "client_metadata.x-codex-window-id", "client_metadata.x-codex-turn-metadata"],
    # 首帧不带 turn-state → 网关用入站握手上的值补进帧内；第二帧自带 → 原样保留。
    "turn_state_by_turn": {1: WS_TURN_STATE, 2: WS_OWN_TURN_STATE},
    # 两帧都必须带十进制毫秒；第二帧自带的值必须被发送边界重盖，出站不得仍是它。
    "stream_start_by_turn": {1: None, 2: None},
    "stream_start_rejects": {2: WS_OWN_STREAM_START},
    "body_order": WS_CREATE_ORDER,
    "relations": [
        ("b:client_metadata.thread_id", "h:thread-id"),
        ("b:client_metadata.x-codex-window-id", "expr:thread_window"),
        ("bm:session_id", "b:client_metadata.session_id"),
        ("bm:thread_id", "b:client_metadata.thread_id"),
        ("bm:installation_id", "b:client_metadata.x-codex-installation-id"),
        ("bm:window_id", "b:client_metadata.x-codex-window-id"),
        ("bm:window_number", "expr:window_number"),
        ("bm:turn_id", "bm:root_turn_id"),
        ("bm:turn_id", "m:turn_id"),
        ("bm:root_turn_id", "m:root_turn_id"),
        ("bm:context_window_id", "m:context_window_id"),
    ],
}


def ws_frame(payload, opcode=1):
    data = payload.encode() if isinstance(payload, str) else payload
    header = bytes([0x80 | opcode])
    mask = os.urandom(4)
    n = len(data)
    if n < 126:
        header += bytes([0x80 | n])
    elif n < 65536:
        header += bytes([0x80 | 126]) + struct.pack("!H", n)
    else:
        header += bytes([0x80 | 127]) + struct.pack("!Q", n)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return header + mask + masked


def ws_drain(sock, seconds):
    """把网关回推的帧读掉，防止内核缓冲写满导致网关侧阻塞。内容不做解析。"""
    deadline = time.time() + seconds
    sock.settimeout(0.5)
    while time.time() < deadline:
        try:
            if not sock.recv(65536):
                return False
        except socket.timeout:
            continue
        except OSError:
            return False
    return True


def ws_turn_state(session):
    """探针入站握手上的不透明 turn-state。真客户端握手传 None（core/src/client.rs:1241），只在
    帧内 client_metadata 携带（client.rs:1792-1793）；探针在入站握手上带一份，证明网关会把它
    从出站握手上剥掉、补进不带 turn-state 的首帧，而第二帧自带的值原样保留。"""
    _ = session
    return WS_TURN_STATE


def ws_probe(session, problems):
    """连一次网关 WS 入口，同一条连接上发两轮 response.create。失败直接记 problem。"""
    host, port = "127.0.0.1", 18080
    tm = turn_meta(session)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
        "authorization: Bearer %s\r\noriginator: codex-tui\r\nuser-agent: %s\r\nversion: 0.153.4\r\n"
        "session-id: %s\r\nthread-id: %s\r\nx-client-request-id: %s\r\n"
        "x-codex-installation-id: %s\r\nx-codex-window-id: %s:%d\r\nx-codex-turn-metadata: %s\r\n"
        "x-codex-turn-state: %s\r\nx-codex-inference-call-id: %s\r\n%s\r\n"
        % (WS_PATH, host, port, key, KEY, UA, session, session, session, INSTALL,
           session, WINDOW_NUMBER, tm, ws_turn_state(session), INFER_ID,
           "".join("%s: %s\r\n" % kv for kv in sorted(WS_CONDITIONAL.items())))
    )
    try:
        sock = socket.create_connection((host, port), timeout=20)
        sock.settimeout(20)
        sock.sendall(req.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = sock.recv(4096)
            if not chunk:
                break
            head += chunk
        status = head.split(b"\r\n", 1)[0].decode(errors="replace")
        if "101" not in status:
            problems.append("WS 握手未升级：%s" % status[:160])
            sock.close()
            return
        for turn in (1, 2):
            cm = meta(session, WINDOW_NUMBER + turn - 1)
            if turn == 2:
                cm["x-codex-turn-state"] = WS_OWN_TURN_STATE
                cm["x-codex-ws-stream-request-start-ms"] = WS_OWN_STREAM_START
            payload = {"type": "response.create", "model": "gpt-5.4", "stream": True,
                       "prompt_cache_key": session, "client_metadata": cm,
                       "input": [{"type": "message", "role": "user", "content": "hi %d" % turn}]}
            sock.sendall(ws_frame(json.dumps(payload)))
            print("  sent: WS 第%d轮 response.create" % turn, flush=True)
            if not ws_drain(sock, 4):
                if turn == 1:
                    problems.append("WS 连接在第二轮之前就被关闭，无法验证复用同一连接的第二轮")
                break
        try:
            sock.sendall(ws_frame(struct.pack("!H", 1000), 8))
        except OSError:
            pass
        sock.close()
    except Exception as e:
        problems.append("WS 探测异常：%r" % e)


def check_ws(rows, problems):
    handshakes = [r for r in rows if r.get("kind") == "ws_handshake"]
    frames = [r for r in rows if r.get("kind") == "ws_message"]
    if not handshakes:
        problems.append("WS 没有捕获到上游握手（网关没有建立 WS 出站）")
        return {}
    if len(handshakes) != 1:
        problems.append("WS 要求恰好一个上游握手，实得 %d" % len(handshakes))
    if len(frames) != 2:
        problems.append("WS 只捕获到 %d 个上游帧，要求首轮与第二轮各一个" % len(frames))

    devices = {}
    hs = handshakes[0]
    meta_hdr = parse_meta(hdr(hs, "x-codex-turn-metadata"), "握手 turn-metadata", "WS", problems)
    ctx = {"row": hs, "body": {}, "meta": meta_hdr}
    spec = WS_HANDSHAKE_CONTRACT
    for name in spec["required_headers"]:
        if not (hdr(hs, name) or "").strip():
            problems.append("WS 握手缺必需头 %s" % name)
    for name in spec["forbidden_headers"]:
        if hdr(hs, name) is not None:
            problems.append("WS 握手不该发的头 %s=%r" % (name, hdr(hs, name)))
    beta = (hdr(hs, "openai-beta") or "").lower()
    if FORBIDDEN_BETA in beta:
        problems.append("WS 握手仍在发旧的 openai-beta %s" % FORBIDDEN_BETA)
    if "responses_websockets" not in beta:
        problems.append("WS 握手的 openai-beta 不是 WS 协商值：%r" % hdr(hs, "openai-beta"))
    if meta_hdr is not None and "tool_namespaces_info" in meta_hdr:
        problems.append("WS 握手兼容头未剥掉 tool_namespaces_info")
    for name, want in WS_CONDITIONAL.items():
        got = hdr(hs, name)
        if got is not None and got != want:
            problems.append("WS 握手条件头 %s 被改写：%r != %r" % (name, got, want))
    if meta_hdr is not None and meta_hdr.get("window_number") != WINDOW_NUMBER:
        problems.append("WS 握手 turn-metadata 的 window_number 被改写：%r != %r"
                        % (meta_hdr.get("window_number"), WINDOW_NUMBER))
    hs_install = jget(meta_hdr or {}, "installation_id")
    if not hs_install:
        problems.append("WS 握手设备载体 meta_install 缺失")
    else:
        devices.setdefault(hs_install, []).append("WS 握手")
    for left, right in spec["relations"]:
        lv, rv = resolve(left, ctx), resolve(right, ctx)
        if lv is None or rv is None:
            problems.append("WS 握手关系 %s == %s 无法判定（%r / %r）" % (left, right, lv, rv))
        elif lv != rv:
            problems.append("WS 握手关系不成立 %s(%r) != %s(%r)" % (left, lv, right, rv))

    # 每一轮帧的 client_metadata 必须与握手同源——真实客户端一条连接内两者
    # 出自同一份 CodexResponsesMetadata。
    for i, fr in enumerate(frames[:2], start=1):
        raw = fr.get("body")
        fb = raw if isinstance(raw, dict) else (parse_meta(raw, "帧体", "WS", problems) or {})
        cm = fb.get("client_metadata") or {}
        if not isinstance(cm, dict):
            problems.append("WS 第%d轮帧 client_metadata 必须是对象" % i)
            cm = {}
        meta_body = parse_meta(cm.get("x-codex-turn-metadata"), "帧 turn-metadata", "WS", problems)
        frame_ctx = {"row": hs, "body": fb, "meta": meta_hdr, "body_meta": meta_body,
                     "window_number": WINDOW_NUMBER + i - 1}
        for field in WS_FRAME_CONTRACT["required_body"]:
            value = jget(fb, field)
            if not isinstance(value, str) or not value.strip():
                problems.append("WS 第%d轮帧缺必需 body 字段 %s" % (i, field))
        if fb.get("type") != "response.create":
            problems.append("WS 第%d轮帧不是 response.create" % i)
        want_state = WS_FRAME_CONTRACT["turn_state_by_turn"].get(i)
        got_state = cm.get("x-codex-turn-state")
        if not got_state:
            problems.append("WS 第%d轮帧缺 client_metadata.x-codex-turn-state（首帧应由握手值补入）" % i)
        elif got_state != want_state:
            problems.append("WS 第%d轮帧 turn-state 被覆盖或错位：%r != %r" % (i, got_state, want_state))
        want_start = WS_FRAME_CONTRACT["stream_start_by_turn"].get(i)
        got_start = cm.get("x-codex-ws-stream-request-start-ms")
        if not isinstance(got_start, str) or not got_start.isdigit():
            problems.append("WS 第%d轮帧缺 client_metadata.x-codex-ws-stream-request-start-ms 或不是十进制毫秒：%r"
                            % (i, got_start))
        elif want_start is not None and got_start != want_start:
            problems.append("WS 第%d轮帧的 stream-request-start-ms 不是期望值：%r != %r" % (i, got_start, want_start))
        elif got_start == WS_FRAME_CONTRACT["stream_start_rejects"].get(i):
            problems.append("WS 第%d轮帧的 stream-request-start-ms 没有在发送边界重盖：%r" % (i, got_start))
        if fb:
            problems += order_problems("WS 第%d轮帧" % i, list(fb.keys()), WS_FRAME_CONTRACT["body_order"])
        for left, right in WS_FRAME_CONTRACT["relations"]:
            lv, rv = resolve(left, frame_ctx), resolve(right, frame_ctx)
            if lv is None or rv is None or lv != rv:
                problems.append("WS 第%d轮帧关系不成立 %s(%r) != %s(%r)" % (i, left, lv, right, rv))
        if meta_body is not None and meta_body.get("tool_namespaces_info") != TOOLS:
            problems.append("WS 第%d轮帧体内工具清单被误删或改写" % i)
        for field in ("session_id", "thread_id", "x-codex-installation-id"):
            if not cm.get(field):
                problems.append("WS 第%d轮帧 client_metadata 缺 %s" % (i, field))
        if cm.get("session_id") and cm["session_id"] != hdr(hs, "session-id"):
            problems.append("WS 第%d轮帧 session 与握手头不同源（%r != %r）" %
                            (i, cm["session_id"], hdr(hs, "session-id")))
        if isinstance(cm.get("x-codex-installation-id"), str) and cm["x-codex-installation-id"].strip():
            devices.setdefault(cm["x-codex-installation-id"], []).append("WS 第%d轮帧" % i)
        if fb.get("prompt_cache_key") != hdr(hs, "session-id"):
            problems.append("WS 第%d轮帧 prompt_cache_key != 握手 session-id" % i)
    return devices


# ── 发送 ──────────────────────────────────────────────────────────────────

def send(case):
    args = ["curl", "-s", "-o", "/dev/null", "-m", "25", "-X", "POST", GW + case["path"],
            "-H", "authorization: Bearer " + KEY, "-H", "content-type: application/json"]
    for k, v in case["headers"].items():
        args += ["-H", "%s: %s" % (k, v)]
    args += ["-d", json.dumps(case["body"])]
    proc = subprocess.run(args, capture_output=True)
    case["curl_rc"] = proc.returncode
    print("  sent: %-16s curl_rc=%d" % (case["label"], proc.returncode), flush=True)


def main():
    if not KEY or any(ch in KEY for ch in "\r\n"):
        print("需要通过 SUB2API_REHEARSAL_API_KEY 提供有效的预演凭据", file=sys.stderr)
        return 2
    cases = build_cases()
    start = sum(1 for _ in open(CAP))
    problems = []
    print("== 发送 %d 个 HTTP 用例 + 1 条 WS 会话（全部落在 echo server，不出网） ==" % len(cases),
          flush=True)
    for case in cases:
        send(case)
    ws_probe(sid(7), problems)
    time.sleep(2)

    rows = [json.loads(l) for l in open(CAP)][start:]
    http_rows = [r for r in rows if r.get("kind") == "http"]
    ws_rows = [r for r in rows if str(r.get("kind", "")).startswith("ws_")]
    print("\n== 捕获 %d 条 HTTP 出站 + %d 条 WS 事件 ==" % (len(http_rows), len(ws_rows)), flush=True)
    for r in http_rows:
        print("  http %s" % r.get("path"))
    for r in ws_rows:
        print("  %s %s" % (r.get("kind"), r.get("path") or ""))

    http_problems, devices = check_rows(http_rows, cases, cross_check=False)
    problems += http_problems
    for k, v in check_ws(ws_rows, problems).items():
        devices.setdefault(k, []).extend(v)
    problems += cross_device_problems(devices)

    print("\n== 设备一致性（按端点声明的载体取值）==")
    for k, v in devices.items():
        print("  %s <- %s" % (k, sorted(set(v))))

    print("\n== 结论 ==")
    if problems:
        for p in sorted(set(problems)):
            print("  [FAIL] " + p)
        return 1
    print("  探针契约通过（%d HTTP 用例 + WS 两轮；第二轮窗口递增）" % len(cases))
    return 0


if __name__ == "__main__":
    sys.exit(selftest() if "--selftest" in sys.argv else main())
