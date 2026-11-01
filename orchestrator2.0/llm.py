# -*- coding: utf-8 -*-
"""LLM 算力层：dify / sfm 双后端，统一两个入口：

    llm_complete(instruction, context)   中间步骤（意图识别），非流式拿全文本
    llm_stream(instruction, context)     最终回答，流式 yield 文本块
    reset_caches()                       配置变更后清探测/会话缓存（settings 调用）

instruction = 角色指令（prompts.py 里的 *_PROMPT）
context     = 拼好的上下文/用户输入

平台侧一个 开始→LLM→回复 三节点工作流，双入参约定：
    Dify: inputs.query = context，inputs.metadata = instruction
          （自动探测 workflow/chatflow 两种应用类型，chatflow 的
           conversation_id 恒空保证每次调用相互独立、无记忆串味）
    SFM:  message.text = context，message.metadata = {"prompt": instruction}
          （会话由本层自管理：createSession 缓存复用，失效自动重建）
"""
from __future__ import print_function

import json
import re

import requests

import config

THINK_RE = re.compile(u"<think>.*?</think>", re.S)


def strip_think(text):
    return THINK_RE.sub(u"", text or u"").strip()


def _to_text(line):
    """D4：统一把 iter_lines 的产物归一化为 unicode。
    requests 的 iter_lines(decode_unicode=True) 在 py2 下依赖 Response.encoding，
    未设置时会返回 bytes；py3 下遇到异常编码可能抛错。统一按 utf-8 容错解码，
    SSE 事件解析分支就不用在 str/unicode/bytes 之间来回判定。"""
    if line is None:
        return u""
    if isinstance(line, bytes):
        try:
            return line.decode("utf-8", "replace")
        except Exception:
            return u""
    return line


# ---------------- Dify（自动兼容 workflow / chatflow 两种应用类型） ----------------
# 前提：开始节点有 query（chatflow 用 sys.query）+ metadata 两个入参，
# metadata 绑定为 LLM 系统提示词。
_dify_mode = {"mode": None}  # "workflow" | "chat"，首次调用自动探测


def _dify_headers():
    return {"Authorization": "Bearer " + config.get("DIFY_API_KEY"),
            "Content-Type": "application/json"}


def _dify_call(instruction, context, mode):
    """按应用类型调 Dify，返回 (kind, response)。kind: workflow/chat"""
    if _dify_mode["mode"] in (None, "workflow"):
        payload = {"inputs": {"query": context or u"", "metadata": instruction},
                   "response_mode": mode, "user": "pipeline"}
        r = requests.post(config.get("DIFY_API_BASE") + "/workflows/run",
                          headers=_dify_headers(), json=payload,
                          timeout=config.LLM_TIMEOUT, stream=(mode == "streaming"))
        if r.status_code == 400 and "not_workflow_app" in r.text:
            _dify_mode["mode"] = "chat"   # 是 Chatflow，换路由重试
        else:
            if r.status_code != 200:
                raise RuntimeError(u"Dify 调用失败（HTTP {}）：{}".format(
                    r.status_code, r.text[:300]))
            return "workflow", r
    # Chatflow：query 是用户消息，metadata 是开始节点变量；conversation_id 恒空
    # 保证每次调用相互独立（无记忆串味）
    payload = {"inputs": {"metadata": instruction}, "query": context or u"",
               "response_mode": mode, "conversation_id": "", "user": "pipeline"}
    r = requests.post(config.get("DIFY_API_BASE") + "/chat-messages",
                      headers=_dify_headers(), json=payload,
                      timeout=config.LLM_TIMEOUT, stream=(mode == "streaming"))
    if r.status_code != 200:
        raise RuntimeError(u"Dify 调用失败（HTTP {}）：{}".format(
            r.status_code, r.text[:300]))
    return "chat", r


def _dify_pick_output(kind, resp):
    """blocking 模式取完整回答"""
    data = resp.json()
    if kind == "chat":
        answer = data.get("answer")
        if answer:
            return answer
        raise RuntimeError(u"Dify 无回答：{}".format(json.dumps(data, ensure_ascii=False)[:300]))
    d = (data.get("data") or {})
    if d.get("status") not in (None, "succeeded"):
        raise RuntimeError(u"Dify 工作流失败：{}".format(d.get("error")))
    outputs = d.get("outputs") or {}
    if isinstance(outputs.get("text"), str):
        return outputs["text"]
    for v in outputs.values():
        if isinstance(v, str) and v:
            return v
    raise RuntimeError(u"Dify 工作流无文本输出：{}".format(
        json.dumps(outputs, ensure_ascii=False)[:300]))


def dify_complete(instruction, context):
    kind, resp = _dify_call(instruction, context, "blocking")
    return strip_think(_dify_pick_output(kind, resp))


def dify_stream(instruction, context):
    kind, resp = _dify_call(instruction, context, "streaming")
    # D4：拿 bytes，自己解；避免 py2 下 iter_lines(decode_unicode=True) 行为抖动
    for raw_line in resp.iter_lines(decode_unicode=False):
        line = _to_text(raw_line)
        if not line or not line.startswith(u"data:"):
            continue
        raw = line[len(u"data:"):].strip()
        if not raw or raw == u"[DONE]":
            continue
        try:
            evt = json.loads(raw)
        except ValueError:
            continue
        ev = evt.get("event")
        if ev == "text_chunk":          # workflow 流式
            t = (evt.get("data") or {}).get("text") or ""
            if t:
                yield t
        elif ev == "message":           # chatflow 流式
            t = evt.get("answer") or ""
            if t:
                yield t
        elif ev == "workflow_finished":
            d = evt.get("data") or {}
            if d.get("status") != "succeeded":
                raise RuntimeError(u"Dify 工作流失败：{}".format(d.get("error")))


# ---------------- SFM（/run，每次调用一次性 createSession） ----------------
# 不使用模块级缓存：Flask threaded 模式下多用户并发会复用同一 sessionId，
# SFM 侧按 session 积累上下文时会跨用户串味。每次 LLM 调用都创建新会话，
# 用完即弃（和 Dify 的 conversation_id="" 等价）。


def _sfm_headers():
    return {"Authorization": "Bearer " + config.get("SFM_API_KEY"),
            "Content-Type": "application/json"}


def sfm_get_session():
    body = {"agentCode": config.get("SFM_AGENT_CODE")}
    if config.get("SFM_AGENT_VERSION"):
        body["agentVersion"] = config.get("SFM_AGENT_VERSION")
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/createSession",
                          headers=_sfm_headers(), json=body, timeout=30)
    except requests.RequestException as e:
        return None, u"SFM 创建会话失败：{}".format(e)
    try:
        resp = r.json()
    except ValueError:
        resp = {}
    code = ((resp.get("data") or {}).get("uniqueCode")) if resp.get("success") else None
    if not code:
        return None, u"SFM 创建会话失败（HTTP {}）：{}".format(r.status_code, r.text[:300])
    return code, None


def _sfm_run_payload(session, instruction, context, stream):
    return {
        "sessionId": session,
        "stream": bool(stream),
        "delta": True,
        "trace": False,
        "message": {"text": context or u"",
                    "metadata": {"prompt": instruction},
                    "attachments": []},
    }


def _sfm_text_from_content(content):
    parts = []
    for c in content or []:
        if isinstance(c, dict) and c.get("type") == "text":
            t = c.get("text")
            v = t.get("value") if isinstance(t, dict) else t
            if v:
                parts.append(v)
    return u"".join(parts)


def sfm_complete(instruction, context):
    session, err = sfm_get_session()
    if err:
        raise RuntimeError(err)
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/run", headers=_sfm_headers(),
                          json=_sfm_run_payload(session, instruction, context, False),
                          timeout=config.LLM_TIMEOUT)
    except requests.RequestException as e:
        raise RuntimeError(u"SFM 调用失败：{}".format(e))
    try:
        resp = r.json()
    except ValueError:
        raise RuntimeError(u"SFM 返回非 JSON（HTTP {}）：{}".format(r.status_code, r.text[:300]))
    if not resp.get("success"):
        raise RuntimeError(u"SFM 执行失败：{}".format(r.text[:300]))
    msg = ((resp.get("data") or {}).get("message")) or {}
    return strip_think(_sfm_text_from_content(msg.get("content")))


def sfm_stream(instruction, context):
    session, err = sfm_get_session()
    if err:
        raise RuntimeError(err)
    try:
        r = requests.post(config.get("SFM_API_BASE") + "/run", headers=_sfm_headers(),
                          json=_sfm_run_payload(session, instruction, context, True),
                          stream=True, timeout=config.LLM_TIMEOUT)
    except requests.RequestException as e:
        raise RuntimeError(u"SFM 调用失败：{}".format(e)
                           )
    if r.status_code != 200:
        raise RuntimeError(u"SFM 调用失败（HTTP {}）：{}".format(r.status_code, r.text[:300]))
    buf = []
    # D4：拿 bytes，自己解；避免 py2 下 iter_lines(decode_unicode=True) 行为抖动
    for raw_line in r.iter_lines(decode_unicode=False):
        line = _to_text(raw_line)
        if line.startswith(u"data:"):
            if buf:
                for t in _sfm_parse_event(u"\n".join(buf)):
                    if t:
                        yield t
                buf = []
            buf = [line[len(u"data:"):].lstrip()]
        elif line == u"":
            if buf:
                for t in _sfm_parse_event(u"\n".join(buf)):
                    if t:
                        yield t
                buf = []
        elif buf:
            buf.append(line)
    if buf:
        for t in _sfm_parse_event(u"\n".join(buf)):
            if t:
                yield t


def _sfm_parse_event(raw):
    """解析一条 SFM SSE 事件，返回 [文本增量]（message.delta）"""
    try:
        evt = json.loads(raw)
    except ValueError:
        return []
    if evt.get("object") != "message.delta":
        return []
    return [_sfm_text_from_content(evt.get("content"))]


# ---------------- 对外统一入口 ----------------
def llm_complete(instruction, context):
    """中间步骤：非流式，返回完整文本（已剥 <think>）。
    instruction = 角色指令（进平台侧系统提示词），context = 上下文/用户输入"""
    if config.get("LLM_PLATFORM") == "dify":
        return dify_complete(instruction, context)
    return sfm_complete(instruction, context)


def llm_stream(instruction, context):
    """最终回答：流式生成器，yield 文本块（think 由上层流式过滤器剥离）"""
    if config.get("LLM_PLATFORM") == "dify":
        return dify_stream(instruction, context)
    return sfm_stream(instruction, context)


def reset_caches():
    """配置变更后调用：清空平台探测缓存（SFM 会话不缓存，无需清理）"""
    _dify_mode["mode"] = None
