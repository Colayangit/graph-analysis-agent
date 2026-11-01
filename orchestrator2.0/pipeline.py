# -*- coding: utf-8 -*-
"""编排层：对话主流程。run_pipeline(query, conv_id) 生成器：

    yield ("status", 文本)   阶段提示（前端灰色状态行，不进历史）
    yield 文本块             最终回答（流式）
    yield ("error", 消息)    出错

流程与设计原则（确定性层与 LLM 层严格分工）：

    ┌─ 意图识别（LLM+规则兜底）→ search / targeted / fact / chat 四分支
    │
    ├─ chat：通用回答（注入数据概况，可答图谱/系统类问题；图库断连时概况带提示）
    ├─ fact：事实/清单直答（定位实体→确定性证据查询→直答，不套报告模板；
    │       模板查不到时 _try_adhoc：LLM 生成只读 Cypher 过校验后直查直答）
    ├─ targeted：state 里有已选业务/候选 → 序号/ID/全名定位 → 影响分析；
    │       旧 selected + 无关新名称 → 自动转 search（可退出已选业务）
    └─ search：搜索候选 → 0 个(模糊兜底/adhoc/未找到) / 1 个(直接分析) / 多个(列清单)

    影响分析 _analyze（所有路径汇合）：
        确定性证据查询（store.evidence_rows，代码构建，不经 LLM 写 Cypher）
        → 代码分桶预计算结论（受影响候选/上游依赖/按角色关联，直接/间接分层，
           受影响业务连带承载系统，证据路径写死）
        → LLM 仅按 ANALYZE_PROMPT 排版成报告

    确定性层（零误差）：搜索定位 / 证据查询 / 去重 / 方向分桶 / 序号解析
    LLM 层（只做擅长的）：理解意图、组织语言、给建议
"""
from __future__ import print_function

import json
import re
import sys
import traceback

import config
import llm
import metrics
import prompts
import store

log = config.get_logger("pipeline")

# 保留给其它逻辑使用的宽松 JSON 定位（_extract_json 不再依赖它来切片，
# 而是用花括号平衡扫描第一段合法 JSON，避免 LLM 多段/带前后文本时贪婪吞并，E4）
JSON_RE = re.compile(u"\\{.*\\}", re.S)


def _balanced_json_slice(text):
    """从文本里扫出第一段花括号平衡且 json.loads 成功的子串，失败返回 None。
    避开字符串字面量里的括号；比 r'\\{.*\\}' 贪婪匹配稳。"""
    if not text:
        return None
    s = text
    n = len(s)
    i = 0
    while i < n:
        if s[i] != u"{":
            i += 1
            continue
        depth = 0
        in_str = False
        esc = False
        for j in range(i, n):
            ch = s[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == u"\\":
                    esc = True
                elif ch == u"\"":
                    in_str = False
                continue
            if ch == u"\"":
                in_str = True
            elif ch == u"{":
                depth += 1
            elif ch == u"}":
                depth -= 1
                if depth == 0:
                    chunk = s[i:j + 1]
                    try:
                        obj = json.loads(chunk)
                        return obj if isinstance(obj, dict) else None
                    except ValueError:
                        break
        i += 1
    return None
# 句尾动作词：'…底账变更'的'变更'是动作不是名称；'企业变更登记'的'变更'在中间不受影响
_TRAIL_RE = re.compile(
    u"(的变更|变更|变化|修改|调整|的影响|影响|有什么|什么|怎么样|如何|"
    u"吗|啊|吧|呀|啦|呢|了|么|嘛|哈|噢|哟|哇)$")
# 统计类问题（问数量）：候选搜索有截断上限，必须走 count() 聚合而不是数样本
# （py2 re 不支持 \uXXXX 转义，汉字写面量）
_COUNT_Q_RE = re.compile(
    u"(有多少|有多少个|有几个|有几条|有几家|有几种|多少个|多少家|多少条|"
    u"多少种|多少家|几家|几种|若干|约有|约为|大约有|总共有|总共多少|"
    u"合计有|合计多少|数量是|数量为|总数|总量)")
# 追问句式（无业务名的清单跟进，如"具体哪16个"）：靠对话上下文延续话题
_FOLLOWUP_RE = re.compile(u"(具体哪|具体有哪|哪几个|哪些|列出来|列一下|列个清单|清单|明细|分别是什么|分别有哪|展开说说|展开讲讲)")
# Schema 元数据问题必须由代码读取官方模型回答，不能用 MATCH (n) 统计非空标签代替。
_NODE_TYPE_Q_RE = re.compile(u"(点类型|节点类型|节点标签|标签类型)")
_TYPE_FOLLOWUP_RE = re.compile(u"(哪.{0,4}种|分别.{0,4}(什么|哪些)|列.{0,3}(出|一下|清单))")


def clean_keyword(kw):
    return _TRAIL_RE.sub(u"", (kw or u"").strip()).strip() or (kw or u"").strip()


def _kw_related(kw, name):
    """意图关键词与业务名是否相关。不相关=用户在提新业务/新话题。
    短关键词（<=1 字）或非中英字符时直接判为无关：
    例如 selected='税收优惠办理'，用户再提'税'，短字符 CONTAINS 会误判为相关而
    困在旧 selected 上（A6）。要求：至少 2 字 且 存在实质字符 且 实质子串关系。"""
    kw = _text(kw).strip()
    name = _text(name).strip()
    if not kw or not name:
        return False
    core = re.sub(u"[^一-龥A-Za-z0-9]+", u"", kw)
    if len(core) < 2:
        return False
    return core in name or name in core


def _text(v):
    """任意值转 unicode 文本（py2 下 str(中文) 会炸，统一走这里）"""
    if v is None:
        return u""
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return u"{}".format(v)


def _sample_rows_by_role(rows, per_group=40, total_cap=400):
    """按 (role, rel, dir, nb_type) 分桶后抽样，避免把全部 rows 塞 LLM。
    返回 (sampled, note)；note 非空表示抽样过，提示 LLM 原始行只是复核样本。"""
    if not rows:
        return [], u""
    total = len(rows)
    if total <= total_cap:
        return rows, u""
    buckets = {}
    order = []
    for r in rows:
        key = (_text(r.get("role")), _text(r.get("rel")),
               _text(r.get("dir")), _text(r.get("nb_type")))
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(r)
    sampled = []
    dropped = {}
    for key in order:
        bucket = buckets[key]
        keep = bucket[:per_group]
        sampled.extend(keep)
        if len(bucket) > per_group:
            dropped[key] = len(bucket) - per_group
    # 总上限再做一次整体封顶（避免分桶特别多时失控）
    extra = len(sampled) - total_cap
    if extra > 0:
        sampled = sampled[:total_cap]
    note = (u"（说明：原始行共 {total} 条，按角色分桶每桶最多留 {per} 条，"
            u"抽样后 {kept} 条进入上下文；完整候选以上面【受影响与关联候选】"
            u"分组为准，禁止用样本数量当总数）").format(
                total=total, per=per_group, kept=len(sampled))
    return sampled, note


def _extract_json(text):
    """从 LLM 输出抠 JSON（剥 <think>、容错前后废话），失败返回 {}。
    E4：用花括号平衡扫描首个合法 JSON，不再用 r'\\{.*\\}' 贪婪匹配——
    LLM 把两段 JSON 拼在一起或带花括号的解释文本时后者会一次吞光。"""
    text = llm.strip_think(text)
    obj = _balanced_json_slice(text)
    if obj is not None:
        return obj
    # 兜底：旧的宽松正则（极端场景 LLM 返回只有尾段 }）
    m = JSON_RE.search(text)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else {}
    except ValueError:
        return {}


def _intent(query, ctx, tasks=False):
    """意图识别 + 兜底（对应节点 4/5）。tasks=True（自动模式）时附带
    qa/writing 扩展规则，action 可能多返回两个任务域。"""
    text = llm.llm_complete(
        prompts.intent_prompt(tasks),
        prompts.context_block(store.history_text(ctx),
                              json.dumps(ctx["state"], ensure_ascii=False))
        + u"\n【本轮输入】\n" + query)
    r = _extract_json(text)
    action = r.get("action")
    keyword = _text(r.get("business_keyword")).strip()
    valid = ("search", "targeted", "fact", "chat") + \
        (("qa", "writing") if tasks else ())
    if action not in valid:
        # 兜底收紧：提取到了业务名 → search（宁可重搜，不可困在旧 selected 上）；
        # 没有业务名但有已选/候选 → targeted；都没有 → search（会在后面转 chat）
        state = ctx["state"]
        if keyword:
            action = "search"
        elif state.get("selected") or state.get("candidates"):
            action = "targeted"
        else:
            action = "search"
    if action in ("qa", "writing"):
        return {"action": action, "keyword": u"",
                "change_desc": _text(r.get("change_desc")).strip()}
    if action in ("search", "fact") and not keyword:
        action = "chat"
    return {"action": action, "keyword": keyword,
            "change_desc": _text(r.get("change_desc")).strip()}


def run_pipeline(query, conv_id, mode=u"auto"):
    """主入口。mode：auto（自动分流）/ analysis / qa / writing，
    手动模式优先；yield str（回答文本块）或 ("error", 消息)。
    顶层 except 把 traceback 打到 stdout（O3）——线上排障靠 systemd 日志。
    O2：记录分支占比与总耗时。"""
    metrics.inc("pipeline.requests")
    with metrics.Timer("pipeline.total"):
        try:
            for item in _run(query, conv_id, mode):
                yield item
        except Exception as e:  # LLM/图库等任何未预期异常
            metrics.inc("pipeline.exceptions")
            try:
                log.exception(u"未预期异常：%s", e)
            except Exception:
                pass
            yield ("error", u"处理出错：{}".format(e))


# 写作续写短语（上次任务是写作时，这些修改类输入继续写作而非重判）
_WRITE_REV_RE = re.compile(
    u"(再.{0,6}(正式|口语|简洁|精简|详细|长|短一些|改|润色|写一版)|正式(一些|点)|"
    u"口语(一点|些)|缩短|加长|扩写|精简|润色|改写|篇幅|换个语气|语气(正式|轻松)|"
    u"再修改|再润色|简短(一些|点)|详细(一些|点))")
_MODES = (u"auto", u"analysis", u"qa", u"writing")


def _run(query, conv_id, mode=u"auto"):
    yield ("status", u"正在理解问题…")
    ctx = store.get_context(conv_id)
    state = ctx["state"]
    mode = (_text(mode) or u"auto").lower()
    if mode not in _MODES:
        mode = u"auto"

    # “点类型有多少/哪几种”是 Schema 元数据，不交给 LLM 生成 MATCH (n)。
    # 后者只能看到有实例的标签，会把本图定义的 15 类错误回答成非空的 6 类。
    recent_history = store.history_text(ctx)
    asks_node_types = bool(_NODE_TYPE_Q_RE.search(query))
    follows_node_types = (bool(_TYPE_FOLLOWUP_RE.search(query))
                          and bool(_NODE_TYPE_Q_RE.search(recent_history)))
    if mode in (u"auto", u"analysis") and (asks_node_types or follows_node_types):
        yield ("status", u"正在读取图 Schema…")
        yield store.node_type_overview()
        merged = dict(state)
        merged["task"] = u"analysis"
        store.save_state(conv_id, merged)
        return

    # ---- 任务域分流：手动模式优先；auto 扩展意图 + 写作续写守卫 ----
    task = u"analysis"
    if mode in (u"qa", u"writing"):
        task = mode
        intent = {"action": task, "keyword": u"", "change_desc": u""}
    else:
        if _text(state.get("task")) == u"writing" and _WRITE_REV_RE.search(query):
            # 上次任务是写作且本轮是修改类短语：继续修改当前文稿，不重判
            task = u"writing"
            intent = {"action": u"writing", "keyword": u"", "change_desc": u""}
        else:
            intent = _intent(query, ctx, tasks=(mode == u"auto"))
            if intent["action"] in (u"qa", u"writing"):
                task = intent["action"]

    if task == u"qa":
        metrics.inc("pipeline.task.qa")
        for chunk in _qa(query, ctx, conv_id):
            yield chunk
        return
    if task == u"writing":
        metrics.inc("pipeline.task.writing")
        for chunk in _writing(query, ctx, conv_id):
            yield chunk
        return
    metrics.inc("pipeline.task.analysis")

    # ---- 业务分析域（原四分支流程）；顺带收集回答供智能写作当素材 ----
    # C5：无论正常结束 / 中断（GeneratorExit）/ 异常，都要把 task/last_report 落盘，
    #     否则下一轮读到的 last_report 还是上一次的旧内容（状态与消息不同步）。
    buf = []
    try:
        for item in _analysis(query, intent, state, ctx, conv_id):
            if isinstance(item, tuple):
                yield item
            else:
                buf.append(item)
                yield item
    finally:
        report = u"".join(buf).strip()
        # 用 merge_state 做锁内 read-modify-write，_analysis 在生成器中可能已经
        # save_state 过 candidates/selected，这里只覆盖 task/last_report 两键（C4）
        patch = {"task": u"analysis"}
        if intent.get("action") != u"chat" and report:
            patch["last_report"] = report[-4000:]   # 分析结果作为写作素材
        try:
            store.merge_state(conv_id, patch)
        except Exception:
            pass


def _qa(query, ctx, conv_id):
    """智能问答：通用知识/概念解释（与图数据库无关），可引用最近分析结果"""
    yield ("status", u"正在思考…")
    state = ctx["state"]
    last = _text(state.get("last_report"))
    ctx_text = (u"【对话历史】\n{}\n".format(store.history_text(ctx) or u"（无）") +
                (u"【最近分析结果】（问题相关时可引用其中事实）\n{}\n"
                 .format(last[-2500:]) if last else u"") +
                u"【本轮输入】\n" + query)
    # C5：中断也要把 task 落盘，否则下一轮 state.task 还是上一次的值
    try:
        for chunk in _stream(prompts.QA_PROMPT, ctx_text):
            yield chunk
    finally:
        try:
            store.merge_state(conv_id, {"task": u"qa"})
        except Exception:
            pass


def _writing(query, ctx, conv_id):
    """智能写作：起草/修改文稿。草稿与累计写作要求存会话状态——同一会话内
    连续修改（手动 writing 模式，或自动模式 + 修改类短语）始终基于当前文稿。"""
    state = ctx["state"]
    draft = _text(state.get("writing_draft"))
    material = _text(state.get("last_report"))
    reqs = [_text(r) for r in (state.get("writing_reqs") or [])
            if _text(r).strip()]
    if draft:
        yield ("status", u"正在修改文稿…")
        ctx_text = (u"【当前文稿】\n{}\n【历史写作要求】\n{}\n"
                    u"【分析素材】（事实依据，仅供核对）\n{}\n【修改要求】\n{}"
                    .format(draft, u"；".join(reqs[-8:]) or u"（无）",
                            material[-2000:] or u"（无）", query))
    else:
        yield ("status", u"正在起草…")
        ctx_text = (u"【写作要求】\n{}\n【分析素材】（事实依据，有则必须引用）\n{}\n"
                    u"【对话历史】\n{}"
                    .format(query, material[-3000:] or u"（无）",
                            store.history_text(ctx) or u"（无）"))
    # C5：中断也要把 draft/task/reqs 落盘；user 下次打开仍看到部分改稿
    buf = []
    try:
        for chunk in _stream(prompts.WRITING_PROMPT, ctx_text):
            buf.append(chunk)
            yield chunk
    finally:
        new_draft = u"".join(buf).strip()
        patch = {"task": u"writing",
                 "writing_reqs": (reqs + [query])[-8:]}
        if new_draft:
            patch["writing_draft"] = new_draft
        try:
            store.merge_state(conv_id, patch)
        except Exception:
            pass


def _analysis(query, intent, state, ctx, conv_id):
    # 纯序号/ID 回复且状态里有候选：一定是选择行为，强制 targeted
    # （意图识别偶尔会把"2"判成 search 重搜一轮，导致用户选择失效）
    if store._ordinal(query) and (state.get("candidates")
                                  or state.get("selected")):
        intent["action"] = "targeted"

    # ---------- chat 分支 ----------
    metrics.inc("pipeline.branch." + (_text(intent.get("action")) or "unknown"))
    if intent["action"] == "chat":
        # 统计/清单类追问（"总数多少""具体哪16个"）不带业务名，意图易误判成
        # chat 而拿全图概况回答（问企业开办总数却答 26775 个节点）。只要对话
        # 里有上下文，就转 fact 让 adhoc 延续话题；真问全图概况（无上下文）
        # 仍走 chat 的数据概况
        if (_COUNT_Q_RE.search(query) or _FOLLOWUP_RE.search(query)) \
                and (state.get("selected") or ctx["history"]):
            intent = {"action": "fact", "keyword": u"", "change_desc": u""}
        else:
            for chunk in _stream(prompts.DIRECT_ANSWER_PROMPT,
                                 prompts.context_block(store.history_text(ctx),
                                                       json.dumps(state, ensure_ascii=False))
                                 + u"\n【系统能力与数据概况】\n" + store.data_overview()
                                 + u"\n【本轮输入】\n" + query):
                yield chunk
            return  # 状态原样保留，不写

    # ---------- fact 分支：事实/清单类问题，直答不套影响分析模板 ----------
    if intent["action"] == "fact":
        for chunk in _fact(query, intent, state, conv_id, ctx):
            yield chunk
        return

    # ---------- targeted 分支 ----------
    if intent["action"] == "targeted":
        selected, src = store.pick_selected(query, intent["keyword"], state)
        # 退出守卫：本轮消息没有任何显式选择（只是落在旧 selected 上），
        # 而意图里提取到了与该业务无关的新名称 → 用户在提新业务，转 search 重搜
        if (selected and src == u"selected" and intent["keyword"]
                and not _kw_related(intent["keyword"], selected.get("name"))):
            selected = None  # 落入下方 search 分支
        else:
            if not selected:
                for chunk in _stream(prompts.CLARIFY_PROMPT,
                                     prompts.context_block(store.history_text(ctx),
                                                           json.dumps(state, ensure_ascii=False))
                                     + u"\n【本轮输入】\n" + query):
                    yield chunk
                return  # 保留原状态（候选还在）
            # 本轮是序号选择时，接回候选保存时的原始问题与变更描述
            if store._ordinal(query) and _text(state.get("original_query")):
                eff_query = _text(state.get("original_query"))
            else:
                eff_query = query
            eff_intent = {"change_desc": intent["change_desc"] or
                          _text(state.get("change_desc"))}
            for chunk in _analyze(eff_query, eff_intent, selected, ctx, conv_id):
                yield chunk
            return

    # ---------- search 分支 ----------
    yield ("status", u"正在搜索相关业务…")
    intent["keyword"] = clean_keyword(intent["keyword"])
    cand_labels = (store.core_sem().get("candidate_labels")
                   or config.CANDIDATE_LABELS)

    # 对象级变更优先：用户常说“修改某对象的业务规则”（如“卫生证书”），
    # 但该对象会命中多个业务子项。若图中存在唯一的非候选实体精确名称，
    # 直接以该实体为分析锚点，让 evidence_rows 按配置归并出全部相关业务，
    # 避免把同一对象拆成多个子项让用户盲选一个。
    exact_entities = store.business_search(intent["keyword"], labels=None, limit=50)
    candidate_set = set(cand_labels or [])
    exact_non_candidates = [c for c in exact_entities
                            if _text(c.get("name")).strip().lower() ==
                            _text(intent["keyword"]).strip().lower()
                            and c.get("business_type") not in candidate_set]
    if len(exact_non_candidates) == 1:
        selected = store._norm_selected(exact_non_candidates[0])
        if selected:
            for chunk in _analyze(query, intent, selected, ctx, conv_id):
                yield chunk
            return

    candidates = store.business_search(intent["keyword"],
                                       labels=cand_labels,
                                       limit=config.SEARCH_CANDIDATE_LIMIT)
    if len(candidates) == 0:
        # 全类兜底：业务类没命中，可能问的是系统/部门/表等支撑实体
        candidates = store.business_search(intent["keyword"], labels=None,
                                           limit=config.SEARCH_CANDIDATE_LIMIT)
    if len(candidates) == 0:
        # 模糊兜底：2 字滑窗再搜一次，命中则把相近业务列成可选候选
        # 分层模糊检索：候选业务无命中后再查全实体类型，优先返回对象/系统/
        # 部门等更高层节点；用户确认后由 evidence_rows 沿结构路径归并业务。
        fuzzy = store.fuzzy_search(intent["keyword"], limit=config.CANDIDATE_LIMIT,
                                   labels=None)
        if fuzzy:
            lines = [u"没有找到与“{}”精确匹配的业务，是不是想找以下业务？请回复序号选择："
                     .format(intent["keyword"])]
            for i, c in enumerate(fuzzy):
                status = store._text(c.get(u"办理状态")).strip()
                suffix = u"，" + status if status else u""
                lines.append(u"{}. 【{}】{}（{}{}）".format(
                    i + 1, c.get("business_type"), c.get("name"),
                    c.get("id"), suffix))
            yield u"\n".join(lines) + u"\n"
            store.save_state(conv_id, {
                "step": u"等待用户选择（模糊匹配）",
                "selected": None,
                "candidates": fuzzy,
                "original_query": query,
                "change_desc": intent["change_desc"],
            })
            return
        for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                             u"【用户问题】{}\n【搜索关键词】{}".format(query, intent["keyword"])):
            yield chunk
        store.save_state(conv_id, None)
        return

    if len(candidates) == 1:
        selected = store._norm_selected(candidates[0])
        for chunk in _analyze(query, intent, selected, ctx, conv_id):
            yield chunk
        return

    # 多个候选：代码直接生成带序号的列表（确定性，序号与 state.candidates 顺序一致），
    # 不过 LLM——之前 LLM 排版会把序号丢掉/打乱顺序，导致用户回复序号对不上
    # 命中达到上限时用 count() 查真实匹配总数并在表头声明，避免用户误以为只有这些
    cap_note = u""
    if len(candidates) >= config.SEARCH_CANDIDATE_LIMIT:
        counts = store.keyword_counts(intent["keyword"], labels=cand_labels)
        total = sum(c for _, c in counts)
        cap_note = (u"（匹配到 {} 个，已按相关度列出前 {} 个；没找到请回复业务 "
                    u"ID 或换更具体的名称）".format(total, len(candidates)))
    lines = [u"找到 {} 个相关业务{}，请回复序号选择：".format(
        len(candidates), cap_note)]
    for i, c in enumerate(candidates):
        status = store._text(c.get(u"办理状态")).strip()
        suffix = u"，" + status if status else u""
        lines.append(u"{}. 【{}】{}（{}{}）".format(
            i + 1, c.get("business_type"), c.get("name"), c.get("id"), suffix))
    yield u"\n".join(lines) + u"\n"
    store.save_state(conv_id, {
        "step": u"等待用户选择",
        "selected": None,
        "candidates": candidates,
        "original_query": query,
        "change_desc": intent["change_desc"],
    })
    return


def _try_adhoc(query, topic=None, hist=None):
    """自定义问题兜底：固定证据模板查不到时，让 LLM 按【图结构】生成一条
    只读 Cypher（store.graph_query 自带：写关键字/CALL 拒绝 + 标签/关系白名单
    校验 + 强制 LIMIT），执行后基于原始行直答。
    topic：业务关键词；hist：最近对话文本——追问类问题（"具体哪些/总数多少"）
    靠它们延续上下文，否则会脱离话题查全图。
    返回回答文本块列表；不适用/失败返回 []（上层走原 NOT_FOUND/空证据流程）。
    失败自动带错误信息重试一次。"""
    ctx = u"【图结构】\n{}".format(store.schema_text())
    if _text(topic).strip():
        ctx += u"\n【业务关键词】{}".format(_text(topic).strip())
    if _text(hist).strip():
        ctx += u"\n【最近对话】\n{}".format(_text(hist).strip())
    ctx += u"\n【用户问题】{}".format(query)
    for _attempt in range(2):
        r = _extract_json(llm.llm_complete(prompts.ADHOC_CYPHER_PROMPT, ctx))
        cypher = _text(r.get("cypher")).strip()
        params = r.get("parameters")
        if not isinstance(params, dict):
            params = {}
        if not cypher:
            log.warning(u"adhoc 未生成有效 Cypher: %s", query)
            ctx += u"\n【上次未生成有效 Cypher，请重新输出 JSON】"
            continue
        res = store.graph_query(cypher, params)
        if res.get("success"):
            rows = res.get("rows") or []
            if not rows:
                log.info(u"adhoc 查询结果为空: %s | %s", query, cypher)
                return []  # 查询合法但结果为空，交给原"未找到"话术更合适
            # 截断判定：行数达到生成语句里的 LIMIT → 确定性断言"不全"，
            # 不依赖回答模型自觉
            lim_m = re.search(u"LIMIT\\s+(\\d+)", cypher, re.I)
            truncated = bool(lim_m and len(rows) >= int(lim_m.group(1)))
            log.info(u"adhoc ok: %d 行%s | %s | %s", len(rows),
                     u"（截断）" if truncated else u"", query, cypher)
            trunc_note = (u"\n【重要】结果行数已达到查询 LIMIT 上限，【确定不全】："
                          u"回答必须明确说明「仅列出前 {N} 条、总数更多」，"
                          u"并建议用户缩小范围（如指定某类/某名称）后再问，"
                          u"【禁止】把本次行数说成总数").format(N=len(rows)) \
                if truncated else u""
            ans_ctx = (u"【用户问题】{}\n{}{}【查询语句】{}\n【查询结果】（共 {} 行）\n{}{}"
                       ).format(query,
                                (u"【业务关键词】{}\n".format(_text(topic).strip())
                                 if _text(topic).strip() else u""),
                                (u"【最近对话】\n{}\n".format(_text(hist).strip())
                                 if _text(hist).strip() else u""),
                                cypher, len(rows),
                                json.dumps(rows, ensure_ascii=False), trunc_note)
            chunks = []
            for ch in _stream(prompts.ADHOC_ANSWER_PROMPT, ans_ctx):
                chunks.append(ch)
            if not chunks:
                return [u"（查询已执行，但未能生成回答，请换个问法）"]
            # 确定性截断声明：回答模型可能不守规矩，代码层面再保底一次
            if truncated:
                chunks.append(u"\n\n---\n（系统提示：本回答基于前 {} 条查询结果，"
                              u"已达查询上限，并非全量。如需完整清单，"
                              u"请缩小范围（指定某类/某名称）后再问。）".format(len(rows)))
            return chunks
        log.warning(u"adhoc 查询失败: %s | %s",
                    _text(res.get("error")), cypher)
        # P5：错误里有时夹着长长的 Cypher 栈/原始行，截断避免 ctx 失控
        err_short = _text(res.get("error"))[:400]
        ctx += (u"\n【上次生成的查询执行失败，原因：{}】"
                u"请修正后重新输出 JSON").format(err_short)
        # 再加一道整体上限：极端情况下 schema+hist+错误 可能顶到 token 限制
        if len(ctx) > 12000:
            ctx = ctx[:4000] + u"\n……（省略中段）……\n" + ctx[-8000:]
    return []


def _match_fact_template(query, keyword):
    """按 schema_notes.query_templates 匹配用户问题，命中则直接执行 Cypher 直答。
    返回 (rows, template_name)。未命中/执行失败/结果为空 → (None, None)。

    label_from 字段指向 pattern 的命名组（如 lbl）：该组值要通过 aliases 或 type_registry
    解析成真实 label 再拼进 Cypher，解析不到就跳过该模板（避免拼出不存在的类型）。
    其他命名组（kw/a/b 等）作为 $参数 传入，不做字面拼接——避免 Cypher 字面量注入。"""
    import gateway as _gateway
    notes = _gateway.load_schema_notes() or {}
    templates = notes.get("query_templates") or []
    if not templates:
        return None, None
    q = _text(query).strip()
    if not q:
        return None, None
    reg = store.type_registry() or {}
    aliases = notes.get("aliases") or {}
    alias_first = {}
    for ak, lbs in (aliases.items() if isinstance(aliases, dict) else []):
        if isinstance(lbs, list) and lbs:
            alias_first[_text(ak).strip()] = _text(lbs[0]).strip()
    for tpl in templates:
        if not isinstance(tpl, dict):
            continue
        pat = _text(tpl.get("pattern"))
        cyp = _text(tpl.get("cypher"))
        if not pat or not cyp:
            continue
        try:
            m = re.search(pat, q)
        except re.error as e:
            log.warning(u"fact template pattern 非法: %s | %s",
                        tpl.get("name"), e)
            continue
        if not m:
            continue
        groups = m.groupdict() or {}
        label_from = _text(tpl.get("label_from")).strip() or None
        label = u""
        np_prop = u""
        if label_from:
            raw = _text(groups.get(label_from) or u"").strip()
            if not raw:
                continue
            # 别名命中优先，否则直接认 label；都认不到就放弃本模板
            resolved = alias_first.get(raw)
            if not resolved and raw in reg:
                resolved = raw
            if not resolved:
                continue
            label = resolved
            np_prop = _text((reg.get(label) or {}).get("name_prop")) or u"名称"
        params = {}
        for gname, gval in groups.items():
            if gname == label_from:
                continue
            if gval is None:
                continue
            v = _text(gval).strip()
            if v:
                params[gname] = v
        # label/np 来自 registry/aliases 已经过 _sanitize_identifier 净化（gateway 侧），
        # 其他参数走 $占位符，不直接拼进语句
        try:
            cyp_final = cyp.format(lbl=label, np=np_prop)
        except (KeyError, IndexError, ValueError):
            continue
        res = store.graph_query(cyp_final, params)
        if not res.get("success"):
            log.warning(u"fact template '%s' 执行失败: %s | %s",
                        tpl.get("name"), res.get("error"), cyp_final)
            continue
        rows = res.get("rows") or []
        if not rows:
            # 命中模板但图里没证据，继续试下一个模板（避免误 fallback 到空结果）
            continue
        return rows, _text(tpl.get("name")) or u""
    return None, None


def _fact(query, intent, state, conv_id, ctx=None):
    """事实直答：定位业务（已选/搜索/模糊）→ 拉证据 → 直答用户问的维度；
    模板证据查不到时走 _adhoc（LLM 生成只读 Cypher 兜底）。
    追问类问题（"具体哪些/总数多少"）本身不含业务名，靠最近对话延续话题。"""
    yield ("status", u"正在查询业务信息…")
    hist = u"\n".join(u"{}: {}".format(m.get("role"),
                                       (m.get("content") or u"")[:150])
                      for m in ((ctx or {}).get("history") or [])[-3:])
    # 固定模板优先：schema_notes.query_templates 命中则直接执行 Cypher 直答，
    # 跳过 LLM 生成（高频问法：统计数量/清单/A 承载在哪些系统 等）
    tpl_rows, tpl_name = _match_fact_template(query, intent.get("keyword") or u"")
    if tpl_rows is not None:
        yield ("status", u"模板命中：{}".format(tpl_name or u"query_template"))
        rel_meanings = json.dumps(
            (app_schema() or {}).get("relationship_meanings") or {},
            ensure_ascii=False)
        ctx_text = (u"【用户问题】{}\n【模板】{}\n【证据】（共 {} 行）\n{}\n"
                    u"【关系含义】{}".format(
                        query, tpl_name, len(tpl_rows),
                        json.dumps(tpl_rows, ensure_ascii=False),
                        rel_meanings))
        for chunk in _stream(prompts.FACT_PROMPT, ctx_text):
            yield chunk
        return
    topic = intent["keyword"] or u""
    sel, sel_src = store.pick_selected(query, intent["keyword"], state)
    # 退出守卫：只是落在旧 selected 上、而意图提取到了无关新名称 → 不沿用旧选择
    if (sel and sel_src == u"selected" and intent["keyword"]
            and not _kw_related(intent["keyword"], sel.get("name"))):
        sel = None
    # E3 共用：search 分支里才赋值，sel 分支默认留空
    candidates = None
    fuzzy_matched = False
    if sel:
        businesses = [sel]
        scope = u"已选定实体：{}".format(json.dumps(sel, ensure_ascii=False))
    else:
        # 追问句式优先：问法本身是"具体哪些/总数多少"且上下文有话题时，
        # 无论意图提取出什么关键词（常是"16个""哪16个"这类垃圾），都交给
        # adhoc 带最近对话延续话题
        if (_FOLLOWUP_RE.search(query) or _COUNT_Q_RE.search(query)) and hist:
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, intent["keyword"], hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【最近对话】\n{}".format(query, hist)):
                    yield chunk
            return
        intent["keyword"] = clean_keyword(intent["keyword"])
        # 不含汉字/字母的关键词（如"具体哪16个"被提取出的 16、16个）不是
        # 业务名，搜出来全是垃圾匹配，视作无关键词走追问链路
        if intent["keyword"] and not re.search(u"[一-龥A-Za-z]", intent["keyword"]):
            intent["keyword"] = u""
        # 无关键词但有对话上下文：追问句式（"具体哪些/总数多少"），直接让
        # adhoc 带着最近对话延续话题；比候选搜索拿垃圾匹配可靠
        if not intent["keyword"] and hist:
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【最近对话】\n{}".format(query, hist)):
                    yield chunk
            return
        # 关键词恰好是实体类型名（如"信息系统""业务事项"）：用户问的是整类
        # 实体，名称 CONTAINS 搜索只会命中描述里带该词的任意 10 条（以偏概全），
        # 直接走 adhoc 按问题生成聚合/清单查询
        if intent["keyword"] and intent["keyword"] in store.type_registry():
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
            else:
                for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                     u"【用户问题】{}\n【搜索关键词】{}".format(
                                         query, intent["keyword"])):
                    yield chunk
            return
        # 统计类问题（"有几个/多少个"）：候选搜索有截断上限，拿截断样本数个数
        # 必然答错（16 个只搜到 10 个就答 10）；优先 adhoc 生成 count() 聚合，
        # 失败再用代码确定性 count 兜底——问数量的必须给数字，不能退化成列表
        if _COUNT_Q_RE.search(query):
            yield ("status", u"正在按问题生成统计查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
                return
            anchor = store.core_sem().get("anchor_label")
            counts = (store.keyword_counts(intent["keyword"], labels=[anchor])
                      if anchor else [])
            if not counts:
                counts = store.keyword_counts(intent["keyword"])
            if counts:
                total = sum(c for _, c in counts)
                lines = [u"「{}」相关实体共 **{}** 个（按名称/别名包含关键词精确统计）："
                         .format(intent["keyword"], total)]
                lines.extend(u"- 【{}】{} 个".format(l, c) for l, c in counts)
                lines.append(u"想看具体清单，可以问「{}都有哪些」。".format(
                    intent["keyword"]))
                yield u"\n".join(lines) + u"\n"
                return
        candidates = store.business_search(
            intent["keyword"],
            labels=(store.core_sem().get("candidate_labels")
                    or config.CANDIDATE_LABELS),
            limit=config.FACT_CANDIDATE_LIMIT)
        if not candidates:
            # 全类兜底：可能是系统/部门/表等支撑实体
            candidates = store.business_search(
                intent["keyword"], labels=None, limit=config.FACT_CANDIDATE_LIMIT)
        fuzzy_matched = False
        if not candidates:
            # adhoc 优先于 fuzzy：全量/统计/关系类问题（"都承载在哪些系统"）
            # 靠 fuzzy 只能拿到任意 10 个实体，会以偏概全；adhoc 能真实聚合。
            # adhoc 不适用（返回空）再 fuzzy（名称打不准的场景）
            yield ("status", u"正在按问题生成图查询…")
            adhoc_chunks = _try_adhoc(query, topic, hist)
            if adhoc_chunks:
                for chunk in adhoc_chunks:
                    yield chunk
                return
            candidates = store.fuzzy_search(intent["keyword"],
                                            limit=config.CANDIDATE_LIMIT)
            fuzzy_matched = bool(candidates)
        if not candidates:
            for chunk in _stream(prompts.NOT_FOUND_PROMPT,
                                 u"【用户问题】{}\n【搜索关键词】{}".format(
                                     query, intent["keyword"])):
                yield chunk
            return
        businesses = candidates
        # 有名称完全匹配的候选时，只答它（用户问的是具体实体）
        kw = intent["keyword"].strip()
        exact = [c for c in candidates
                 if _text(c.get("name")).strip() == kw]
        if len(exact) == 1:
            businesses = exact
        # scope 列到 FACT_CANDIDATE_LIMIT，与证据查询对齐，不再硬切 30
        scope = u"匹配到的实体（{} 个）：{}".format(
            len(businesses),
            u"；".join(u"【{}】{}（{}）".format(c.get("business_type"),
                                              c.get("name"), c.get("id"))
                       for c in businesses[:config.FACT_CANDIDATE_LIMIT]))
        # 搜索命中数达到上限：说明实际可能更多（被截断），必须声明非全量，
        # 禁止把样本数当总数（"企业开办有 10 个业务"这种确定性错答就是这么来的）
        if len(candidates) >= config.FACT_CANDIDATE_LIMIT:
            scope += (u"\n（注意：搜索命中已达上限 {} 个，实际可能更多；回答必须说明"
                      u"仅覆盖这些实体，总数请用统计问法如\"XX共有多少个\"）"
                      .format(config.FACT_CANDIDATE_LIMIT))
        if fuzzy_matched:
            # fuzzy 是滑窗任意子集（如"系统"命中全部信息系统但只取前10个），
            # 必须让回答模型声明"仅覆盖这些实体、非全量"
            scope += u"\n（注意：以上实体为模糊匹配的部分结果，非全量；回答必须说明仅覆盖这些实体）"

    # 按实体类型分组拉证据（不同类型的主键/子结构不同）
    rows = []
    groups = {}
    for c in businesses:
        groups.setdefault(c.get("type") or c.get("business_type"), []).append(c)
    for etype, items in groups.items():
        rows.extend(store.fact_rows(etype, [i.get("id") for i in items]))
    rel_meanings = json.dumps(
        (app_schema() or {}).get("relationship_meanings") or {},
        ensure_ascii=False)
    ctx_text = (u"【用户问题】{}\n【实体范围】{}\n【证据】（共 {} 行）\n{}\n"
                u"【关系含义】{}".format(
                    query, scope, len(rows),
                    json.dumps(rows, ensure_ascii=False), rel_meanings))
    # E3：候选截断或模糊匹配时，LLM 若忘声明"非全量"就会被当成完整答案，
    # 这里不管模型怎么写，代码在流尾强制补一行提示
    truncated_hint = u""
    if candidates is not None and len(candidates) >= config.FACT_CANDIDATE_LIMIT:
        truncated_hint = (
            u"\n\n---\n（系统提示：本回答基于前 {N} 条候选，候选命中已达上限，"
            u"实际可能更多；并非全量。如需完整清单，请换更具体的名称或问"
            u"\"XX共有多少个\"等统计问法。）"
        ).format(N=config.FACT_CANDIDATE_LIMIT)
    elif fuzzy_matched:
        truncated_hint = (
            u"\n\n---\n（系统提示：以上实体由模糊匹配截取，并非全量。如需完整"
            u"清单，请换更具体的名称后再问。）")
    if rows:
        for chunk in _stream(prompts.FACT_PROMPT, ctx_text):
            yield chunk
        if truncated_hint:
            yield truncated_hint
    else:
        # 模板证据为空：该问题维度可能不在固定邻接模板里，adhoc 兜底直查
        yield ("status", u"正在按问题生成图查询…")
        adhoc_chunks = _try_adhoc(query, topic, hist)
        if adhoc_chunks:
            for chunk in adhoc_chunks:
                yield chunk
        else:
            for chunk in _stream(prompts.FACT_PROMPT, ctx_text):
                yield chunk
            if truncated_hint:
                yield truncated_hint
    # 单一业务时顺手记住，方便用户下一句直接"分析下影响"；
    # 候选列表保留（不清空），用户可用序号换选其他候选
    if sel or len(businesses) == 1:
        c = businesses[0]
        yield ("graph", {"entity": store._norm_selected(c) or {
            "type": c.get("business_type"), "id": c.get("id"),
            "name": c.get("name")}})
        store.save_state(conv_id, {
            "step": u"已选定业务",
            "selected": store._norm_selected(c),
            "candidates": (state.get("candidates") or []),
            "original_query": query,
            "change_desc": u"",
        })


def app_schema():
    import gateway
    data, err = gateway.build_schema()
    return data if not err else {}


def _analyze(query, intent, selected, ctx, conv_id):
    """已确定业务 → 确定性证据查询（代码构建，不经 LLM 生成 Cypher）→ 影响分析。
    会话状态维护在本包装层；分析主体见 stream_analysis（工作台节点分析共用）。"""
    for item in stream_analysis(query, intent, selected):
        yield item
    # 证据成功/失败两条路径保存内容一致，统一在此保存：
    # 保留已选业务（支持追问）和候选列表（支持序号换选）
    store.save_state(conv_id, {
        "step": u"已选定业务",
        "selected": selected,
        "candidates": (ctx.get("state") or {}).get("candidates") or [],
        "original_query": query,
        "change_desc": intent.get("change_desc") or u"",
    })


def stream_analysis(query, intent, selected):
    """影响分析主体（无会话副作用）：确定性证据查询 → 代码分桶预计算 → LLM 排版。
    供对话 _analyze 与工作台节点智能分析（server /graph/analyze）共用。
    yield ("status", 文本) / 文本块 / ("graph", {"entity": ...}) / ("error", 消息)。"""
    yield ("status", u"正在查询图数据库…")
    rows, cypher_text, rollup_bizs, trunc_hint = store.evidence_rows(
        selected.get("type"), selected.get("id"))
    if rows is None:
        # O4：图库证据查询失败，先道歉再给出具体可执行的下一步，
        # 不要只甩一句"请稍后重试"
        err_short = _text(cypher_text)[:300]
        for chunk in _stream(
                prompts.APOLOGIZE_PROMPT,
                u"【错误信息】{}\n【请在回答末尾附上以下建议】\n"
                u"- 稍等 1-2 分钟后重试同一条分析\n"
                u"- 若多次失败，到「系统配置 → 测试图库连接」确认 Galaxybase 地址"
                u"/账号/图名是否正确、服务是否可达\n"
                u"- 急需结果时可改用「图谱工作台」直接浏览该实体邻接，"
                u"查看结构关联（不走依赖传播推理）".format(err_short)):
            yield chunk
        return

    # 证据查询成功：发图谱卡片（前端"展开图谱"在工作台打开该实体邻接图）
    yield ("graph", {"entity": selected})

    # 代码预计算结论（不让 LLM 解码行方向，杜绝误读）：
    # 依赖关系入边=依赖候选（直接/间接）；实体自身传播入边=按角色分组的关联候选；
    # 依赖关系出边=上游依赖；其余=一般关联证据（关系类型/锚定类型来自核心语义配置）
    sem = store.core_sem()
    dep_rel = _text(sem.get("dependency_rel")).strip()
    anchor = _text(sem.get("anchor_label")).strip() or u"业务事项"
    np_ = _text(sem.get("name_prop")).strip() or u"名称"
    ent = store.entity_entry(selected.get("type"))
    role_defs = []
    for p in (ent.get("propagate") or []):
        rel = _text(p.get("rel"))
        if rel and rel != dep_rel:
            role_defs.append((rel, _text(p.get("role")) or rel,
                              u"出边" if _text(p.get("direction") or u"in").lower() == u"out"
                              else u"入边", _text(p.get("label")).strip()))
    dep_in = [r for r in rows if dep_rel and _text(r.get("rel")) == dep_rel
              and r.get("dir") == u"入边"]
    dep_out = [r for r in rows if dep_rel and _text(r.get("rel")) == dep_rel
               and r.get("dir") == u"出边"]
    others = [r for r in rows
              if not (dep_rel and _text(r.get("rel")) == dep_rel)]

    lines = []
    if dep_in:
        has_hop = any(r.get("hop") for r in dep_in)
        # 八-P2：direct_ids 补查 与 dependent_systems 查询相互独立，并行跑。
        #        之前两条串行等于把图库 RTT × 2，大图热点上肉眼可感。
        import threading as _thr
        direct_ids = set()
        direct_names = set()
        direct_err = {"v": None}

        # 先把 by_src 的键序建出来（dependent_systems 查询需要），
        # direct 标记稍后在两查完成后合入
        by_src = {}
        order = []
        for r in dep_in:
            n = _text(r.get("src"))
            if not n:
                continue
            sid = _text(r.get("src_id"))
            key = sid if sid else n
            if key not in by_src:
                by_src[key] = {"name": n, "direct": (r.get("hop") == u"直接"),
                               "nb_type": r.get("nb_type"), "anchors": []}
                order.append(key)
            e = by_src[key]
            if r.get("hop") == u"直接":
                e["direct"] = True
            a = _text(r.get("nb_name"))
            if a and a not in e["anchors"]:
                e["anchors"].append(a)

        def _query_direct():
            # A1：原查询硬写 LIMIT 200，入边密集的锚点会被截断，部分真·直接依赖
            # 被漏判成间接。放到 dep_in 行数的 2 倍兜底（最少 500，最多 5000）；
            # 并额外记录 src_id，用于与 by_src 的 ID 主键对齐，避免同名合并。
            cap = max(500, min(len(dep_in) * 2 + 100, 5000))
            dr = store.graph_query(
                u"MATCH (a:`{a}`)-[:`{d}`]->(x:`{l}`) WHERE x.`{pk}` = $id "
                u"RETURN DISTINCT a.`{apk}` AS aid, a.`{np}` AS n LIMIT {cap}".format(
                    a=anchor, d=dep_rel, l=ent.get("label") or anchor,
                    pk=ent.get("pk") or np_, np=np_,
                    apk=(store.type_registry().get(anchor) or {}).get("pk") or np_,
                    cap=cap),
                {"id": selected.get("id")})
            if dr.get("success"):
                for x in dr.get("rows") or []:
                    nm = _text(x.get("n"))
                    aid = _text(x.get("aid"))
                    if nm:
                        direct_names.add(nm)
                    if aid:
                        direct_ids.add(aid)
            else:
                direct_err["v"] = dr.get("error")

        sys_map_box = {"v": {}}

        def _query_systems():
            # 承载系统查询：by_src 的键若是 src_id 则按 ID 精确匹配，避免同名多主体错位；
            # 旧行无 ID 时按名称回退，sys_map 以 by_src 的键一致方式返回。
            sys_map_box["v"] = store.dependent_systems(
                [{"id": k if k != by_src[k]["name"] else "",
                  "name": by_src[k]["name"]} for k in order])

        threads = []
        if not has_hop:
            t_d = _thr.Thread(target=_query_direct)
            t_d.daemon = True
            t_d.start()
            threads.append(t_d)
        t_s = _thr.Thread(target=_query_systems)
        t_s.daemon = True
        t_s.start()
        threads.append(t_s)
        for t in threads:
            t.join()
        sys_map = sys_map_box["v"] or {}

        # direct 补查结果合入 by_src（hop-already-known 的条目之前已标过）
        if not has_hop:
            if direct_err["v"]:
                # 查询失败时所有候选会退化成"间接"分桶；在日志里留痕以便排障，
                # 不吞掉错误。继续走完整个分析流程（候选仍可用，仅直接/间接判定失准）。
                log.warning(u"direct_query 失败，直接/间接分桶可能不准：%s",
                            direct_err["v"])
            for key, e in by_src.items():
                if e["direct"]:
                    continue
                sid = key if key != e["name"] else u""
                if (sid and sid in direct_ids) or (not sid and e["name"] in direct_names):
                    e["direct"] = True
        direct = [(k, by_src[k]) for k in order if by_src[k]["direct"]]
        indirect = [(k, by_src[k]) for k in order if not by_src[k]["direct"]]

        def dep_line(k, e):
            n = e["name"]
            sys_list = sys_map.get(k) or sys_map.get(n) or []
            sys_text = u"、".join(sys_list) if sys_list else u"未记录"
            anchors = u"、".join(e["anchors"]) if e["anchors"] else u"—"
            return (u"- {}（{}｜承载系统：{}｜证据：{} -[{}]-> {}）"
                    .format(n, e["nb_type"], sys_text, n, dep_rel, anchors))

        if direct:
            lines.append(u"依赖候选——直接（1跳，{} 个）：".format(len(direct)))
            lines.extend(dep_line(k, e) for k, e in direct)
        if indirect:
            lines.append(u"依赖候选——间接（经依赖链传导，{} 个）：".format(len(indirect)))
            lines.extend(dep_line(k, e) for k, e in indirect)

    # 实体自身传播入边按角色分组（如：来源子项/承载的业务/主管的业务）
    for rel, role, direction, other_label in role_defs:
        group = [r for r in rows if _text(r.get("rel")) == rel
                 and r.get("dir") == direction
                 and _text(r.get("role")) == role
                 and (not other_label or _text(r.get("nb_type")) == other_label)]
        if group:
            lines.append(u"{}（{}，{} 个）：".format(role, rel, len(group)))
            lines.extend(u"- {}（{}）".format(r.get("nb_name"), r.get("nb_type"))
                         for r in group)

    if lines:
        impact_text = (u"【受影响与关联候选】（已由系统从图查询按角色精确分组，"
                       u"逐条引用并保留角色标签，不得增删、并组或改判）：\n{}").format(
            u"\n".join(lines))
    else:
        if dep_rel:
            impact_text = (u"【受影响与关联候选】无——系统已执行配置的依赖路径查询，"
                           u"当前图谱未发现依赖传播候选")
        else:
            impact_text = (u"【受影响与关联候选】无——当前 Schema 未定义依赖传播关系；"
                           u"系统不会把“包括/承载/执行”等结构关系误判为影响传播")
    upstream_text = u"（无）"
    if dep_out:
        upstream_text = u"\n".join(u"- {}（{}）".format(r.get("nb_name"), r.get("nb_type"))
                                   for r in dep_out)

    rollup_note = u""
    if rollup_bizs:
        qualifier = (u"后按依赖关系计算影响候选" if dep_rel
                     else u"；当前仅作为结构归属，不推导跨业务影响")
        rollup_note = (u"\n【归属业务】（分析对象经配置的结构路径归并到以下业务{}）：{}\n".format(
                           qualifier, u"、".join(rollup_bizs)))
    schema = store.schema_text()
    # 八-P3：rows 过大会把 analyze_ctx token 爆掉；按 (role, rel, dir, nb_type) 分桶
    # 每桶留 40 条，总共不超过 400 条——impact_text 已经把完整分组摘要给到了
    # LLM，原始行此处只用于复核，抽样后在前面声明"仅代表性样本"
    sampled, sample_note = _sample_rows_by_role(rows, per_group=40, total_cap=400)
    rows_block = (u"【原始查询行{tag}】（受影响候选/上游即从中按传播关系提取，"
                  u"复核结果应与上面一致）：\n{body}").format(
                      tag=u"（代表性抽样）" if sample_note else u"",
                      body=json.dumps(sampled, ensure_ascii=False))
    if sample_note:
        rows_block = rows_block + u"\n" + sample_note
    analyze_ctx = (u"【用户问题】{}\n【分析的业务】{}{}\n{}\n"
                   u"【上游依赖条件】（被分析业务依赖它们，属前置条件，不是受影响方）：\n{}\n"
                   u"{}{}\n"
                   u"【图结构与关系含义参考】\n{}".format(
                       query, json.dumps(selected, ensure_ascii=False),
                       rollup_note, impact_text, upstream_text,
                       rows_block, trunc_hint or u"", schema))
    for chunk in _stream(prompts.analyze_prompt(), analyze_ctx):
        yield chunk
    # A8：代码层保底 —— 回答模型即便没读 trunc_hint，也从流尾补一句免责声明
    if trunc_hint:
        yield (u"\n\n---\n（系统提示：本次证据查询有分段达到 LIMIT 上限，"
               u"分析结论可能不包含全部受影响对象。建议缩小范围后再问。）")


def _stream(instruction, context):
    """流式调用 LLM 并逐块剥离 <think>（复用 gateway.py 的状态机）。
    O2：记录首字节延迟（第一次可见 chunk 的到达时间）"""
    import gateway
    import time as _time
    feed, flush = gateway.make_think_filter()
    t0 = _time.time()
    first_shown = False
    metrics.inc("llm.calls")
    for chunk in llm.llm_stream(instruction, context):
        shown = feed(chunk)
        if shown:
            if not first_shown:
                metrics.observe("llm.first_byte_ms",
                                (_time.time() - t0) * 1000.0)
                first_shown = True
            yield shown
    shown = flush()
    if shown:
        if not first_shown:
            metrics.observe("llm.first_byte_ms",
                            (_time.time() - t0) * 1000.0)
        yield shown
