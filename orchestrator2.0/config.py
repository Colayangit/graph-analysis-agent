# -*- coding: utf-8 -*-
"""编排服务配置（默认值）。

优先级：页面「系统配置」保存的运行时配置（runtime_config.json，经 config.get()
动态读取）> 本文件默认值。即：本文件只是首次部署的兜底，日常改配置走界面。

两类配置：
- 图数据库（Galaxybase）：默认值在 gateway.py 配置区（GALAXY_*），本服务
  通过 import gateway 复用其图客户端与本地存储（history.db）；运行时由
  settings.py 覆盖 gateway 模块变量实现热更新。
- LLM 算力平台（dify/sfm）：默认值就在本文件。平台侧工作流只有 开始→LLM→回复
  三个节点，角色指令由 Python 拼进每次请求（见 llm.py / README.md）。
"""
import os
import logging as _logging
import sys as _sys

# ---------------- 日志（O1） ----------------
# print 替换为 logging：带级别/时间戳。级别由 KIRO_LOG_LEVEL 环境变量控制，
# 默认 INFO。Flask werkzeug 的访问日志不接管，各自落 stdout。
LOG_LEVEL = (os.environ.get("KIRO_LOG_LEVEL") or "INFO").upper()


def _setup_logging():
    root = _logging.getLogger("kiro")
    if root.handlers:
        return root   # 已初始化
    h = _logging.StreamHandler(_sys.stdout)
    h.setFormatter(_logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S"))
    root.addHandler(h)
    try:
        root.setLevel(getattr(_logging, LOG_LEVEL, _logging.INFO))
    except Exception:
        root.setLevel(_logging.INFO)
    root.propagate = False
    return root


def get_logger(name):
    """所有模块统一走 kiro.<name>，一处设格式/级别。"""
    _setup_logging()
    return _logging.getLogger("kiro." + name)


# LLM 算力平台：dify（本地测试）/ sfm（生产）
LLM_PLATFORM = "dify"

# ---- Dify：建一个 workflow 类型应用（不是聊天助手/Chatflow！）----
#   开始节点：query（段落，必填）
#   LLM 节点：系统提示词贴 prompts.py 里的 PLATFORM_PROMPT，关思考
#   结束节点：输出变量命名 text，引用 LLM 的 text
#   发布后把 app- 开头的 key 填到下面
DIFY_API_BASE = ""       # 本地测试平台地址（带 /v1）；真实值放 runtime_config.json
DIFY_API_KEY = ""        # 本地测试平台 Key；真实值放 runtime_config.json（不入库）

# ---- SFM（生产）：平台建 开始→LLM→回复 三节点工作流，系统提示词同上 ----
SFM_API_BASE = "http://127.0.0.1/sfm-agent-studio/sfm-api-gateway/gateway/agent/api"
SFM_API_KEY = ""       # 平台创建的 APP_KEY
SFM_AGENT_CODE = ""    # 该工作流的智能体编码
SFM_AGENT_VERSION = "" # 可选

# 中间步骤（意图/Cypher）与最终回答的 LLM 调用超时（连接, 读取）
LLM_TIMEOUT = (10, 300)

# /sessions 上下文返回的最大历史条数
HISTORY_MAX = 20

# 生成的 Cypher 未写 LIMIT 时强制追加
QUERY_LIMIT = 100

# 图谱工作台可视化限制（前端镜像同值）：首屏最多节点数 / 双击展开单次新增上限
GRAPH_FIRST_LIMIT = 200
GRAPH_EXPAND_LIMIT = 100

# 候选业务搜索：只搜这些类型（用户要选的是"业务"，不是底账表/实体等支撑对象），
# 按精确>名称>别名>描述排序，最多列这么多条
CANDIDATE_LABELS = [u"业务事项", u"业务子项"]
CANDIDATE_LIMIT = 10
# 影响分析的选择列表上限：候选多于该数时列表头会显示真实匹配总数，
# 并提示用户回复 ID 或换更具体的名称（前端默认折叠只展示 5 条，可展开）
SEARCH_CANDIDATE_LIMIT = 50
# 事实直答（fact 分支）的候选搜索上限：比选择列表宽（问"有哪些"要尽量列全），
# 命中达到该上限时回答会声明"非全量"；统计类问题（"有几个"）不走搜索而是 count() 聚合
FACT_CANDIDATE_LIMIT = 50

# 本服务监听端口（原 orchestrator 占 5100，2.0 用 5200，可同时跑对照）
PORT = 5200

# ---------------- 运行时配置（系统配置界面写入，覆盖上面的默认值） ----------------
import io
import json

RUNTIME_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "runtime_config.json")

_runtime = {}


def _tighten_perm(path):
    """运行时配置含图库 / LLM 密钥，缩到 0600 防被同组用户读取（POSIX only）"""
    try:
        if hasattr(os, "chmod") and os.name == "posix" and os.path.exists(path):
            os.chmod(path, 0o600)
    except (IOError, OSError):
        pass


def load_runtime():
    """加载 runtime_config.json；文件不存在 = 使用默认（静默），
    存在但解析失败 / 读错 = 退回默认但 warn 到日志，避免用户改坏 JSON 没感知。"""
    global _runtime
    if not os.path.exists(RUNTIME_CONFIG_PATH):
        _runtime = {}
        return _runtime
    try:
        with io.open(RUNTIME_CONFIG_PATH, encoding="utf-8") as f:
            _runtime = json.load(f) or {}
    except ValueError as e:
        _runtime = {}
        try:
            get_logger("config").warning(
                u"runtime_config.json 解析失败，已回退默认配置：%s", e)
        except Exception:
            pass
    except (IOError, OSError) as e:
        _runtime = {}
        try:
            get_logger("config").warning(
                u"runtime_config.json 读取失败，已回退默认配置：%s", e)
        except Exception:
            pass
    _tighten_perm(RUNTIME_CONFIG_PATH)
    return _runtime


def get(key, default=None):
    """运行时配置优先，其次本文件默认值"""
    v = _runtime.get(key)
    if v not in (None, ""):
        return v
    return globals().get(key, default)


load_runtime()
