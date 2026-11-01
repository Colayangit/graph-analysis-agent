# -*- coding: utf-8 -*-
"""运行时配置：系统配置界面 → runtime_config.json → 覆盖默认值并实时生效。

图数据库连接配置默认值在 gateway.py 配置区，通过运行时覆盖其模块变量实现
热更新；LLM 平台配置经 config.get() 动态读取。
"""
from __future__ import print_function

import io
import json

import config

try:
    unicode     # py2
except NameError:
    unicode = str    # py3

# 允许通过界面修改的键（防止任意写入）
ALLOWED_KEYS = [
    u"galaxy_base",    # 图数据库地址 http://IP:18088
    u"galaxy_user",
    u"galaxy_pass",
    u"galaxy_graph",
    u"galaxy_rsa_key", # S5：图库 RSA 登录公钥，换环境可以在界面覆盖
    u"llm_platform",   # dify / sfm
    u"dify_base",
    u"dify_key",
    u"sfm_base",
    u"sfm_key",
    u"sfm_agent_code",
    u"sfm_agent_version",
]
# 键 → config.get 名 / gateway.py 模块变量名
_GALAXY_MAP = {
    u"galaxy_base": "GALAXY_BASE",
    u"galaxy_user": "GALAXY_USER",
    u"galaxy_pass": "GALAXY_PASS",
    u"galaxy_graph": "GALAXY_GRAPH",
    u"galaxy_rsa_key": "GALAXY_RSA_KEY",
}
_LLM_MAP = {
    u"llm_platform": "LLM_PLATFORM",
    u"dify_base": "DIFY_API_BASE",
    u"dify_key": "DIFY_API_KEY",
    u"sfm_base": "SFM_API_BASE",
    u"sfm_key": "SFM_API_KEY",
    u"sfm_agent_code": "SFM_AGENT_CODE",
    u"sfm_agent_version": "SFM_AGENT_VERSION",
}
_MASK = u"******"
# S3：掩码语义仅适用于密钥类字段（前端这些才会回显为 ******），
# 普通字段里 "******" 应当作字面值保存，而不是被当成"保留原值"
_SECRET_KEYS = frozenset([u"galaxy_pass", u"dify_key", u"sfm_key",
                          u"galaxy_rsa_key"])


def load():
    return {k: v for k, v in (config._runtime or {}).items() if k in ALLOWED_KEYS}


def save(data):
    """合并保存：
      - 字段缺失（None / key 不在 data 中） = 保留原值
      - 密钥类字段取到 ****** = 保留原值（UI 回填掩码不覆盖真实值）
      - 其余字段的 ****** 当字面值写入
      - 空字符串 = 保留原值（避免误清空关键配置）
    """
    if not isinstance(data, dict):
        return False, u"请求体须为 JSON 对象"
    merged = load()
    for k in ALLOWED_KEYS:
        if k not in data:
            continue
        v = data.get(k)
        if v is None:
            continue
        v = u"{}".format(v).strip()
        if v == u"":
            continue  # 留空 = 不修改
        if v == _MASK and k in _SECRET_KEYS:
            continue  # 密钥字段的掩码 = 保留原值；非密钥字段则按字面值落
        merged[k] = v
    # 平台合法值校验
    if merged.get(u"llm_platform") not in (None, u"dify", u"sfm"):
        return False, u"llm_platform 只能是 dify 或 sfm"
    try:
        content = json.dumps(merged, ensure_ascii=False, indent=2)
        if not isinstance(content, unicode):
            content = content.decode("utf-8")
        with io.open(config.RUNTIME_CONFIG_PATH, "w", encoding="utf-8") as f:
            f.write(content)
    except (IOError, OSError) as e:
        return False, u"写入配置文件失败：{}".format(e)
    # 密钥文件缩权限到 0600（POSIX），避免同组用户可读
    try:
        config._tighten_perm(config.RUNTIME_CONFIG_PATH)
    except Exception:
        pass
    config._runtime = merged
    apply(merged)
    return True, u"已保存并实时生效"


def apply(data):
    """把配置覆盖到各模块并清空缓存（schema/token/会话/类型参数表）"""
    import gateway as gw  # 图数据库模块变量 + 缓存
    for key, attr in _GALAXY_MAP.items():
        if data.get(key):
            setattr(gw, attr, data[key])
    # LLM 配置以大写别名同步进 config._runtime——config.get() 按大写键读，
    # 只存小写键的话界面改的 LLM 平台/密钥永远不会生效（1.0 的隐藏 bug）
    import config as _config
    for key, attr in _LLM_MAP.items():
        if data.get(key):
            _config._runtime[attr] = data[key]
    # C3：锁内 clear-in-place，避免整体重赋导致并发写丢失或读到被 GC 的旧对象
    with gw._token_lock:
        gw._token_cache["token"] = None         # Galaxybase 登录 token
    with gw._schema_lock:
        gw._schema_cache["ts"] = 0              # 图结构缓存
        gw._schema_cache["data"] = None
    import store
    store._reg_cache = {"data": None, "ts": 0}  # 实体类型参数表
    import llm
    llm.reset_caches()                          # 平台探测 / SFM 会话


def load_and_apply_on_start():
    """服务启动时应用运行时配置"""
    apply(load())
