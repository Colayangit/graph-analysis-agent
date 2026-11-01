# -*- coding: utf-8 -*-
"""轻量指标埋点（O2）：进程内 counter / timer，/metrics 端点以 JSON 导出。

不引入 Prometheus client（离线专网不装额外依赖），只覆盖排障必需维度：
  - 请求计数：/api/chat 总量、各分支占比（chat/fact/targeted/search/qa/writing）
  - 失败计数：pipeline 异常、adhoc 失败/截断、意图识别 fallback
  - 延迟：LLM 首字节、总 pipeline 耗时（最近 N 次简单直方图/平均）
  - 图库/Cypher：graph_query 调用数 / 失败数 / 平均耗时

所有操作线程安全（threading.Lock）。
"""
from __future__ import print_function

import threading
import time

_lock = threading.Lock()
_counters = {}      # name -> int
_timers = {}        # name -> {"count": N, "sum_ms": X, "max_ms": Y}
_HIST_KEEP = 200


def inc(name, by=1):
    with _lock:
        _counters[name] = _counters.get(name, 0) + by


def observe(name, ms):
    try:
        v = float(ms)
    except (TypeError, ValueError):
        return
    with _lock:
        t = _timers.get(name)
        if t is None:
            t = {"count": 0, "sum_ms": 0.0, "max_ms": 0.0}
            _timers[name] = t
        t["count"] += 1
        t["sum_ms"] += v
        if v > t["max_ms"]:
            t["max_ms"] = v


class Timer(object):
    """with metrics.Timer("pipeline.total"): ..."""
    def __init__(self, name):
        self.name = name
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *_):
        observe(self.name, (time.time() - self.t0) * 1000.0)


def snapshot():
    """导出当前所有指标（JSON 友好）"""
    with _lock:
        counters = dict(_counters)
        timers = {}
        for k, v in _timers.items():
            avg = (v["sum_ms"] / v["count"]) if v["count"] else 0.0
            timers[k] = {
                "count": v["count"],
                "avg_ms": round(avg, 2),
                "max_ms": round(v["max_ms"], 2),
                "sum_ms": round(v["sum_ms"], 2),
            }
    return {"counters": counters, "timers": timers}


def reset():
    """测试/调试用：清空计数器（生产勿调）"""
    with _lock:
        _counters.clear()
        _timers.clear()
