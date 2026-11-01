#!/bin/sh
# 分析智能体服务（orchestrator2.0，端口 5200）免安装启动：自动按 CPU 架构挂载离线依赖
# 用法: ./start.sh   （端口改 orchestrator2.0/config.py 的 PORT）
cd "$(dirname "$0")" || exit 1

case "$(uname -m)" in
  aarch64|arm64) LIB=offline/lib_aarch64 ;;
  *)             LIB=offline/lib_x86_64 ;;
esac

export PYTHONPATH="$PWD/$LIB${PYTHONPATH:+:$PYTHONPATH}"

if command -v python2 >/dev/null 2>&1; then
  exec python2 -u orchestrator2.0/server.py
fi
exec python -u orchestrator2.0/server.py
