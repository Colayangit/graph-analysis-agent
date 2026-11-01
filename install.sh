#!/bin/sh
# 离线一键安装：识别架构 → 离线装 pip（若没有）→ 离线装依赖
# 装完后直接运行: python app.py  （或 ./start.sh）
cd "$(dirname "$0")" || exit 1

PIP_WHL=offline/pip-20.3.4-py2.py3-none-any.whl

# ---- 选 Python 解释器（优先 python2，代码双兼容）----
if command -v python2 >/dev/null 2>&1; then
  PY=python2
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "错误：找不到 python 解释器"; exit 1
fi

# ---- 识别 CPU 架构 ----
case "$(uname -m)" in
  aarch64|arm64) WHEELS=offline/aarch64 ;;
  *)             WHEELS=offline/x86_64 ;;
esac
echo "解释器: $PY ($(uname -m), 依赖目录: $WHEELS)"

# ---- 1. 确保 pip ----
if "$PY" -m pip --version >/dev/null 2>&1; then
  echo "[1/2] 已有 pip，跳过安装"
else
  echo "[1/2] 离线安装 pip..."
  if ! "$PY" "$PIP_WHL/pip" install --no-index "$PIP_WHL" >/dev/null 2>&1; then
    # 无 root 权限写系统目录时，退回用户目录安装
    "$PY" "$PIP_WHL/pip" install --no-index --user "$PIP_WHL" >/dev/null 2>&1 || {
      echo "错误：pip 安装失败（无 root 时请加 sudo 重试）"; exit 1; }
  fi
fi

# ---- 2. 离线安装依赖 ----
echo "[2/2] 离线安装依赖..."
if ! "$PY" -m pip install --no-index --find-links "$WHEELS" -r requirements.txt; then
  echo "退回用户目录安装（--user）..."
  "$PY" -m pip install --no-index --user --find-links "$WHEELS" -r requirements.txt || exit 1
fi

echo ""
echo "安装完成！启动方式："
echo "  ./start.sh                        # 推荐：免安装挂载方式启动（端口 5200）"
echo "  python orchestrator2.0/server.py  # 或直接运行"
