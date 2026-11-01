# 系统字段变更影响分析 · 分析智能体

对话式的字段变更影响分析前端与编排服务。**全部流程逻辑在 Python，
智能体平台（SFM/Dify）只当 LLM 算力**，图数据在 Galaxybase。

## 架构

```
浏览器 ──SSE──> orchestrator2.0/server.py (端口 5200)
                 └─ pipeline.py 编排（意图→分支→搜索→证据→分析→存状态）
                     ├─ store.py    图工具 + 证据查询
                     ├─ gateway.py  Galaxybase 客户端 + SQLite 存储
                     ├─ prompts.py  全部提示词
                     ├─ llm.py      LLM 算力层（dify/sfm 可界面切换）
                     └─ settings.py 运行时配置（实时生效）
```

详细设计见 [orchestrator2.0/README.md](orchestrator2.0/README.md)。

## 目录

```
orchestrator2.0/   全部服务代码（融合了旧根目录网关 app.py）
offline/           双架构离线依赖（wheels + 免 pip 解压库）
install.sh         离线一键安装依赖
start.sh           免安装启动（自动挂离线依赖）
```

## 快速开始

```bash
./install.sh    # 有 pip 环境时离线装依赖（也可跳过直接 start.sh）
./start.sh      # 启动，页面 http://<本机IP>:5200
```

python 2.7 / 3.x 双兼容。配置走页面左下「⚙ 系统配置」（存
`orchestrator2.0/runtime_config.json`），图数据库默认连接在
`orchestrator2.0/gateway.py` 配置区。
