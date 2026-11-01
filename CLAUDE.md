# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

系统字段变更影响分析的对话式智能体服务。架构：

```
浏览器 ──SSE──> orchestrator2.0/server.py (端口 5200)
                 └─ pipeline.py 编排（模式分流→意图→分支→搜索→证据→分析）
                     ├─ store.py    图工具 + 证据查询（进程内直调）
                     ├─ gateway.py  Galaxybase REST 客户端 + SQLite 存储
                     ├─ prompts.py  全部提示词
                     ├─ llm.py      LLM 算力层（SFM 生产 / Dify 本地测试）
                     └─ settings.py 运行时配置热更新
```

**三能力分流（/api/chat 带 mode：auto/analysis/qa/writing）**：手动模式优先直入对应流程；
auto 用扩展意图识别任务域（概念问答→qa、成文请求→writing），并有写作续写守卫
（state.task=writing 时修改类短语继续改稿）。state 额外存 last_report（分析结果，
写作素材）、writing_draft/writing_reqs（当前文稿与累计要求）。

**编排全部在 Python 内完成，智能体平台只当 LLM 算力**（平台侧工作流仅
开始→LLM→回复 三节点）。旧的平台转发网关（根目录 app.py）已删除，其
Galaxybase 客户端/存储/schema 逻辑融合为 `orchestrator2.0/gateway.py`。

## 关键约束

- **兼容 Python 2.7 与 3.x**：所有 py 文件保持双兼容（`from __future__ import print_function`、`u""` 字符串、`.format()` 而非 f-string、`dict.get()` 而非 `dict[]`）。目标生产环境是无网络的旧机器，不能引入仅 py3 的依赖。
- 无测试、无 lint。验证改动就是启动服务后在浏览器里对话测试。
- 配置优先级：页面「系统配置」（`orchestrator2.0/runtime_config.json`，实时生效）> 代码默认值。图数据库默认连接（GALAXY_*）在 `gateway.py` 配置区，LLM 平台默认值在 `config.py`。

## 常用命令

```bash
./start.sh                   # 免安装启动（PYTHONPATH 挂载 offline/lib_*），端口 5200
./install.sh                 # 离线一键安装依赖（识别架构，装 pip + wheels）
python orchestrator2.0/server.py   # 直接运行
pip install -r requirements.txt    # 有网环境装依赖
```

## 架构要点

- `server.py`：Flask 入口。前端接口（页面 / SSE 聊天 /api/chat / 历史会话 / 停止 / 系统配置 /api/settings）+ 外部调试图库的工具接口（`/graph/schema`、`/graph/business/search`、`/graph/query`、`/sessions/{id}/context`、`/sessions/{id}/turns`，包装 store/gateway 现有函数）。
- `pipeline.py`：对话主流程，四分支（chat / fact / targeted / search），`run_pipeline` 生成器 yield `("status", ...)` / 文本块 / `("error", ...)`。**确定性层与 LLM 层严格分工**：搜索定位、证据查询、去重、分桶、序号解析全部代码完成，LLM 只做意图理解和排版。
- `gateway.py`：Galaxybase REST 客户端（`POST /api/login` RSA PKCS#1 v1.5 纯标准库实现 → token 缓存 → `POST /api/cypher/commit`，graphName 在请求体顶层，返回同 Neo4j columns/data/row）+ SQLite（`orchestrator2.0/history.db`，表 conversations/messages/session_state）+ `build_schema`（官方 `GET /api/buildGraph/getSchema` > Cypher 扫描 > `schema_notes.json` 人工配置，带 TTL 缓存）+ `<think>` 流式剥离状态机。
- `settings.py`：界面保存的运行时配置通过 setattr 覆盖 gateway 模块变量（GALAXY_*）并清空 token/schema/类型表缓存，热生效。
- 该版本 openCypher **不支持 `toString()`**（`toLower` 可用），也不支持 `CALL db.labels()` 等 Neo4j 过程。

### schema_notes.json 与多图绑定

`orchestrator2.0/schema_notes.json` 是分析质量的"深度控制器"：名称属性、关系含义、影响传播方向、entity_roles 类型角色标注、已验证查询示例、business_search 搜索配置。新增图实体类型时在 entity_roles 加语义标注即可，代码零改动。

**换图复用（无硬编码）**：代码里不写死 业务事项/业务依赖/承载于 等概念，全部读 schema_notes 的 `core_semantics` 段（anchor_label 锚定类型 / dependency_rel 依赖传播关系 / dependency_depth / hosting_rel+system_label 承载系统 / name_prop / rollup_exclude_rels 归属链排除 / candidate_labels 候选搜索类型），默认值在 gateway.py `DEFAULT_CORE_SEMANTICS`。提示词 ANALYZE_PROMPT 里的关系语义经 `prompts.analyze_prompt()` 按当前图动态替换。

**多图绑定**：优先加载 `schema_notes.<图名>.json`（图专属），不存在回退默认 schema_notes.json（带 graph 字段标记归属图）。系统配置弹窗「绑定图模型」按钮 → `POST /api/schema/bind`：读取目标图 schema，无配置时自动生成骨架（核心语义启发式猜测：按命名+关系连接度+两端类型推断，需人工校对）。换图流程：系统配置改图名保存 → 点绑定 → 校对生成的 schema_notes.<图名>.json → 即可对话使用。

### 前端

`orchestrator2.0/templates/index.html` 是无构建的单文件聊天界面（原生 JS，SSE 消费、markdown 渲染、历史会话侧栏、系统配置面板）。

## 离线部署

`offline/x86_64/`、`offline/aarch64/` 是 wheel 备用；`offline/lib_*` 是解压好的免 pip 依赖（`start.sh` 用）。ARM 的 MarkupSafe 是纯 Python 重打包版。改动依赖时两个架构都要同步。

图数据库是 **Galaxybase**（REST 默认端口 18088，文档在原 field-impact-analysis 目录 `01REST使用说明.pdf`）。REST 层没有"列出所有图"的接口，`GALAXY_GRAPH` 必须人工指定（当前图：业务底账测试图）。
