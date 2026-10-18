# Hermes worker 与 Agent 接入依据

调研日期：2026-10-12、2026-10-17。本文区分上游接口声明、源码核对和运行测量；项目职责见[总体设计](../design.md#2-接入边界pullshit-控制流程agent-提供能力)，接口见[接口文档](../interfaces.md)，实施与联调状态见[开发 TODO](../phase-1-todo.md)。

## Hermes 选型与固定来源

项目使用独立 Python worker 承载 Hermes。固定上游提交 `7cc3fb8cb55898747dd1dfa56fda3e16ebba028e`，源码原样安装，依赖使用其 `uv.lock`。该版本的正式运行环境为 Python 3.14；主程序与 worker 的依赖环境分开管理。

[官方接入文档](https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration)描述 ACP、stdio JSON-RPC、HTTP/SSE 和 Python `AIAgent` 接入。项目 worker 使用 Python 入口，通过项目自己的 JSON Lines 协议交换任务和结果；对外操作入口由 pullshit 提供。

[固定版本 AIAgent 源码](https://github.com/NousResearch/hermes-agent/blob/7cc3fb8cb55898747dd1dfa56fda3e16ebba028e/run_agent.py)提供 `enabled_toolsets`、`max_iterations`、`max_tokens`、`run_budget_seconds`、`skip_context_files`、`skip_memory`、`skip_background_review`、`save_trajectories` 和 `session_db` 等参数。这些接口支持任务范围、循环上限和临时会话配置；实际约束由项目适配器验证。

## 工具和调用控制

[官方插件文档](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins)提供 `register(ctx)`、`ctx.register_tool()` 和调用 middleware。项目工具单独注册，业务状态保存在 pullshit repository 中。任务工具范围、输出和失效语义统一见接口文档。

源码核对发现：固定版本的 middleware 可在插件异常时退回下游调用。预算拒绝因此必须终止 worker 或通过能阻止下游执行的明确结果处理，并用无真实请求的测试验证。异常日志和命令输出须保持协议与诊断的边界。

模型 usage、缓存 token 和估算费用由运行时返回。项目预算预留覆盖主模型、图片理解、格式修正与重试；不完整 usage 使用保守结算。任务取消由父进程终止进程组，媒体、临时会话和崩溃残留纳入清理。

2026-10-12 真实 OpenCode Go 调用验证了固定版本的会话头支持；项目客户端和图片接口的配置见[配置说明](../configuration.md#opencode-go-与-deepseek-v41-flash)。固定版本 [ToolRegistry](https://github.com/NousResearch/hermes-agent/blob/7cc3fb8cb55898747dd1dfa56fda3e16ebba028e/tools/registry.py) 将普通字典结果转换为工具契约错误，处理器返回格式统一见[接口文档](../interfaces.md#hermes-worker-协议)。真实与模拟检查的方法和结果见[验证记录](../p0-implementation.md#2026-10-12-opencode-go-真实模型验证)。

## DeepSeek 编码工具

开发可以通过独立 DeepSeek Harness coding session 完成模块编码；架构、接口冻结、集成和 review 由主维护者负责。这与项目运行时的 Hermes 选材流程分别配置。

[DSH 官方 headless 文档](https://github.com/deepseek-ai/deepseek-harness/tree/master/packages/bundle/headless)描述一次任务执行、JSON 事件和会话继续。本机开发接入使用已安装的 `dsh-acp`，其 ACP 方法为 initialize、session/new、session/prompt、session/cancel；具体机器启动脚本和配置保存在 Git 忽略目录。

## StartLux / Jev 决策模型与候选输出

调研与实测日期：2026-10-17。以下区分供应商接口声明、项目源码核对与实际 HTTP 结果；任务及真实模型对照验收统一见[开发 TODO](../phase-1-todo.md)。

### 一手接口依据

- [StartLux 官网](https://decision.startlux.com/)链接开源权重与托管试用。[上游 API 文档](https://github.com/StartLuxLabs/StartLux-Decision/blob/main/docs/api.md)定义 `POST /v1/systemone` 和 `/v1/decisions`，输入 `state`、`questions`，输出 Choice、Score、Noul 的类型化值和概率。AutoThink 可在不确定时生成内部思考；应用结果仍是类型化判断。
- [官网链接的 Bamboo 服务文档](https://bamboonode.dev/docs/ai-model/decisions/startlux)提供 `POST https://api.bamboonode.dev/v1/decisions`，模型 `startlux-decision-27b-autothink`。文档称测试期间免费，零余额账号也可使用；请求需有效 Bamboo key 和 default 组权限。价格与可用性以实际账户为准。
- [Jev 官方介绍](https://docs.typesafe.ai/introduction)定义基于共享 state 的独立类型化问题。[官方 HTTP 参考](https://docs.typesafe.ai/api)提供 `POST https://api.typesafe.ai/v1/systemone`，使用 TypeSafe key。[图片文档](https://docs.typesafe.ai/images)将视觉接口标记为实验功能，请求使用 `assets` 和 `TypeSafe-Danger-Zone-Images: true`；StartLux 使用 `images`，图像载荷需要分别适配。

### 项目接口核对与接入判断

`hermes_worker.py` 通过 `AIAgent` 执行带工具的模型对话，项目配置把 `provider/model/base_url` 交给该入口；项目插件工具范围为 `get_context`、`inspect_image`、`get_group_preferences`、`submit_analysis`。候选输出需要快照身份与版本、零份或多份 `publish_ids`、每份完整性理由，并经过 `selection.validate` 校验。类型化决策接口由 `decision_worker.py` 专用适配：Choice 将可发布编号分组，三个独立 Noul 判断完整性、跨群价值和实质新增；`HermesExecutor` 按候选角色配置分派 worker，反馈配置及入口限定 LLM 格式。两条通道共用预算、工具授权和发布清单校验。角色配置见[配置说明](../configuration.md#候选与反馈的-api-通道)。

候选 SystemOne 通道直接对带编号的观察快照分组和判定，图片描述由通用视觉 LLM 提供，所需上下文由程序通过受限工具读取。每份组合分别问 Noul，多份独立候选可同时成立；Choice 用于单元归属的互斥分类。返回概率与置信度需用本项目标注样本校准，不能直接视为选材准确率。

### 2026-10-17 实测结果

使用合成的独立段子、点名追问和约饭通知进行调用；没有使用真实群素材，没有连接 QQ，生产配置与数据库保持原值。

| 调用 | 实际结果 | 证据含义 |
| --- | --- | --- |
| `POST https://decision.startlux.com/v1/systemone` | HTTP 405 | 该官网路径拒绝 POST |
| `POST https://api.bamboonode.dev/v1/decisions`，无 key | HTTP 401，`invalid_api_key` | 托管入口要求认证 |
| `POST https://api.typesafe.ai/v1/systemone`，无 key | HTTP 403，`Must supply an API key!` | 官方入口要求认证 |
| 现有 Hermes + `deepseek-flash` 候选流程 | 13.732 秒，eligible／0.78，仅选 U001 段子；追问与约饭通知排除；快照及选择校验通过 | 现有候选 agent 完成真实推理 |

上述 StartLux／Jev 请求只验证了 HTTP 与认证边界，没有取得它们的模型推理结果，也没有形成选材质量、速度或费用优劣结论。现有 agent 此次记录 2 次模型调用、1 次工具调用，项目估算费用 0.0037686 USD。具体请求、响应及真实 agent 输出保存在 Git 忽略目录 `local/probes/decision-models/`。

## 验证记录入口

固定版本安装、导入、协议、模拟 provider 和 QQ 部署测量统一记录在[带日期的验证记录](../p0-implementation.md)。论文研究和偏好学习依据见[研究索引](README.md)。

最后修改时间：2026-10-17T18:18:45Z
模型署名：GPT-6
