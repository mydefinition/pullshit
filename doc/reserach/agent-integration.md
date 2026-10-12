# Hermes worker 与 Agent 接入依据

调研日期：2026-10-12。本文区分上游接口声明、源码核对和运行测量；项目职责见[总体设计](../design.md#2-接入边界pullshit-控制流程agent-提供能力)，接口见[接口文档](../interfaces.md)，实施与联调状态见[开发 TODO](../phase-1-todo.md)。

## Hermes 选型与固定来源

项目使用独立 Python worker 承载 Hermes。固定上游提交 `7cc3fb8cb55898747dd1dfa56fda3e16ebba028e`，源码原样安装，依赖使用其 `uv.lock`。该版本的正式运行环境为 Python 3.14；主程序与 worker 的依赖环境分开管理。

[官方接入文档](https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration)描述 ACP、stdio JSON-RPC、HTTP/SSE 和 Python `AIAgent` 接入。项目 worker 使用 Python 入口，通过项目自己的 JSON Lines 协议交换任务和结果；对外操作入口由 pullshit 提供。

[固定版本 AIAgent 源码](https://github.com/NousResearch/hermes-agent/blob/7cc3fb8cb55898747dd1dfa56fda3e16ebba028e/run_agent.py)提供 `enabled_toolsets`、`max_iterations`、`max_tokens`、`run_budget_seconds`、`skip_context_files`、`skip_memory`、`skip_background_review`、`save_trajectories` 和 `session_db` 等参数。这些接口支持任务范围、循环上限和临时会话配置；实际约束由项目适配器验证。

## 工具和调用控制

[官方插件文档](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins)提供 `register(ctx)`、`ctx.register_tool()` 和调用 middleware。项目工具单独注册，业务状态保存在 pullshit repository 中。任务工具范围、输出和失效语义统一见接口文档。

源码核对发现：固定版本的 middleware 可在插件异常时退回下游调用。预算拒绝因此必须终止 worker 或通过能阻止下游执行的明确结果处理，并用无真实请求的测试验证。异常日志和命令输出须保持协议与诊断的边界。

模型 usage、缓存 token 和估算费用由运行时返回。项目预算预留覆盖主模型、图片理解、格式修正与重试；不完整 usage 使用保守结算。任务取消由父进程终止进程组，媒体、临时会话和崩溃残留纳入清理。

## DeepSeek 编码工具

开发可以通过独立 DeepSeek Harness coding session 完成模块编码；架构、接口冻结、集成和 review 由主维护者负责。这与项目运行时的 Hermes 选材流程分别配置。

[DSH 官方 headless 文档](https://github.com/deepseek-ai/deepseek-harness/tree/master/packages/bundle/headless)描述一次任务执行、JSON 事件和会话继续。本机开发接入使用已安装的 `dsh-acp`，其 ACP 方法为 initialize、session/new、session/prompt、session/cancel；具体机器启动脚本和配置保存在 Git 忽略目录。

## 验证记录入口

固定版本安装、导入、协议、模拟 provider 和 QQ 部署测量统一记录在[带日期的验证记录](../p0-implementation.md)。论文研究和偏好学习依据见[研究索引](README.md)。

最后修改时间：2026-10-12T17:47:09Z
模型署名：GPT-6
