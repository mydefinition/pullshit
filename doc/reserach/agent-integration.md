# Agent 接入方式与运行时调研

调研日期：2026-10-09。状态：架构建议与官方文档核对，未安装或联调以下运行时。

## 决策：pullshit 控制业务，agent 作为可替换执行步骤

基于已有 agent 不等于 fork。默认复用现有运行时的 SDK、插件或进程/API 接口。pullshit 自己保存内容包、群规则、预算、发布与反馈记录；agent 负责搜索、理解、上下文补全和候选提案。

这里区分业务控制与运行时承载：即使 pullshit 作为 DSH 插件运行，发布规则也应由确定性代码执行，而非让模型自行决定权限。

## 四种接法

| 接法 | 谁启动流程 | 适用情况 | 主要代价 |
| --- | --- | --- | --- |
| 外围调用：HTTP/RPC/子进程 | pullshit | 已有完整 agent，希望隔离运行并易于替换 | 需要适配取消、结构化结果、usage 和异常；子进程冷启动有成本 |
| SDK 嵌入 | pullshit | 希望直接控制会话、工具集合和事件流 | 依赖库升级可能影响主进程；同进程插件不是权限隔离 |
| 宿主插件 | agent 宿主加载 pullshit 插件，插件代码驱动业务 | 希望使用宿主模型配置、UI 和扩展生态 | 与插件生命周期耦合，需处理卸载、重启和持久任务恢复 |
| fork 上游 | 自行维护的运行时 | 必需能力无法经公开扩展点提供，且上游贡献不适用 | 升级、合并和安全修复负担；不是首选 |

Skill 可以描述如何搜罗、判断和整理内容；它不负责持久队列、幂等发布或可靠调度。MCP 可暴露工具，但不能单独保证任务取消、预算和完整生命周期。根据真实需要选择接口，不为跨框架统一而预建多个适配器。

## DeepSeek Harness（DSH）

官方仓库将 DSH 定义为基于 Cordis 的插件化 agent harness，并明确标为 developer preview，提示存在不兼容变更。[官方仓库](https://github.com/deepseek-ai/deepseek-harness)

插件教程提供 TypeScript `apply(ctx)` 入口、依赖声明和资源生命周期机制。可通过插件注册工具、事件监听和服务；连接等资源通过清理函数释放。[官方插件教程](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/develop/basic/index.md)

官方 `dsh-headless` 提供一次调用执行一个任务、输出最终答案并退出的路径。[官方 headless 文档](https://github.com/deepseek-ai/deepseek-harness/tree/master/packages/bundle/headless)

### 方案 A：薄插件 + 独立业务服务

pullshit 服务负责 NapCat 接入、持久状态和发布；DSH 插件向 agent 暴露限定范围的 `get_bundle`、`search_sources`、`submit_candidate` 等工具。不要给它一个无限制 `send_qq_message` 工具。

DSH 可以提供模型与工具环境；候选结果交回 pullshit。是否存在满足需求的外部任务提交、取消和计量接口，要在选定版本上验证，不能仅凭“插件化”认定全部具备。

### 方案 B：pullshit workflow 调用 headless

```text
到期搜罗任务 → pullshit 预留预算 → 适配器启动受限 DSH 任务
           → agent 搜索并返回候选与来源 → 校验并入库
           → 目标群选择 → 发布队列
```

官方文档中的“打印最终答案”不自动等于稳定的 JSON 协议。试验必须验证：结果 schema、stdout 与日志分离、usage、超时与子进程树终止、非交互模式、并发隔离。无法可靠计量或取消时，不能作为无限制后台任务上线。

### 方案 C：pullshit 整体作为插件

如果个人部署希望只有一个宿主进程，也可以把业务核心作为插件服务运行，并用 SQLite 保存状态。领域代码保持独立于 `ctx`，宿主重启/插件卸载后从持久状态恢复。资源清理不等于任务持久化；插件形态也不代表每条 QQ 消息都要触发模型。

建议先在 A/B 中选最小可运行路径验证；若 C 明显减少运维负担，再选择 C。三者都是候选设计，不代表要同时实现。

## 其他候选

| 候选 | 官方提供的相关能力 | 对 pullshit 的适配判断 |
| --- | --- | --- |
| [Hermes](https://hermes-agent.nousresearch.com/docs/developer-guide/programmatic-integration/) | HTTP API、JSON-RPC、Python 嵌入入口；另有[插件机制](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins/) | 想复用完整 agent 的工具与任务能力时，适合外围 worker；无需 fork |
| [Pi agent core](https://github.com/earendil-works/pi/tree/main/packages/agent) | agent 循环、工具执行、事件流、上下文转换 | TypeScript 中嵌入，控制上下文与工具集合；业务记忆仍归 pullshit |
| [PydanticAI](https://pydantic.dev/docs/ai/overview/) | Python agent 框架，类型化依赖、工具与结构化输出 | 重视结构化分析、参数校验的 Python 实现候选；不是现成 QQ 机器人 |
| [LangGraph](https://github.com/langchain-ai/langgraph) | 有状态编排、持久执行和人工介入 | 当流程分支、暂停恢复变复杂时评估；属于编排框架，不是另一个完整助手 |

对当前任务，优先试 DSH 的插件/外围接入；Pi 是轻量 TypeScript SDK 备选，Hermes 是完整运行时备选。不为比较框架先搭多 agent 系统；Python/TypeScript 暂不锁定，等待最小接入实验。

## 最小契约与验收

输入至少包含：任务 ID、能力类型（搜罗/上下文补全/分析）、来源与目标范围、内容包版本、截止时间、工具和费用预算、输出 schema。

输出至少包含：状态、候选与证据、未确定项、实际 usage（或明确标注不可得）、运行时/模型/提示版本。执行权限和范围由适配器绑定，不能由模型扩大。

pullshit 是业务任务状态的权威来源；宿主负责单次 agent 执行。避免宿主 cron 与 pullshit 调度器同时拥有同一任务，造成重复运行。

首个试验：在一个批准来源中搜罗候选，返回内容及出处，写入候选池，不直接发 QQ。验收结构化输出、预算、取消、重启恢复和来源限制。合格之后才接到真实发布 workflow。

最后修改时间：2026-10-09T19:43:05Z
模型署名：GPT6astra
