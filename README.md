# pullshit

> *聚是一坨 **shit**，散是漫天稀。*

QQ 群内容策展机器人。将源群对话拆成带编号的文字、图片等内容单元，交给候选 agent 从上下文中选出零份或多份完整内容，按明确的编号清单广播到全部目标群，排除内容来源群，并从明确反馈中更新群口味。

普通消息默认保留 3 天，正文、图片和具体反馈最多 7 天。程序自动清理内容、媒体和临时文件，并回收 SQLite 与 WAL 占用。

- [总体设计](doc/design.md)：模块、流程和数据生命周期。
- [接口文档](doc/interfaces.md)：Python 接口、worker 协议、CLI 和群命令。
- [配置说明](doc/configuration.md)：连接、规则、预算与环境隔离。
- [开发 TODO](doc/phase-1-todo.md)：实施和联调验收状态。
- [验证记录](doc/p0-implementation.md)：有日期的方法与实测结果。
- [Agent 接入依据](doc/reserach/agent-integration.md)、[反馈学习研究](doc/reserach/README.md)。

## 安装与检查

主程序使用 Linux、Python 3.12+；Hermes worker 使用独立 Python 3.14 环境。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip check
```

Hermes 固定使用上游提交 `7cc3fb8cb55898747dd1dfa56fda3e16ebba028e`，原始依赖按其 `uv.lock` 安装；安装流程见[配置说明](doc/configuration.md)。

## 配置与运行

复制 `config/local.example.json` 为 `config/local/development.json`，填写连接、来源与目标群集合、Hermes 解释器、模型、单价及每日费用预算。候选通过 `agent.selection.api_format` 选择 `llm`（通用 LLM，经 Hermes）或 `systemone`（Jev 类决策接口）；反馈的 `agent.feedback.api_format` 固定为 `llm`。示例采用自托管 StartLux 候选和独立 LLM 反馈，分别注入 API key，具体字段见[配置说明](doc/configuration.md#候选与反馈的-api-通道)。真实配置与密钥文件存于 Git 忽略目录。发送由 `sending.enabled` 控制。

```bash
.venv/bin/python -m pullshit check-config --profile development
set -a
. config/local/development.env
set +a
.venv/bin/python -m pullshit check-config --profile development --ready
.venv/bin/python -m pullshit run --profile development
```

QQ 登录由 NapCat 管理。主程序认证后绑定账号，每环境只运行一个实例。`runtime.mode=auto` 执行自动发布，`shadow` 执行发现和判断。

每个观察窗口执行一次选材，agent 为每份候选给出发布编号与完整性理由；程序只发送选中内容。选中内容由独立 LLM 任务按可配置策略审查，通过后入发送队列。默认启用涉政审查，覆盖文字、图片和整张合并转发；不明确、超时、图片未检查或预算不足时暂缓发送。策略和审查模型配置见[配置说明](doc/configuration.md#独立发布审查)。图片检查针对所选图片，上下文补查共用任务预算和历史处理记录。选中内容发送到目标集合 `T` 中除来源群 `s` 外的所有群，即 `T − {s}`；源群和目标群允许重叠。单条源消息的所选图文使用普通消息发送；连续选中的多条源消息组成合并转发卡片，每条原消息对应一个节点，保留发送者、时间及节点内图文顺序。每个分片最多 100 个内容消息段，超限时按原消息边界分片，单条过长消息按原片段顺序拆分。合并转发卡片整体作为一个原子，选中后使用原消息转发，保留整张卡片及内部记录；其中所有图片均须检查。默认每群间隔 10 分钟、每天最多 20 条和 500 个消息段；每日下载及发布内容额度各 1 GiB。成功分片保留回执；回执丢失进入 `unknown`。

## 操作

```bash
.venv/bin/python -m pullshit status --profile development
.venv/bin/python -m pullshit candidates --profile development
.venv/bin/python -m pullshit pause --profile development
.venv/bin/python -m pullshit resume --profile development
.venv/bin/python -m pullshit cleanup --profile development --compact
```

`pause/resume` 默认控制整个环境，也可指定 `--account`、`--group`。核对确切目标消息后执行 `confirm DELIVERY_ID --message-id=12345 --profile development`。明确失败的分片可用 `retry DELIVERY_ID --profile development` 重试；重新判断候选使用 `reanalyze BUNDLE_ID --profile development`。

源群引用消息发送 `/搬` 可提名。在任一授权群引用一条可读取的消息并明确 @机器人，再说「我喜欢这个」「这类以后多发点」或「这种少来点」，agent 会理解喜好并引用回复确认；素材可以是群内原消息或 bot 已发布的内容。也可在引用并 @机器人后使用 `/喜欢`、`/少来点`。近期有效反馈会作为后续选材的偏好例子；反馈本身不触发搬运。管理员可使用 `/暂停`、`/恢复`、`/状态`。具体权限及返回行为见[接口文档](doc/interfaces.md)。

本机操作员可使用[模拟转发 HTTP 接口](doc/interfaces.md#模拟转发-http-接口)，指定 QQ 号和纯文本内容。卡片固定包含「模拟测试、非本人发言」说明；响应分别报告发送回执、请求 QQ 号和回读身份。接口使用独立 Bearer token、幂等请求键及已授权目标群。

通过私有环境文件设置至少 32 字符的 `PULLSHIT_SIMULATION_API_TOKEN`，并加载 NapCat 凭据后启动：

```bash
.venv/bin/python -m pullshit simulation-api --profile development \
  --account 900001 --port 18700
```

`900001` 为机器人 QQ 示例，填写实际登录账号。接口固定监听 `127.0.0.1`，发送遵守环境的 auto 模式、发送开关及持久暂停。

## 离线回放

回放仅使用模拟发送器和明确提供的模拟判断，输出带有 `simulation=true`。数据库位于独立的 `data/replay/`。

```bash
.venv/bin/python -m pullshit replay tests/fixtures/synthetic-events.jsonl \
  --database data/replay/example/state.sqlite3 \
  --account 900001 --source-group 900101 --target-group 900201
```

通过 `--decisions PATH` 提供按内容指纹或内容包 ID 索引的 `AgentResult` JSON 对象；模拟文件中的 `*` 可提供统一选材判断，`review:<内容指纹>` 或 `review:*` 单独提供审查结论。未提供判断的内容显示为需复核。回放将事件时间按相对间隔映射到运行时间，保留真实配置和真实发送的隔离边界。

最后修改时间：2026-10-20T07:30:38Z
模型署名：GPT-6
