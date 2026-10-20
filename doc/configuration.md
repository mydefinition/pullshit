# 配置、预算与部署

配置默认值以 [config/defaults.json](../config/defaults.json) 为准。加载顺序为默认配置 → `config/local/<profile>.json`；密钥从环境变量读取。操作命令见 [README](../README.md)，接口和字段行为见[接口文档](interfaces.md)，联调状态见[开发 TODO](phase-1-todo.md)。

## 环境与连接

`profile` 为 `development` 或 `production`。数据库、媒体、worker 和日志位于各自环境目录；路径解析检查目录与符号链接隔离。真实配置和环境变量文件保持 `600` 权限。

- `napcat.endpoint`：正向联合 WebSocket 根地址，协议为 ws/wss。凭据使用 `napcat.access_token_env` 指定的环境变量。
- `authorization`：`source_group_ids`、`target_group_ids`、`operator_qq_ids` 分别定义来源集合、广播目标集合及操作者；ID 使用十进制字符串。来自源群的选中内容发送到全部目标群，排除其来源群。
- `runtime.mode`：`auto` 自动发布，`shadow` 仅发现和分析。
- `sending.enabled`：发布开关。持久暂停独立生效。

配置示例见 [local.example.json](../config/local.example.json)。主程序从认证连接获取 QQ 账号，来源与目标集合允许重叠，广播收件范围在处理和发送边界重新检查。

## 本机模拟接口

`simulation-api` 使用现有 profile 配置连接 NapCat，`--account` 固定预期 QQ 登录账号，`--port` 默认 18700；监听地址固定为 `127.0.0.1`。`--token-env` 默认 `PULLSHIT_SIMULATION_API_TOKEN`，对应环境变量至少 32 字符，使用独立私有文件管理。API token 与 NapCat token 分别管理，API token 的环境变量名称通过命令参数指定。

接口在请求及发送边界重新加载目标授权、auto 模式、发送开关和持久暂停。操作员模拟请求每进程每分钟最多 5 次，固定发送含说明的两个纯文本节点；自动策展发布额度继续由发布队列管理。HTTP 请求与返回字段见[接口文档](interfaces.md#模拟转发-http-接口)。接口使用当前环境数据库同目录的独占锁及有界回执文件，可与业务进程同时运行；业务数据库保持六张表。

## Hermes worker

主程序 Python 3.12+；固定 Hermes 版本的 worker 使用独立 Python 3.14 环境。安装时固定上游源码提交，并使用其依赖锁：

```bash
python3 -m pip install uv
mkdir -p local
git clone --filter=blob:none --no-checkout https://github.com/NousResearch/hermes-agent.git local/hermes-runtime
git -C local/hermes-runtime fetch --depth=1 origin 7cc3fb8cb55898747dd1dfa56fda3e16ebba028e
git -C local/hermes-runtime checkout --detach 7cc3fb8cb55898747dd1dfa56fda3e16ebba028e
uv sync --project local/hermes-runtime --python 3.14 --locked --no-dev
```

`agent.python` 填写该环境解释器绝对路径。`provider/model/base_url/api_key_env` 配置主模型；`vision_model/vision_base_url/vision_api_key_env` 配置图片理解。密钥值通过环境变量注入。

`daily_budget_usd` 和 `task_budget_usd` 是显式费用上限；`input_usd_per_million`、`output_usd_per_million` 填写文本模型单价；`vision_input_usd_per_million`、`vision_output_usd_per_million` 填写视觉模型单价。配置视觉模型时同时校验连接、密钥与独立单价。供应商 usage 缺失时占用预留额度。首版只启用项目工具，发布凭据由主程序持有。

默认并发 2、时限 90 秒、模型与工具调用各最多 8 次。输出 token 和输入字符上限共同限制任务。错误修正属于同一任务额度。素材和指令超过输入上限时返回需复核。模型、内容窗口和保留期配置在服务启动时加载；群授权、发送模式、开关、发送额度与持久暂停在处理或发布边界复查。

## 候选与反馈的 API 通道

`agent` 顶层提供共享执行限制、预算、视觉 LLM 及通用 LLM 连接。`agent.selection` 与 `agent.feedback` 按任务覆盖连接；空字符串和数值 0 继承顶层值。角色字段如下：

| 字段 | 行为 |
| --- | --- |
| api_format | selection 允许 `llm`／`systemone`；feedback 固定 `llm`，配置及执行入口均校验 |
| provider / model / base_url / api_key_env | 独立连接；llm 经 Hermes 使用 provider，systemone 使用类型化 HTTP 请求 |
| timeout_seconds / max_input_characters | 覆盖任务时限及输入字符上限；决策输入按每题实际渲染所需的共享 state 与单题问题计算字符上限 |
| free | 显式设为 true 的自托管 selection systemone 按零 API 费用计量，仍记录 token 与调用数；视觉 LLM 和反馈按独立价格结算 |

`agent.decision.max_units` 限制整个观察快照的原子单元数（默认 16，范围 1–24），超限进入复核。`think_mode` 为空时只发送通用 SystemOne 字段，兼容 Jev；StartLux 可设 `never`／`auto`／`always`，启用思考时发送 `think_budget`（默认 512）。选材及完整性概率沿用 `agent.minimum_score` 门槛；这些概率需通过项目样本评估，不能当成业务准确率。

自托管纯文本 StartLux 示例：

```json
{
  "agent": {
    "selection": {
      "api_format": "systemone",
      "model": "startlux-decision-9b-autothink-q4km",
      "base_url": "https://gosleep.top/startlux",
      "api_key_env": "PULLSHIT_STARTLUX_API_KEY",
      "free": true,
      "timeout_seconds": 120,
      "max_input_characters": 4000
    },
    "feedback": {"api_format": "llm"},
    "decision": {"max_units": 16, "think_mode": "never", "think_budget": 512}
  }
}
```

HK 宿主机可使用 `http://127.0.0.1:18090`；其他机器使用 HTTPS 入口，具体地址与密钥保存在本机私有配置。base_url 可带代理前缀，或直接填写 `/v1/systemone`／`/v1/decisions` 完整地址；以 `/v1` 结尾时追加 `/systemone`。API key 通过指定环境变量注入。

SystemOne 候选在同一执行器内保持单并发，使用主程序 Python 的独立子进程。它先通过 Choice 按语义关系将可发布原子单元分组，再以 Noul 分别判断独立完整性、跨群传播价值与实质新增贡献，构造零份或多份发布清单。需要上下文时通过项目工具补取并使用更新后的快照；超出输入、调用或上下文限制时进入复核。HTTP 401／422／429／502、缺失答案或 usage、非法概率和 choice=null 进入复核。分组中的低置信度与 none_probability 告警只暂缓对应单元及依赖它的组合，其他独立组合继续接受三个 Noul 校验；各阶段共用既有门槛，不自动切换模型或重试请求。

纯文本决策模型通过现有 `vision_*` 通用 LLM 读取图片描述，并向父进程登记检查证据；视觉请求继续使用其独立密钥、单价及共享任务预算。文档中的 4096 token 服务上限由后端最终校验；字符上限用于前置限制，程序保留完整素材，超限返回复核。模型技术依据见[Agent 接入调研](reserach/agent-integration.md)，真实联调与任务状态见[开发 TODO](phase-1-todo.md)。

## 独立发布审查

review.enabled 默认为 true；启用时每份选中内容需先通过独立审查才可入队。review.minimum_score 默认为 0.8，表示放行结论的最低置信度。review.policies 为最多 20 项的替换列表，每项包含唯一的 ASCII id、enabled 及非空 instructions；启用审查时至少启用一条策略。默认 politics 策略拦截现实政治人物、政党、政权、政治事件／运动的宣传、争议、讽刺与影射，普通生活、技术或商业话题仅有国家或地名按普通内容判断，无法确认时暂缓。策略描述可按群运营要求修改。

```json
{
  "review": {
    "enabled": true,
    "minimum_score": 0.8,
    "policies": [
      {"id": "politics", "enabled": true, "instructions": "拦截现实政治相关内容及影射；无法确认时暂缓发布。"}
    ]
  },
  "agent": {"review": {"api_format": "llm"}}
}
```

agent.review 使用与 feedback 相同的角色字段并限定 llm，默认继承顶层 LLM 模型、连接、时限和输入上限，支持独立覆盖；选材为 systemone 时，审查仍使用 llm。审查启动独立任务与会话，逐张重新检查待发图片。单任务费用及全局日费用沿用 agent 预算，原队列的额度在审查通过、实际发送前预留。review 开关、策略文本及最低置信门槛在入队与发送边界重新读取；旧候选缺少审查记录、策略变更或待发内容变化均阻止发送，记录保存于候选 JSON，六张业务表保持既有结构。审查禁用为显式配置行为；启用时故障与不确定结论均暂缓。

独立审查适用于自动策展发布链路；反馈确认、管理员状态回复及操作员模拟接口按各自调用契约处理。离线回放需使用 review:<内容指纹> 或 review:* 显式提供审查结果，选材模拟结果单独提供。

## DeepSeek 官方 API

核对日期：2026-10-17。[官方接口说明](https://api-docs.deepseek.com/)提供 OpenAI 兼容的 Chat Completions；配置 `provider=custom`、`model=deepseek-flash`、`base_url=https://api.deepseek.com`，密钥由 `PULLSHIT_AGENT_API_KEY` 注入。官方模型名 `deepseek-flash` 对应 DeepSeek V4.1 Flash，支持工具调用和[图片理解](https://api-docs.deepseek.com/guides/vision/)；视觉配置使用相同模型、入口和密钥环境变量。

按[官方价格表](https://api-docs.deepseek.com/quick_start/pricing/)的峰值价格，将文本和视觉的每百万输入／输出 token 单价均配置为 0.30 / 1.20 USD；缓存命中和非高峰折扣仍按配置价保守估算。每日与单任务预算分别独立配置。部署切换时先验证认证、真实文本和图片任务，再替换私有配置并重启业务进程；验证结果与具体设备记录保存在 `local/`，开发及联调状态见[开发 TODO](phase-1-todo.md)。

## OpenCode Go 与 DeepSeek V4.1 Flash

核对日期：2026-10-19。[官方推理接口](https://opencode.ai/v2/docs/console/api/inference)提供 OpenAI Chat Completions；工作区 `GET /console/api/v2/config` 提供模型、入口和单价。[Go 文档](https://opencode.ai/v2/docs/console/go/)要求客户端标识和每次对话稳定的 `x-opencode-session`。

配置 `provider=custom`、`model=deepseek-v4.1-flash`、`base_url=https://opencode.ai/inference/go/openai/v1`。密钥使用 `PULLSHIT_AGENT_API_KEY`。视觉配置可使用同一模型、入口和环境变量；文本与视觉费用分别配置。

候选与反馈都使用该连接时，将两个角色配置分别设为 `{"api_format":"llm"}`，由顶层继承模型、入口、密钥、时限和输入上限。私有配置中的完整 `selection` 对象只保留通用 LLM 字段；`free` 取默认 false。视觉字段 `vision_model`、`vision_base_url` 和 `vision_api_key_env` 分别填写相同模型、入口及密钥环境变量。修改后执行 ready 配置检查并重启业务服务。

worker 标识为 `pullshit/0.2`；固定 Hermes 版本发送主模型会话头，图片请求使用同一任务 ID。项目工具插件把父进程返回的对象编码为 Hermes 接受的 JSON 字符串。

联调配置每日 1 USD、单任务 0.05 USD；每百万输入／输出 token 分别按 0.30 / 1.20 USD 峰值单价计量。缓存输入也按配置输入价计算，`usage.cost_usd` 为项目的保守费用估算；供应商实际账单与该估算分别核对。真实调用结果见[验证记录](p0-implementation.md#2026-10-12-opencode-go-真实模型验证)。

2026-10-19 认证读取的 Go 模型配置声明支持文字、图片及工具，每百万输入／输出 token 为 0.15 / 0.60 USD、缓存读取为 0.003 USD。上述联调单价继续用作保守预算上限；实际调用与部署结果见[开发 TODO](phase-1-todo.md)。

## 内容、流量与保留期

| 配置 | 默认值 |
| --- | --- |
| content.quiet_seconds / max_wait_seconds | 60 / 300 秒 |
| context.reply_depth / forward_depth | 3 / 3 |
| context.max_nodes / max_fetch_calls / timeout_seconds | 1000 / 5 / 20 秒 |
| context.expansion_calls / expansion_units | 每选材任务累计 2 次 / 40 个原子单元 |
| context.expansion_characters / expansion_images | 每选材任务累计 16000 字符 / 4 张图片 |
| context.nearby_seconds | 相邻上下文前后各 600 秒；明确引用受期限及总预算约束 |
| limits.max_message_segments | 100，应用配置上限 100 |
| limits.min_interval_seconds | 每目标群 600 秒 |
| limits.daily_batches / daily_segments | 每账号／目标群 20 条发布消息 / 500 个内容消息段 |
| limits.daily_download_bytes / daily_publish_bytes | 全局各 1 GiB |
| media.max_image_bytes / limits.max_batch_media_bytes | 10 / 20 MiB |
| media.cache_max_bytes | 2 GiB |
| retention.message_days / content_days / log_days | 3 / 7 / 7 天，内容上限 7 天 |
| retention.delivery_days / budget_days | 90 / 30 天 |
| preferences.min_publications / min_members / max_step | 至少 3 份不同内容 / 3 名成员 / 单次步长 0.05 |

窗口按账号与群收集连续对话，消息片段编号用于选择实际发布内容。上下文扩展通过 get_context 的 anchor_id、mode 与 reason 定位，已有单元复用编号；补充文本按字符计量，模型请求仍受输入字符上限与费用预算限制。

自然语言引用反馈在来源及目标授权范围内受理。确认回复每群每分钟最多 5 次，受 auto 模式和发送开关控制；持久发布暂停独立于反馈受理及确认。近期正负反馈每群最多 6 例提供给选材任务，权重汇总继续遵守表中的最少内容数及成员数。

日额度按 UTC 日期分桶。发送尝试占用消息条数、消息段数及发布额度；构造的聊天合并转发按节点内文本／图片消息段累计，原样转发卡片按一段计量；失败或回执不确定同样占用。每个广播目标独立计算间隔和日条数／段数额度，分片逐条执行；下载和发布字节使用全局额度。

新库使用 `state.sqlite3`。程序只打开新 schema。具体设备的启动服务、路径、凭据和运维脚本存放在 Git 忽略的 `local/`。

最后修改时间：2026-10-20T08:01:40Z
模型署名：GPT-6
