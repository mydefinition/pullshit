# 契约与验证记录

本文保留 2026-10-10 的接入测试证据，并记录 2026-10-12 重建版本的验证方法和结果。业务接口以[接口文档](interfaces.md)为准。QQ 账号由 NapCat 管理，部署凭据保存在 Git 忽略的本地配置或仓库外机器清单。开发任务与阶段状态见[开发 TODO](phase-1-todo.md)。

## 运行与接口依据

Python 3.12+，运行环境为 Linux。业务代码位于 [src/pullshit](../src/pullshit/)，平台适配采用 OneBot 11 正向联合 WebSocket；依赖固定为 websockets 17.2，安装校验值见 [requirements.lock](../requirements.lock)。运行与检查命令见 [README](../README.md)。

模块职责、接口和依赖见[QQ 接入层设计](qq-adapter.md)。

生产配置与开发配置采用独立数据库、媒体目录、日志目录和 token 环境变量。真实凭据和账号保存在私有配置及证据目录。

## 2026-10-10 事件与发送契约记录

原始事件保存授权范围内的 JSON 证据、账号、群、事件类型、发生/接收时间与去重键。群消息的幂等身份为账号、群、平台消息 ID；其余事件按完整原始载荷指纹识别重复。接收程序忽略私聊、其他账号、未授权群和支持范围之外的事件类型。

真实普通群消息载荷包含 `message_id`、`message_seq` 与字符串 `real_seq`；已检查样本中的 `message_seq` 等于 `message_id`。这与 [NapCat 消息构造实现](https://github.com/NapNeko/NapCatQQ/blob/main/packages/napcat-onebot/api/msg.ts)（2026-10-10 核对）中 `message_seq=msg.id`、`real_seq=msg.msgSeq` 一致。应用通过账号、群与消息 ID 定位记录，保留原始序号字段作为证据，这些字段不构成历史消息连续性的保证。

消息记录保留有序消息段及 `issues`。引用消息 ID 使用字符串；接收阶段保留引用及 `forward_not_fetched` 证据，查询入口组装有界引用链和转发树，并返回缺失或截断原因。CQ 字符串作为不透明文本保留并标记；联调时要求 NapCat 使用数组消息格式。查询与独立证据缓存的契约见[QQ 接入层设计](qq-adapter.md)。

探测入口是本地操作界面，使用当前系统用户对本地配置及 token 的访问权限。`operator_id` 用于授权范围校验和审计；它不构成远程 QQ 身份认证。

探测请求内容在发送前生成哈希，`(account_id, probe_id)` 保证同一任务不重复发起；同 ID 修改内容报错。配额与全部分条意图在事务中预留。单个发送进程持有环境级独占锁，每条发送前重读路由与开关并检查持久暂停状态。

已发送消息逐条保存回执。平台明确拒绝标为 `failed`；超时、连接中断、无效回执和中途取消标为 `unknown`。同一探测再次执行时，持锁恢复该探测的中断状态并返回账本。其余尚未执行的条目在可确定的失败后标为 `cancelled`；进程崩溃时它们也可能保守进入 `unknown`，由人工核对。

引用反馈按账号、目标群和确切消息 ID 匹配探测。回执晚到时可以补关联。表情通知保存为 `emoji_unverified` 原始证据。真实测试中，授权操作者对上述测试文本添加表情，收到 `notice.group_msg_emoji_like`：包含操作者 `user_id`、对应 `message_id`、`is_add=true` 和 `likes` 列表；一次表情计数为 1。该事件已按账号、目标群与回执 ID 正确关联到探测账本。诊断查阅使用本地参考缓存 `data/reference/qq-emoji-map.json`；其中已缓存官方参考表列出的全部 167 项（116 项 QQ 系统表情、51 项 Unicode emoji），包含 `265 → 辣眼睛`；每项保留平台 ID、类型和名称。官方页面说明参考表只包含部分平台表情，缓存覆盖该页面全部条目。来源为 [QQ 官方表情对象](https://bot.q.qq.com/wiki/develop/api-v2/openapi/emoji/model.html#emoji列表)，核对日期为 2026-10-10。原始事件保留平台 ID；参考缓存使用 Git 忽略目录。名称用于展示，原始事件的归因与展示名称分别保存。

图片缓存检查文件头和哈希，保持媒体证据可读取。人工发送探测中的图片由操作者确认。

## 2026-10-10 本地验证结果

2026-10-10T04:49:29Z，完成安装、依赖校验、编译检查和自动测试。自动检查使用 [tests](../tests/) 中的模拟输入和本机模拟 WebSocket 服务。

- 自动测试：60 项通过，覆盖配置隔离、类型错误、未知字段、环境目录符号链接、幂等、消息 ID 范围、撤回、反馈归因、配额、暂停、超时、取消、部分失败、并发回执及断线恢复，以及[上下文查询验证](#上下文查询验证)。
- `pip check`：无依赖冲突；`compileall`：通过。
- 开发和生产配置基础检查：通过；QQ 登录与账号由 NapCat 管理，测试账号已登录且真实认证连接通过；授权群与路由已补齐，在线接收配置校验通过。
- [synthetic-events.jsonl](../tests/fixtures/synthetic-events.jsonl) 首次回放：7 个新事件、1 个重复、1 个越权群事件忽略，保存 5 条消息和 3 条待归因反馈证据。
- 同一数据库二次回放：新增 0，重复 8，保存数量保持一致。
- 真实脱敏 fixture：首次新增 5，保存 4 条消息、1 条表情证据；二次新增 0、重复 5。

模拟输入不代表真实平台载荷，媒体测试检查下载与缓存行为。实际 QQ 收发及表情支持情况以固定版本联调结果为准。

## 2026-10-10 接收与探测记录

2026-10-10T03:18:54Z，开发环境程序认证连接 NapCat，并将连接事件写入私有 SQLite 事件库。此次测试配置两个源群到一个测试目标群的两条授权路由，其中一个源群同时作为目标群；发送保持关闭。采集到机器人自身图片、普通成员 @ 与文本、授权操作者图片及心跳事件，消息段顺序与自身消息标记符合预期。

两张真实 QQ JPEG 经手动下载、缓存和哈希读取检查，大小分别为 42,074 与 40,765 字节，并在开发会话中展示。接收流程将事件写入数据库；开发会话通过查询数据库与日志查看证据。

2026-10-10T03:43:21Z，经用户授权，在测试目标群执行一次受控文本发送，平台返回成功回执；探测意图及消息 ID 记入私有账本。发送后恢复 `runtime.mode=receive`、发送关闭、配置暂停及持久暂停。接收程序保存对应自身文本消息，内容与回执 ID 匹配。

## 上下文查询验证

2026-10-10，完整自动检查 60 项通过，包含原有接入和发送检查，以及上下文的账号/群隔离、负缓存、权限撤销、身份变化、查询期间撤回、引用循环、转发节点形状和循环、全局限额、连接/查询超时、子进程终止、过期和损坏缓存、图片地址刷新及离线复用。

在 NapCat 4.18.36 / Linux QQ 3.2.34-53644 上进行了只读检查：

- 已有测试文本的本地查询成功；使用独立私有验证库回取一次 `get_msg`，正文与本地一致，第二次内容回取为 0。
- 已有图片的旧访问地址下载失败；重新读取原消息后，新地址可下载并校验。接入层自动刷新路径实测成功：1 次内容回取、2 次下载尝试，后续离线查询命中缓存。
- 查询期间发送配置保持关闭、暂停；后台接收程序持续运行。加载配置与迁移时重启一次接收程序，`04:36:36Z–04:37:25Z` 的约 49 秒维护空窗已记入覆盖缺口表。
- [真实脱敏 fixture](../tests/fixtures/napcat-sanitized-events.jsonl) 的 5 条事件可离线回放，保存 4 条消息和 1 条表情证据；重复回放新增为 0。采集和替换说明见[fixture 说明](../tests/fixtures/README.md)。

## 模块整理验证

2026-10-10T11:55:19Z，按职责整理 Python 源码和测试目录后，执行以下检查：

- `.venv/bin/python -m unittest discover -s tests`：60 项通过，包含实际下载子进程的地址拒绝检查，以及接收、上下文、发送账本、媒体和回放检查。
- `compileall` 通过；`pip check` 无依赖冲突；开发和生产配置基础检查均通过。
- 重新安装本地可编辑包，`python -m pullshit --help` 与 `pullshit --help` 均可启动。
- 构建 wheel，在独立临时目录校验全部业务模块的导入来源、命令入口及媒体子进程；媒体检查使用本机地址拒绝路径，没有发起网络请求。

源码目录与包职责见[分层与模块](design.md#3-模块与依赖)。

## 2026-10-12 业务重建验证

使用 Python 3.12.3 主程序和独立 Python 3.14.8 Hermes worker。Hermes 源码固定于 `7cc3fb8cb55898747dd1dfa56fda3e16ebba028e`，通过其锁文件安装 67 个依赖包，`AIAgent` 导入成功。三个独立 DeepSeek ACP 编码会话分别产出接入／内容、存储和 agent 模块；接口冻结、集成修正及 review 由 GPT-6 执行。

- `.venv/bin/python -m unittest discover -s tests -v`：137 项通过。包含 99/100/101/200/201 节点顺序拆分，日配额与事务并发，分片失败／unknown 恢复，发布边界暂停与撤回，精确反馈、每日偏好和回滚。
- 子进程验证：实际媒体 worker 拒绝私网地址；下载取消及超时回收真实进程；Hermes worker 超时、取消与临时目录清理通过。取消场景包含忽略 SIGTERM 的子进程。
- 实际文件回收验证：过期媒体文件删除；批量 SQLite 内容删除后数据库大小下降、空闲页归零、WAL 截断为 0 字节；读事务阻塞时报告 busy，解除读锁后回收成功。
- Hermes SDK 验证：使用真实上游 `AIAgent` 和本机模拟 HTTP/SSE 模型服务，完成项目工具调用、流式 usage 结算与图片理解。暴露的工具为 get_context、inspect_image、get_group_preferences、submit_analysis。两次文本调用模拟费用合计 0.00032 USD；视觉使用独立单价，三次调用合计 0.00044 USD。额度拒绝和过长上下文均没有发起模型请求。
- CLI 回放：正常文本提名、模拟选择、发布回执与 `/喜欢` 形成完整流程；同库重复回放未重复发布。未提供模拟判断时没有发布；真实环境路径及回放目录符号链接被拒绝。
- 安装验证：可编辑安装 0.2.0、源码编译、开发／生产基础配置检查和主环境依赖检查通过。wheel 在独立环境安装后，从项目目录外导入 15 个模块，CLI 和媒体子进程运行成功。
- 只读 QQ 连接探测：使用私有开发配置及其环境变量，返回 OutcomeUnknown，未取得登录和版本信息。本轮没有真实群发送，私有配置发送开关为关闭。真实模型连接和平台联调的任务见[开发 TODO](phase-1-todo.md#真实联调与部署)。

模拟模型响应、模拟发送器与上述只读连接结果分别记录；费用数字用于验证计量逻辑，生产费用按供应商 usage 与配置单价计算。

## 2026-10-12 OpenCode Go 真实模型验证

从本机私有凭据文件提取 OpenCode 密钥，模型使用 `deepseek-v4.1-flash`。官方调用依据见[推理 API](https://opencode.ai/v2/docs/console/api/inference)与[Go 接入要求](https://opencode.ai/v2/docs/console/go/)，核对日期为 2026-10-12。工作区配置接口和模型列表读取成功；普通付费入口返回 HTTP 402、Insufficient account funds，Go 入口补齐会话头后返回有效响应。

- 真实 Hermes 文本任务（2026-10-12T18:36:23Z）：3 次模型调用、3 次工具调用，返回 needs_review 和 0.60 分，证据与结果通过校验；本任务预留金额归零，任务目录删除。
- 真实图片任务（2026-10-12T18:33:19Z）：使用本地生成的 128×128 纯色 PNG，完成图片理解、群偏好读取与结构化提交；3 次模型调用、3 次工具调用，返回 rejected 和 0.05 分，图片检查凭据通过校验，任务目录删除。
- 真实调用暴露 Hermes 工具结果必须为 JSON 字符串的契约；插件完成编码后，模型取得有效上下文、偏好与图片结果。SDK 模拟测试逐项解析实际工具内容，检查 accepted 和错误字段。
- 完整测试 140 项通过，包含客户端标识、图片会话头、提交阶段的越权证据拒绝和上下文字符限额。费用预算使用配置峰值单价；缓存计费差异保留为保守估算。
- 开发配置的 --ready 检查通过；重测 QQ 只读连接仍返回 OutcomeUnknown。发送关闭，验证使用模拟 QQ 身份与自行生成的素材，没有真实群发送。

## 协议与安装来源

核对日期：2026-10-10。外部文档支持接入设计，实测证据见本文带日期的验证记录。

- [OneBot 11 正向 WebSocket](https://github.com/botuniverse/onebot-11/blob/master/communication/ws.md)：联合连接、JSON API 与 echo 关联。
- [OneBot 11 公开 API](https://github.com/botuniverse/onebot-11/blob/master/api/public.md)：登录信息、群消息发送与消息读取。
- [NapCat 请求接口兼容表](https://napneko.github.io/develop/api)：正向 WebSocket 和 API 兼容声明。
- [NapCat 上报事件兼容表](https://napneko.github.io/develop/event)：群消息、撤回与部分表情通知支持。
- [NapCat Docker](https://github.com/NapNeko/NapCat-Docker)：官方容器部署和持久化目录。
- [websockets asyncio 客户端](https://websockets.readthedocs.io/en/stable/reference/asyncio/client.html)：客户端调用、超时与缓冲限制。

## 2026-10-17 普通消息广播验证

选材任务携带全部广播目标，按目标群等权汇总偏好，每份内容生成一次判断。来源集合与目标集合分别配置；同一内容发送到目标集合中除全部已知来源群外的目标。发布器使用 `send_group_msg` 发送普通文本与图片，原记录文字以换行连接，按 100 个消息段及媒体字节上限分片，每目标独立保留配额、状态和回执。

源码完整 169 项测试通过，耗时 64.933 秒。隔离构建的 wheel 经独立目标目录安装后执行相同完整 169 项测试，耗时 65.954 秒；实际导入路径指向该安装目录，31 个 Python 模块与源码逐字节一致。验证覆盖选材仅调用一次、源目标重叠排除、每个目标的完整收件、普通文本／图片数组、发送边界检查、目标移除独立取消、99/100/101/200/201 消息段顺序与分片、图片内容校验及 base64 发送、暂停／间隔／日额度、确切引用反馈与 unknown 防重发。真实 Hermes SDK 使用本机模拟模型验证工具调用和结果提交。

2026-10-17T08:02:41Z 安装生产 wheel，SHA-256 为 `913f5b2f0464b20dadf3414b90f4cb32fa600c6035b89e0c42b641bf77d78098`。生产配置与依赖检查通过，业务、隧道及模拟接口均为 RUNNING；QQ 认证、预期账号和全部目标群检查通过。3 个来源可用，配置包含 4 个源群及 1 个目标群。切换前待发投递为 0，保留 6 条 sent 与 2 条 unknown。本次没有额外向 QQ 群发送测试消息，也没有调用真实模型；完整实际群发送行为的验收任务统一见[开发 TODO](phase-1-todo.md)。发行、安装测试、部署与健康证据保存在 Git 忽略的 `local/ops/pullshit/`。

## 2026-10-17 群内自然引用反馈验证

授权源群和目标群可引用可读取的消息自然表达喜好，范围包含 bot 已发布内容及群内原素材。反馈请求同步保存为 pending，模型解释后更新正负态度、题材标签及素材摘要。周期调度恢复待解释事件，重复事件复用既有记录。反馈素材作为独立内容包保存，反馈处理独立于发布队列；后续选材读取目标群及来源群的近期有效反馈例子。正负反馈使用绑定用户事件的普通引用消息确认，每群每分钟最多 5 次。

最终 wheel 的完整 177 项测试通过，耗时 64.341 秒；31 个 Python 模块与最终源码逐字节一致。新增验证覆盖未发布原素材、源群及目标群作用域、普通聊天不计偏好、自然正负态度、重复事件不重复调用、处理中断后恢复、数据库重开查询 pending、反馈确认及后续选材获取近期例子。Hermes 固定 SDK 与本机模拟模型的集成检查包含在完整套件中。

使用真实 DeepSeek 官方 Flash、独立临时 SQLite 和合成文字执行两项预检：我喜欢这个并希望多分享同类笑话，结果 positive，11.345 秒；不喜欢此类笑话并要求少发，结果 negative，20.416 秒。两次 usage 均为 complete，按项目峰值单价合计估算 0.0144198 USD。预检不连接 QQ、不发送群消息，未修改生产反馈或候选。

2026-10-17T10:01:08Z 生产安装通过配置及依赖检查，业务、隧道和模拟接口均为 RUNNING；QQ 账号匹配、6 个来源与 2 个目标均可用。wheel SHA-256 为 `ccf8cab1cae9bc6aa69c7d20dec3887cdcec4dd333bd85f3353d8daa2d0ea912`。发行、语义预检及部署证据分别为 Git 忽略目录中的 `local/ops/pullshit/run/natural-feedback-release.json`、`natural-feedback-preflight.json`、`natural-feedback-deployment.json`。真实群内输入到确认回读的验收任务统一见[开发 TODO](phase-1-todo.md)。

## 2026-10-17 原子选材与有界上下文验证

隔离 wheel 的完整 195 项测试通过，耗时 62.675 秒；32 个 Python 模块与部署源码逐字节一致。回归用例将两张截图、表情和提及放在同一观察包，明确选中两张截图后，发送载荷仅含对应图片；同条 QQ 消息的文字、图片及提及可独立编号，提及供理解。验证同时覆盖零份／多份选择、编号越界、旧快照、版本变化、跨项重复、不完整输出、新增贡献、所选图片检查、队列载荷篡改、历史分配持久化、图像核心去重、嵌入节点外层来源及未选上下文撤回。

上下文检查覆盖以已有编号获取引用、追加编号与版本、历史材料标记、累计调用和字符额度、重复读取及图片下载次数。扩展仅包含历史材料时，发布校验返回 historical_content_only；结合新增回复时要求新增贡献说明。物理可读性由程序校验，语义完整性由 agent 的逐份理由表达。

真实 DeepSeek 官方 Flash 使用隔离合成素材验证：混合文字／图片／提及／闲聊任务耗时 17.664 秒，仅选中 U001 笑话正文，score=0.85；普通聊天耗时 11.373 秒，返回 rejected、空选择。混合素材中的 1×1 测试图片请求返回 vision_http_failed，对应任务保留不确定费用预留，文字仍独立成篇。单独使用 128×128 RGB 合成图片执行相同 inspect_image 路径，耗时 6.38 秒，识别纯红色，返回完整用量。预检使用独立 SQLite，不连接 QQ，不建立真实发送。

2026-10-17T12:19:14.650055Z 安装生产 wheel，SHA-256 为 `e452f9cdb55228fc8bcd205922598ff54403fe78cacf0fa8babf4debacb08c34`。生产配置、依赖及 32 个模块核验通过，业务、隧道、模拟接口均为 RUNNING，6 个源群与 2 个目标群健康检查通过。生产发布额度为每群间隔 60 秒、每日 100 条及 500 个消息段。切换时无 queued 投递，保留 16 条 sent 与 2 条 unknown。三份证据为 Git 忽略目录下的 `local/ops/pullshit/run/atomic-selection-release.json`、`atomic-selection-preflight.json`、`atomic-selection-deployment.json`；自然群聊验收任务见[开发 TODO](phase-1-todo.md)。

## 2026-10-17 候选 SystemOne 与反馈 LLM 通道验证

候选 worker 按角色配置选择通用 LLM 或 SystemOne，反馈配置和执行入口限定通用 LLM。SystemOne 的 Choice 单元分组和完整性／传播价值／实质新增三个 Noul 产生原编号发布清单，仍接受统一快照、所选图片和发布资格校验。自托管候选按显式 free 配置记录零 API 费用，视觉描述与反馈保持 LLM 计费；未知 usage 和失败结果进入复核。

本机 HTTP 服务经真实子进程验证多份独立选择、分段组合、闲聊排除、缺失答案／usage、非法概率、空 Choice、none 告警、401／422／429／502、输入／调用／费用限制、回复上下文追加、图片证据、单并发、排队过期和取消清理。反馈配置为独立 LLM 时没有访问决策端点。新增 11 项测试，隔离安装后的完整 206 项测试通过（93.338 秒），33 个 Python 模块与源码逐字节一致；pip check 和开发配置检查通过。

真实 DeepSeek 反馈预检使用合成段子及「这个冷笑话我很喜欢，以后多发点」，候选连接已配置为 systemone。15.943 秒返回 eligible、positive、score=0.92，提取冷笑话／幽默／自嘲／健身标签；4 次模型调用、4 次工具调用，usage 完整，估算费用 0.0058857 USD。预检使用临时 SQLite，没有连接 QQ 或访问笔记本模型；生产发送和自启保持关闭。

发行 SHA-256：`8e35a1bca7f31930dd660321818f67670cf39eb37df7984d74b5b634c6c2aa40`。发行记录、暂存配置和真实反馈证据分别位于 Git 忽略的 `local/ops/pullshit/run/systemone-release.json`、`local/ops/pullshit/run/systemone-staged-production.json`、`local/probes/decision-models/real-feedback-preflight.json`。真实 StartLux 验收与安装切换任务统一见[开发 TODO](phase-1-todo.md)。

## 2026-10-18T06:05:39Z 自托管 StartLux 网络与认证边界验证

从项目机器访问 `https://gosleep.top/startlux/health` 返回 HTTP 200，响应模型为 `startlux-decision-9b-autothink-q4km`、kind=auto_think；通过已有固定主机密钥 SSH 通道检查 HK 宿主机，`127.0.0.1:18090` 正在监听，loopback health 返回相同模型和 HTTP 200。公网 `/v1/systemone` 的无凭据合成请求返回 HTTP 401，错误明确要求 Bearer key；此请求没有通过认证。

项目进程环境、既有私有生产环境文件及约定的 `config/local/startlux.env` 路径均未提供专用 key，因此本次没有认证模型推理或候选输出结果。33 个源码模块与此前通过完整测试的发行包一致；生产发送为 false，自启标记缺失。检查未连接 QQ、未修改生产配置或业务库。原始结果保存在 Git 忽略的 `local/probes/decision-models/connectivity-20261018.json`，认证联调任务统一见[开发 TODO](phase-1-todo.md)。

## 2026-10-18 StartLux 9B 认证联调与停用状态安装

专用 key 通过指定私有环境变量保存，权限 600；认证后的 health 和 models 均返回 200。接口报告 AutoThink 9B Q4_K_M、llama.cpp、4096 token、纯文本、默认 think_mode=never。预检保持单并发、0.75 门槛与 4000 字符前置限制。

初次混合样本的 Choice 将独立段子在自身归组与丢弃间给出接近概率，触发整轮复核。适配修正为独立语义分组，再进行传播价值判断；分组不确定只暂缓对应单元及依赖组合。真实原始响应与分步诊断保存在 Git 忽略目录；没有降低既有阈值。新增回归验证一个不确定组合不会阻止其他独立内容通过，最终发行包完整 207 项测试通过（98.470 秒），33 个模块与源码逐字节一致。

最终发行包真实预检结果：

| 合成样本 | 结果 | 耗时 |
| --- | --- | --- |
| 单独程序员段子 | needs_review，无选择 | 5.905 秒 |
| AI 与手机依赖段子 | needs_review，无选择 | 4.554 秒 |
| 分两条发送的面试对话段子 | needs_review，无选择 | 4.729 秒 |
| 两份独立段子的同一观察 | eligible，仅 U001 程序员段子 | 5.382 秒 |
| 集合约饭通知 | needs_review，无选择 | 3.037 秒 |
| 128×128 合成纯红图片 | DeepSeek 视觉检查完成，needs_review，无选择 | 12.119 秒 |
| 超长输入 | 请求前 decision_input_character_limit，模型调用 0 | 1.623 秒 |

六组内容样本只有一组产生候选，各组选择校验均通过，实际发布内容组装仅含所选 U001。相同程序员段子单独出现与伴随其他段子时结果不同，说明结果受观察上下文影响；这些样本验证连接与执行语义，不能据此声称选材质量改善。初轮纯红图为 rejected，最终轮为 needs_review，两轮均没有选择图片；视觉检查按独立 LLM 费用结算，最终轮 0.0014373 USD，usage 完整。自托管决策请求记录零 API 费用，输出 token 为 0。

2026-10-18T06:15:52.512063Z 将最终 wheel、角色配置与模型凭据安装到既有生产环境。生产 ready 配置、依赖及 33 个模块哈希验证通过；候选为 systemone／startlux-decision-9b-autothink-q4km，反馈为 llm／deepseek-flash。发送开关为 false，自启标记缺失；没有连接 QQ、启动业务进程或发送消息。安装前快照与旧 wheel 可用于恢复。发行 SHA-256 为 `c6dad895539103bf4c4949442cb972317e8a2255726eaa7711b6221956a9d25f`。

发行、真实样本和安装证据分别为 Git 忽略的 `local/ops/pullshit/run/systemone-release.json`、`local/probes/decision-models/extended-real-results.json`、`local/ops/pullshit/run/systemone-stopped-installation.json`。质量校准及恢复运行任务统一见[开发 TODO](phase-1-todo.md)。

## 2026-10-18T07:00:24.572454Z SystemOne 生产启动验证

按用户明确部署要求恢复自动运行。启动前验证 StartLux 认证 health 为 HTTP 200、生产 ready 配置通过，读取投递状态为 22 条 sent、2 条 unknown，queued 与 sending 均为 0，持久暂停为空。保留私有配置备份后启用现有部署守护，待 QQ 账号与全部授权群检查通过，再通过原子配置替换开启 sending.enabled；runtime.mode=auto。

业务、QQ SSH 隧道与模拟接口三个托管进程均为 RUNNING；确认业务 PID 正在执行安装包的 `python -m pullshit ... run`。健康检查显示预期账号匹配、6 个来源全部可用、2 个目标全部可用、10 个来源排除后的广播组合可用；本机模拟接口 health=ready，既有两个 crontab 守护入口保持配置。候选使用 systemone／startlux-decision-9b-autothink-q4km，反馈使用 llm／deepseek-flash。发行包为此前通过 207 项测试的版本，SHA-256 `c6dad895539103bf4c4949442cb972317e8a2255726eaa7711b6221956a9d25f`。

启动后读取到 9 条自然消息，未向 QQ 额外发送测试消息；历史 unknown 投递保持原有核对入口。每目标群 60 秒间隔、每日 100 条消息／500 段及模型预算保持私有配置原值，选材门槛保持 0.75。守护标记启用；主机实际重启、自托管笔记本长期可用性及自然选材质量验收统一见[开发 TODO](phase-1-todo.md)。

启动前核对、配置备份和部署结果保存在 Git 忽略的 `local/ops/pullshit/run/systemone-startup-precheck.json`、`local/ops/pullshit/run/systemone-deployment.json`。本次操作仅修改私有运行配置和启用标记，验证采用配置、模型健康、进程、QQ 认证／群列表和已有自然消息记录。

## 2026-10-18T11:38:54.703524Z @机器人反馈触发限制验证与部署

反馈入口要求当前消息含顶层 OneBot at 片段，qq 精确等于接收消息的机器人账号；自然引用与 `/喜欢`／`/少来点` 共用条件。确认发送复查该条件。持久恢复保留经过验证的提及标记，历史 pending 根据缓存原消息确认；缺少有效提及证据时完成为 unknown。普通源群内容继续进入观察窗口，管理命令及搬运提名沿用其明确命令入口。

相关 24 项测试及回放 3 项测试通过。完整检查首次发现回放反馈样本缺少 @，更新样本后最终完整 214 项测试通过（108.396 秒），33 个模块与发行包一致。新增 7 项测试验证无提及、@他人／全体、正文伪提及、嵌入节点提及、整数 qq、正确 @ 的正常处理、持久恢复和发送确认；独立的实际用户消息隔离回放验证「@他人，试试这个」不调用模型且不写反馈，补上 @机器人后允许捕获。

部署短暂停止业务与模拟接口，QQ 隧道保持运行。安装已测 wheel 后校验生产 ready 配置、依赖及 33 个模块哈希，随后启动两个业务进程。业务、QQ 隧道、模拟接口均 RUNNING，预期 QQ 账号、全部 6 个来源／2 个目标及 10 个广播组合检查通过。私有配置逐字节保持一致，自动发送和部署守护保持开启；候选 StartLux SystemOne 与反馈 DeepSeek LLM 沿用既有配置。验证没有额外向 QQ 发送测试消息，群内自然触发验收统一见[开发 TODO](phase-1-todo.md)。

发行 SHA-256：`e51741dad5bb9b2fb0d1b4122c99d53e0766d4bdd58701c93944611bf95669ee`。测试、原消息隔离回放和部署证据分别为 Git 忽略的 `local/ops/pullshit/run/mention-feedback-release.json`、`local/ops/pullshit/run/mention-feedback-preflight.json`、`local/ops/pullshit/run/mention-feedback-deployment.json`；部署记录保留旧 wheel 与配置快照以供恢复。

## 2026-10-20 多条聊天合并转发验证

同一份选中内容内，连续多条普通源消息分别构造转发节点，每条保留源发送者、昵称、时间及其所选文字／图片顺序，使用 send_group_forward_msg 发布。单条普通源消息使用 send_group_msg；源合并转发卡片通过原消息转发保持完整。卡片分片优先保留消息边界，单条过长消息按片段顺序拆分，消息段额度按节点内内容累计；嵌套图片发送前继续核验有效期、大小与哈希。

发布、整卡、运行、回放与选材相关 55 项检查通过。最终独立安装包的完整 230 项测试通过（103.750 秒），34 个模块与源码及生产安装逐字节一致；新增用例核对不同发送者的图文节点、真实合并发送入口、每节点内部顺序、分片与媒体限制、发送者篡改拦截及缓存图片变动阻断。最近两条实际投递的只读重建分别得到 3 个和 7 个节点，文字与 2 张图片按原顺序匹配，图片载荷可转换；重建没有连接 QQ、调用模型或重新发送历史内容。

部署后生产 ready、依赖、QQ 账号、全部授权群、三个业务进程及模拟接口健康检查通过。私有模型连接、发送与预算配置逐字节保持一致。发行包 SHA-256 为 `2b9f61c6df94a4c997d83e940e258271556435746d91b33637df78487d6b551a`。具体部署时间、进程、配置快照与回滚 wheel 保存在 Git 忽略的 `local/ops/pullshit/run/merged-chat-deployment.json`；测试及实际投递重建证据分别为 `merged-chat-release.json`、`merged-chat-preflight.json`。群内自然投递回读验收统一见[开发 TODO](phase-1-todo.md)。

最后修改时间：2026-10-20T04:45:49Z
模型署名：GPT-6
