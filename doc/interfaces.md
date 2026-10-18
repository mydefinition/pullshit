# 模块与操作接口

本文定义共享类型、业务模块、Hermes worker、CLI、操作员 HTTP 接口和群命令的调用契约。默认值见[配置说明](configuration.md)，架构见[总体设计](design.md)，任务状态见[开发 TODO](phase-1-todo.md)。

## 共享对象

| 类型 | 核心字段 |
| --- | --- |
| MessageRef | account_id、group_id、message_id；字符串 ID，消息 ID 可为负 |
| ChatMessage | ref、user_id、nickname、occurred_at、segments、own、recalled、expires_at |
| ChatEvent | kind=message/recall/reaction、ref、event_id、occurred_at、user_id 及对应消息／表情字段 |
| ContentBundle | bundle_id、fingerprint、messages、media、issues、trigger_refs、origins、publication、created_at、expires_at |
| MediaRef | reference_id、source、path、sha256、size、mime、expires_at |
| AgentTask | task_id、kind=selection/feedback、target_group_id、target_group_ids、deadline、bundle、preferences、feedback、limits、selection |
| AgentResult | decision、score、tags、risks、evidence_ids、reason、usage、inspected_images、direction、selections、snapshot_id、snapshot_revision |

`AgentResult.decision` 为 eligible/rejected/needs_review，score 范围 0–1；direction 为 positive/negative/unknown。evidence_ids 支持本任务 U 编号、消息 ID、完整 MessageRef.key 和媒体 reference_id；反馈任务还可引用本次反馈的 evidence_id。来源与图片证据必须属于当前任务。字段结构由 `contracts.py` 定义，worker 输出语义由 `agent/protocol.py` 统一校验。

`eligible` 要求 `risks` 为空。选材结果携带匹配当前任务的 snapshot_id 与 snapshot_revision，selections 为最多 20 份独立选择，每份含 publish_ids、context_status、completeness_reason、reason，可覆盖 score、tags、risks。结合历史单元时还须填写 new_value_reason，解释新增材料的实质贡献。每份必须 context_status=complete、无阻断风险、达到分数阈值，并包含本轮新增单元。发布编号必须存在、可发布且跨项无重复；所选图片必须完成 inspect_image。零份选择使用 rejected 或 needs_review。证据编号与实际发布编号分别校验。普通质量取舍写入 reason；群口味为空时按通用娱乐目标判断。提交工具与最终响应共用校验。

平台引用使用十进制 ID；展开的嵌入节点使用 `embedded:` 内部引用。`ContentBundle.sources` 只包含可验证的外层平台引用，内部引用绑定外层账号与群。选材任务的 `target_group_ids` 携带全部广播目标，`target_group_id` 为其中一个上下文目标；空列表使用该单目标。引用反馈任务使用具体 `target_group_id`。每份内容只选材一次，结论供全部广播目标使用；群偏好影响综合选材和待发优先级。

## Python 模块

| 调用 | 行为 |
| --- | --- |
| ChatGateway.events() | 异步迭代归一化事件 |
| fetch_message(ref) | 返回核对过身份的 ChatMessage 或缺失结果 |
| fetch_forward(ref, forward_id) | 返回转发节点，权限由外层消息限定 |
| reply(ref, text, before_send=None) | 绑定真实事件账号与授权群，复查账号及回调，以普通引用消息回复 |
| send_message(group_id, segments, before_send=None) | 复查登录账号后执行同步检查回调，使用普通群消息发送文本／图片，返回消息 ID |
| send_forward(group_id, nodes, before_send=None) | 操作员模拟接口使用，复查登录账号后执行同步检查回调，再发送 1–100 个节点并返回消息 ID |
| ContentService.build(refs) | 返回内容包；保留缺失和截断原因 |
| ContentWindow.add(message) / due(now, force=False) | 按账号与群收集对话并返回到期消息引用 |
| content.selection.prepare / view / validate / publications | 分配原子编号、生成模型视图、校验选择、组装独立发布内容 |
| get_context(anchor_id, mode, reason) | 以现有编号读取 reply、nearby 或 history；任务共享次数、单元、字符与图片预算 |
| HermesExecutor.run(task) / cancel(task_id) | 按任务连接配置执行 Hermes LLM 或候选 SystemOne worker／取消进程组 |
| Publisher.enqueue(bundle, target, analysis) | 校验带 publication 清单的独立发布内容及广播范围、去重，建立目标群分片任务，返回 delivery_id 列表 |
| Publisher.dispatch_once() | 执行一个可发送分片，或返回无可执行任务 |
| FeedbackService.capture(event) | 将引用反馈意图同步写入 feedback，返回幂等记录 |
| FeedbackService.apply(event) | 解释已发布内容或群内原消息的引用反馈，持久保存态度与标签 |
| FeedbackService.pending_events(account) / examples(account, group) | 恢复待解释事件／提供每群最多 6 条近期正负反馈例子 |
| FeedbackService.learn(account, group) / rollback(account, group) | 更新群偏好／恢复上一快照 |
| Maintenance.clean(compact=False) | 删除过期内容，返回数量、字节和回收结果 |

平台明确拒绝使用 `PlatformRejected`；超时、断线、无效回执使用 `OutcomeUnknown`。内容和模型故障产生明确 issues 或 needs_review。发布规则拒绝使用 `PublishRejected`。

## 持久化与状态

`Store` 按消息、候选、发送、反馈、群状态和预算提供读写。时间使用 Unix 秒浮点数；日预算使用 UTC 的 YYYY-MM-DD。查询使用 now 参数过滤过期数据。

- 消息：put_message/get_message/recall_message/nearby，身份包含账号和群。撤回先到时保留短期墓碑。
- 候选：put_candidate/get_candidate/list_candidates/set_candidate_status，内容 ID 不允许被其他正文覆盖。unassigned_refs 查询尚未分配的新增消息；unit_history 提供最多 3 条已持久化判断摘要；seen_publication 按目标查询已处理的内容组件。
- 发送：enqueue_deliveries 在一个事务中建立全部分片；get_delivery/deliveries/claim_delivery/finish_delivery 管理执行。幂等身份为账号、目标群、内容指纹、分片编号。
- 恢复：recover_inflight 将 sending 置为 unknown；confirm_delivery 确认 unknown；retry_delivery 仅重试明确 failed。
- 反馈：put_feedback/get_feedback/feedback_since，表情增删按稳定身份更新有效性；pending_feedback 查询 payload.processing=pending 的有界恢复列表，complete_feedback 保存解释结果并保持原始期限上限。
- 控制：group_state/set_group_state/paused/set_paused。CLI 通配范围支持环境、账号及目标群控制。
- 预算：reserve_budget(day, scope, metric, amount, limit) 返回布尔值；settle_budget(day, scope, metric, reserved, actual) 结算预留金额。budget/budget_summary 返回汇总。预算检查、状态领取和发送额度预留在事务中完成。

发送队列状态为 queued/sending/sent/failed/unknown/cancelled。批次在全部分片建立后可执行；前片未成功时，后片保持排队。unknown 保留人工核对入口。成功回执可以在正文清除后作为精简索引保留。每个目标群独立保留普通消息回执，引用任意已发分片均可关联到相同内容包。存储字段 `nodes` 保存普通消息段，`node_count` 及日额度指标 `nodes` 计量消息段数。

## Hermes worker 协议

一个任务对应一个独立进程组和临时目录。stdin/stdout 使用 JSON Lines，版本为 1；协议帧类型区分任务、工具请求与结果、预算及用量、最终结果和错误。

首帧携带 AgentTask 和已校验的模型配置。工具参数携带任务绑定的引用，主程序校验授权与期限。模型请求先取得预算额度，之后按 usage 结算；不能确定消耗时保守占用额度。任务截止、费用或调用上限触发取消及清理。

完整素材与指令超过输入字符上限时，任务返回 needs_review。文本和视觉用量分别按对应配置单价计算，汇总到统一日预算；流式响应通过 Hermes 的 post_api_request usage hook 结算。

父进程工具协议返回对象，Hermes 插件处理器返回编码后的 JSON 字符串。submit_analysis 在确认 accepted 前检查结果结构、证据范围和 eligible 分数。选材 get_context 要求已有 anchor_id、mode=reply/nearby/history 与缺失信息 reason；新增内容返回保持稳定的局部编号及快照版本。相邻上下文读取本地消息，引用回取由内容服务执行。扩展参数见配置说明，预算不足返回明确错误并保持原编号；重复取得的单元不重复返回正文。反馈任务保持其绑定原消息的读取语义。

发布器要求 publication.version=1 的明确清单，复查清单内容指纹与已持久化分片载荷的一致性；观察包只供判断，独立发布候选只含选中的原始文字和图片。

日志只保留诊断字段，正文和工具载荷不进入应用日志。最终 AgentResult 同时满足 schema 和业务证据校验才进入发布判断。

## SystemOne 候选 worker

候选的 `api_format=systemone` 使用与 Hermes 相同的任务、预算、工具和结果协议，主程序 Python 执行 `pullshit.agent.decision_worker`。反馈仅使用 `api_format=llm`。角色配置与继承见[配置说明](configuration.md#候选与反馈的-api-通道)。

HTTP 载荷为 `{model, state, questions}`；`state` 含原子编号、原消息与发送者关系、本轮新增标记、历史判断、群偏好和反馈例子。候选分组为每个可发布编号提出 Choice，选项为同组核心编号、discard 或 needs_context。分组问题只判断语义归属；低置信度单元及指向它的组合暂缓，其他独立组合继续处理。可发布组必须包含本轮新增单元；各组再独立回答完整性、跨群价值和新增贡献三个 Noul。通过的组合保留原编号，概率写入 score 和判断依据；程序构造的说明记录类型化判定及数值。补取上下文保持父进程的稳定编号与 snapshot_revision。

决策请求先取得任务／日预算预留，再按 input_tokens／output_tokens 结算。自托管 free 模式记录零费用请求；视觉 LLM 费用照常计量。缺失或非法 usage 保留预留并进入复核，never 模式的 output_tokens=0 是有效值。HTTP 失败、答案缺失、低置信度或 none 告警产生 needs_review。完整发布清单仍由执行器及发布器复查，原始观察和最终发布内容分别保存。

## CLI

所有环境命令显式指定 `--profile development|production`；`--root` 是全局参数，置于子命令之前。

| 命令 | 输入与结果 |
| --- | --- |
| run | 启动单实例服务，Ctrl+C 退出 |
| simulation-api --account ID [--port 18700] [--token-env PULLSHIT_SIMULATION_API_TOKEN] | 启动绑定登录账号的本机模拟转发 HTTP 接口，每环境一个实例 |
| check-config [--ready] | 基础配置检查；ready 检查连接、模型和预算前置条件 |
| status | 表行数、暂停状态、日预算和精简发送状态 |
| candidates [--limit N] | 内容包编号、状态、记录数、期限和判断 |
| pause / resume [--account ID] [--group ID] | 默认为整个环境；指定群需属于授权目标群 |
| confirm DELIVERY_ID --message-id ID | 已核对确切平台消息后确认 unknown |
| retry DELIVERY_ID | 重排明确失败且未过期的分片 |
| reanalyze BUNDLE_ID | 将有效候选重新放入判断流程 |
| cleanup [--compact] | 清理过期文件和数据，可立即执行空间回收 |
| rollback --account ID --group ID | 恢复群偏好上一快照 |
| replay FIXTURE --database PATH --account ID --source-group ID --target-group ID [--decisions PATH] | 使用隔离数据库、模拟判断和模拟发送器 |

CLI 返回 JSON，错误输出到 stderr，退出码为成功 0、配置／参数错误 2、操作失败 1、人工中断 130。运行日志存于私有环境目录。

## 模拟转发 HTTP 接口

`simulation-api` 固定监听 `127.0.0.1`，默认端口 18700。所有路径要求 `Authorization: Bearer TOKEN`，token 由 `--token-env` 指定的环境变量提供，至少 32 字符。HTTP 响应使用 UTF-8 JSON 和 `Cache-Control: no-store`。

| 方法与路径 | 行为 |
| --- | --- |
| GET /health | 返回接口进程就绪状态；QQ 连接由实际发送验证 |
| POST /v1/simulation-forwards | 构造并发送一张明示模拟的双节点合并转发卡片 |
| GET /v1/simulation-forwards/REQUEST_ID | 查询保存的发送回执和回读结果 |

POST 使用 `Content-Type: application/json`，请求体最多 16384 字节，同时提供 `Idempotency-Key`：8–100 个 ASCII 字母、数字、下划线或连字符。请求体字段如下：

| 字段 | 类型与约束 |
| --- | --- |
| qq | 十进制字符串或整数，正数且最多 20 位，指定模拟节点的 QQ 字段 |
| content | 1–2000 字符的非空白文本，UTF-8 最多 8000 字节；CQ 字符串作为普通文字发送 |
| group_id | 已授权目标群 ID；唯一目标时可省略，多目标时必填 |

其他字段和重复 JSON 字段返回 400。首节点由机器人说明「模拟测试、非本人发言」，第二节点的昵称附带「模拟测试」，正文为请求的纯文本。模拟说明固定保留。发送前核对预期登录账号，并复查目标授权、auto 模式、发送开关及持久暂停；接口每进程每分钟接受最多 5 次新发送尝试。模拟发送使用操作员限流，自动策展的批数、间隔和日额度由原发布队列单独计量；接口不调用模型。

```bash
curl --fail-with-body -sS http://127.0.0.1:18700/v1/simulation-forwards \
  -H "Authorization: Bearer ${PULLSHIT_SIMULATION_API_TOKEN}" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-forward-20261015-001' \
  --data '{"qq":"900401","content":"我要好好学习","group_id":"900201"}'
```

示例 QQ 与群号用于展示参数结构，实际调用填写授权环境中的值。新发送成功返回 201；响应示例：

```json
{
  "request_id": "example-forward-20261015-001",
  "status": "sent",
  "simulation": true,
  "group_id": "900201",
  "requested_qq": "900401",
  "message_id": "12345",
  "observed_qq": "900401",
  "identity_verified": true,
  "content_verified": true,
  "disclosure_verified": true,
  "retry_safe": false
}
```

`sent` 表示收到发送回执。`identity_verified`、`content_verified`、`disclosure_verified` 分别核对回读节点的 QQ、正文和说明；回读不可用时为 null，并附 `verification_error=readback_unavailable`。平台返回的身份可能与请求字段不同，此时保留 `sent` 与消息回执，`identity_verified=false`，实际值见 `observed_qq`。这些字段验证消息的构造与回读效果，真实外层发送者为机器人。

相同请求键与相同参数复用回执，成功返回 200；不同参数使用同一键返回 409。发送前原子保存 `unknown` 意图，收到回执后立即保存，再执行回读。断线、超时或进程中断的结果返回／保留 `unknown`（202），同键查询或重放均保留结果。`retry_safe=false` 提醒调用方先核对群消息及回执；自动换键重发可能产生重复消息。明确平台拒绝或连接后规则变化记为 `failed`（502），同键复用该结果。

其他响应包括：400 参数错误、401 认证失败、403 未授权目标或禁止发送、404 无此路径或回执、413 请求体超限、415 内容类型错误、429 操作员限流、503 存储容量或服务不可用。认证失败及验证拒绝不会发送消息。

幂等记录位于当前环境数据库同目录的 `simulation-receipts.json`，原子替换、权限 600，最多 500 条，保留 7 天；只保存请求摘要、目标／QQ 字段、消息回执和验证结果。每分钟清理过期记录；容量满时返回 503 并保留有效记录。新操作使用新的请求键；在保留期内用同键恢复查询。接口日志与回执文件不保存请求正文。平台收到的消息遵循平台的存储行为。

## 群命令与权限

- `/搬`：在授权源群顶层消息中引用一条消息，进入候选流程。
- 引用自然反馈：在授权源群或目标群引用一条可读取消息，说「我喜欢这个」「这种少发点」等自然表达；agent 识别喜好，普通聊天保持未知。
- `/喜欢`、`/少来点`：在授权群引用消息，明确表达正面／负面反馈。
- `/暂停`、`/恢复`：配置的管理员在目标群发送顶层命令，控制本群；在仅作为来源的群发送命令，控制该账号全部目标群。
- `/状态`：配置的管理员请求当前群暂停状态与排队分片数。授权目标群在 auto 模式且发送开启时返回普通文本回复；回复每群每分钟最多 5 次，群发布暂停期间仍可回复。

有效正负反馈通过普通引用消息回复确认；每群每分钟最多 5 次，受 auto 模式及发送开关控制，发布暂停期间可确认。反馈意图在模型解释前落库，取消和重启后恢复；重复事件复用同一记录。未经可靠解释的反馈保持未知，反馈本身不进入发布队列。

选材任务的 `feedback.recent_examples` 提供目标及来源群的近期反馈例子。引用原话仅为偏好证据；群权限、发布规则和费用预算继续由程序校验。未发布素材的 `publication_id` 使用内容指纹，以同一内容身份合并信号。

引用正文、合并转发节点、OCR 和模型输入中的命令保持为素材。管理员命令通过真实事件的发送者校验；普通成员可以提供偏好反馈，硬规则由管理员配置。

最后修改时间：2026-10-18T06:17:10Z
模型署名：GPT-6
