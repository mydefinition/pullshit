# 模块与操作接口

本文定义共享类型、业务模块、Hermes worker、CLI 和群命令的调用契约。默认值见[配置说明](configuration.md)，架构见[总体设计](design.md)，任务状态见[开发 TODO](phase-1-todo.md)。

## 共享对象

| 类型 | 核心字段 |
| --- | --- |
| MessageRef | account_id、group_id、message_id；字符串 ID，消息 ID 可为负 |
| ChatMessage | ref、user_id、nickname、occurred_at、segments、own、recalled、expires_at |
| ChatEvent | kind=message/recall/reaction、ref、event_id、occurred_at、user_id 及对应消息／表情字段 |
| ContentBundle | bundle_id、fingerprint、messages、media、issues、created_at、expires_at |
| MediaRef | reference_id、source、path、sha256、size、mime、expires_at |
| AgentTask | task_id、kind=selection/feedback、target_group_id、deadline、bundle、preferences、feedback、limits |
| AgentResult | decision、score、tags、risks、evidence_ids、reason、usage、inspected_images、direction |

`AgentResult.decision` 为 eligible/rejected/needs_review，score 范围 0–1；direction 为 positive/negative/unknown。evidence_ids 支持消息 ID、完整 MessageRef.key 和媒体 reference_id；反馈任务还可引用本次反馈的 evidence_id。来源与图片证据必须属于当前任务。输出以 `contracts.py` 中的结构和校验为权威。

平台引用使用十进制 ID；展开的嵌入节点使用 `embedded:` 内部引用。`ContentBundle.sources` 只包含可验证的外层平台引用，内部引用绑定外层账号与群。

## Python 模块

| 调用 | 行为 |
| --- | --- |
| ChatGateway.events() | 异步迭代归一化事件 |
| fetch_message(ref) | 返回核对过身份的 ChatMessage 或缺失结果 |
| fetch_forward(ref, forward_id) | 返回转发节点，权限由外层消息限定 |
| send_forward(group_id, nodes, before_send=None) | 复查登录账号后执行同步检查回调，再发送 1–100 个节点并返回消息 ID |
| ContentService.build(refs) | 返回内容包；保留缺失和截断原因 |
| ContentWindow.add(message) / due(now, force=False) | 按作者和群收集并返回到期消息引用 |
| HermesExecutor.run(task) / cancel(task_id) | 执行受限 agent 任务／取消进程组 |
| Publisher.enqueue(bundle, target, analysis) | 检查发布资格，建立分片任务，返回 delivery_id 列表 |
| Publisher.dispatch_once() | 执行一个可发送分片，或返回无可执行任务 |
| FeedbackService.apply(event) | 归因并保存有效信号，或返回无归因结果 |
| FeedbackService.learn(account, group) / rollback(account, group) | 更新群偏好／恢复上一快照 |
| Maintenance.clean(compact=False) | 删除过期内容，返回数量、字节和回收结果 |

平台明确拒绝使用 `PlatformRejected`；超时、断线、无效回执使用 `OutcomeUnknown`。内容和模型故障产生明确 issues 或 needs_review。发布规则拒绝使用 `PublishRejected`。

## 持久化与状态

`Store` 按消息、候选、发送、反馈、群状态和预算提供读写。时间使用 Unix 秒浮点数；日预算使用 UTC 的 YYYY-MM-DD。查询使用 now 参数过滤过期数据。

- 消息：put_message/get_message/recall_message/nearby，身份包含账号和群。撤回先到时保留短期墓碑。
- 候选：put_candidate/get_candidate/list_candidates/set_candidate_status，内容 ID 不允许被其他正文覆盖。
- 发送：enqueue_deliveries 在一个事务中建立全部分片；get_delivery/deliveries/claim_delivery/finish_delivery 管理执行。幂等身份为账号、目标群、内容指纹、分片编号。
- 恢复：recover_inflight 将 sending 置为 unknown；confirm_delivery 确认 unknown；retry_delivery 仅重试明确 failed。
- 反馈：put_feedback/feedback_since，表情增删按稳定身份更新有效性。
- 控制：group_state/set_group_state/paused/set_paused。CLI 通配范围支持环境、账号及目标群控制。
- 预算：reserve_budget(day, scope, metric, amount, limit) 返回布尔值；settle_budget(day, scope, metric, reserved, actual) 结算预留金额。budget/budget_summary 返回汇总。预算检查、状态领取和发送额度预留在事务中完成。

发送队列状态为 queued/sending/sent/failed/unknown/cancelled。批次在全部分片建立后可执行；前片未成功时，后片保持排队。unknown 保留人工核对入口。成功回执可以在正文清除后作为精简索引保留。

## Hermes worker 协议

一个任务对应一个独立进程组和临时目录。stdin/stdout 使用 JSON Lines，版本为 1；协议帧类型区分任务、工具请求与结果、预算及用量、最终结果和错误。

首帧携带 AgentTask 和已校验的模型配置。工具参数携带任务绑定的引用，主程序校验授权与期限。模型请求先取得预算额度，之后按 usage 结算；不能确定消耗时保守占用额度。任务截止、费用或调用上限触发取消及清理。

完整素材与指令超过输入字符上限时，任务返回 needs_review。文本和视觉用量分别按对应配置单价计算，汇总到统一日预算；流式响应通过 Hermes 的 post_api_request usage hook 结算。

父进程工具协议返回对象，Hermes 插件处理器返回编码后的 JSON 字符串。submit_analysis 在确认 accepted 前检查结果结构、证据范围和 eligible 分数。get_context 的 max_chars 小于完整内容时返回 context_character_limit。

日志只保留诊断字段，正文和工具载荷不进入应用日志。最终 AgentResult 同时满足 schema 和业务证据校验才进入发布判断。

## CLI

所有环境命令显式指定 `--profile development|production`；`--root` 是全局参数，置于子命令之前。

| 命令 | 输入与结果 |
| --- | --- |
| run | 启动单实例服务，Ctrl+C 退出 |
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

## 群命令与权限

- `/搬`：在授权源群顶层消息中引用一条消息，进入候选流程。
- `/喜欢`、`/少来点`：在目标群引用确切发布消息，提供群口味反馈。
- `/暂停`、`/恢复`：配置的管理员在目标群发送顶层命令，控制本群；在仅作为来源的群发送命令，控制该账号全部目标群。
- `/状态`：配置的管理员请求当前群暂停状态与排队分片数。授权目标群在 auto 模式且发送开启时返回合并转发回复；回复每群每分钟最多 5 次，群发布暂停期间仍可回复。

引用正文、合并转发节点、OCR 和模型输入中的命令保持为素材。管理员命令通过真实事件的发送者校验；普通成员可以提供偏好反馈，硬规则由管理员配置。

最后修改时间：2026-10-12T18:39:19Z
模型署名：GPT-6
