# QQ 接入契约

QQ 接入实现位于 `ingestion`，业务调用与错误语义见[接口文档](interfaces.md)，整体依赖见[总体设计](design.md)。开发与联调状态见[开发 TODO](phase-1-todo.md)。

## 连接与事件

使用 NapCat OneBot 11 正向联合 WebSocket。连接认证后通过 `get_login_info` 绑定账号，单个 reader 分流平台事件和带 echo 的 API 回执。事件缓冲最多 1000 条，帧最多 2 MiB；缓冲溢出丢弃最早事件并记录计数与诊断。连接中断由运行层退避重连。

消息按账号、群和消息 ID 定位，消息 ID 支持负数。事件仅覆盖授权群消息、自身消息、撤回和表情增删；心跳及其他载荷由接入流程过滤。数据库保存归一化业务消息。

## 平台读取与发送

- `fetch_message(ref)` 使用 `get_msg`，核对可验证的账号、群、消息 ID 和消息类型。
- `fetch_forward(ref, forward_id)` 使用 `get_forward_msg`，节点展开、循环检测与预算由内容模块控制。嵌入节点的群声明不授予回取权限。
- `reply(ref, text)` 对授权源群或目标群的真实事件使用 `send_group_msg`，包含精确 reply 消息段和确认文字；发送前核对账号并执行规则检查回调。
- `send_message(group_id, segments)` 使用 OneBot 11 `send_group_msg`，以 `message` 数组发送普通文本与图片并返回消息 ID。应用限制单条 1–100 个消息段，发送前验证登录账号并执行规则检查回调。
- `forward_message(ref, group_id)` 使用 `forward_group_single_msg`，参数为目标 group_id 和原外层 message_id。发送前使用 get_msg 核验来源身份及完整卡片类型，验证登录账号并执行发布规则回调；使用平台已有原消息保留合并转发卡片，缺失新消息 ID 视为 OutcomeUnknown。
- `send_forward(group_id, nodes)` 使用 `send_group_forward_msg`，用于多条源消息的策展合并转发及独立的操作员模拟转发，要求 1–100 个实际节点，返回消息 ID。图片在发送前从校验过的缓存转换成平台可访问的内容。

平台明确拒绝返回 `PlatformRejected`；超时、断线或无效回执返回 `OutcomeUnknown`。发布模块保留相应状态，发送前复查广播范围、暂停与预算。

## 来源

核对日期：2026-10-20。协议文档和源码能力与真实部署测量分别记录，实际结果见[验证记录](p0-implementation.md)。

- [OneBot 11 正向 WebSocket](https://github.com/botuniverse/onebot-11/blob/master/communication/ws.md)。
- [OneBot 11 普通群消息发送](https://github.com/botuniverse/onebot-11/blob/master/api/public.md#send_group_msg-发送群消息)。
- [OneBot 11 消息段](https://github.com/botuniverse/onebot-11/blob/master/message/segment.md)。
- [NapCat API 文档](https://napneko.github.io/onebot/api)：登录、消息查询、转发查询、合并转发发送和 `forward_group_single_msg` 原消息转发。

最后修改时间：2026-10-20T04:42:48Z
模型署名：GPT-6
