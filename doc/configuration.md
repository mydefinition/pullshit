# 默认、开发与生产配置

本文定义配置文件、加载顺序、字段校验和环境隔离。开发任务与联调状态见[开发 TODO](phase-1-todo.md)。

## 文件与加载顺序

| 文件 | 用途 | Git 管理 |
| --- | --- | --- |
| [config/defaults.json](../config/defaults.json) | 完整默认参数 | 提交 |
| [config/development.json](../config/development.json) | 开发环境覆盖项 | 提交 |
| [config/production.json](../config/production.json) | 生产环境覆盖项 | 提交 |
| [config/local.example.json](../config/local.example.json) | 本地连接和群授权模板 | 提交；字段留空 |
| `config/local/development.json` | 本地开发连接和群授权配置 | Git 忽略 |
| `config/local/production.json` | 本地生产连接和群授权配置 | Git 忽略 |

加载器按 `defaults.json → <profile>.json → local/<profile>.json` 合并。profile 必须显式选为 `development` 或 `production`；每次读取所选环境的三个文件。本地文件缺失时启动失败。

对象递归合并；数组、字符串、数字和布尔值整体覆盖。空数组表示明确清空授权范围。`null`、未知字段、字段类型错误和未知 profile 校验失败。本地配置可覆盖业务参数，`schema_version` 与 `profile` 由默认和环境文件确定，本地文件禁止设置这两个字段。

文件路径以项目根目录解析。开发与生产的数据库、媒体和日志目录分别位于各自环境目录；禁止通过本地覆盖让两个环境共享这些目录。离线回放须使用独立目录，并强制禁用发送。

QQ 登录与账号配置由 NapCat 管理。pullshit 从已认证连接的 `get_login_info` 获取运行账号，应用配置仅保存群授权与连接信息；真实群号、操作者、连接地址及 token 环境文件统一维护在 `config/local/`。运行账号会出现在事件和账本中，数据库保存在 Git 忽略的 `data/`。该目录已加入 [.gitignore](../.gitignore)，文件应保持仅当前用户可读写。

## 字段约定与运行前校验

默认值以 `defaults.json` 为权威来源；环境差异以对应环境文件为准。

- 应用配置包含群号和操作者 ID，均使用十进制字符串；QQ 账号由平台登录信息绑定，事件账号与当前连接账号匹配后才接受。
- `napcat.transport` 使用 `websocket`；`endpoint` 使用同时提供事件与 API 的根地址，协议为 `ws` 或 `wss`，地址携带的凭据和查询参数会被拒绝。具体设备的连接和部署信息维护在本地配置与 `local/` 运维记录中。
- `napcat.access_token_env` 是读取 token 的环境变量名。运行时读取对应变量，值缺失时启动失败；日志过滤 token、访问密钥和完整敏感配置。
- `authorization.source_group_ids`、`target_group_ids` 和 `operator_qq_ids` 是各自独立的白名单，空列表表示零授权。
- `authorization.routes` 的每项格式为 `{"source_group_id": "…", "target_group_id": "…"}`；两端必须属于对应白名单，重复路由校验失败。
- 源群、目标群和路由均为数组，可配置多源与多目标。路由表示有方向的内容发布授权，每个允许方向单独列出；NapCat 联合连接同时提供事件与 API。`probe` 入口限定一个测试目标群。
- `runtime.mode` 为 `receive`、`replay` 或 `probe`。默认 `receive`。接收模式要求至少一个源群；发送探测模式要求 `runtime.mode=probe`、一个测试目标群、操作者及已授权路由。
- 发送探测要求 `sending.enabled=true` 且 `sending.paused=false`，执行前再次检查。发送数量受每日上限约束。生产配置同样从受控接收模式开始。
- 超时、重连间隔、保留天数、下载大小、转发限制和查询次数应为正值；重连最大间隔须大于或等于初始间隔。
- 原始事件进入受保留期限约束的事件库；日志默认只记录脱敏诊断字段。开发 debug 日志也受同一数据边界约束。
- 媒体网络限制由下载实现校验，并对每次重定向和实际目标地址重新检查。

Python 程序执行字段校验、发送控制、事件清理和媒体缓存清理。环境存储路径固定；原始载荷日志、私网媒体下载和表情轮询配置启用时会被拒绝。运行命令见 [README](../README.md)。

## 上下文查询参数

`context` 参数按三个配置层合并；开发与生产共享默认预算，各自使用独立存储。所有数值必须为正整数。

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `context.max_reply_depth` | 3 | 原消息之后的引用层数 |
| `context.max_fetch_calls` | 5 | 每次查询的内容回取次数，包含图片地址刷新；身份核对另行统计 |
| `context.total_timeout_seconds` | 20 | 连接、登录、回取、下载和组装总时限 |
| `context.nearby_messages_each_side` | 5 | 本地前后消息各自的数量上限 |
| `context.nearby_window_seconds` | 120 | 原消息前后各自的时间窗口 |
| `context.failure_cache_seconds` | 60 | 回取或图片下载失败的缓存时长 |
| `context.max_media_per_query` | 5 | 下载尝试总数，包含地址刷新后的重试 |
| `context.max_text_characters` | 20000 | 整次查询的文本消息段字符预算 |

`messages.forward_max_depth=3` 控制转发嵌套；`messages.forward_max_nodes=100` 同时作为上下文组装的总节点预算，计入消息段、引用、转发结构节点与附近消息。`--online` 允许只读回取，`--online --cache-media` 允许图片下载。查询阶段持续重读群授权和限额，发送开关独立控制发送入口。成功回取缓存使用 `storage.raw_event_retention_days`，图片有效性使用 `storage.media_retention_days`；处理方式见[QQ 接入层设计](qq-adapter.md)。

## 本地配置准备

真实 ID 写入 Git 忽略的本地文件。准备对应环境的连接信息、群白名单、操作者和路由，通过对应环境变量注入 token。开发 token 环境文件使用 `config/local/development.env`，权限保持 `600`。完成基础配置后执行 `check-config`；在线接收前执行 `check-config --ready receive`。

新部署可以用下列命令创建尚不存在的本地文件；已有文件保持原内容：

```bash
mkdir -p config/local
chmod 700 config/local
for profile in development production; do
  if [ ! -e "config/local/$profile.json" ]; then
    (umask 077; cp config/local.example.json "config/local/$profile.json")
  fi
done
chmod 600 config/local/development.json config/local/production.json
git check-ignore config/local/development.json config/local/production.json
```

基础配置检查允许待填连接字段，`check-config --ready receive` 或 `--ready probe` 检查在线运行前置条件。人工发送请求从本地文件读取，模板见 [probe.example.json](../config/probe.example.json)。

最后修改时间：2026-10-10T07:50:43Z
模型署名：GPT-6
