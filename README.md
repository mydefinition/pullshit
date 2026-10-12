# pullshit

> *聚是一坨 **shit**，散是漫天稀。*

面向 QQ 群的内容策展机器人：发现值得搬的内容，保留必要上下文，按目标群口味选择发布，并从可追溯的反馈中改进。

使用 Python，提供 QQ 事件接收、上下文查询、人工发送探测、媒体缓存和离线回放的命令行入口。开发任务与阶段进度统一见[开发 TODO](doc/phase-1-todo.md)。

源码目录与模块职责见[分层与模块](doc/design.md#32-分层与模块)；命令行可通过 `python -m pullshit` 或安装后的 `pullshit` 命令运行。

- [架构与调研](doc/design.md)：产品边界、框架选择、上下文组装、偏好学习、成本模型、发布闭环和评估原则。
- [开发 TODO](doc/phase-1-todo.md)：阶段排期、开发进度、交付物与验收条件。
- [P0 契约与验证记录](doc/p0-implementation.md)：事件与发送契约、协议来源，以及有日期的模拟和真实测试证据。
- [QQ 接入层设计](doc/qq-adapter.md)：平台连接、事件契约，以及接收、查询、发送与存储模块的配合。
- [配置说明](doc/configuration.md)：默认、开发、生产配置及本地账号隔离约定。
- [Agent 接入调研](doc/reserach/agent-integration.md)：插件、外围调用、SDK 与框架参考资料。
- [反馈学习研究](doc/reserach/README.md)：相关论文、适用边界与首个对照实验。

首版目标：在少量授权源群与一个测试目标群中，验证选材质量、发布可靠性与每日成本。可以实验性地主动搜罗 shit。

## 安装与检查

使用 Linux、Python 3.12+，Python 安装需提供 `venv` 与 `ensurepip`。在项目根目录执行：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m pip check
```

依赖版本与分发包校验值固定在 `requirements.lock`。缺少 `ensurepip` 的系统先安装对应 Python 版本的 venv 支持，或通过 [pip 官方引导方式](https://pip.pypa.io/en/stable/installation/)准备虚拟环境。

## 配置与接收

按[配置说明](doc/configuration.md)准备 `config/local/development.json` 和 `config/local/production.json`。真实账号、群号、连接信息与 token 文件保存在 Git 忽略的本地目录。

```bash
.venv/bin/python -m pullshit check-config --profile development
.venv/bin/python -m pullshit check-config --profile production
```

基础检查允许待填的连接字段。QQ 登录与账号由 NapCat 管理，pullshit 从认证连接获取运行账号。在线接收需要授权源群、正确的联合 WebSocket 根地址及 token 环境变量。本地 `config/local/development.env` 可用于保存环境变量，文件权限保持 `600`：

```bash
set -a
. config/local/development.env
set +a
.venv/bin/python -m pullshit check-config --profile development --ready receive
.venv/bin/python -m pullshit receive --profile development
```

接收程序仅保存授权范围内的事件，核对登录账号，并记录重连覆盖缺口。按 `Ctrl+C` 退出。

## 查询上下文

以下群号和消息 ID 使用模拟值，运行时填写本地授权群及已有消息 ID。默认只读本地记录，返回原文、引用链、转发树、附近消息和缺失原因：

```bash
.venv/bin/python -m pullshit context --profile development \
  --group-id 900101 --message-id=12345
.venv/bin/python -m pullshit context --profile development \
  --group-id 900101 --message-id=12345 --online --cache-media
```

`--online` 使用已配置的连接和 token，允许只读回取；`--cache-media` 允许有界下载图片，已有缓存可离线复用。查询输出的图片提供哈希和本地路径，访问 URL 留在私有证据中。附近消息标为时间线索，引用关系与可确认的来源单独保留。预算、缓存和失败状态见[QQ 接入层设计](doc/qq-adapter.md)。

## 离线回放

以下 ID 和事件均为模拟数据。回放读取独立输入，数据库限定在 `data/replay/`，重复执行保持幂等：

```bash
.venv/bin/python -m pullshit replay tests/fixtures/synthetic-events.jsonl \
  --database data/replay/example.sqlite3 \
  --account 900001 --source-group 900101 --target-group 900201
```

## 人工发送、暂停与清理

复制 [config/probe.example.json](config/probe.example.json) 到 `config/local/probe.json`，填写授权路由、操作者及待发送文本；一次探测可包含多条消息。图片使用人工确认的 `base64://` 内容。每次探测使用唯一 `probe_id`。

发送需要本地配置中的 `runtime.mode="probe"`、`sending.enabled=true`、`sending.paused=false`，以及一个目标群和有效路由。每日上限按消息条数计算；同一探测 ID 重复执行只返回账本。

```bash
.venv/bin/python -m pullshit probe --profile development config/local/probe.json
.venv/bin/python -m pullshit pause --profile development
.venv/bin/python -m pullshit status --profile development --probe-id your-probe-id
.venv/bin/python -m pullshit resume --profile development
.venv/bin/python -m pullshit cleanup --profile development
```

`pause` 与 `resume` 操作持久暂停状态；启用发送仍须通过配置开关。发送超时、断线或进程中断保留 `unknown`，核对目标群确切消息后可执行：

```bash
.venv/bin/python -m pullshit confirm --profile development \
  --probe-id your-probe-id --part 0 --message-id=12345
```

媒体下载入口接受存有 URL 的本地文件，校验公共地址、重定向、下载大小和时限，缓存按 SHA-256 索引：

```bash
.venv/bin/python -m pullshit cache-image --profile development \
  --url-file config/local/image-url.txt
```

`cleanup` 清理超过保留期限的原始事件、关联消息/反馈、上下文回取证据和图片缓存。测试环境、方法与实测结果见[验证记录](doc/p0-implementation.md)。

最后修改时间：2026-10-10T11:55:51Z
模型署名：GPT-6
