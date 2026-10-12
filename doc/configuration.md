# 配置、预算与部署

配置默认值以 [config/defaults.json](../config/defaults.json) 为准。加载顺序为默认配置 → `config/local/<profile>.json`；密钥从环境变量读取。操作命令见 [README](../README.md)，接口和字段行为见[接口文档](interfaces.md)，联调状态见[开发 TODO](phase-1-todo.md)。

## 环境与连接

`profile` 为 `development` 或 `production`。数据库、媒体、worker 和日志位于各自环境目录；路径解析检查目录与符号链接隔离。真实配置和环境变量文件保持 `600` 权限。

- `napcat.endpoint`：正向联合 WebSocket 根地址，协议为 ws/wss。凭据使用 `napcat.access_token_env` 指定的环境变量。
- `authorization`：源群、目标群、操作者 ID 及有向路由，ID 使用十进制字符串。
- `runtime.mode`：`auto` 自动发布，`shadow` 仅发现和分析。
- `sending.enabled`：发布开关。持久暂停独立生效。

配置示例见 [local.example.json](../config/local.example.json)。主程序从认证连接获取 QQ 账号，授权变化在处理和发送边界重新检查。

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

默认并发 2、时限 90 秒、模型与工具调用各最多 8 次。输出 token 和输入字符上限共同限制任务。错误修正属于同一任务额度。素材和指令超过输入上限时返回需复核。模型、内容窗口和保留期配置在服务启动时加载；群授权、路由、发送模式、开关与持久暂停在处理或发布边界复查。

## OpenCode Go 与 DeepSeek V4.1 Flash

核对日期：2026-10-12。[官方推理接口](https://opencode.ai/v2/docs/console/api/inference)使用 OpenAI Chat Completions；工作区 `GET /console/api/v2/config` 提供模型、入口和单价。[Go 文档](https://opencode.ai/v2/docs/console/go/)要求客户端标识和每次对话稳定的 `x-opencode-session`。

配置 `provider=custom`、`model=deepseek-v4.1-flash`、`base_url=https://opencode.ai/inference/go/openai/v1`。密钥使用 `PULLSHIT_AGENT_API_KEY`。视觉配置可使用同一模型、入口和环境变量；文本与视觉费用分别配置。

worker 标识为 `pullshit/0.2`；固定 Hermes 版本发送主模型会话头，图片请求使用同一任务 ID。项目工具插件把父进程返回的对象编码为 Hermes 接受的 JSON 字符串。

联调配置每日 1 USD、单任务 0.05 USD；每百万输入／输出 token 分别按 0.30 / 1.20 USD 峰值单价计量。缓存输入也按配置输入价计算，`usage.cost_usd` 为项目的保守费用估算；供应商实际账单与该估算分别核对。真实调用结果见[验证记录](p0-implementation.md#2026-10-12-opencode-go-真实模型验证)。

## 内容、流量与保留期

| 配置 | 默认值 |
| --- | --- |
| content.quiet_seconds / max_wait_seconds | 60 / 300 秒 |
| context.reply_depth / forward_depth | 3 / 3 |
| context.max_nodes / max_fetch_calls / timeout_seconds | 1000 / 5 / 20 秒 |
| limits.max_forward_nodes | 100，配置上限 100 |
| limits.min_interval_seconds | 每目标群 600 秒 |
| limits.daily_batches / daily_nodes | 每账号／目标群 20 批 / 500 节点 |
| limits.daily_download_bytes / daily_publish_bytes | 全局各 1 GiB |
| media.max_image_bytes / limits.max_batch_media_bytes | 10 / 20 MiB |
| media.cache_max_bytes | 2 GiB |
| retention.message_days / content_days / log_days | 3 / 7 / 7 天，内容上限 7 天 |
| retention.delivery_days / budget_days | 90 / 30 天 |
| preferences.min_publications / min_members / max_step | 3 / 3 / 0.05 |

日额度按 UTC 日期分桶。发送尝试占用批数、节点及发布额度；失败或回执不确定同样占用。每片合并转发都受间隔限制。

新库使用 `state.sqlite3`。程序只打开新 schema。具体设备的启动服务、路径、凭据和运维脚本存放在 Git 忽略的 `local/`。

最后修改时间：2026-10-12T18:39:19Z
模型署名：GPT-6
