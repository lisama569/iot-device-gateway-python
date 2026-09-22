# IoT Device Gateway (Python / FastAPI)

一套本地自包含的设备接入网关：设备注册与凭据生命周期、遥测写入与设备影子合并、
分批固件升级编排。所有验证均通过内置的模拟设备流量完成，**不依赖外部消息代理、
真实设备或真实凭据**。

## 运行

```bash
uv sync                 # 安装依赖
uv run uvicorn main:app # 启动服务（默认 8000 端口，/docs 查看 OpenAPI）
uv run pytest           # 运行全部测试（-s 已配置，日志打印判定依据）
```

## 设备生命周期与权限规则

设备状态机：`active → disabled ⇄ active`，任意状态可 `deregister`（终态）。

- **注册幂等**：`POST /devices/register` 对同一 `(device_id, secret)` 重复调用返回
  完全一致的结果；同一 `device_id` 携带不同 secret 注册返回 `409 device_id_conflict`。
- **认证错误可区分**：`authenticate` 对未知设备（`device_not_found`）、已停用
  （`device_disabled`）、秘密错误（`credential_invalid`）、凭据过期
  （`credential_expired`）、凭据已撤销（`credential_revoked`）分别返回不同的
  错误码与 HTTP 状态（404/403/401/401/401）。
- **立即生效**：`disable` / `enable` / `deregister` / `revoke` / `rotate` 在注册表
  锁内完成状态切换，下一次 `authenticate` 即可观察到。
- 凭据带 TTL（`GATEWAY_CREDENTIAL_TTL_SECONDS`，默认 3600s），`rotate` 会签发新
  凭据并复位过期时间。

## 遥测与影子状态的兼容与冲突处理约定

`POST /devices/{id}/telemetry`（需 `X-Device-Secret` 头）写入遥测并合并设备影子。

- **幂等去重**：`message_id` 已见 → 返回 `duplicate_message`，不改状态、不占配额。
- **确定性合并**：影子按字段做 last-writer-wins，版本序为 `(device_ts, seq)`。
  仅当新消息的字段版本严格更大时才应用，因此乱序、重复、时间偏移（设备时钟滞后）
  的消息不会造成状态回退或静默覆盖。每个字段的合并判定（`applied` /
  `stale_rejected`）连同双方版本号一并返回，作为可追溯的判定依据。
- **不静默丢弃**：配额超限（`quota_exceeded`，429）与校验失败
  （`validation_failed`，422，如空 payload、非法字段类型、时钟超前超过
  `GATEWAY_MAX_CLOCK_SKEW_SECONDS`）都会记录到设备的拒绝日志
  （`GET /devices/{id}/telemetry/rejected`）。
- 并发安全：所有合并与计数在单锁内完成；并发重复投递只计数一次。

## 固件升级编排与验证方法

`POST /campaigns` 创建任务（按 `campaign_id` 幂等，参数不同返回
`task_state_conflict`），`POST /campaigns/{id}/run|pause|resume|cancel` 控制，
`GET /campaigns/{id}` 查询。

- **分批推进**：目标设备按创建顺序、每批 `batch_size` 台依次下发。
- **确定性结论**：单台设备失败重试 `GATEWAY_FOTA_MAX_RETRIES` 次后标记 `failed`，
  当前批执行完毕后任务整体进入 `failed`，未下发的设备标记 `skipped`；
  全部成功为 `completed`；取消为 `cancelled`。终态唯一且不可再迁移，
  整体结论可由每台设备的状态与 `attempts` 完全解释。
- **互斥**：同一设备处于任何未终结任务中（pending/in_progress）时，创建包含它的
  新任务返回 `409 device_busy`。
- **重启恢复**：设置 `GATEWAY_FOTA_STATE_PATH` 后每次状态迁移都落盘（原子替换）。
  进程重启后 `recover()` 自动加载：处于 `running` 的任务降级为 `paused`（不会
  卡死），中断的 `in_progress` 设备回退为 `pending`；内置 `SimulatedFlasher`
  幂等（设备已是目标版本则空操作），恢复后续跑不会重复下发。
- **验证方法**：`tests/test_fota.py` 用 `SimulatedFlasher.fail_next()` 脚本化
  设备失败，覆盖分批成功、重试预算内成功、重试耗尽失败、暂停/恢复/取消、
  设备互斥与崩溃恢复；测试日志打印任务状态、尝试次数与下发记录。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_CREDENTIAL_TTL_SECONDS` | 3600 | 凭据有效期（秒） |
| `GATEWAY_TELEMETRY_QUOTA` | 1000 | 每设备可接受的去重后消息数上限 |
| `GATEWAY_MAX_CLOCK_SKEW_SECONDS` | 300 | 允许的设备时钟超前上限 |
| `GATEWAY_FOTA_MAX_RETRIES` | 2 | 单台设备失败后的重试次数 |
| `GATEWAY_FOTA_STATE_PATH` | 空（不持久化） | 升级任务状态 JSON 落盘路径 |

## 代码结构

- `app/models.py` — 枚举、错误码、`GatewayError`
- `app/registry.py` — 设备注册（幂等）与凭据生命周期
- `app/telemetry.py` — 遥测摄入、配额、按字段 LWW 影子合并与判定日志
- `app/fota.py` — 分批升级编排、暂停/恢复/取消、持久化与重启恢复
- `app/simulator.py` — 模拟设备固件刷写器（幂等、可脚本化失败）与遥测流发生器
- `main.py` — FastAPI 接线与错误码 → HTTP 状态映射
- `tests/` — 注册、遥测（含并发/乱序/重复/偏移/配额）、升级编排、端到端 API 测试
