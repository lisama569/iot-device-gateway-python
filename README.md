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

## 在线备份、迁移与恢复

支持**不停服务导出完整备份包**，在另一台干净环境恢复后继续运行，用于整机搬迁与
故障演练。

### 备份覆盖范围

备份包是单个自描述 JSON，顶层带 `format` / `format_version` / `created_at` /
`checksum`（payload 规范 JSON 的 SHA-256），`payload` 覆盖四个部分：

| 部分 | 覆盖内容 |
| --- | --- |
| `registry` | 每台设备的生命周期状态（active/disabled/deregistered）、凭据哈希、签发时间、**绝对过期时间**、是否被撤销（含 rotate 后的新凭据）、注册表记录的当前固件版本 |
| `telemetry` | 每台设备的当前影子、每个字段的 LWW 版本号 `(device_ts, seq)`、已见 message_id（去重表）、已接受计数、**已拒绝记录明细及原因码** |
| `campaigns` | 每个升级任务的状态、分批进度 `current_batch`、每台设备的升级状态与 attempts、错误信息 |
| `flasher` | 每台设备**本地实际安装**的固件版本（设备侧事实；脚本化失败与调用日志属临时测试设施，不导出） |

**在线一致性**：导出时按固定全局顺序一次性持有
`orchestrator → registry → telemetry → flasher` 四把锁，得到的是跨组件的同一个
原子状态；导出期间即使有设备注册、遥测写入或升级下发，也不会出现半新半旧、无法
自洽的包（`test_online_export_self_consistent_under_concurrent_writes` 在 4 个
并发写线程下连续导出并校验）。

### 恢复语义

- **凭据规则不变**：恢复后老设备用原 secret 即可接入；disable/revoke/rotate/
  过期判定与恢复前一致（过期时间是绝对时间戳，随墙钟继续生效）。
- **影子查询一致**：影子、字段版本、拒绝日志逐字节一致；恢复前已见的
  message_id 重放仍是 `duplicate_message`。
- **未完成任务可续跑**：恢复时对任务做与重启相同的归一化——捕获时处于
  `running` 的任务降级为 `paused`（**不会永远卡在 running**），中断的
  `in_progress` 设备回到 `pending`。之后可继续 `pause/resume/cancel`，只处理真正
  未完成的部分；`succeeded` 设备不会被重新下发（flasher 幂等 + 每台设备的终态
  均保留）。
- **恢复是全量替换且先验证后生效**：整个包先在“临时组件”里完成解析、校验和
  交叉检查，通过后才在同一组锁内原子替换四个在线组件。任何拒绝原因都不会破坏
  当前可用状态；同一份包重复导入结果一致（确定性全量替换）。

### 格式版本、完整性与拒绝原因

导入严格按下列顺序检查，失败原因可用错误码区分：

1. `backup_corrupted`（HTTP 400）：不是合法 JSON（被截断/二进制垃圾）、缺少
   必要顶层字段、`format` 不认识、`checksum` 形态非法、`format_version` 不是整数。
2. `backup_integrity_failed`（422）：JSON 合法但 payload 被改过、与封装时的
   SHA-256 不符（内容篡改可与结构损坏区分）。
3. `backup_version_unsupported`（422）：`format_version` 不在当前支持列表
   （目前仅 `1`）。高版本包不能导回低版本程序。
4. `backup_incomplete`（422）：缺少任一 payload 部分或必要字段、枚举值非法、
   影子与字段版本键不匹配、遥测/任务引用了不存在的设备、同一设备出现在两个未
   终结任务中、`succeeded` 设备的注册表版本或设备侧实际版本与目标版本不一致。

   > 即使篡改者重新计算了校验和，第 4 步的结构性与交叉引用检查仍会拒绝导入。

### 接口

- `POST /admin/backup`：无 body 时直接流式返回备份 JSON；带
  `{"path": "/data/bk.json"}` 时原子写文件（临时文件 + `fsync` + rename），返回
  各部分计数。
- `POST /admin/restore`：请求体即备份包内容，校验通过后恢复。
- `POST /admin/restore-file`：`{"path": "..."}`，从本机文件恢复（单机搬迁）。

### 本地迁移与恢复验证步骤

```bash
# 1) 机器 A：设置状态目录并启动（每个组件的状态都原子落盘，可扛普通重启）
export GATEWAY_STATE_DIR=/data/gateway
uv run uvicorn main:app
#    业务运行中随时导出（无需停服务）
curl -s -X POST localhost:8000/admin/backup \
     -H 'Content-Type: application/json' \
     -d '{"path":"/data/migration/bk.json"}'

# 2) 机器 B：拷贝备份包，使用相同的程序版本，指向一个干净状态目录启动
export GATEWAY_STATE_DIR=/data/gateway   # 新机器上的空目录
uv run uvicorn main:app
curl -s -X POST localhost:8000/admin/restore-file \
     -H 'Content-Type: application/json' \
     -d '{"path":"/data/migration/bk.json"}'

# 3) 验证：老凭据接入、影子一致、未完成任务续跑
curl -s localhost:8000/devices/<id>/shadow
curl -s -X POST localhost:8000/campaigns/<id>/resume
```

兼容性约定：

- **能互相恢复的配置**：同一 `format_version`（当前为 1）、同一主版本的网关
  程序；凭据 TTL 等配额参数是运行配置、不进备份，目标机可用不同环境变量覆盖，
  但已签发凭据按包内的绝对过期时间判定。
- 高版本格式不能导回旧程序；同版本内缺少任何部分或引用不自洽都会被拒绝。
- 自动化验证见 `tests/test_backup.py`：在线导出覆盖度、并发写入下快照自洽、
  换目录 + 进程重启恢复、任务中途备份后续跑（成功设备不重复下发）、
  running/in_progress 归一化、损坏/篡改/版本不兼容/缺字段分别拒绝、
  导入失败不破坏在线状态、重复导入幂等。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_CREDENTIAL_TTL_SECONDS` | 3600 | 凭据有效期（秒） |
| `GATEWAY_TELEMETRY_QUOTA` | 1000 | 每设备可接受的去重后消息数上限 |
| `GATEWAY_MAX_CLOCK_SKEW_SECONDS` | 300 | 允许的设备时钟超前上限 |
| `GATEWAY_FOTA_MAX_RETRIES` | 2 | 单台设备失败后的重试次数 |
| `GATEWAY_STATE_DIR` | 空（不持久化） | 状态目录：设置后注册表/遥测/升级任务/设备实际版本各写一个原子 JSON，重启自动恢复 |
| `GATEWAY_FOTA_STATE_PATH` | 空 | 单独指定升级任务状态路径（设置后覆盖 `GATEWAY_STATE_DIR` 下的默认位置） |

## 代码结构

- `app/models.py` — 枚举、错误码（含备份四类拒绝码）、`GatewayError`
- `app/registry.py` — 设备注册（幂等）与凭据生命周期、快照/恢复与可选落盘
- `app/telemetry.py` — 遥测摄入、配额、按字段 LWW 影子合并与判定日志、快照/恢复
- `app/fota.py` — 分批升级编排、暂停/恢复/取消、持久化与重启恢复
- `app/backup.py` — 在线全量备份/恢复协调器：一致性快照、格式版本、SHA-256
  完整性校验、跨组件引用校验、先验证后原子替换
- `app/simulator.py` — 模拟设备固件刷写器（幂等、可脚本化失败、版本可落盘）
  与遥测流发生器
- `main.py` — FastAPI 接线、错误码 → HTTP 状态映射、`/admin/backup|restore` 接口
- `tests/` — 注册、遥测（含并发/乱序/重复/偏移/配额）、升级编排、备份恢复、
  端到端 API 测试
