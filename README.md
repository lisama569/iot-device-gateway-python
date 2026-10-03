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

## 在线备份与跨环境恢复（迁移 / 故障演练）

网关支持**不停服导出完整备份包**，并在一台干净环境（新机器或空数据目录）里
恢复后接着跑。备份是一个 ZIP：

```
gateway-backup.zip
├── manifest.json   # {"format_version": 1, "created_at": ...,
│                   #  "data_file": "data.json", "sha256": <data.json 的 SHA-256>}
└── data.json       # 快照正文（见下方覆盖范围）
```

### 备份覆盖范围

| 类别 | 具体内容 |
| --- | --- |
| 设备生命周期 | 每台设备的 `active / disabled / deregistered` 状态、注册时间 |
| 凭据有效性 | 秘密哈希（**不存明文**）、签发时间、过期时间、是否已撤销；轮换后的新凭据与过期语义原样保留 |
| 设备影子 | 每设备影子当前值、每字段 `(device_ts, seq)` 版本 |
| 幂等与拒绝记录 | 已接受的 `message_id` 集合（恢复后重复投递仍判重）、已接受计数、每条 `validation_failed / quota_exceeded` 拒绝记录 |
| 升级任务进度 | 每个 campaign 的状态、批次位置、每设备 `pending/in_progress/succeeded/failed/skipped`、`attempts`、错误信息 |
| 设备实装版本 | 每台设备**实际刷上的固件版本**（刷写器侧），并与注册表记账交叉校验 |

**恢复之后重启仍然成立**：以上全部内容在恢复完成后会被立即写入本机状态文件
（设置 `GATEWAY_STATE_PATH` 时），此后无论进程重启还是重新部署，老设备仍能用
原来的接入信息连上，影子、判重、拒绝记录、任务进度与实装版本都与重启前一致；
每次后续变更也会即时落盘。详见下文"重启持久化"。

**不备份**：配置项本身（环境变量，按下文兼容性约定在新机设置）、模拟刷写器的
失败脚本与调用审计日志（仅测试脚手架）。

### 一致性保证

- **在线快照是单一全局一致点**：导出时按固定的全局加锁顺序
  （编排器 → 注册表 → 遥测 → 刷写器，也是代码里唯一的跨组件嵌套顺序）一次性
  取齐四份状态，序列化与磁盘 I/O 在锁外进行。导出期间仍在写入遥测或执行升级，
  也只会观察到"某一时刻的完整状态"，不会出现半新半旧、无法自洽的包。
- **恢复是"先校验、后换入"**：导入时先在一次性的临时组件实例上重建并做结构
  校验 + 跨组件一致性校验，全部通过后才在同一把锁序下整体换入；任何一步失败
  当前运行状态都不受影响。
- **未完成任务安全续跑**：恢复时 `running` 一律降级为 `paused`（不会卡死）、
  中断的 `in_progress` 设备回退为 `pending`；`succeeded` 设备保持不变。恢复后
  可继续 `pause / resume / cancel`，且只会处理真正未完成的部分。刷写器幂等，
  即使快照正好取在"固件已刷上、记账尚未完成"的窗口，续跑也只是空操作成功，
  **已成功的设备不会被重新下发**。
- **失败导入不改状态**；**同一份备份重复导入结果一致**（恢复的是包内确定状态，
  与导入次数无关）。

### 重启持久化（恢复之后重启仍然成立）

设置 `GATEWAY_STATE_PATH` 后，网关把**整份状态**（注册表、遥测、升级任务、
实装版本）以备份包同款格式（ZIP + manifest + SHA-256）作为本机状态文件：

- **每次变更即时落盘**：注册/停用/吊销/轮换、每条遥测的接受或拒绝、每台设备
  的刷写记账、每次任务状态迁移、以及**每一次备份恢复**，都会在锁释放后原子
  写入（临时文件 + `os.replace`，崩溃最多留下一个被忽略的 `.tmp`，不会截断
  上一份完好检查点）。
- **启动时整份加载**：开机加载复用备份导入的"先校验、后换入"全流程——状态
  要么完整恢复，要么明确报错（`GET /admin/state` 可见 `state=error`、错误码与
  隔离路径），损坏文件被重命名为 `<path>.corrupt` 隔离保留，**不会半恢复、
  不会静默丢数据，也不会因此无法再导入备份**（空网关可直接恢复任何合法备份包）。
- **未完成任务续跑**：与备份恢复同一套归一化规则——`running` 降级为 `paused`
  （不会卡死）、中断的 `in_progress` 设备回退 `pending`、`succeeded` 不变；
  刷写器幂等，续跑只处理真正未完成的部分，已成功的设备不会被重新下发。
- **连续重启、重复导入结果一致**：检查点内容只取决于状态本身，与重启/导入
  次数无关。
- `GATEWAY_STATE_PATH` 与 `GATEWAY_FOTA_STATE_PATH` 同时设置时，前者优先
  （整份状态文件已包含任务进度，FOTA 单独的状态文件不再启用）。

### 格式与兼容边界（拒绝原因可区分）

`manifest.json.format_version` 为格式版本，当前为 `1`。导入按下列顺序检查并以
不同错误码（HTTP 422/409）拒绝，可直接据码区分原因：

| 错误码 | 触发情形 |
| --- | --- |
| `backup_truncated` | 空文件、不是合法 ZIP、ZIP CRC 失败、缺少 `manifest.json/data.json`、manifest 不可解析或缺关键字段 |
| `backup_integrity_failed` | 归档可读但 `data.json` 的 SHA-256 与 manifest 不符（传输截断或内容被篡改） |
| `backup_format_unsupported` | manifest 或正文的 `format_version` 不在当前构建支持集合 `{1}` 内（**先于**校验和判定） |
| `backup_missing_section` | 正文缺少任一必需段：`registry / telemetry / fota / flasher` |
| `backup_payload_invalid` | 段存在但结构非法（枚举未知、类型错误、ID 重复等），或跨段自相矛盾（如任务引用未知设备、标记 succeeded 但实装版本对不上） |
| `backup_nonempty_target` | 目标网关已有状态（HTTP 409）；干净环境可直接恢复，覆盖导入需显式带 `X-Allow-Overwrite: true` |

**配置/版本互相恢复约定**：格式版本必须兼容（同为 v1）；目标环境的配置无需与
源机逐字相同，但语义相关配置建议保持一致——`GATEWAY_CREDENTIAL_TTL_SECONDS`
**不影响已存在凭据**（过期时间绝对值随包恢复，TTL 只作用于恢复后新签发/轮换的
凭据）；`GATEWAY_FOTA_MAX_RETRIES` 等恢复后才生效。`GATEWAY_FOTA_STATE_PATH`
指向新目录即可，恢复会自动建目录并把任务状态重新落盘到新机路径。

### 管理接口

| 接口 | 说明 |
| --- | --- |
| `POST /admin/backup/export` | 在线导出，返回 `application/zip`（服务无需停止） |
| `POST /admin/backup/inspect` | 上传一个包，只做解析与全部校验、返回摘要，**不改任何运行状态** |
| `POST /admin/backup/restore` | 在干净网关上恢复；目标非空返回 409，带 `X-Allow-Overwrite: true` 可整体覆盖 |
| `GET /admin/state` | 重启持久化健康状态：启动检查点是 `loaded / absent / error`、错误原因与隔离文件路径 |

### 本地做一次迁移 / 恢复演练

```bash
# 1) 旧机：带状态目录启动，造一些真实状态
GATEWAY_STATE_PATH=/tmp/gw-old/state.zip \
  uv run uvicorn main:app --port 18901 &
curl -s -X POST localhost:18901/devices/register \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"drill-1","secret":"snow"}'
curl -s -X POST localhost:18901/devices/drill-1/telemetry \
  -H 'X-Device-Secret: snow' -H 'Content-Type: application/json' \
  -d '{"message_id":"t1","seq":1,"device_ts":100.0,"payload":{"rpm":1200}}'
curl -s -X POST localhost:18901/campaigns \
  -H 'Content-Type: application/json' \
  -d '{"campaign_id":"drill-c","target_version":"5.5.5",
       "device_ids":["drill-1"],"batch_size":1}'
curl -s -X POST localhost:18901/campaigns/drill-c/pause

# 2) 在线导出（服务保持运行）
curl -s -X POST localhost:18901/admin/backup/export -o /tmp/gw-backup.zip

# 3) 新机 / 空目录：先校验，再恢复
GATEWAY_STATE_PATH=/tmp/gw-new/state.zip \
  uv run uvicorn main:app --port 18902 &
curl -s -X POST localhost:18902/admin/backup/inspect \
  --data-binary @/tmp/gw-backup.zip
curl -s -X POST localhost:18902/admin/backup/restore \
  --data-binary @/tmp/gw-backup.zip

# 4) 关键一步：把新机的进程重启（或重新部署），状态必须仍然成立
kill %2
GATEWAY_STATE_PATH=/tmp/gw-new/state.zip \
  uv run uvicorn main:app --port 18903 &
curl -s localhost:18903/admin/state        # state 应为 "loaded"

# 5) 重启后验证：老凭据可用、影子一致、重复投递仍判重、暂停任务续跑完成
curl -s -X POST localhost:18903/devices/drill-1/telemetry \
  -H 'X-Device-Secret: snow' -H 'Content-Type: application/json' \
  -d '{"message_id":"t2","seq":2,"device_ts":200.0,"payload":{"rpm":1300}}'
curl -s -X POST localhost:18903/devices/drill-1/telemetry \
  -H 'X-Device-Secret: snow' -H 'Content-Type: application/json' \
  -d '{"message_id":"t1","seq":1,"device_ts":100.0,"payload":{"rpm":1200}}'
#   -> "code":"duplicate_message"（判重记忆跨重启保留）
curl -s localhost:18903/devices/drill-1/shadow
curl -s -X POST localhost:18903/campaigns/drill-c/resume   # -> completed
```

单元测试 `tests/test_backup.py` 覆盖：全生命周期在线导出、导出期间并发写入+
并发升级（慢刷写器拉长窗口，期间采到的每个包都必须能恢复且自洽）、换目录
恢复后凭据/影子/判重一致、半完成任务续跑且不重复下发已成功设备、被"抓在
running"的任务归一化后可暂停/取消、截断/篡改/版本不兼容/缺段/结构非法分别被
不同错误码拒绝、失败导入不破坏当前状态、重复导入结果一致与覆盖导入收敛。

单元测试 `tests/test_persistence.py` 覆盖"恢复之后重启仍然成立"（按业务分组，
`pytest -v` 输出可直接看到各组用例与通过情况，测试日志打印判定依据）：

- `TestRestoreThenRestart` — 干净环境恢复后重启：老凭据接入、停用/吊销/过期/
  秘密错误各自可区分、影子与字段版本、重复消息判重、拒绝记录、任务进度与
  实装固件版本全部与重启前一致；连续多次重启状态不漂移。
- `TestUnfinishedCampaignAcrossRestart` — 带着跑到一半的任务恢复并重启：
  暂停/继续/取消都可用，续跑只下发真正未完成的设备（已成功的绝不重复下发），
  任务不会卡在 running；模拟崩溃窗口（检查点里仍是 running/in_progress）开机
  归一化后同样能续跑到完成。
- `TestCorruptStateOnBoot` — 重启时状态异常：截断/篡改的检查点被明确报错并
  隔离（不半恢复、不静默丢数据），网关保持可用且备份照样能导入；崩溃残留的
  `.tmp` 不影响上一份完好检查点；失败的导入既不改运行状态也不碰检查点文件。
- `TestRepeatImportAndRestartConsistency` — 同一份备份重复导入、导入后再重启，
  得到的组件状态逐一相等。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_CREDENTIAL_TTL_SECONDS` | 3600 | 凭据有效期（秒） |
| `GATEWAY_TELEMETRY_QUOTA` | 1000 | 每设备可接受的去重后消息数上限 |
| `GATEWAY_MAX_CLOCK_SKEW_SECONDS` | 300 | 允许的设备时钟超前上限 |
| `GATEWAY_FOTA_MAX_RETRIES` | 2 | 单台设备失败后的重试次数 |
| `GATEWAY_STATE_PATH` | 空（不持久化） | 整份网关状态文件路径：每次变更原子落盘、重启整份恢复（恢复备份后重启仍然成立） |
| `GATEWAY_FOTA_STATE_PATH` | 空（不持久化） | 仅升级任务状态的 JSON 落盘路径；设置 `GATEWAY_STATE_PATH` 时被其取代 |

## 代码结构

- `app/models.py` — 枚举、错误码、`GatewayError`
- `app/registry.py` — 设备注册（幂等）与凭据生命周期
- `app/telemetry.py` — 遥测摄入、配额、按字段 LWW 影子合并与判定日志
- `app/fota.py` — 分批升级编排、暂停/恢复/取消、持久化与重启恢复
- `app/backup.py` — 跨组件一致性在线快照、ZIP 备份包（版本+SHA-256 校验）、
  分层校验与"先建后换"的原子恢复
- `app/persistence.py` — 重启持久化：每次变更把整份状态以备份包格式原子落盘，
  开机整份加载（损坏即报错并隔离，不半恢复）
- `app/simulator.py` — 模拟设备固件刷写器（幂等、可脚本化失败）与遥测流发生器
- `main.py` — FastAPI 接线与错误码 → HTTP 状态映射（含 `/admin/backup/*` 与
  `/admin/state`）
- `tests/` — 注册、遥测（含并发/乱序/重复/偏移/配额）、升级编排、备份恢复、
  重启持久化、端到端 API 测试
