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
恢复后接着跑；**恢复之后无论重启进程还是重新部署，"换台机器接着跑"都继续成立**。
备份是一个 ZIP：

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

### 恢复之后重启仍然成立（全量持久化）

只让 FOTA 任务状态落盘是不够的——设备、凭据、影子、判重集合、拒绝记录和设备上
实装的固件版本若只在内存里，恢复后一旦重启进程（或重新部署），网关就退化成
"只剩任务、任务里的设备却不存在"的空壳。为此，只要设置
`GATEWAY_STATE_PATH`，**整份快照就是持久化的**：

- **每次变更原子落盘（高频遥测合并写）**：注册/停用/撤销/轮换、升级任务的
  **每台设备**状态推进（含实际刷上的版本与注册表记账）这类低频且崩溃敏感的
  变更，会**同步**取一份完整快照，用"临时文件 + 原子替换"覆盖 `state.zip`，
  调用返回即已持久化；高频遥测（被接受与被拒绝的消息）则走**合并写**
  （coalescing）：一个短窗口（`GATEWAY_STATE_FLUSH_INTERVAL_SECONDS`，默认
  0.1s）内的大量消息只触发少量几次整快照写入，避免每条消息一次全量落盘拖垮
  吞吐。任何一次同步落盘（如下一台设备升级）都会把尚未写出的遥测一并带上，
  进程收到 SIGTERM/SIGINT 优雅停机时也会强制把尾部落盘。任何时刻崩溃，磁盘上
  要么是上一整份、要么是下一整份，不会出现只写了一半的包；同目录另有
  `state.zip.bak` 始终保留**上一份完好快照**。合并写失败会在下一次写入时立即
  报错并回滚对应内存变更（`state_persist_failed`），不会静默丢数据。
- **重启即整体恢复**：进程启动时用与"导入备份"完全相同的"暂存 → 校验 → 换入"
  流程加载快照，并再写一版归一化结果，因此**连续重启任意次都收敛到同一份
  状态**。恢复时 `running` 归一为 `paused`、中断的 `in_progress` 设备回退为
  `pending`；落盘点细到每台设备，所以已经 `succeeded` 的设备连同其实装版本一定
  已在盘上，**续跑时绝不重新下发**，任务也不会卡死。
- **状态异常时明确报错而不是静默退化为空网关**：启动时若 `state.zip` 与
  `state.zip.bak` 都无法读取或校验不过，网关仍会启动但进入 **faulted** 状态：
  业务接口统一返回 `503 state_unavailable`（错误信息说明原因与恢复方式），
  `GET /health` 可观察；此时**导入一份有效备份即可恢复**，恢复成功后 fault
  清除，且该恢复本身也能再扛一次重启。主文件损坏但 `.bak` 可用时会自动回退到
  上一份完好快照并在 `/health` 的 `recovered_from` 中说明来源。
- **写不进去就不算成功**：某次变更若无法把整份快照落盘（如状态目录暂时
  不可写），注册/凭据/遥测类变更会回滚内存修改并返回 `500 state_persist_failed`；
  升级执行在某次"每设备落盘"失败时中止本次请求（返回 500），此前每设备成功点
  之前的设备都已在盘上，当前这台即使实际已刷上、重启后也会因刷写器幂等而被
  安全地空操作重试一次，不会重复下发或回退版本。磁盘 I/O 恢复后下一次变更会
  重新同步——绝不允许内存与磁盘长期分叉。只有**启动时确认两份文件都不可用**
  才进入 faulted（见上条）。
- **导入失败不碰当前可用状态**：恢复仍然是"先校验、先写盘、后换内存"——磁盘
  替换失败则直接中止，当前运行的网关与现有快照都不受影响。

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
凭据）；`GATEWAY_FOTA_MAX_RETRIES` 等恢复后才生效。新机把
`GATEWAY_STATE_PATH` 指向新目录即可，恢复会自动建目录、原子写入全量快照并把
任务状态一并持久化到新机路径（旧的 `GATEWAY_FOTA_STATE_PATH` 若同时设置也会
照常写入，仅用于兼容旧行为）。**不设置 `GATEWAY_STATE_PATH` 时状态仍是纯内存
的**——要让"恢复之后重启仍然成立"，新机必须配置该变量。

### 管理接口

| 接口 | 说明 |
| --- | --- |
| `POST /admin/backup/export` | 在线导出，返回 `application/zip`（服务无需停止）；faulted 时拒绝，避免导出残缺状态 |
| `POST /admin/backup/inspect` | 上传一个包，只做解析与全部校验、返回摘要，**不改任何运行状态** |
| `POST /admin/backup/restore` | 在干净网关上恢复；目标非空返回 409，带 `X-Allow-Overwrite: true` 可整体覆盖；也是 faulted 后的恢复入口 |
| `GET /health` | 持久化健康：`healthy`、`state_path`、本次启动从哪个文件恢复（`recovered_from`）、fault 原因与启动恢复摘要 |

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

# 3) 新机 / 空目录：先校验，再恢复（关键：新机也要带 GATEWAY_STATE_PATH）
GATEWAY_STATE_PATH=/tmp/gw-new/state.zip \
  uv run uvicorn main:app --port 18902 &
curl -s -X POST localhost:18902/admin/backup/inspect \
  --data-binary @/tmp/gw-backup.zip
curl -s -X POST localhost:18902/admin/backup/restore \
  --data-binary @/tmp/gw-backup.zip

# 4) 验证：老凭据可用、影子一致、重复投递仍判重、暂停任务续跑完成
curl -s localhost:18902/devices/drill-1/shadow
curl -s -X POST localhost:18902/campaigns/drill-c/resume
```

#### 关键一步：重启 / 重新部署后再验证一遍

第 4 步完成后（或故意留一个没跑完的任务再做这一步），**把 18902 的进程停掉、
用完全相同的命令重新拉起**（模拟重启进程，或在新机器上重新部署），然后重复
验证——恢复后的状态必须原样还在，而不是退化成空网关：

```bash
# 重启后健康检查：healthy=true，recovered_from 指向本次加载的快照
curl -s localhost:18902/health
#   {"healthy": true, "state_path": "/tmp/gw-new/state.zip",
#    "recovered_from": "/tmp/gw-new/state.zip", "fault": null, ...}

# 设备接入：老凭据照常连上；撤销/停用/过期仍按恢复前的规则区分
curl -s -X POST localhost:18902/devices/drill-1/telemetry \
  -H 'X-Device-Secret: snow' -H 'Content-Type: application/json' \
  -d '{"message_id":"t1","seq":1,"device_ts":100.0,"payload":{"rpm":1200}}'
#   code 仍是 "duplicate_message"（判重集合随重启保留）
curl -s localhost:18902/devices/drill-1/shadow            # 影子与重启前一致
curl -s localhost:18902/devices/drill-1/telemetry/rejected # 拒绝记录仍在

# 任务续跑：恢复时没跑完的任务重启后仍是 paused，resume 只处理未完成部分；
# 已经 succeeded 的设备不会被重新下发，任务最终 completed，不会一直挂着
curl -s localhost:18902/campaigns/drill-c                 # status=paused
curl -s -X POST localhost:18902/campaigns/drill-c/resume  # -> completed
```

连续重启多次结果必须一致；若 `state.zip` 与 `state.zip.bak` 都损坏，`/health`
会显示 `healthy:false` 与原因，业务接口返回 `503 state_unavailable`，此时重新
执行第 3 步的 `restore`（对非空目标加 `X-Allow-Overwrite: true` 头）即可恢复。

上述全过程也由自动化用例固化：`tests/test_restart_http.py` 会真正启动两个
uvicorn 进程代际、共用同一个 `GATEWAY_STATE_PATH` 跑完整迁移-重启-续跑链路；
`tests/test_restart.py` 覆盖恢复后重启仍能接入与查询（含撤销/停用/过期区分、
影子、判重、拒绝记录、实装版本）、带未完成任务恢复后重启再续跑且不重复下发、
以及重启期间主文件损坏回退 `.bak`、双文件损坏 faulted 与导入自愈等。

单元测试 `tests/test_backup.py` 覆盖：全生命周期在线导出、导出期间并发写入+
并发升级（慢刷写器拉长窗口，期间采到的每个包都必须能恢复且自洽）、换目录
恢复后凭据/影子/判重一致、半完成任务续跑且不重复下发已成功设备、被"抓在
running"的任务归一化后可暂停/取消、截断/篡改/版本不兼容/缺段/结构非法分别被
不同错误码拒绝、失败导入不破坏当前状态、重复导入结果一致与覆盖导入收敛。
重启持久化由 `tests/test_restart.py`（16 个用例，含高频遥测合并落盘与优雅
停机）与真实双进程 HTTP 演练 `tests/test_restart_http.py`（2 个用例，含
SIGTERM 停机与状态损坏 503 自愈）覆盖，见上节"恢复之后重启仍然成立"。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `GATEWAY_CREDENTIAL_TTL_SECONDS` | 3600 | 凭据有效期（秒） |
| `GATEWAY_TELEMETRY_QUOTA` | 1000 | 每设备可接受的去重后消息数上限 |
| `GATEWAY_MAX_CLOCK_SKEW_SECONDS` | 300 | 允许的设备时钟超前上限 |
| `GATEWAY_FOTA_MAX_RETRIES` | 2 | 单台设备失败后的重试次数 |
| `GATEWAY_STATE_PATH` | 空（不持久化） | **全量网关快照落盘路径**（`state.zip`，同目录另有 `.bak`）。设置后恢复的状态可跨重启/重部署保留；迁移演练时新机务必设置 |
| `GATEWAY_STATE_FLUSH_INTERVAL_SECONDS` | 0.1 | 高频遥测合并落盘的窗口（秒）。窗口内的消息合并成少量整快照；设备/凭据与每设备升级进度不受此影响、始终同步落盘 |
| `GATEWAY_FOTA_STATE_PATH` | 空（不持久化） | 仅升级任务的旧版 JSON 落盘路径，保留用于兼容；开启全量持久化后任务已包含在 `GATEWAY_STATE_PATH` 中 |

## 代码结构

- `app/models.py` — 枚举、错误码、`GatewayError`
- `app/registry.py` — 设备注册（幂等）与凭据生命周期
- `app/telemetry.py` — 遥测摄入、配额、按字段 LWW 影子合并与判定日志
- `app/fota.py` — 分批升级编排、暂停/恢复/取消、持久化与重启恢复
- `app/backup.py` — 跨组件一致性在线快照、ZIP 备份包（版本+SHA-256 校验）、
  分层校验与"先建后换"的原子恢复
- `app/durable.py` — 全量快照的变更落盘、启动整体恢复（含 `.bak` 回退）、
  状态损坏 faulted 与 `/health` 健康报告，保证恢复后重启/重部署仍然成立
- `app/simulator.py` — 模拟设备固件刷写器（幂等、可脚本化失败）与遥测流发生器
- `main.py` — FastAPI 接线与错误码 → HTTP 状态映射（含 `/admin/backup/*` 与
  `/health`，faulted 时业务接口返回 503）
- `tests/` — 注册、遥测（含并发/乱序/重复/偏移/配额）、升级编排、备份恢复、
  **恢复后重启持久化（含真实双进程 HTTP 演练）**、端到端 API 测试；
  `tests/conftest.py` 在测试日志末尾按业务分组列出每个用例及其通过情况
