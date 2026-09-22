# IoT Device Gateway (Python / FastAPI)

本地可控的设备接入网关：设备注册与凭据生命周期、遥测写入与设备影子合并、
分批固件升级编排。**不依赖外部消息代理、真实设备或真实凭据**——设备侧由
内存中的确定性模拟器扮演，状态持久化到本地单个 JSON 文件。

## 快速开始

```bash
uv sync                       # 安装依赖（含 pytest 开发依赖）
uv run pytest                 # 运行全部测试（含关键判定日志）
uv run python scripts/demo.py # 本地模拟流量演示
uv run uvicorn main:app --reload   # 启动 HTTP 服务
```

服务启动后：

* `GET  /` 健康检查
* `POST /devices/register` 注册（幂等，返回一次性 `credential_secret`）
* `POST /devices/{id}/enable|disable|deregister`
* `POST /devices/{id}/credentials/rotate|.../revoke`
* `POST /telemetry`、`GET /telemetry/{id}/shadow`
* `POST /upgrades`（创建即启动）、`POST /upgrades/{id}/pause|resume|cancel`、
  `GET /upgrades/{id}`

完整字段见 `gateway/models.py`，交互式文档见 `/docs`。

## 1. 设备生命周期与权限规则

设备状态机：`enabled -> disabled -> enabled`，任意状态可 `deregistered`
（注销不可逆，注销会同时撤销全部凭据）。

* **注册幂等**：相同 `device_id + name + metadata` 的重复注册返回**同一个**
  `credential_id` 和 `credential_secret`，`registration_seq` 不变。
* **冲突可区分**（HTTP 状态码 + 响应体 `error` / `details.reason`）：
  * 同一 `device_id` 但 name/metadata 不同 → `409 device_conflict`，
    `reason=identity_mismatch`；
  * 设备已注销再注册 → `409 device_conflict`，`reason=device_deregistered`；
  * 重复 `job_id` / 未知设备等冲突各自带独立 `reason`（见升级章节）。
* **凭据生命周期**：凭据在注册或轮换时签发，带可选过期时间
  （`credential_ttl_seconds`，默认见配置）。每次接入必须同时提供
  `credential_id` 与 `credential_secret`，失败原因严格区分：
  * 凭据不存在 / secret 不匹配 → `401 credential_invalid`；
  * 已过 `expires_at` → `401 credential_expired`（响应含 `expires_at`）；
  * 已被轮换撤销或手动撤销 → `401 credential_revoked`（响应含 `revoked_at`）；
  * 设备被停用 → `403 device_disabled`；设备不存在/已注销 → `404 device_not_found`。
* **立即生效**：enable/disable/deregister/revoke 都先持久化再返回，下一次
  鉴权请求即按新状态判定；注销与停用不会放行任何后续接入。
* 轮换（`POST .../credentials/rotate`）默认撤销旧凭据并签发新凭据，
  `registration_seq` 递增，用于标识凭据代际。

## 2. 遥测与设备影子：兼容与冲突处理

消息字段：`version`（设备侧单调状态版本，≥1）、`timestamp`（设备时钟）、
`seq`（同版本/时间戳内的序号）、`nonce`（可选去重盐值）、`state`（增量键值）。

### 确定性全序与判定

冲突比较元组为 **`(version, timestamp, seq, nonce)`**，逐分量比较：

1. 高 `version` 永远获胜，低版本消息判定为 `stale`，**绝不回退状态**；
2. 版本相同则较新的 `timestamp` 获胜（容忍乱序与时钟偏移，旧消息 `stale`）；
3. 时间戳也相同则较大的 `seq`，再相同则字典序较大的 `nonce` 获胜。

判定结果：`merged`（合并入影子）、`stale`（元组更小，拒绝）、`duplicate`
（完全相同的元组重放）。无论到达顺序如何，最终影子状态唯一确定。

### 合并、去重与配额

* **合并语义**：获胜消息的 `state` 键合并进影子（未上报的键保留）；失败
  消息不触碰影子。乱序消息因此不会造成静默覆盖。
* **精确去重**：最近 256 个已应用元组保存在影子中，重放返回 `duplicate`，
  影子不变、**配额计数不增加**，杜绝重复计数。
* **不静默丢弃**：配额超限抛 `429 quota_exceeded`（滑动窗口，响应含
  `retry_after`，只有真正 merge 的消息计数）；载荷校验失败（非有限浮点、
  不可序列化值等）抛 `422 validation_failure`；Pydantic 结构错误同样返回 422。
* **可追溯**：每条消息的 `incoming_tuple`、当前 `applied_tuple`、`decision`
  与 `reason` 都写入审计日志（持久化文件中的 `audit`）。
* 同一设备的写入经每设备异步锁串行化，因此并发上报的冲突判定是全序且确定的。

## 3. 固件升级编排与验证方法

`POST /upgrades` 创建并自动启动任务，按 `batch_size` 分批、按目标设备列表
的确定顺序推进；`max_retries` 为每台设备失败后的额外重试次数
（总尝试 = 1 + max_retries）。

* **设备级状态**：`pending → in_progress → succeeded | failed | skipped | cancelled`。
  分发时设备若已停用/注销，该设备记为 `skipped` 并附原因，不阻断整批推进。
* **批次失败/重试可预期**：一批并发分发，成功即终态；失败设备回到队尾
  （按原始设备顺序排序）进入下一轮，直到成功或重试耗尽记 `failed`。
  单台模拟器按 `(device, version, attempt)` 做确定性 RNG，重试可成功、结果可复现。
* **暂停 / 恢复 / 取消**：暂停在**批次边界**生效，进行中的批次会跑完，随后
  任务停在 `paused` 且不再推进，直到 `resume`；取消立即把未分发设备置为
  `cancelled`，在途批次跑完后按结果落 `succeeded`（幂等成功）或 `cancelled`。
  取消/暂停/恢复对已处于相应/终态的任务幂等；从终态调用非法转移返回
  `409 job_conflict`。
* **唯一且可解释的终态**（写入 `terminal_reason`）：
  显式取消 → `cancelled`；否则全部设备终态化后，只要有一台 `failed` →
  `failed`（原因中列出失败设备）；否则 `completed`。`skipped` 不导致失败。
  任务终态只会进入一次。
* **同一设备不得并行升级**：创建任务时对所有非终态任务做占用检查，冲突返回
  `409 job_conflict`，`reason=device_busy`，`details.conflicts` 指明占用的
  `job_id` 与设备状态；任务终态后设备即被释放。
* **重启恢复（无卡死、不重复下发）**：所有状态转移先持久化。启动时
  `resume_pending_jobs` 会：
  * 把已 `succeeded` 的设备版本回放到新模拟器（重复分发幂等成功）；
  * 将崩溃时停留在 `in_progress` 的设备重新排队（**不额外消耗重试次数**，
    保持确定顺序），任务自动续跑；
  * `pending`/`paused` 任务维持原状（本就未分发，不可能卡死）；
  * 终态任务结算可能残留的在途设备。整体为 **at-least-once + 幂等安装**语义。
* **本地模拟器**：`DeviceSimulator` 提供 `fail_rate` 随机（确定性种子）失败率、
  `force_failure(device, version)` 永久失败注入；安装过的版本重复安装直接成功，
  作为重复下发的第二道防线。

验证方式：

```bash
uv run pytest tests/test_firmware.py -q   # 失败终态/暂停恢复/取消/互斥/重启恢复
uv run python scripts/demo.py             # 观察分批、失败与取消的任务状态输出
```

## 4. 配置项

全部通过环境变量覆盖（见 `gateway/config.py`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GATEWAY_STATE_FILE` | `gateway_state.json` | 本地 JSON 持久化文件（原子写） |
| `GATEWAY_CREDENTIAL_TTL` | `3600` | 新凭据默认有效期（秒）；`0` 表示不过期 |
| `GATEWAY_TELEMETRY_QUOTA` | `1000` | 每设备每窗口允许 merge 的遥测条数 |
| `GATEWAY_TELEMETRY_QUOTA_WINDOW` | `60` | 遥测配额滑动窗口（秒） |
| `GATEWAY_BATCH_SIZE` | `5` | 升级任务默认每批设备数 |
| `GATEWAY_MAX_RETRIES` | `2` | 每设备默认额外重试次数 |
| `GATEWAY_UPGRADE_DELAY` | `0.01` | 批次间间隔（秒），便于观察暂停/取消 |
| `GATEWAY_SIM_FAIL_RATE` | `0.2` | 模拟设备确定性失败概率 |

## 项目结构

```
gateway/
  config.py      # 环境变量配置
  errors.py      # 错误分类（错误码 + HTTP 状态码）
  models.py      # 请求/响应模型与状态枚举
  store.py       # JSON 原子持久化 + 审计日志
  devices.py     # 设备注册/启停/注销与凭据生命周期
  telemetry.py   # 配额、去重与确定性影子合并
  simulator.py   # 本地模拟设备（确定性失败、幂等安装）
  firmware.py    # 分批升级编排、暂停/恢复/取消、重启恢复
  routers/       # HTTP 路由
  app.py         # 服务装配与异常映射
scripts/demo.py  # 本地模拟流量演示
tests/           # pytest 套件（25 个用例，日志打印关键判定依据）
```
