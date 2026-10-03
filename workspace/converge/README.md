# 多站点变更汇合服务（de-identified change convergence）

供去标识流水线使用的多站点变更汇合服务。各站点只上报**假名化**字段（对象键、前驱摘要、覆盖区段、变更摘要、站点单调序号、请求键），服务端持久化重建有向版本图，自动挂起/续办缺失前驱、按确定顺序合成互斥区段兄弟分支、对同区段异内容进入双人人工判定，并为每个对象维护**唯一当前决定**和**可校验的哈希回执链**。

服务、控制台、回执中不出现任何原始身份或实际像素——输入层就只接受摘要（64 位十六进制）与合成区段坐标。

## 可验证的历史压缩与冷归档

操作员可按对象创建带校验摘要的**检查点**：把检查点之前的版本图节点与 WAL 日志段复制进**只读、内容寻址、HMAC 签名**的归档段（0444 WORM），再原子旋转热 WAL 回收存储。压缩后当前视图、历史证明与下载验真返回与压缩前**完全相同的业务结果**；后来到达且引用归档祖先的变更，会通过**归档索引**恢复依赖，绝不会被误当成新的根节点。

| 关注点 | 机制 |
| --- | --- |
| 可校验归档 | 归档段 `archive/segments/<id>.seg`：与 WAL 同帧格式（magic `CA`），逐帧 payload sha256 + `content_digest` + HMAC 清单尾帧；内容寻址（同内容同文件，重复任务不产生第二段） |
| 热/冷等价 | 热 WAL 仅保留最新检查点**桩帧**（边界节点/回执 + 信封幂等/站点序号索引）；当前决定、对象视图、回执链、脱敏下载、验真结果压缩前后逐字节一致 |
| 归档祖先晚到 | 新信封引用的前驱若已在冷段，先经归档索引**校验并恢复**祖先闭包（落一条 `T_RESTORED` 帧）再受理；归档被篡改则以确定错误码拒绝写入，绝不挂起成新根 |
| 并发隔离 | 一切变更在进程互斥锁内串行；压缩与写入/判定/下载并发时，调用方只可能看到压缩前或压缩后的**完整**版本，热 WAL 旋转为临时文件 + `rename(2)` 原子替换 |
| 崩溃续办/回滚 | `archive/tasks/<id>.json` 标记三阶段 `PLAN → VERIFIED → TRIMMED`：段已落盘而桩未落盘→重启**完整回滚**删段；桩已落盘→重启**续办裁剪**；裁剪只删除“本对象检查点链已验证覆盖”的帧，重复执行/重复请求**绝不多删** |
| 保留策略 | `CONVERGE_RETAIN_DECISIONS=n`：压缩后热存储必须保留至少 n 个决定，否则 `409 CHECKPOINT_BLOCKED/retention_policy` |
| 篡改稳定态 | 段截断、帧/索引/内容摘要不符、清单签名失效均返回确定错误码（`ARCHIVE_TRUNCATED/TAMPERED/INDEX_MISMATCH/MISSING/PERMISSION`），服务照常启动并在状态接口列出阻塞，不静默丢历史 |
| 角色边界 | 压缩与恢复仅 operator；reviewer 只读状态；inspector 仅查询/下载**脱敏**归档证明（不含站点/请求键/序号），不能发起压缩或恢复 |

```bash
npm run verify:archive   # 96 项：正常压缩/晚到祖先/并发隔离/重复任务/崩溃恢复/保留策略/篡改/越权
```

零运行时依赖，仅需 Node.js ≥ 18。

## 运行

```bash
npm start                 # 默认 http://127.0.0.1:8080，数据目录 ./data
CONVERGE_PORT=9000 CONVERGE_DATA_DIR=/var/lib/converge \
CONVERGE_RECEIPT_SECRET=$(openssl rand -hex 32) \
CONVERGE_RETAIN_DECISIONS=2 npm start
```

数据目录布局：

```
<CONVERGE_DATA_DIR>/
  converge.wal                热 WAL（压缩后原子旋转、回收空间）
  converge.wal.replace        仅崩溃瞬间残留（重启自动清理）
  archive/
    segments/<segment-id>.seg 只读内容寻址归档段（0444，含 HMAC 清单尾帧）
    tasks/<task-id>.json       进行中压缩任务的阶段标记（续办/回滚依据）
```

- `CONVERGE_RETAIN_DECISIONS`（默认 0）：压缩后每个对象热存储至少保留的决定数；更早的检查点被拒绝。
- `CONVERGE_CRASH_POINT`（测试用）：`after_segment` / `after_stub` 注入崩溃点，正常运行不设置。

打开 `http://127.0.0.1:8080/` 即网页控制台。

## 一键验证全部要求场景

```bash
npm run verify          # 既有 62 项汇合/判定/崩溃/RBAC/篡改场景
npm run verify:archive  # 归档 96 项（见上“可验证的历史压缩与冷归档”）
npm run verify:subscriptions  # 事件订阅 97 项（见下“可靠事件订阅”）
```

`verify` 启动真实 HTTP 服务（临时端口 + 临时数据目录），覆盖：

1. 正常合成（互斥区段兄弟并发到达 → 单个 `merge` 决定，区段按字典序确定排列）
2. 前驱晚到（子信封先挂起并给出缺失摘要；祖先补齐后自动续办成决定）
3. 请求键：同键同正文返回原结果（`replayed`）；同键异正文 `409 REQUEST_KEY_BODY_CONFLICT`；站点同序号异正文 `409 SITE_SEQ_CONFLICT`
4. 同区段异内容 → 冲突案；两名**不同**复核员依次处理，二审分歧时原子拒绝（不落盘、案件仍开放），二审一致才出 `adjudicate` 决定；判定期间到达的新分支自动并入
5. 8+ 路并发上报（含完全重复）收敛为**恰好一个**决定
6. 越权：监察员不能上报/看图谱，操作员不能判定，缺令牌/坏令牌被拒；监察员只能查询与下载脱敏收据
7. 收据被改内容 → `RECEIPT_HASH_MISMATCH`；改签名 → `RECEIPT_SIG_INVALID`；伪造哈希 → 确定错误
8. 崩溃续办：WAL 尾帧残缺（落盘中途被 kill）自动截断续跑；在“二审已 fsync、决定尚未落盘”窗口崩溃，重启后补出**唯一**决定；再次重启不重复
9. WAL 中段被篡改 → 服务**拒绝启动**（不静默跳过已提交事件）

## 角色与令牌

| 角色 | 默认令牌 | 权限 |
| --- | --- | --- |
| operator | `operator-token` | 上报信封、查看对象/版本图/挂起原因、查看案件与回执 |
| reviewer-01 | `reviewer1-token` | 查看对象/案件、提交第一复核 |
| reviewer-02 | `reviewer2-token` | 查看对象/案件、提交第二复核（须与一审人不同） |
| inspector | `inspector-token` | 仅查询/下载脱敏收据、校验收据；看不到站点、请求键、序号；只读脱敏通知证明 |
| admin | `admin-token` | 登记/暂停/恢复下游订阅、更换签名密钥、死信再次发送；只读查看状态与图谱 |

生产环境通过 `OPERATOR_TOKEN` / `REVIEWER1_TOKEN` / `REVIEWER2_TOKEN` / `INSPECTOR_TOKEN` / `ADMIN_TOKEN` 覆盖。

## HTTP 接口

所有 `/api/*` 需 `Authorization: Bearer <token>`。

| 方法与路径 | 角色 | 说明 |
| --- | --- | --- |
| `POST /api/envelopes` | operator | 上报信封；返回 `suspended` / `settled` / `conflict_open` |
| `POST /api/envelopes/digest` | operator | 仅预览将生成的节点摘要（不落盘） |
| `GET /api/objects` | operator/reviewer | 对象列表（状态、tips、挂起摘要） |
| `GET /api/objects/:key` | operator/reviewer | 完整节点、边、缺失前驱、当前决定、开放案件 |
| `GET /api/cases` · `GET /api/cases/:id` | operator/reviewer | 待判定冲突与候选摘要 |
| `POST /api/reviews/:id` | reviewer | 提交 `{choices:{region:change_digest}}`；一审返回等待二审，二审一致出决定，分歧 `409 REVIEW_MISMATCH` |
| `GET /api/receipts?object_key=` | 全部 | 脱敏回执列表（含已归档冷历史） |
| `GET /api/receipts/:hash` | 全部 | 单条脱敏回执（冷段自动校验读取） |
| `GET /api/objects/:key/receipts/download` | 全部 | 下载该对象脱敏回执链 JSON（监察员可用） |
| `POST /api/receipts/verify` | 全部 | 校验（可能被外部改过的）回执，返回确定错误码 |
| `POST /api/archive/checkpoints` | operator | 按对象创建检查点并回收热存储（`{object_key, boundary_seq?, task_key?}`） |
| `POST /api/archive/restore` | operator | 手动把某对象归档闭包恢复进热存储（晚到祖先通常自动恢复） |
| `GET /api/archive/status` | operator/reviewer | 热/冷规模、检查点位置、归档段、恢复进度与阻塞原因 |
| `GET /api/archive/proofs` | inspector/operator | 脱敏归档证明列表（含实时验真结果） |
| `GET /api/archive/segments/:id/pack` | inspector/operator | 下载脱敏证明包（交付前强制验真） |
| `GET /api/archive/segments/:id/raw` | operator | 下载原始段（含站点/请求键，operator 专属） |
| `POST /api/subscriptions` | admin | 登记下游回调（地址 + 关注事件 + 签名密钥） |
| `GET /api/subscriptions` | admin/operator | 订阅状态：待发送/在途/死信数量、最近确认游标、死信原因 |
| `GET /api/subscriptions/:id` | admin/operator | 单订阅完整状态与队首/死信条目 |
| `GET /api/subscriptions/:id/items?state=&limit=` | admin | 投递条目明细（pending/sending/acked/dead） |
| `POST /api/subscriptions/:id/pause` · `/resume` | admin | 暂停积压 / 恢复投递（游标不动） |
| `POST /api/subscriptions/:id/rotate-key` | admin | 更换签名密钥（新版本；在途批次继续用旧版本） |
| `POST /api/subscriptions/:id/redeliver` | admin | 死信人工再次发送（原事件标识与业务顺序不变） |
| `POST /api/subscriptions/pump` | admin | 立即驱动一轮投递周期（默认另有后台泵） |
| `GET /api/subscriptions/proofs` | inspector | 脱敏订阅证明列表（无回调地址/密钥） |
| `GET /api/subscriptions/:id/proofs` | inspector | 已确认通知的脱敏证明（稳定标识、业务游标、签名信封） |
| `GET /api/whoami` | 全部（已认证） | 返回当前令牌角色（工作台据此渲染） |

## 可靠事件订阅（下游合规系统）

管理员登记回调地址、选择关注的状态变化并设置签名密钥。对象产生**新决定**、**冲突案闭合**或**冷段生成**时，业务提交与事件入箱是**同一个持久化结果**：业务帧（决定/检查点桩）与发件箱事件帧在一次 `write+fsync` 的批次里落盘，崩溃不可能出现“业务已生效却永远没有通知”的空档。

每条通知正文（`converge-event/v1`）含：稳定 `event_id`、全局 `event_seq`、对象替代键 `object_key`、`kind`、`business_cursor`（回执序号/回执哈希/决定节点或段 id）、事件时间、密钥版本与 HMAC 签名头 `x-converge-signature`。**绝不含站点标识、请求键、站点序号或任何图像内容**。

| 关注点 | 机制 |
| --- | --- |
| 原子入箱 | 业务帧与事件帧一次 fsync 批次；事件帧（WAL 类型 6）是全局事件日志，热/冷压缩从不裁剪 |
| 启动补缝 | 启动时按业务帧重放对账：业务帧在而事件帧缺失则确定性补齐（同一稳定事件标识），并为订阅重建投递项 |
| 每订阅游标 | 订阅各自维护投递条目与确认游标；游标只沿连续已确认前缀推进，**不倒退、不跳过未确认事件** |
| 退避重试 | 超时与非 2xx 一律按 `base·2^(n-1)`（封顶）退避重试；确认过的不再主动发送；接收方以稳定 `event_id` 去重 |
| 租约与崩溃 | 批次绑定租约；进程死亡或确认丢失后租约到期回收，同一事件用同一标识重复投递；重启后积压存活、游标不变 |
| 死信 | 重试达上限进入死信并**堵住院首**（其后事件不跳过）；人工再次发送沿用原 `event_id` 与原业务顺序，开新批次 |
| 密钥版本边界 | 密钥只增版本；**已出箱批次创建时绑定的版本不变**，新批次使用当前版本；旧密钥保留供在途批次验签 |
| 暂停/恢复 | 暂停期间只积压不投递；恢复后按业务顺序排空 |
| 角色边界 | admin 管理订阅与再次发送；operator 只读状态；inspector 只能读脱敏通知证明；reviewer 不可见 |
| 故障隔离 | 投递泵在业务互斥锁之外运行，回调缓慢/失败/死信**只延迟通知，不阻塞汇合请求、判定或历史查询** |

关注事件种类：`decision.created`（含 accept/extend/merge/adjudicate 全部新决定）、`case.closed`（双人判定一致闭合冲突）、`cold_segment.generated`（检查点冷段落盘）。判定闭合在同一持久化批次内同时产生 `decision.created` 与 `case.closed` 两个事件。

投递参数可由环境变量调整：`CONVERGE_PUMP_INTERVAL_MS`（默认 500；设 0 关闭后台定时器、仅由 `/pump` 驱动，便于测试）、`CONVERGE_DELIVERY_TIMEOUT_MS`（4000）、`CONVERGE_BACKOFF_BASE_MS`（200）、`CONVERGE_BACKOFF_MAX_MS`（30000）、`CONVERGE_DELIVERY_MAX_ATTEMPTS`（5）、`CONVERGE_DELIVERY_BATCH_SIZE`（4）、`CONVERGE_DELIVERY_LEASE_MS`（60000）。管理令牌默认 `admin-token`（可用 `ADMIN_TOKEN` 覆盖）。

```bash
npm run verify:subscriptions   # 97 项：成功回调/超时恢复/确认丢失重复/密钥边界/
                               #   暂停积压/死信再发/进程恢复/启动补缝/角色/故障隔离/案件与冷段事件
```


### 信封正文

```json
{
  "object_key": "obj-1001",
  "site": "station-alpha",
  "site_seq": 3,
  "coverage_region": "tiles:012:007",
  "change_digest": "<64 hex：变更内容摘要，服务从不接触原始像素>",
  "predecessor_digests": ["<64 hex>", "..."],
  "request_key": "req-7f3a...（同键同正文幂等，同键异正文拒绝）"
}
```

### 检查点正文

```json
{
  "object_key": "obj-1001",
  "boundary_seq": 12,            // 可空：默认压缩到当前最新决定
  "task_key": "cp-7f3a...（可选，同任务键幂等；同键异对象 409）"
}
```

- 开放冲突、存在挂起信封、目标决定未到达或违反保留策略时返回 `409 CHECKPOINT_BLOCKED`，`details.reasons` 给出稳定原因。
- 重复请求（无更新决定）与相同 `task_key` 返回同一 `segment_id` 且 `idempotent_replay=true`，不多删任何数据。
- 创建动作本身不落业务审计，但归档段清单含创建时间、前一段链锚与逐帧摘要，可离线逐级验真。

## 关键机制

- **WAL**（`src/wal.mjs`）：`magic|type|length|payload|crc32` 分帧，每条 `fsync`。尾帧残缺/CRC 错（崩溃窗口）截断；中段损坏拒绝启动。`replaceFrames` 以临时文件 + 目录 fsync + 原子 `rename` 完成热存储旋转，`.replace` 残留重启自动丢弃。帧类型：`1 信封 / 2 决定 / 3 复核 / 4 检查点桩 / 9 归档清单 / 5 恢复包装`。
- **冷归档**（`src/archive.mjs`）：段内容寻址、0444 WORM、逐帧摘要 + 内容摘要 + HMAC 清单尾帧；读取即七层验真（帧 CRC → 清单签名 → 条目数/类型/序 → 逐帧 payload 摘要 → 内容摘要 → 段 id）。任务标记驱动 `PLAN/VERIFIED/TRIMMED` 续办或回滚。
- **版本图**：信封节点按前驱摘要连边；前驱缺失则信封挂起，祖先补齐后级联激活并自动续办。
- **确定合成**：tip 按 `(站点, 站点序号, 摘要)` 全序排序；静默窗口做 debounce（代际号使陈旧定时器自失效），并发/乱序只产生一次合成。互斥区段取并集；重复兄弟与已被既有决定包含的原始变更做“去重 + 包含”消除；同区段出现不同摘要则进入人工判定。
- **双人判定**：一审落盘等待二审；二审必须是另一名复核员。分歧不写任何记录、原子失败；一致才出 `adjudicate` 决定。被否决方作为决定节点的前驱保留在图中（可审计），但其摘要不进入合成内容。
- **回执链**：每对象每决定一条，字段含 `seq / decision_node / parents / contributions / prev_receipt / hash / sig`。`hash = sha256(canonical(fields))`，`sig = HMAC-SHA256(secret, canonical(fields))`，环环相扣。对监察员输出时剔除站点、请求键、站点序号。
- **恢复确定性**：重启只重放 WAL；合成是纯函数，已存在的同内容决定按内容去重，不产生第二张收据；二审与决定之间的崩溃窗口在启动时补完一次。
