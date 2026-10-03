# diffdiag · 面向 converge 的因果差异诊断器

接收两组**已脱敏的 converge 决定收据**，把每组收据按前驱关系编成可比较的**因果骨架**，
异步计算并输出：**第一处分歧**、**受影响的下游节点**、以及**不含站点、请求键和图像内容
的解释证据**（逐条哈希链），并为整份诊断出具可验证的**证明**。

零运行时依赖，Node.js ≥ 18。收据格式与 converge 的脱敏回执完全一致（`GET
/api/objects/:key/receipts/download` 的输出可直接作为本服务的入库包正文）。

```bash
npm start                 # 默认 http://127.0.0.1:8090，数据目录 ./data
npm run verify            # 122 项端到端场景验证
npm run verify:integration  # 与真实 converge 联跑：收据下载→入库→比较→验真
DIFFDIAG_PORT=9000 DIFFDIAG_DATA_DIR=/var/lib/diffdiag \
DIFFDIAG_RECEIPT_SECRET=<与 converge 相同的收据密钥> npm start
```

## 安全不变量

| 关注点 | 机制 |
| --- | --- |
| 坏包进入诊断 | 入库包**两阶段提交**：先全量校验（摘要格式 → 内容哈希 → 链签名 → 序号唯一/连续 → 链锚 → 同节点异正文 → 拓扑），任何一项失败**整包拒绝**，存储逐字节不变。拓扑检查区分两类前驱：指向本对象更早**决定节点**的内部边（必须同对象、严格先于子节点；成环 `TOPOLOGY_CYCLE`、跨对象 `TOPOLOGY_PARENT_CROSS_OBJECT` 即拒）与指向变更/信封节点的**外部引用**（内容摘要、无收据，作为外部节点并入因果骨架） |
| 非脱敏内容混入 | 收据字段白名单（`seq/object_key/decision_kind/decision_node/parents/contributions/prev_receipt/emitted_at/hash/sig/case_id/reviewers`），出现 `site`、`request_key` 等任何白名单外字段即拒；证据与证明只由摘要、序号、对象键、合成区段坐标构成 |
| 任务键幂等 | `task_key` 唯一；同键同输入返回**原任务、原分歧位置、原证明**（`idempotent_replay=true`）；同键任一输入变化 `409 TASK_INPUT_CONFLICT`；并发提交同一比较在互斥锁内串行，**只留下一个任务** |
| 水位之后混入 | 创建任务时在单个临界区内钉住**两侧读取水位**（组内到达序号高水位）并**冻结输入哈希集**；水位之后到达的收据到达序号更高且不在冻结集内，绝不混入 |
| 中断恢复 | 计算分 `SKELETON_LEFT→SKELETON_RIGHT→DIFF→EXPLAIN→DONE` 五个**持久化阶段**，每阶段原子落盘；崩溃后重启：有持久阶段则从该阶段继续，无任何阶段则回到未开始；解释节点逐条原子落盘、按序号幂等跳过，**绝不重复写** |
| 算法切换 | 任务创建时钉住当前算法版本；切换只影响之后创建的任务，进行中的任务继续用旧版本；所有已注册版本永久可用于证明验真 |
| 取消竞态 | 取消是**条件状态推进**（仅 `queued→cancelled`），与 worker 领取互斥，恰有一方成功；取消释放冻结输入与阶段产物；`done` 诊断永不取消、永不覆盖 |
| 证明被改 | 证明 = 确定性载荷 + 摘要 + HMAC 签名；验真时重算解释链（逐条摘要 + 链锚）、输入快照摘要、证明摘要与签名四层，任一层被改返回确定错误码（`EXPLANATION_CHAIN_BROKEN` / `PROOF_MISMATCH` / `PROOF_SIG_INVALID`） |
| 错误回显 | 错误响应只含稳定错误码、静态文案与字段名/序号，**绝不回显输入包内容**；控制台只用 `textContent` 渲染服务端字符串 |

## 角色与令牌

| 角色 | 默认令牌 | 权限 |
| --- | --- | --- |
| operator（运维者） | `operator-token` | 收据包入库、登记/切换算法版本、取消未开始任务、驱动 worker、读任务/对象/证明 |
| analyst（分析者） | `analyst-token` | 选择两组对象创建比较、查看差异树与脱敏证据、读任务/对象/版本/证明 |
| observer（只读观察者） | `observer-token` | **只能**读证明摘要与验证证明 |

生产环境通过 `OPERATOR_TOKEN` / `ANALYST_TOKEN` / `OBSERVER_TOKEN` 覆盖。

## HTTP 接口

所有 `/api/*` 需 `Authorization: Bearer <token>`；所有响应 `Cache-Control: no-store`。

| 方法与路径 | 角色 | 说明 |
| --- | --- | --- |
| `POST /api/packages` | operator | 收据包入库 `{group, object_key, receipts[]}`；整包校验，返回 `{accepted, duplicates, watermark}` |
| `GET /api/objects` | operator/analyst | 各组对象链与当前水位 |
| `GET /api/algo-versions` | operator/analyst | 算法版本列表（active/available） |
| `POST /api/algo-versions` | operator | 登记版本（须为实现内建版本） |
| `POST /api/algo-versions/activate` | operator | 切换当前版本（只影响之后创建的任务） |
| `POST /api/comparisons` | analyst | 创建比较 `{task_key, left_group, right_group}`；钉住水位与算法版本 |
| `GET /api/comparisons` · `GET /api/comparisons/:key` | operator/analyst | 任务阶段、水位、算法版本、证明摘要（刷新即重读） |
| `GET /api/comparisons/:key/diagnostic` | analyst | 差异树（两侧骨架）、第一处分歧、受影响下游节点、解释证据链 |
| `POST /api/comparisons/:key/cancel` | operator | 取消未开始任务并释放资源；已开始/已完成 409 |
| `POST /api/worker/run` | operator | 驱动 worker 处理队列（`DIFFDIAG_WORKER_INTERVAL_MS>0` 时另有后台周期） |
| `GET /api/proofs` · `GET /api/proofs/:key` | 全部 | 证明摘要（含实时验真结果） |
| `POST /api/proofs/verify` | 全部 | 验证证明 `{task_key}` 或 `{proof:{task_key,digest}}` |
| `GET /api/whoami` | 全部 | 当前令牌角色（控制台据此渲染三种可见范围） |

打开 `http://127.0.0.1:8090/` 即网页控制台：运维者管理算法版本与取消任务，分析者
选择两组对象查看差异树与脱敏证据，观察者只能验证证明。页面不保存任何业务状态，
刷新/轮询/切回标签页都会重新读取任务阶段、比较水位和证明摘要。

## 比较算法

- `chain-walk-v1`（默认）：按序号对齐每对象回执链，第一个不一致序号为分歧；下游 = 该序号之后的链上节点。
- `skeleton-align-v2`：按拓扑序对齐因果骨架节点摘要，第一个单侧节点为分歧；下游 = 骨架中该节点的传递后继闭包。

两版算法都输出确定性结果；分歧、下游与证据计划只含摘要/序号/对象键/合成区段坐标。

## 证明结构

```
proof = { schema, task_key, algo_version,
          left:  {group, watermark, input_digest},
          right: {group, watermark, input_digest},
          divergence, affected, explain_head,
          digest: sha256(canonical(上述字段)),
          sig:    HMAC-SHA256(receipt_secret, 上述字段) }
```

验真四层：解释节点链自洽（逐条重算摘要 + 链锚）→ 链头一致 → 由库存诊断重算证明摘要
→ 重算签名。观察者只能调用验真接口，看不到差异树与证据。

## 运行参数

- `DIFFDIAG_WORKER_INTERVAL_MS`（默认 0）：>0 时后台周期驱动 worker；为 0 时仅由
  `POST /api/worker/run` 驱动（测试与运维手动）。
- `DIFFDIAG_CRASH_POINT`（仅测试）：`after_claim` / `after_stage:<STAGE>` /
  `during_explain:<n>`，在对应持久化点之后 `process.exit`，用于崩溃恢复验证。
- 状态文件 `<data>/diffdiag-state.json`：原子写入（tmp + fsync + rename + 目录 fsync）；
  启动时发现 `.tmp` 残留自动丢弃，状态文件损坏**拒绝启动**（不静默丢历史）。

## 验证场景（`npm run verify`，122 项）

合法比较与同键复用/冲突、坏包整包拒绝（环路、重复序号、同节点异正文、跨对象前驱、
链断裂、摘要格式、链签名、内容哈希、非脱敏字段；外部变更节点引用合法入库）、
8 路并发唯一任务、水位边界、算法切换、三种崩溃点恢复（EXPLAIN 中途 / 领取后 /
DIFF 后）、取消竞态五轮、证明四类篡改、角色越权矩阵、错误页面不回显与证据脱敏深扫描。

`npm run verify:integration`（10 项）启动真实 converge 与 diffdiag：converge 合成决定
→ 监察员端点下载两批回执（不同水位）→ 直接入库 → 分歧恰在新到达批次处；同组对比一致；
传输中篡改收据/签名整包拒绝。
