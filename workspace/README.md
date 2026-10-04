# 医学影像去标识化与受控回链平台

影像入库时**冻结原始 DICOM 摘要**，按项目策略**清理标签、遮盖像素烧录文字、替换为稳定替代身份**；
每个派生版本都保留**完整处理步骤链与质量检查**。策略升级只能产生**新派生版本，永不覆盖旧版**。
临床纠错通过**双角色授权**短暂回链到原身份——授权绑定病例、目的、时间窗，**导出后立即失效**。

诊断作业按**分析团队多租户**隔离：每个团队有独立的**并发额度、等待上限与权重**，
受理时**冻结所属团队与预计成本**，调度器用**确定的平滑加权轮转**领取作业——
某团队持续提交也不能长期挤占其他团队；**用量按团队+结算周期入账且永不双重计费**。

## 安全不变量

| 威胁场景 | 保障机制 |
|---|---|
| 原始身份落盘泄露 | 原始 DICOM 与冻结摘要均以**信封加密 Blob** 存储（DEK + KEK 双层），普通接口不返回原身份 |
| 半成品发布 | 像素遮盖/标签清理任一失败，版本只能停留在 `processing/failed/quarantined`；`latest_version_id` 只指向**人工放行**版本 |
| 策略升级覆盖旧版 | `(study_id, version_no)` 唯一约束 + WORM 内容寻址存储（文件 `0444`）；升级永远新建版本 |
| 同影像并发修订 | 病例级互斥锁 + 锁内单调分配版本号，成功者版本号互不相同、不缺号 |
| 批量重试重复执行 | 任务租约（lease）单事务领取；崩溃租约到期才可被接管；指数退避 + 死信人工 requeue |
| 遮盖失败漏检 | 自动质检门（PHI 标签 / 私有标签 / 像素烧录文字残留）+ 人工放行双门 |
| 回链越权 | 双角色 XOR 分片（临床医生持 A、隐私官持 B），分片指纹校验；授权绑定 study+purpose+时间窗 |
| 导出/撤销竞态 | 状态推进全部为**条件 UPDATE**（`WHERE status=...`，以 rowcount 定胜负），最多一方成功 |
| 导出后再次访问 | 导出提交成功即**抹除内存分片**；状态 `exported`，二次导出 409；会话密钥不可重组 |
| 审计抵赖/篡改 | 审计日志 HMAC 哈希链（每条含前一条 MAC），导出时附链完整性校验结果 |
| 审计结论事后抵赖 | **验真快照**：admin 按筛选与当前审计序号在服务端确定截止点冻结不可变快照（WORM 验真包 + 清单逐级摘要 + 链锚）；幂等键保证刷新/重复点击/并发只返回同一快照；officer 仅查询复核，创建/下载为 admin 专属 |
| 单团队突发挤占他人 | 诊断调度器采用**持久化信用的确定性平滑加权轮转**（权重正比于长期份额、领取交错均匀）；某团队连续提交也只能拿到其权重份额 |
| 重复提交/令牌冲突 | 受理在单事务内冻结团队/成本/正文指纹；**令牌+正文完全相同→指向原作业**（幂等）；**令牌相同而正文或成本变化→409 拒绝且不落记录** |
| 等待上限产生孤儿 | 等待数达 `wait_cap` 或单作业成本超额度→受理 429 拒绝（无记录）；超 `wait_deadline` 的等待作业显式终结为 `wait_expired` 并释放占用（可审计，非孤儿） |
| 降额抢占运行槽 | 额度/权重调整**只作用于之后的领取**；已占用执行槽不强制转移。暂停团队**保留队列位置与轮转信用**，恢复后继续参与 |
| 执行进程退出 | 租约到期且未封存的 running 作业**回到 waiting 保留排队位置**被再领取；已封存（done+sealed）结果**不能再次计算** |
| 取消/领取竞态 | 取消等待作业条件 UPDATE（`WHERE status='waiting'`）立即返还预留；与领取并发时只有一方推进 |
| 重复/重试双重计费 | 状态条件推进与入账同一事务；账本主键 `(period, team_id, job_id)`；重复完成通知幂等返回 `already_billed`，失败重试从不在成功前计费 |

## 版本/任务状态机

```
版本: processing ──自动QC通过──▶ ready ──人工放行──▶ published
                └─QC失败/死信─▶ failed / quarantined  （绝不更新 latest 指针）
任务: queued ──claim──▶ running ──成功──▶ done
                            ├─失败(可重试)─▶ retry(指数退避) ─▶ running
                            └─失败(达上限)─▶ dead ──人工requeue──▶ queued
回链: requested ──step1(clinician)+step2(officer)──▶ armed ──导出──▶ exported(终态)
                └────────────撤销/过期──────────────▶ revoked / expired(终态)
诊断: waiting ──claim(加权轮转)──▶ running ──成功封存+入账──▶ done(终态,不重算)
                 ▲                    ├─失败(未达上限)─▶ waiting(退避重试,不计费)
                 │                    └─失败(达上限)───▶ failed(终态)
                 ├─取消(立即返还预留)─▶ cancelled(终态)
                 ├─超等待上限────────▶ wait_expired(终态)
                 └─租约到期(进程退出,未封存)──┘
```

## 目录结构

```
app/
  config.py            配置（MEDDEID_HOME 指定数据目录）
  core/
    crypto.py          信封加密、双角色分片、PBKDF2、HMAC 链
    dicomutil.py       合成DICOM、冻结摘要、标签策略、像素遮盖、质检
    store.py           SQLite + 内容寻址(WORM)存储、审计链
  services/
    policies.py        策略注册（只增版本）
    locks.py           病例互斥锁
    ingest.py          入库冻结、稳定伪名、幂等
    versions.py        派生版本、自动/人工质检、发布
    queue.py           租约队列、重试退避、死信、worker
    vault.py           回链分片内存保险库（TTL）
    reidentify.py      双角色授权、纠错、导出、撤销、过期清扫
    audit.py           审计查询与 JSON/CSV 导出
    snapshots.py       验真快照：截止点冻结、幂等、WORM 验真包、逐级验真
    diagnostics.py     诊断多租户：团队额度/权重/暂停、平滑加权轮转、租约、幂等计费账本
  web/
    auth.py            Basic 认证与四角色控制
    api.py             REST API + 工作台静态资源（/ 与 /ui/*）
    static/            角色化网页工作台（index.html/app.js/styles.css，零新增依赖）
scripts/
  init_db.py           初始化+播种默认策略
  worker.py            队列 worker 常驻进程
tests/                 35 个端到端/并发/竞态/工作台测试
```

## 快速开始

```bash
pip install -r requirements.txt
python3 scripts/init_db.py
# 终端1：worker
MEDDEID_HOME=./data python3 scripts/worker.py
# 终端2：API
MEDDEID_HOME=./data uvicorn app.web.api:app --port 8088
```

启动后浏览器打开 `http://127.0.0.1:8088/` 即进入**角色化网页工作台**（纯静态 SPA，无额外依赖）。
页面本身不含任何业务数据；登录后所有状态都通过下列既有 API 从服务端恢复。

### 工作台安全约定

| 关注点 | 实现 |
|---|---|
| 独立登录会话 | 凭据仅存当前标签页 `sessionStorage`（不写 cookie/localStorage）。临床医生与隐私官必须在**各自独立的浏览器会话/标签页**依次授权；退出或 401 立即清空凭据与全部 DOM |
| 状态恢复 | 刷新、切回标签页、5 秒轮询都重新请求服务端；前端不推导、不缓存任何“成功状态” |
| 一次性材料 | 分片 A 只在发起响应中出现、纠错包只在导出响应中出现；弹层关闭即从 DOM 清除，二次获取由后端 409 决定 |
| 重复点击/并发 | 按钮按 `对象+动作` 去重并在请求飞行时禁用；结果一律以后续状态机条件 UPDATE 的返回为准 |
| 超时/撤销/越权 | 仅展示服务端返回的确定状态（requested/armed/exported/revoked/expired 与 401/403/409 文案） |
| 无缓存 | 所有页面与接口响应带 `Cache-Control: no-store` |

各角色视图：

- **技师**：处理队列（执行任务、死信人工重试）、人工质检（去标识 PNG 预览、完整步骤链、放行/否决）、病例与版本（入库、按新策略派生、查看不可变版本链）。
- **临床医生**：发起回链并安全保存一次性分片 A → 在本会话粘贴提交步骤 1 → 查看后端真实授权进度；仅在 `armed` 时出现登记纠错与导出入口；导出成功后纠错包只显示一次。
- **隐私官**：在独立会话中查看待批准队列（仅此处返回分片 B，页面不渲染分片），一键提交步骤 2；监督全部回链、按状态筛选、撤销、清扫过期。
- **管理员**：审计多条件筛选、哈希链重新校验、JSON/CSV 导出并经受 admin 保护的 blob 接口下载；**冻结验真快照**（按当前筛选与审计序号取不可变截止点，查看范围/链锚/记录数/现场完整性并下载含清单与记录的验真包）；另可只读查看队列/质检、驱动 worker。
- **隐私官**：可在审计页**查询与复核验真快照**（范围、链锚、记录数、完整性结果），但不能创建快照或下载验真包（后端 403 强制）。
- **诊断运维（operator）**：查看团队队列、额度占用、轮转顺序与 WRR 信用、用量明细；
  调整权重/并发额度/等待上限、暂停/恢复团队（保留队列位置）、驱动调度器与清扫。
- **分析者（analyst，alpha/beta/gamma）**：只看本团队本人作业，提交（冻结团队/成本）
  与取消自己的等待作业；令牌+正文幂等、同令牌内容变化由后端 409 拒绝。
- **观察者（observer）**：仅核验按周期聚合的**脱敏用量摘要**（稳定别名+数字，无身份/作业标识）。

> 工作台只**新增**了三个只读恢复接口：`GET /api/me`、`GET /api/relink/mine`、
> `GET /api/relink/{id}/status`，以及 `/` 与 `/ui/{app.js,styles.css}` 静态资源；
> 既有接口签名、状态机与权限判定均未改动。

演示账户（Basic Auth，生产环境请对接 IdP/MFA）：

| 用户 | 密码 | 角色 |
|---|---|---|
| tech | tech-pw-123 | technician：入库/队列/质检 |
| clinician | clinician-pw-123 | clinician：发起回链/纠错/导出 |
| officer | officer-pw-123 | officer：第二授权/撤销 |
| admin | admin-pw-123 | admin：策略/审计导出 |
| operator | operator-pw-123 | operator：诊断团队队列/权重/暂停/驱动调度器/用量明细 |
| observer | observer-pw-123 | observer：仅核验脱敏用量摘要 |
| alpha / beta / gamma | `<user>-pw-123` | analyst：查看本团队本人作业，提交/取消自己的诊断作业 |

诊断团队默认配额（可由运维调整）：team-alpha 额度 2/权重 1、team-beta 额度 1/权重 2、
team-gamma 额度 3/权重 1；等待上限默认 100，等待截止 `DIAG_WAIT_DEADLINE_SECONDS`（默认 1 小时）。

## API 速览

```
POST /api/studies/ingest-synthetic     入库合成影像（fault_inject: redaction/qc_leak）
POST /api/studies/ingest               上传真实 DICOM
POST /api/versions/derive              按（新）策略新建派生版本
POST /api/queue/run?cycles=&ignore_backoff=   驱动 worker
POST /api/queue/requeue                死信任务人工重试（可清空故障注入）
GET  /api/qc/queue?status=passed       质检队列
GET  /api/versions/{id}/preview        派生影像 PNG 预览（质检界面）
POST /api/qc/review                    人工放行/否决
POST /api/policies                     登记新版本策略（旧版本保留）

POST /api/relink/request               临床医生发起（响应中一次性交付分片A）
POST /api/relink/{id}/share-a          临床医生提交分片A
GET  /api/relink/officer/pending       隐私官待批准队列（取分片B）
POST /api/relink/{id}/share-b          隐私官第二授权 → armed
POST /api/relink/{id}/correction       窗口内登记纠错
POST /api/relink/{id}/export           导出回链包（成功即失效，仅请求人）
POST /api/relink/{id}/revoke           隐私官/管理员撤销
POST /api/relink/sweep                 过期清扫
GET  /api/relink/mine                  临床医生工作台：本人回链真实状态恢复
GET  /api/relink/{id}/status           单条回链状态+双角色步骤（角色隔离，无身份内容）
GET  /api/me                           当前会话真实身份与角色（工作台渲染依据）

GET  /api/audit                        审计查询（officer/admin）
GET  /api/audit/verify                 HMAC 链完整性校验
POST /api/audit/export?fmt=json|csv    审计导出（仅 admin）

POST /api/audit/snapshots              冻结验真快照（仅 admin；幂等键+筛选）
GET  /api/audit/snapshots              快照列表+现场验真结果（officer/admin 复核）
GET  /api/audit/snapshots/{id}         单快照范围/链锚/记录数/完整性（officer/admin）
GET  /api/audit/snapshots/{id}/pack    下载验真包 ZIP（仅 admin，交付前强制验真）
```

### 诊断多租户（配额 / 加权轮转 / 用量）

```
POST /api/diag/jobs                    分析者提交（analyst）：冻结团队与预计成本
                                       令牌+正文相同→幂等原作业；同令牌正文变→409；
                                       等待上限/成本超额度→429（不落孤立记录）
GET  /api/diag/jobs                    队列（运维看全部；分析者自动只看本人）
GET  /api/diag/mine                    分析者：本团队本人作业
GET  /api/diag/jobs/{id}               作业详情（越权与不存在同形 404）
GET  /api/diag/jobs/{id}/result        已封存结果（仅本人/admin；重复查看不重算/计费）
POST /api/diag/jobs/{id}/cancel        取消等待作业（analyst，立即返还预留；运行中 409）
GET  /api/diag/teams                   团队队列+额度占用（operator/admin）
POST /api/diag/teams/{id}/quota        调并发额度（只作用于之后领取，不抢占）
POST /api/diag/teams/{id}/weight       调权重
POST /api/diag/teams/{id}/wait-cap     调等待上限
POST /api/diag/teams/{id}/pause        暂停/恢复（{paused:bool}，保留队列位置）
GET  /api/diag/rotation                轮转顺序/WRR信用/下一个被领取团队
POST /api/diag/scheduler/run?cycles=&claim_only=  驱动调度器（operator/admin）
POST /api/diag/sweep                   终结超等待上限的等待作业
GET  /api/diag/usage?period=YYYY-MM    用量明细（operator/admin）
GET  /api/diag/usage/summary           脱敏用量摘要（observer，operator/admin 亦可）
```

- **加权轮转**：平滑 WRR（持久化信用，`diag_scheduler.credits_json`）。
  每次领取给“当前可服务团队”累加权重，取信用最高者并扣除可服务团队权重之和；
  长期份额 ∝ 权重且领取交错，暂停/满载团队不累积信用，恢复后从原位置继续。
- **用量账本**：`diag_team_usage` 主键 `(period, team_id, job_id)`，
  成功封存与入账同事务；重复完成通知、重启恢复、失败重试都不会双重计费。
- **角色隔离**：operator 调权重/暂停并看全量队列与用量明细；analyst 只见本人作业、
  只能提交/取消自己的作业；observer 只能看到按周期聚合的脱敏别名与数字。

### 验真快照语义

- **服务端截止点**：创建在单个 `BEGIN IMMEDIATE` 事务内完成——确定当时链尾为
  `cutoff_seq`、校验全链完整、按截止点取数、冻结落包；事务持锁期间并发写入的
  审计事件被阻塞，绝不混入已冻结结果。截止点之后的新事件只在验真结果中以
  `new_events_after_cutoff` 报告，不改变快照。
- **幂等**：`idempotency_key` 唯一约束；同键重发（刷新/重复点击/并发创建）返回原
  快照并带 `idempotent_replay=true`；同键不同参数返回 409（带回原快照 id）。
- **筛选为空**：仍冻结当时链锚，返回 `record_count=0` 的可核对快照（空库为
  GENESIS 锚）。
- **验真包**（ZIP，`audit-verify-pack/v1`）：`manifest.json`（范围、链锚、逐条
  记录 sha256、records 根摘要、创建时冻结结论）+ `records.jsonl` + `README.txt`；
  包落 WORM 内容寻址存储（0444），快照行另存 manifest 外部锚。验真七层：
  包文件摘要 → 清单外部锚 → 记录逐条摘要 → 冻结 HMAC 链自洽 → 截止点链锚 →
  冻结行与现库一致 → 当前全链完整。
- **确定状态**：原链已被篡改时创建被拒（409 `audit_chain_broken`）；验真包文件
  或内容被改动时详情给断裂层、下载 409 `verify_pack_tampered`，错误文案不含
  身份内容；通用 `/api/blobs` 接口不提供验真包，必须走带交付前验真的专用接口。
- 快照内容与既有 `/api/audit` 可见范围一致（仅审计元数据，无原身份）；
  创建与下载动作本身写入哈希链（`audit.snapshot.create/download`）。

## 端到端示例

```bash
B=http://127.0.0.1:8088
SID=$(curl -s -u tech:tech-pw-123 -XPOST $B/api/studies/ingest-synthetic -d '{}' \
  -H 'Content-Type: application/json' | python3 -c "import sys,json;print(json.load(sys.stdin)['study_id'])")
curl -s -u tech:tech-pw-123 -XPOST "$B/api/queue/run?cycles=3"
VID=$(curl -s -u tech:tech-pw-123 "$B/api/qc/queue?status=passed" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
curl -s -u tech:tech-pw-123 -XPOST $B/api/qc/review \
  -H 'Content-Type: application/json' \
  -d "{\"version_id\":\"$VID\",\"approve\":true,\"note\":\"无残留\"}"

# 回链
R=$(curl -s -u clinician:clinician-pw-123 -XPOST $B/api/relink/request \
  -H 'Content-Type: application/json' \
  -d "{\"study_id\":\"$SID\",\"purpose\":\"病理纠错\",\"window_minutes\":30}")
RID=$(echo $R|python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
SA=$(echo $R|python3 -c "import sys,json;print(json.load(sys.stdin)['share_for_requester'])")
curl -s -u clinician:clinician-pw-123 -XPOST $B/api/relink/$RID/share-a \
  -H 'Content-Type: application/json' -d "{\"share\":\"$SA\"}"
SB=$(curl -s -u officer:officer-pw-123 $B/api/relink/officer/pending \
  | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['share_for_officer'])")
curl -s -u officer:officer-pw-123 -XPOST $B/api/relink/$RID/share-b \
  -H 'Content-Type: application/json' -d "{\"share\":\"$SB\"}"
curl -s -u clinician:clinician-pw-123 -XPOST $B/api/relink/$RID/export
```

## 测试

```bash
python3 -m pytest tests/ -q
# 88 passed —— 覆盖：冻结/幂等、版本不可变、质检隔离、策略升级、
# 并发修订版本号唯一性、批量领取不重复、租约崩溃接管、重试死信、
# 双角色授权、错误分片拒绝、导出即失效、撤销/导出竞态五轮、时间窗过期、
# 网页工作台（服务端状态恢复、独立会话角色隔离、一次性材料、静态资源无数据）、
# 验真快照（截止点冻结、幂等重放/冲突、空筛选/空库、并发不混入、
#   角色边界、原链篡改、验真包文件与内容篡改、既有审计接口不回归），以及
# 诊断多租户（跨团队轮转、单团队突发、权重份额、等待上限截止与清扫、
#   令牌幂等/同令牌内容冲突/跨团队复用、成本超额度拒绝、
#   运行中降额不抢占、暂停恢复保留位置与信用、暂停不累积信用、
#   租约过期再领取、已封存结果不重算、完成后崩溃重取只计费一次、
#   重复完成通知幂等、失败重试不计费、取消/领取竞态、账本主键去重、
#   角色隔离与脱敏用量摘要、HTTP 状态码与工作台静态契约）
```

## 部署注意（演示实现的边界）

- 分片保险库为**进程内内存**实现；多副本/生产部署应替换为带 TTL 的集中式保险库
  （TLS + KMS 包装），分片仍绝不入库。
- Basic 认证为演示用途；生产应替换为 OIDC/SAML + MFA，并对 officer 授权做步强认证。
- 像素遮盖采用顶部注释带亮笔画检测，足以演示质检门；真实场景应接 OCR/厂商
  BurnedInAnnotation 工作流与双盲复核。
- SQLite 单库适合单节点；并发控制语义（条件 UPDATE、租约、病例锁）可直接平移到
  PostgreSQL/MySQL。
