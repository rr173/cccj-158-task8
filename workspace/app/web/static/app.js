/* 影像安全工作台 —— 纯状态恢复型前端。
 *
 * 安全规则（与需求逐条对应）：
 * 1. 所有数据只来自 FastAPI 接口的响应；前端不推导、不缓存“成功状态”。
 *    任何写操作返回后（无论 2xx/4xx），都重新从服务端拉取真实状态再渲染。
 * 2. 登录凭据仅存当前标签页 sessionStorage，不写 localStorage / cookie。
 *    退出、401、角色切换：清空凭据与全部 DOM，回到登录框。
 * 3. 一次性材料（临床分片 A、导出纠错包）只在产生它的那次响应里展示，
 *    不持久化、不写入任何存储，弹层关闭即清空；二次获取由后端 409 决定。
 * 4. 所有操作按钮在请求飞行期间禁用，并按 id+动作去重，杜绝重复点击。
 * 5. 刷新 / 轮询 / 页面重新可见，一律重新调用接口恢复服务端状态。
 * 6. 401/403/超时/撤销/并发落败只展示服务端给出的确定性结果文本。
 */
"use strict";

// ---------- 全局状态 ----------
const S = {
  auth: sessionStorage.getItem("meddeid_auth"), // 仅 "Basic xxx"
  me: null,
  tab: null,
  pollTimer: null,
  modalOpen: false,
  inflight: new Set(),
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function esc(v) {
  return String(v == null ? "" : v)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d) ? esc(iso) : d.toLocaleString();
}
function remain(windowEndIso) {
  const ms = new Date(windowEndIso).getTime() - Date.now();
  if (ms <= 0) return "已到窗结束时刻（以后端判定为准）";
  const m = Math.floor(ms / 60000), s = Math.floor((ms % 60000) / 1000);
  return `剩余 ${m}分${s}秒`;
}

// ---------- HTTP ----------
async function api(path, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  if (S.auth) headers["Authorization"] = "Basic " + S.auth;
  if (opts.body && !(opts.body instanceof FormData))
    headers["Content-Type"] = "application/json";
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  const ct = res.headers.get("content-type") || "";
  const payload = ct.includes("json") ? await res.json().catch(() => null)
    : await res.text();
  if (res.status === 401) {
    sessionExpired(typeof payload === "object" && payload ? payload.detail : null);
    const e = new Error("登录已失效");
    e.status = 401;
    throw e;
  }
  if (!res.ok) {
    const detail = payload && typeof payload === "object" ? payload.detail : payload;
    const e = new Error(detail || ("HTTP " + res.status));
    e.status = res.status;
    throw e;
  }
  return payload;
}
const apiGet = (p) => api(p);
const apiPost = (p, body) =>
  api(p, { method: "POST", body: body == null ? undefined : JSON.stringify(body) });

async function apiBlob(path) {
  const res = await fetch(path, { headers: { Authorization: "Basic " + S.auth } });
  if (res.status === 401) { sessionExpired(); throw new Error("登录已失效"); }
  if (!res.ok) {
    // 尽量带上服务端的确定性错误文案（如验真包被改动的 409 原因）
    let detail = "";
    try {
      const ct = res.headers.get("content-type") || "";
      if (ct.includes("json")) {
        const j = await res.json();
        detail = typeof j.detail === "object" && j.detail
          ? (j.detail.message || JSON.stringify(j.detail)) : (j.detail || "");
      }
    } catch { /* 忽略错误体解析失败 */ }
    throw new Error(detail || ("HTTP " + res.status));
  }
  return res.blob();
}

// ---------- 弹层 / 提示 ----------
const modal = $("#modal");
function openModal(html) {
  modal.innerHTML = `<div class="dlg-body">${html}</div>`;
  S.modalOpen = true;
  modal.showModal();
}
function addModalActions(html) {
  $(".dlg-body", modal).insertAdjacentHTML("beforeend",
    `<div class="dlg-actions" style="padding-right:0;padding-left:0;">${html}</div>`);
}
function closeModal() {
  S.modalOpen = false;
  modal.innerHTML = ""; // 关闭即销毁一次性内容（分片/纠错包）
  modal.close();
  dropPreviews();
  safeLoad();
}
modal.addEventListener("cancel", (e) => {
  // 不允许 Esc 留下“看似还在”的弹层：统一走关闭清理流程
  e.preventDefault();
  closeModal();
});

let toastTimer = null;
function toast(msg, kind = "") {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast " + kind;
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, 4200);
}
function errText(e) {
  return e && e.status === 403
    ? "服务端拒绝：当前会话角色无权执行此操作（403）。结果以后端状态为准。"
    : (e && e.message ? String(e.message) : String(e));
}

// ---------- 会话 ----------
const loginDialog = $("#login-dialog");
loginDialog.addEventListener("cancel", (e) => e.preventDefault());

function showLogin(note) {
  stopPolling();
  $("#login-error").hidden = !note;
  $("#login-error").textContent = note || "";
  $("#login-user").value = "";
  $("#login-pass").value = "";
  if (!loginDialog.open) loginDialog.showModal();
  $("#login-user").focus();
}

function sessionExpired(detail) {
  S.auth = null;
  S.me = null;
  sessionStorage.removeItem("meddeid_auth");
  $("#topbar").hidden = true;
  $("#view").innerHTML = "";
  closeModalSafe();
  showLogin("登录会话已失效或被服务端拒绝（401），请重新登录。" +
    (detail ? "（" + detail + "）" : ""));
}
function closeModalSafe() {
  if (modal.open) { S.modalOpen = false; modal.innerHTML = ""; modal.close(); }
}

$("#login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const user = $("#login-user").value.trim();
  const pass = $("#login-pass").value;
  const token = btoa(unescape(encodeURIComponent(user + ":" + pass)));
  S.auth = token;
  try {
    const me = await apiGet("/api/me");
    // 登录成功：仅保存凭据本身；页面数据全部从接口恢复
    sessionStorage.setItem("meddeid_auth", token);
    S.me = me;
    loginDialog.close();
    enterApp();
  } catch (err) {
    S.auth = null;
    $("#login-error").hidden = false;
    $("#login-error").textContent =
      err.status === 403 ? "服务端返回 403：该账户角色异常。"
        : "登录失败：服务端拒绝了凭据（401）。前端不会构造任何登录状态。";
  }
});

$("#btn-logout").addEventListener("click", () => {
  // 角色切换 / 退出：清掉本标签页全部凭据与页面内容，绝不残留身份材料
  S.auth = null;
  S.me = null;
  sessionStorage.removeItem("meddeid_auth");
  $("#view").innerHTML = "";
  closeModalSafe();
  showLogin("本会话凭据与页面内容已清除，可更换角色重新登录。");
});

// ---------- 状态徽标 ----------
const JOB_PILL = { queued: "info", running: "info", retry: "warn", dead: "bad", done: "ok" };
const VER_PILL = { processing: "info", ready: "info", published: "ok",
  failed: "bad", quarantined: "bad" };
const QC_PILL = { pending: "", passed: "info", approved: "ok", failed: "bad",
  rejected: "bad" };
const REID_PILL = { requested: "info", armed: "warn", exported: "ok",
  revoked: "bad", expired: "warn" };
function pill(value, map) {
  const k = map[value] || "";
  return `<span class="pill ${k}">${esc(value || "—")}</span>`;
}

// ---------- 写操作统一封装：禁用按钮 → 请求 → 无论成败都按服务端重渲染 ----------
async function withBtn(btn, key, fn) {
  if (key && S.inflight.has(key)) return;
  if (key) S.inflight.add(key);
  if (btn) btn.disabled = true;
  try {
    return await fn();
  } finally {
    if (key) S.inflight.delete(key);
    if (btn) btn.disabled = false;
  }
}

// ---------- 轮询 ----------
function stopPolling() {
  if (S.pollTimer) { clearInterval(S.pollTimer); S.pollTimer = null; }
}
async function safeLoad(silent = true) {
  if (S.tab && TABS[S.tab] && !S.modalOpen) {
    try { await TABS[S.tab].load(); }
    catch (e) { if (!silent) throw e; if (e.status !== 401) console.warn("reload", e); }
  }
}
// 自动刷新守卫：绝不在用户正在输入、弹层打开或存在未提交一次性草稿时重绘视图
function hasPendingDraft() {
  return $$("#view [data-role='share']").some((el) => el.value.trim()) ||
    $$("#view [data-role^='c-']").some((el) => el.value.trim());
}
function autoRefresh() {
  if (S.modalOpen) return;
  const ae = document.activeElement;
  if (ae && ae.closest("#view") &&
      ["INPUT", "TEXTAREA", "SELECT"].includes(ae.tagName)) return;
  if (hasPendingDraft()) return;
  safeLoad();
}
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) autoRefresh();
});

// ---------- 表格通用 ----------
function emptyRow(cols, text) {
  return `<tr><td colspan="${cols}" class="muted">${esc(text)}</td></tr>`;
}
function errPanel(e) {
  return `<div class="banner danger">无法从服务端恢复状态：${esc(errText(e))}</div>`;
}

// =========================================================
// 技师工作台
// =========================================================

const TechQueue = {
  id: "tech-queue",
  label: "处理队列",
  roles: ["technician", "admin"],
  async load() {
    const isTech = S.me.roles.includes("technician");
    let jobs = [];
    try { jobs = await apiGet("/api/queue/jobs"); }
    catch (e) { $("#view").innerHTML = errPanel(e); return; }
    const counts = jobs.reduce((a, j) => (a[j.status] = (a[j.status] || 0) + 1, a), {});
    $("#view").innerHTML = `
      <h2>处理队列（服务端真实状态）<span class="count">共 ${jobs.length} 个任务</span></h2>
      <div class="card">
        <div class="row">
          ${Object.entries(counts).map(([k, v]) =>
            `<span class="pill ${JOB_PILL[k] || ""}">${esc(k)} × ${v}</span>`).join("")}
          <span class="spacer"></span>
          ${isTech ? `
          <label class="hint">循环 <input type="number" id="run-cycles" value="1" min="1" max="50"
                     style="width:70px"></label>
          <label class="hint"><input type="checkbox" id="run-ignore"
                     style="width:auto;margin:0 4px 0 0">忽略退避</label>
          <button class="btn btn-primary" data-act="queue-run">执行处理任务</button>`
            : `<span class="hint">管理员视图：可驱动队列，不可人工 requeue（由后端强制）</span>
               <button class="btn btn-primary btn-sm" data-act="queue-run">执行一次</button>`}
        </div>
      </div>
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr>
            <th>任务</th><th>病例(伪名)</th><th>版本</th><th>状态</th>
            <th>尝试</th><th>下次可取/租约</th><th>最近错误</th><th>操作</th>
          </tr></thead>
          <tbody>${jobs.length ? jobs.map((j) => `
            <tr>
              <td class="mono">${esc(j.id)}</td>
              <td class="mono">${esc(j.study_id)}</td>
              <td class="mono">${esc(j.version_id)}</td>
              <td>${pill(j.status, JOB_PILL)}</td>
              <td>${esc(j.attempts)}/${esc(j.max_attempts)}</td>
              <td class="nowrap">${j.status === "running"
                ? "租约至 " + fmtTime(j.lease_until)
                : fmtTime(j.available_at)}</td>
              <td class="mono" style="max-width:280px">${esc(j.last_error || "")}</td>
              <td class="nowrap">
                <button class="btn btn-sm" data-act="version-detail"
                        data-vid="${esc(j.version_id)}">步骤记录</button>
                ${isTech && j.status === "dead" ? `
                <label class="hint" style="display:inline-flex;align-items:center;">
                  <input type="checkbox" checked data-role="clear"
                         style="width:auto;margin:0 4px 0 4px">清空故障注入</label>
                <button class="btn btn-sm btn-primary" data-act="job-requeue"
                        data-jid="${esc(j.id)}">人工重试</button>` : ""}
              </td>
            </tr>`).join("") : emptyRow(8, "队列中暂无任务")}
          </tbody>
        </table>
      </div>`;
  },
};

const TechQC = {
  id: "tech-qc",
  label: "人工质检 / 放行",
  roles: ["technician", "admin"],
  async load() {
    const isTech = S.me.roles.includes("technician");
    let rows = [];
    try { rows = await apiGet("/api/qc/queue"); }
    catch (e) { $("#view").innerHTML = errPanel(e); return; }
    const groups = { ready: [], quarantined: [], processing: [], published: [] };
    rows.forEach((r) => { (groups[r.status] || groups.processing).push(r); });
    const sec = (title, list, kind) => `
      <h3>${esc(title)} <span class="count">${list.length}</span></h3>
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr><th>版本</th><th>病例(伪名)</th><th>版本号</th>
            <th>版本状态 / 自动QC</th><th>错误</th><th>操作</th></tr></thead>
          <tbody>${list.length ? list.map((v) => `
            <tr>
              <td class="mono">${esc(v.id)}</td>
              <td>${esc(v.pseudo_patient_id)} / ${esc(v.pseudo_patient_name)}
                  <div class="mono muted">${esc(v.study_id)}</div></td>
              <td>v${esc(v.version_no)}</td>
              <td>${pill(v.status, VER_PILL)} ${pill(v.qc_status, QC_PILL)}</td>
              <td class="mono" style="max-width:220px">${esc(v.error || "")}</td>
              <td class="nowrap">
                <button class="btn btn-sm" data-act="version-detail"
                        data-vid="${esc(v.id)}">去标识预览/步骤</button>
                ${kind === "ready" && isTech ? `
                <button class="btn btn-sm btn-primary" data-act="qc-open"
                        data-vid="${esc(v.id)}">人工放行</button>` : ""}
              </td>
            </tr>`).join("") : emptyRow(6, "无")}
          </tbody>
        </table>
      </div>`;
    $("#view").innerHTML = `
      <h2>人工质检（半成品绝不进入最新版本）</h2>
      <div class="banner info">放行/否决是条件更新：重复点击、并发操作时只有第一次生效，
        页面会在操作后重新显示后端的确定结果。</div>
      ${sec("待人工放行（自动 QC 已通过）", groups.ready, "ready")}
      ${sec("已隔离（自动 QC 失败 / 人工否决）", groups.quarantined, "bad")}
      ${sec("处理中", groups.processing, "run")}
      ${sec("已放行（历史）", groups.published.filter((v) => v.qc_status === "approved"), "pub")}`;
  },
};

const TechStudies = {
  id: "tech-studies",
  label: "病例与版本",
  roles: ["technician", "admin"],
  async load() {
    const isTech = S.me.roles.includes("technician");
    let studies = [];
    try { studies = await apiGet("/api/studies"); }
    catch (e) { $("#view").innerHTML = errPanel(e); return; }
    $("#view").innerHTML = `
      <h2>病例与派生版本</h2>
      ${isTech ? `
      <div class="card">
        <h3>入库合成影像 / 新建派生版本</h3>
        <div class="toolbar">
          <label>MRN <input id="in-pid" placeholder="原始 PatientID（输入值不回显到其他视图）"></label>
          <label>姓名 <input id="in-name" placeholder="原始 PatientName"></label>
          <label>Accession <input id="in-acc" placeholder="AccessionNumber"></label>
          <label>故障注入
            <select id="in-fault"><option value="">无</option>
              <option value="redaction">redaction（遮盖异常）</option>
              <option value="qc_leak">qc_leak（跳过遮盖）</option></select></label>
          <button class="btn btn-primary" data-act="ingest">入库合成影像</button>
        </div>
        <div class="toolbar">
          <label>对病例按最新策略再派生
            <input id="derive-sid" placeholder="study_id，如 sty_xxx"></label>
          <input id="derive-reason" value="policy upgrade re-derivation"
                 style="min-width:240px">
          <button class="btn" data-act="derive">新建版本（不覆盖旧版）</button>
        </div>
      </div>` : ""}
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr><th>病例</th><th>伪身份</th><th>状态</th>
            <th>当前放行版本</th><th>创建时间</th><th>操作</th></tr></thead>
          <tbody>${studies.length ? studies.map((s) => `
            <tr>
              <td class="mono">${esc(s.id)}</td>
              <td>${esc(s.pseudo_patient_id)} / ${esc(s.pseudo_patient_name)}</td>
              <td>${pill(s.status, { active: "ok" })}</td>
              <td class="mono">${s.latest_version_id
                ? esc(s.latest_version_id)
                : '<span class="pill bad">无放行版本</span>'}</td>
              <td class="nowrap">${fmtTime(s.created_at)}</td>
              <td><button class="btn btn-sm" data-act="study-detail"
                          data-sid="${esc(s.id)}">版本链</button></td>
            </tr>`).join("") : emptyRow(6, "暂无病例")}
          </tbody>
        </table>
      </div>`;
  },
};

// =========================================================
// 临床医生工作台
// =========================================================

const ClinRelink = {
  id: "clin-relink",
  label: "受控回链（临床纠错）",
  roles: ["clinician"],
  async load() {
    const [studies, mine] = await Promise.all([
      apiGet("/api/studies"),
      apiGet("/api/relink/mine"),
    ]);
    $("#view").innerHTML = `
      <h2>受控回链 · 临床医生会话</h2>
      <div class="banner danger">本会话与隐私官会话必须<strong>相互独立</strong>：
        你在此发起并提交<strong>分片 A</strong>；隐私官须在另一个登录会话提交<strong>分片 B</strong>。
        只有后端进入 <span class="pill warn">armed</span> 后才出现登记纠错/导出入口；
        导出成功即失效，纠错包只显示一次。</div>

      <div class="card">
        <h3>发起回链请求</h3>
        <div class="toolbar">
          <label>病例
            <select id="rl-study">
              ${studies.map((s) => `<option value="${esc(s.id)}"
                ${s.latest_version_id ? "" : "disabled"}>
                ${esc(s.id)} · ${esc(s.pseudo_patient_id)}
                ${s.latest_version_id ? "" : "（无放行版本，不可回链）"}</option>`).join("")}
            </select></label>
          <label>目的 <input id="rl-purpose" value="clinical correction 2026-09"></label>
          <label>时间窗(分钟, 5–240)
            <input id="rl-window" type="number" value="30" min="5" max="240"
                   style="width:90px"></label>
          <button class="btn btn-primary" data-act="relink-request">发起请求</button>
        </div>
      </div>

      <h3>我发起的回链（状态全部由服务端恢复）<span class="count">${mine.length}</span></h3>
      ${mine.length ? mine.map(relinkCard).join("") :
        '<div class="card muted">暂无回链请求。</div>'}`;
  },
};

function stepBar(r) {
  const steps = r.authorization_steps || [];
  const has1 = steps.some((s) => s.step_no === 1);
  const has2 = steps.some((s) => s.step_no === 2);
  const li = (done, text, cls) =>
    `<li class="${done ? "step-ok" : cls}">${text}${done ? "（" +
      esc(steps.find((s) => s.step_no === (text.includes("分片A") ? 1 : 2))?.created_at || "")
      + "）" : ""}</li>`;
  return `<ul class="steps">
    ${li(has1, "步骤1 · 临床医生提交分片A", "step-wait")}
    ${li(has2, "步骤2 · 隐私官独立会话提交分片B", "step-wait")}
    <li class="${r.status === "armed" ? "step-ok" : "step-wait"}">后端武装（armed）</li>
    <li class="${r.status === "exported" ? "step-ok"
      : r.status === "revoked" || r.status === "expired" ? "step-bad" : "step-wait"}">
      终态：${esc(r.status)}${r.status === "exported" ? " · 一次性包已交付且已失效" : ""}</li>
  </ul>`;
}

function relinkCard(r) {
  const canExpire = r.status === "requested" || r.status === "armed";
  return `<div class="card">
    <div class="row">
      <strong class="mono">${esc(r.id)}</strong>
      ${pill(r.status, REID_PILL)}
      <span class="hint">病例 ${esc(r.study_id)} · 目的：${esc(r.purpose)}</span>
      <span class="spacer"></span>
      <span class="hint">${canExpire ? "授权窗 " + fmtTime(r.window_end) + "（" + remain(r.window_end) + "）"
        : "授权窗 " + fmtTime(r.window_end)}</span>
    </div>
    ${stepBar(r)}
    ${relinkActions(r)}
  </div>`;
}

function relinkActions(r) {
  if (r.status === "requested") {
    const has1 = (r.authorization_steps || []).some((s) => s.step_no === 1);
    return has1
      ? `<div class="banner">步骤1 已由你在服务端登记。请等待隐私官在<strong>独立登录会话</strong>
           完成分片 B；本页将自动从服务端刷新。</div>`
      : `<div class="toolbar" style="margin-top:10px;">
           <textarea data-role="share" placeholder="粘贴发起请求时一次性交付给你的分片 A"
             style="min-width:420px"></textarea>
           <button class="btn btn-primary" data-act="share-a"
                   data-rid="${esc(r.id)}">提交分片 A（步骤1）</button>
         </div>
         <p class="hint">分片 A 只在发起响应中出现一次，丢失只能重新发起请求。</p>`;
  }
  if (r.status === "armed") {
    return `
      <div class="banner info">后端已进入 armed（双角色授权齐备且通过指纹校验）。
        可在窗口内登记纠错并导出<strong>一次性纠错包</strong>；导出提交一旦成功，
        分片立即抹除，再次导出将得到服务端 409。</div>
      <div class="toolbar">
        <input data-role="c-PatientID" placeholder="纠正后 PatientID（可留空）">
        <input data-role="c-PatientName" placeholder="纠正后 PatientName（可留空）">
        <input data-role="c-PatientBirthDate" placeholder="PatientBirthDate YYYYMMDD">
        <input data-role="c-PatientSex" placeholder="PatientSex">
        <button class="btn" data-act="correction" data-rid="${esc(r.id)}">登记纠错</button>
        <button class="btn btn-primary" data-act="export" data-rid="${esc(r.id)}">
          导出一次性纠错包</button>
      </div>`;
  }
  if (r.status === "exported")
    return `<div class="banner">终态 exported：纠错包已在导出响应中一次性交付，
      服务端分片已抹除。刷新或再次请求导出都不会重新出现原身份内容。</div>`;
  if (r.status === "revoked")
    return `<div class="banner danger">终态 revoked（已被撤销：${esc(r.revoke_reason || "")}）。
      不存在可导出内容。</div>`;
  return `<div class="banner danger">终态 expired：授权时间窗结束。请重新发起回链请求。</div>`;
}

// =========================================================
// 隐私官工作台
// =========================================================

const OfficerPending = {
  id: "off-pending",
  label: "第二授权（分片B）",
  roles: ["officer"],
  async load() {
    let rows = [];
    try { rows = await apiGet("/api/relink/officer/pending"); }
    catch (e) { $("#view").innerHTML = errPanel(e); return; }
    $("#view").innerHTML = `
      <h2>隐私官待授权队列</h2>
      <div class="banner danger">你必须在<strong>独立于临床医生的登录会话</strong>中操作。
        点击授权时，分片 B 直接从服务端待批准接口取得并提交，页面不展示、不留存分片内容；
        若授权窗超时、已撤销或已被并发终结，服务端返回的确定结果会替换本视图。</div>
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr><th>回链</th><th>病例</th><th>目的</th><th>申请人</th>
            <th>授权窗结束</th><th>步骤</th><th>操作</th></tr></thead>
          <tbody>${rows.length ? rows.map((r) => `
            <tr>
              <td class="mono">${esc(r.id)}</td>
              <td class="mono">${esc(r.study_id)}</td>
              <td>${esc(r.purpose)}</td>
              <td>${esc(r.requested_by)}</td>
              <td class="nowrap">${fmtTime(r.window_end)}<div class="hint">${remain(r.window_end)}</div></td>
              <td>${(r.authorization_steps || []).map((s) =>
                `<span class="pill ok">步骤${esc(s.step_no)} ${esc(s.role)}</span>`)
                .join(" ") || ""}</td>
              <td><button class="btn btn-sm btn-primary" data-act="share-b"
                          data-rid="${esc(r.id)}">提交分片 B 授权</button></td>
            </tr>`).join("") : emptyRow(7, "没有等待第二授权的回链")}
          </tbody>
        </table>
      </div>`;
  },
};

const OfficerOversight = {
  id: "off-oversight",
  label: "回链监督 / 撤销",
  roles: ["officer", "admin"],
  async load() {
    const status = $("#ov-status")?.value || "";
    let rows = [];
    try {
      rows = await apiGet("/api/relink" + (status ? `?status=${encodeURIComponent(status)}` : ""));
    } catch (e) { $("#view").innerHTML = errPanel(e); return; }
    $("#view").innerHTML = `
      <h2>回链监督</h2>
      <div class="card">
        <div class="row">
          <label class="hint">状态筛选
            <select id="ov-status">
              <option value="">全部</option>
              ${["requested", "armed", "exported", "revoked", "expired"].map((s) =>
                `<option ${s === status ? "selected" : ""}>${s}</option>`).join("")}
            </select></label>
          <button class="btn" data-act="oversight-reload">应用筛选</button>
          <span class="spacer"></span>
          <button class="btn" data-act="relink-sweep">清扫过期授权</button>
        </div>
      </div>
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr><th>回链</th><th>病例</th><th>目的</th><th>申请人</th>
            <th>状态</th><th>授权窗</th><th>步骤</th><th>操作</th></tr></thead>
          <tbody>${rows.length ? rows.map((r) => `
            <tr>
              <td class="mono">${esc(r.id)}</td>
              <td class="mono">${esc(r.study_id)}</td>
              <td>${esc(r.purpose)}</td>
              <td>${esc(r.requested_by)}</td>
              <td>${pill(r.status, REID_PILL)}</td>
              <td class="nowrap">${fmtTime(r.window_end)}</td>
              <td><button class="btn btn-sm" data-act="relink-detail"
                          data-rid="${esc(r.id)}">查看步骤</button></td>
              <td>${r.status === "requested" || r.status === "armed"
                ? `<button class="btn btn-sm btn-danger" data-act="revoke-open"
                            data-rid="${esc(r.id)}">撤销</button>`
                : '<span class="hint">终态</span>'}</td>
            </tr>`).join("") : emptyRow(8, "无记录")}
          </tbody>
        </table>
      </div>`;
  },
};

// =========================================================
// 审计（officer 查询；admin 查询 + 校验 + 导出下载）
// =========================================================

const AuditView = {
  id: "audit",
  label: "审计记录",
  roles: ["admin", "officer"],
  async load() {
    const prev = collectAuditFilters();
    const qs = new URLSearchParams(Object.entries(prev).filter(([, v]) => v)).toString();
    let rows = [], verify = null, snaps = null, snapErr = null;
    try {
      [rows, verify] = await Promise.all([
        apiGet("/api/audit" + (qs ? "?" + qs : "")),
        apiGet("/api/audit/verify"),
      ]);
    } catch (e) { $("#view").innerHTML = errPanel(e); return; }
    // 快照区块独立加载：其失败不影响既有审计查询/导出区块
    try { snaps = await apiGet("/api/audit/snapshots"); }
    catch (e) { snapErr = e; }
    const isAdmin = S.me.roles.includes("admin");
    $("#view").innerHTML = `
      <h2>审计记录（哈希链）</h2>
      <div class="card">
        <div class="toolbar">
          <label>actor <input id="f-actor" value="${esc(prev.actor || "")}"></label>
          <label>action <input id="f-action" value="${esc(prev.action || "")}"></label>
          <label>study_id <input id="f-study" value="${esc(prev.study_id || "")}"></label>
          <label>reid_id <input id="f-reid" value="${esc(prev.reid_id || "")}"></label>
          <button class="btn btn-primary" data-act="audit-apply">筛选查询</button>
          <span class="spacer"></span>
          <span class="pill ${verify.ok ? "ok" : "bad"}">
            链校验：${verify.ok ? "完整 ✓" : "断裂于 seq " + verify.broken_at}
            （${verify.count} 条）</span>
          <button class="btn" data-act="audit-verify">重新校验</button>
          ${isAdmin ? `
          <button class="btn" data-act="audit-export" data-fmt="json">导出 JSON</button>
          <button class="btn" data-act="audit-export" data-fmt="csv">下载 CSV</button>`
            : '<span class="hint">officer 仅查询/校验；导出下载为 admin 专属（后端强制）</span>'}
        </div>
      </div>
      <div class="card" style="padding:0;overflow:auto;">
        <table>
          <thead><tr><th>seq</th><th>时间</th><th>actor</th><th>action</th>
            <th>study</th><th>reid</th><th>detail</th><th>mac</th></tr></thead>
          <tbody>${rows.length ? rows.map((r) => `
            <tr>
              <td>${esc(r.seq)}</td>
              <td class="nowrap">${fmtTime(r.ts)}</td>
              <td>${esc(r.actor)}</td>
              <td>${esc(r.action)}</td>
              <td class="mono">${esc(r.study_id || "")}</td>
              <td class="mono">${esc(r.reid_id || "")}</td>
              <td><code>${esc(JSON.stringify(r.detail))}</code></td>
              <td class="mono" style="max-width:120px;overflow:hidden;text-overflow:ellipsis;">
                ${esc(r.mac)}</td>
            </tr>`).join("") : emptyRow(8, "无匹配记录")}
          </tbody>
        </table>
      </div>
      ${snapshotSection(snaps, snapErr, isAdmin)}`;
  },
};

// ---------- 验真快照区块 ----------
const SNAP_CHECK_LABELS = [
  ["pack_file_ok", "包文件 WORM 摘要"],
  ["manifest_ok", "清单外部锚"],
  ["records_digests_ok", "记录逐条摘要"],
  ["frozen_chain_ok", "冻结 HMAC 链"],
  ["anchor_intact", "截止点链锚"],
  ["frozen_rows_match_live", "冻结行与现库一致"],
  ["live_chain_ok", "当前全链完整"],
];

function snapshotSection(snaps, snapErr, isAdmin) {
  let idem = sessionStorage.getItem("meddeid_snap_idem") || "";
  const body = snapErr
    ? errPanel(snapErr)
    : `
    ${isAdmin ? `
    <div class="card">
      <h3>冻结验真快照</h3>
      <div class="banner info">快照由<strong>服务端在确定的审计序号截止点</strong>取数冻结；
        截止点之后并发写入的审计事件不会混入。同一幂等键重复提交（刷新/重复点击/并发）
        只返回原快照；筛选无匹配时也会得到记录数为 0 的可核对冻结结果。</div>
      <div class="toolbar">
        <label>幂等键 <input id="snap-idem" value="${esc(idem)}"
          placeholder="留空将自动生成" style="min-width:220px"></label>
        <button class="btn btn-sm" data-act="snap-newkey" type="button">换新幂等键</button>
        <span class="hint">使用上方 actor/action/study_id/reid_id 筛选作为快照范围</span>
        <button class="btn btn-primary" data-act="snap-create">按当前筛选冻结快照</button>
      </div>
    </div>` : `
    <div class="card">
      <h3>验真快照复核</h3>
      <span class="hint">隐私官仅可查询与复核快照范围、链锚、记录数与完整性结果；
        创建与验真包下载为 admin 专属（后端强制）。</span>
    </div>`}
    <div class="card" style="padding:0;overflow:auto;">
      <table>
        <thead><tr><th>快照</th><th>创建时间</th><th>范围（筛选）</th>
          <th>截止 seq / 链锚</th><th>记录数</th><th>现场验真</th><th>操作</th></tr></thead>
        <tbody>${snaps && snaps.length ? snaps.map((s) => `
          <tr>
            <td class="mono">${esc(s.id)}</td>
            <td class="nowrap">${fmtTime(s.created_at)}<div class="hint">${esc(s.created_by)}</div></td>
            <td><code>${esc(JSON.stringify(s.filters || {}))}</code></td>
            <td class="mono">
              #${esc(s.cutoff_seq)}
              <div class="hint" style="max-width:150px;overflow:hidden;text-overflow:ellipsis;"
                   title="${esc(s.anchor_mac)}">${esc(s.anchor_mac)}</div></td>
            <td>${esc(s.record_count)}</td>
            <td>${snapshotPill(s.verification)}</td>
            <td class="nowrap">
              <button class="btn btn-sm" data-act="snap-detail"
                      data-sid="${esc(s.id)}">验真详情</button>
              ${isAdmin ? `<button class="btn btn-sm btn-primary" data-act="snap-download"
                      data-sid="${esc(s.id)}">下载验真包</button>` : ""}
            </td>
          </tr>`).join("") : emptyRow(7, "尚无验真快照")}
        </tbody>
      </table>
    </div>`;
  return `<h2>可冻结验真快照（不可变）</h2>${body}`;
}

function snapshotPill(v) {
  if (!v) return pill("unknown", {});
  if (v.ok) return `<span class="pill ok">验真通过 ✓</span>`;
  return `<span class="pill bad">异常：${esc(v.reason || "failed")}` +
    (v.broken_at != null ? ` @seq ${esc(v.broken_at)}` : "") + `</span>`;
}

function collectAuditFilters() {
  const g = (id) => ($("#" + id)?.value || "").trim();
  return { actor: g("f-actor"), action: g("f-action"), study_id: g("f-study"),
    reid_id: g("f-reid") };
}

// ---------- 标签页注册（顺序即角色视图顺序） ----------
const TAB_DEFS = [TechQueue, TechQC, TechStudies, ClinRelink,
  OfficerPending, OfficerOversight, AuditView];
const TABS = Object.fromEntries(TAB_DEFS.map((t) => [t.id, t]));

function visibleTabs() {
  return TAB_DEFS.filter((t) => t.roles.some((r) => S.me.roles.includes(r)));
}

function enterApp() {
  $("#topbar").hidden = false;
  $("#session-badge").textContent =
    `${S.me.username} · ${S.me.roles.join(",")}（本标签页独立会话）`;
  const tabs = visibleTabs();
  $("#tabs").innerHTML = tabs.map((t) =>
    `<button data-tab="${t.id}">${t.label}</button>`).join("");
  switchTab(tabs[0].id);
  stopPolling();
  S.pollTimer = setInterval(autoRefresh, 5000);
}

async function switchTab(id) {
  S.tab = id;
  $$("#tabs button").forEach((b) =>
    b.classList.toggle("active", b.dataset.tab === id));
  $("#view").innerHTML = '<div class="card muted">正在从服务端恢复状态…</div>';
  try { await TABS[id].load(); }
  catch (e) { if (e.status !== 401) $("#view").innerHTML = errPanel(e); }
}

// ---------- 事件委托 ----------
document.addEventListener("click", async (e) => {
  const tabBtn = e.target.closest("[data-tab]");
  if (tabBtn) { switchTab(tabBtn.dataset.tab); return; }
  const btn = e.target.closest("[data-act]");
  if (!btn || btn.disabled) return;
  const act = btn.dataset.act;
  const card = btn.closest(".card, .dlg-body") || $("#view");
  const input = (role, root = card) =>
    root.querySelector(`[data-role="${role}"]`);

  try {
    switch (act) {
      // ---- 队列 ----
      case "queue-run": {
        const cycles = parseInt($("#run-cycles")?.value || "1", 10) || 1;
        const ignore = $("#run-ignore")?.checked || false;
        await withBtn(btn, "queue-run", async () => {
          const r = await apiPost(
            `/api/queue/run?cycles=${cycles}&ignore_backoff=${ignore}`, {});
          toast(`已驱动 ${r.processed.length} 个任务（结果以服务端状态为准）`, "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      case "job-requeue": {
        const clear = input("clear", btn.closest("tr"))?.checked ?? true;
        await withBtn(btn, "requeue:" + btn.dataset.jid, async () => {
          await apiPost("/api/queue/requeue",
            { job_id: btn.dataset.jid, clear_fault: clear });
          toast("已请求人工重试；任务状态由后端条件更新决定", "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      // ---- 质检 ----
      case "version-detail":
        await openVersion(btn.dataset.vid);
        break;
      case "qc-open":
        openQCDialog(btn.dataset.vid);
        break;
      case "qc-submit": {
        const vid = btn.dataset.vid;
        const approve = btn.dataset.approve === "1";
        const note = $("#qc-note").value.trim();
        await withBtn(btn, "qc:" + vid, async () => {
          await apiPost("/api/qc/review", { version_id: vid, approve, note });
          toast("后端已接受复核结论", "ok");
          closeModal();
        });
        await TABS[S.tab].load();
        break;
      }
      // ---- 病例 ----
      case "ingest": {
        await withBtn(btn, "ingest", async () => {
          const r = await apiPost("/api/studies/ingest-synthetic", {
            patient_id: $("#in-pid").value, patient_name: $("#in-name").value,
            accession: $("#in-acc").value,
            fault_inject: $("#in-fault").value || null,
          });
          toast(r.idempotent ? "幂等：该影像已存在 " + r.study_id
            : "已入库并排队首版派生 " + r.study_id, "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      case "derive": {
        const sid = $("#derive-sid").value.trim();
        if (!sid) { toast("请填写 study_id"); break; }
        await withBtn(btn, "derive:" + sid, async () => {
          const v = await apiPost("/api/versions/derive",
            { study_id: sid, reason: $("#derive-reason").value });
          toast("已创建新版本 v" + v.version_no + "（旧版本永不覆盖）", "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      case "study-detail":
        await openStudy(btn.dataset.sid);
        break;
      // ---- 临床回链 ----
      case "relink-request": {
        await withBtn(btn, "relink-request", async () => {
          const r = await apiPost("/api/relink/request", {
            study_id: $("#rl-study").value, purpose: $("#rl-purpose").value.trim(),
            window_minutes: parseInt($("#rl-window").value, 10) || 30,
          });
          showShareAModal(r); // 仅本次响应中展示
        });
        await TABS[S.tab].load();
        break;
      }
      case "share-a": {
        const ta = input("share");
        const share = ta.value.trim();
        if (!share) { toast("请粘贴分片 A"); break; }
        await withBtn(btn, "share-a:" + btn.dataset.rid, async () => {
          await apiPost(`/api/relink/${btn.dataset.rid}/share-a`, { share });
          toast("步骤1 已由后端登记", "ok");
          ta.value = ""; // 用后即清
        });
        await TABS[S.tab].load();
        break;
      }
      case "correction": {
        const corrected = {};
        ["PatientID", "PatientName", "PatientBirthDate", "PatientSex"].forEach((k) => {
          const v = input("c-" + k)?.value.trim();
          if (v) corrected[k] = v;
        });
        if (!Object.keys(corrected).length) { toast("请至少填写一项纠错字段"); break; }
        await withBtn(btn, "correction:" + btn.dataset.rid, async () => {
          await apiPost(`/api/relink/${btn.dataset.rid}/correction`, { corrected });
          toast("纠错已在 armed 窗口内登记（内容信封加密，页面不回显）", "ok");
          card.querySelectorAll('input[data-role^="c-"]').forEach((i) => (i.value = ""));
        });
        break;
      }
      case "export": {
        await withBtn(btn, "export:" + btn.dataset.rid, async () => {
          const r = await apiPost(`/api/relink/${btn.dataset.rid}/export`);
          showBundleModal(r); // 一次性：只在本次响应里显示
        });
        await TABS[S.tab].load();
        break;
      }
      // ---- 隐私官 ----
      case "share-b": {
        await withBtn(btn, "share-b:" + btn.dataset.rid, async () => {
          // 从受角色保护的待批准接口取得分片 B，立即提交；不渲染分片
          const pend = await apiGet("/api/relink/officer/pending");
          const item = pend.find((p) => p.id === btn.dataset.rid);
          if (!item) throw new Error(
            "该回链已不在待授权队列（可能已授权、超时、撤销或被并发终结）");
          const r = await apiPost(`/api/relink/${btn.dataset.rid}/share-b`,
            { share: item.share_for_officer });
          toast("后端状态：" + r.status, r.status === "armed" ? "ok" : "");
        });
        await TABS[S.tab].load();
        break;
      }
      case "revoke-open":
        openRevokeDialog(btn.dataset.rid);
        break;
      case "revoke-submit": {
        const rid = btn.dataset.rid;
        await withBtn(btn, "revoke:" + rid, async () => {
          const r = await apiPost(`/api/relink/${rid}/revoke`,
            { reason: $("#revoke-reason").value.trim() || "privacy officer revocation" });
          toast(r.revoked ? "撤销已在后端生效" : "撤销未生效（终态竞态）",
            r.revoked ? "ok" : "bad");
          closeModal();
        });
        await TABS[S.tab].load();
        break;
      }
      case "relink-sweep": {
        await withBtn(btn, "sweep", async () => {
          const r = await apiPost("/api/relink/sweep", {});
          toast("清扫完成，转过期：" + (r.expired || []).length + " 项", "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      case "relink-detail":
        await openRelinkDetail(btn.dataset.rid);
        break;
      case "oversight-reload":
        await TABS[S.tab].load();
        break;
      // ---- 审计 ----
      case "audit-apply":
        await TABS[S.tab].load();
        break;
      case "audit-verify":
        await TABS[S.tab].load();
        break;
      case "audit-export": {
        await withBtn(btn, "audit-export:" + btn.dataset.fmt, async () => {
          // 导出由后端按审计链生成全量文件（附链校验结果）；筛选仅作用于上方查询视图
          const r = await apiPost(`/api/audit/export?fmt=${btn.dataset.fmt}`, {});
          // 导出落 WORM blob，通过受 admin 保护的接口下载；校验结果以服务端为准
          const blob = await apiBlob(`/api/blobs/${r.cas_id}`);
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `audit-export-${r.cas_id.slice(0, 12)}.${btn.dataset.fmt === "csv" ? "csv" : "json"}`;
          document.body.appendChild(a);
          a.click();
          a.remove();
          setTimeout(() => URL.revokeObjectURL(url), 4000);
          toast(`导出完成：${r.records} 条，链校验 ok=${r.chain_verification.ok}`, "ok");
        });
        await TABS[S.tab].load();
        break;
      }
      // ---- 验真快照 ----
      case "snap-newkey": {
        const k = "idem-" + Date.now().toString(36) + "-" +
          Math.random().toString(36).slice(2, 10);
        sessionStorage.setItem("meddeid_snap_idem", k);
        const el = $("#snap-idem");
        if (el) el.value = k;
        break;
      }
      case "snap-create": {
        let idem = ($("#snap-idem")?.value || "").trim();
        if (!idem) {
          idem = "idem-" + Date.now().toString(36) + "-" +
            Math.random().toString(36).slice(2, 10);
        }
        // 幂等键在“冻结”动作飞行期间固定：刷新/重复点击/并发都重放同一键
        sessionStorage.setItem("meddeid_snap_idem", idem);
        const f = collectAuditFilters();
        const body = Object.assign({ idempotency_key: idem },
          Object.fromEntries(Object.entries(f).filter(([, v]) => v)));
        await withBtn(btn, "snap-create:" + idem, async () => {
          try {
            const r = await apiPost("/api/audit/snapshots", body);
            toast(r.idempotent_replay
              ? `幂等重放：返回原快照 ${r.id}（记录 ${r.record_count} 条）`
              : `快照已冻结：${r.id}，截止 seq ${r.cutoff_seq}，记录 ${r.record_count} 条`,
              "ok");
          } catch (e) {
            // 原链被篡改/参数冲突：展示服务端确定状态，不猜测结果
            toast(errText(e), "bad");
            throw e;
          }
        });
        await TABS[S.tab].load();
        break;
      }
      case "snap-detail":
        await openSnapshotDetail(btn.dataset.sid);
        break;
      case "snap-download": {
        const sid = btn.dataset.sid;
        await withBtn(btn, "snap-download:" + sid, async () => {
          try {
            // 专用接口在交付前强制验真；包被改动时服务端 409，不下载被改内容
            const blob = await apiBlob(`/api/audit/snapshots/${sid}/pack`);
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `audit-verify-${sid}.zip`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            setTimeout(() => URL.revokeObjectURL(url), 4000);
            toast("验真包已通过交付前验真并开始下载", "ok");
          } catch (e) {
            toast("验真包未通过服务端验真，已拒绝下载：" + errText(e), "bad");
          }
        });
        await TABS[S.tab].load();
        break;
      }
      case "modal-close":
        closeModal();
        break;
    }
  } catch (err) {
    if (err.status !== 401) toast(errText(err), "bad");
    // 任何错误之后都回到服务端确定状态（延迟到状态机更新落盘后）
    setTimeout(() => { if (!S.modalOpen) safeLoad(); }, 300);
  }
});

// 筛选下拉即时应用（change 不需要按钮也能工作）
document.addEventListener("change", (e) => {
  if (e.target.id === "ov-status") TABS[S.tab]?.load();
});

// ---------- 弹层内容（均取自服务端响应） ----------
let previewUrls = [];
function dropPreviews() {
  previewUrls.forEach((u) => URL.revokeObjectURL(u));
  previewUrls = [];
}

async function openVersion(vid) {
  const [v, blob] = await Promise.all([
    apiGet(`/api/versions/${vid}`),
    apiBlob(`/api/versions/${vid}/preview`).catch(() => null),
  ]);
  let imgHtml = "";
  if (blob) {
    const url = URL.createObjectURL(blob);
    previewUrls.push(url);
    imgHtml = `<img class="preview-img" src="${url}" alt="去标识预览">`;
  } else {
    imgHtml = '<p class="hint">尚无可预览的派生影像（处理中/失败）。</p>';
  }
  openModal(`
    <h2>版本 ${esc(v.id)} <span class="hint">v${esc(v.version_no)}</span></h2>
    <div class="row" style="margin:6px 0 12px;">
      ${pill(v.status, VER_PILL)} ${pill(v.qc_status, QC_PILL)}
      <span class="hint">病例 ${esc(v.study_id)}</span>
    </div>
    <div class="grid2">
      <div>${imgHtml}</div>
      <div>
        <h3>自动质检报告</h3>
        <pre>${esc(JSON.stringify(v.qc_report, null, 2))}</pre>
      </div>
    </div>
    <h3>完整处理步骤链（不可变记录）</h3>
    <ul class="steps">${(v.steps || []).map((s) =>
      `<li><strong>${esc(s.step)}</strong> <code>${esc(JSON.stringify(s.detail))}</code></li>`
    ).join("") || '<li class="hint">暂无步骤</li>'}</ul>
    ${v.error ? `<p class="error">失败原因：${esc(v.error)}</p>` : ""}
    <div class="dlg-actions"><button class="btn" data-act="modal-close">关闭</button></div>`);
}

async function openStudy(sid) {
  const d = await apiGet(`/api/studies/${sid}`);
  openModal(`
    <h2>病例 ${esc(d.study.id)}</h2>
    <dl class="kv">
      <dt>伪身份</dt><dd>${esc(d.study.pseudo_patient_id)} / ${esc(d.study.pseudo_patient_name)}</dd>
      <dt>当前放行版本</dt><dd class="mono">${esc(d.study.latest_version_id || "无")}</dd>
    </dl>
    <h3>版本链（旧版本永不覆盖）</h3>
    <table>
      <thead><tr><th>版本</th><th>号</th><th>状态</th><th>QC</th><th>放行信息</th></tr></thead>
      <tbody>${d.versions.map((v) => `
        <tr>
          <td class="mono">${esc(v.id)}</td><td>v${esc(v.version_no)}</td>
          <td>${pill(v.status, VER_PILL)}</td><td>${pill(v.qc_status, QC_PILL)}</td>
          <td>${v.qc_by ? esc(v.qc_by) + " " + fmtTime(v.qc_at)
            + (v.qc_note ? " · " + esc(v.qc_note) : "") : '<span class="hint">—</span>'}</td>
        </tr>`).join("")}</tbody></table>
    <div class="dlg-actions"><button class="btn" data-act="modal-close">关闭</button></div>`);
}

function openQCDialog(vid) {
  openModal(`
    <h2>人工放行复核</h2>
    <p class="hint">版本 <span class="mono">${esc(vid)}</span>：
      先查看去标识预览与步骤记录，再给出结论。结论是后端条件更新，重复点击只生效一次。</p>
    <label>复核备注 <textarea id="qc-note" placeholder="如：无标签/像素残留，同意放行"></textarea></label>
    <div class="dlg-actions">
      <button class="btn btn-danger" data-act="qc-submit" data-vid="${esc(vid)}"
              data-approve="0">否决（隔离，需重新派生）</button>
      <button class="btn btn-primary" data-act="qc-submit" data-vid="${esc(vid)}"
              data-approve="1">确认放行</button>
    </div>`);
}

// 分片 A 一次性交付：内容只存在于本次响应与当前弹层 DOM 中
function showShareAModal(r) {
  openModal(`
    <h2>回链已建立 · 请立即保存分片 A</h2>
    <div class="banner danger">
      <strong>这是分片 A 的唯一一次出现。</strong>它不会被页面持久化，也无法再次查询。
      请复制到安全位置，随后在<strong>你本人的临床医生会话</strong>中粘贴提交（步骤1）；
      隐私官须在<strong>另一个独立登录会话</strong>完成分片 B（步骤2）。
      授权窗：${fmtTime(r.window_end)}。
    </div>
    <dl class="kv">
      <dt>回链 ID</dt><dd class="mono">${esc(r.id)}</dd>
      <dt>分片指纹</dt><dd class="mono">${esc(r.share_fingerprint)}</dd>
    </dl>
    <label>分片 A（仅本次交付）
      <textarea id="share-a-once" readonly>${esc(r.share_for_requester)}</textarea></label>
    <div class="dlg-actions">
      <button class="btn" data-act="copy-share">复制分片A</button>
      <button class="btn btn-primary" data-act="modal-close">我已安全保存，关闭并清除</button>
    </div>`);
  // 复制动作不产生任何状态变更
  modal.querySelector('[data-act="copy-share"]').addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText($("#share-a-once").value);
      toast("已复制到剪贴板，请尽快安全保存", "ok");
    } catch {
      $("#share-a-once").select();
      toast("浏览器拒绝剪贴板访问，请手动全选复制", "bad");
    }
  });
}

// 一次性纠错包：仅导出响应中展示，关闭即从 DOM 清除
function showBundleModal(r) {
  const b = r.bundle || {};
  openModal(`
    <h2>一次性纠错包 · ${esc(r.reid_id)}</h2>
    <div class="banner danger">
      后端状态已推进到 exported，授权分片已在服务端<strong>立即抹除</strong>。
      请现在保存本包；关闭本窗口或刷新页面后，原身份内容不会再出现，重复导出得到 409。
    </div>
    <dl class="kv">
      <dt>导出 Blob</dt><dd class="mono">${esc(r.export_cas_id)}</dd>
      <dt>病例</dt><dd class="mono">${esc(b.study_id)}</dd>
      <dt>伪身份</dt><dd>${esc(b.pseudo_patient_id)}</dd>
      <dt>目的</dt><dd>${esc(b.purpose)}</dd>
      <dt>导出时间</dt><dd>${fmtTime(b.exported_at)}</dd>
    </dl>
    <h3>原始身份（一次性）</h3>
    <pre>${esc(JSON.stringify(b.original_identity, null, 2))}</pre>
    <h3>纠错后身份</h3>
    <pre>${esc(JSON.stringify(b.corrected_identity, null, 2))}</pre>
    <div class="dlg-actions">
      <button class="btn btn-primary" data-act="modal-close">我已保存，关闭并清除</button>
    </div>`);
}

function openRevokeDialog(rid) {
  openModal(`
    <h2>撤销回链 ${esc(rid)}</h2>
    <p class="hint">撤销对 requested/armed 生效；若与导出并发，后端条件更新保证只有一方胜出，
      本页会显示服务端的确定结果。</p>
    <label>撤销原因 <textarea id="revoke-reason">privacy officer revocation</textarea></label>
    <div class="dlg-actions">
      <button class="btn" data-act="modal-close">取消</button>
      <button class="btn btn-danger" data-act="revoke-submit"
              data-rid="${esc(rid)}">确认撤销</button>
    </div>`);
}

async function openRelinkDetail(rid) {
  const r = await apiGet(`/api/relink/${rid}/status`);
  openModal(`
    <h2>回链 ${esc(r.id)} ${pill(r.status, REID_PILL)}</h2>
    <dl class="kv">
      <dt>病例</dt><dd class="mono">${esc(r.study_id)}</dd>
      <dt>目的</dt><dd>${esc(r.purpose)}</dd>
      <dt>申请人</dt><dd>${esc(r.requested_by)}</dd>
      <dt>授权窗</dt><dd>${fmtTime(r.window_start)} → ${fmtTime(r.window_end)}</dd>
      <dt>撤销原因</dt><dd>${esc(r.revoke_reason || "—")}</dd>
    </dl>
    <h3>双角色授权步骤（服务端记录）</h3>
    ${stepBar(r)}
    <h3>原始记录（无身份内容）</h3>
    <pre>${esc(JSON.stringify(r, null, 2))}</pre>
    <div class="dlg-actions"><button class="btn" data-act="modal-close">关闭</button></div>`);
}

async function openSnapshotDetail(sid) {
  const r = await apiGet(`/api/audit/snapshots/${sid}`);
  const v = r.verification || {};
  const checks = SNAP_CHECK_LABELS.map(([key, label]) => `
    <li class="${v[key] ? "step-ok" : "step-bad"}">${esc(label)}：${v[key] ? "通过 ✓" : "未通过 ✗"}</li>`).join("");
  openModal(`
    <h2>验真快照 ${esc(r.id)}</h2>
    <div class="row" style="margin:6px 0 12px;">
      ${snapshotPill(v)}
      <span class="hint">创建于 ${fmtTime(r.created_at)}（${esc(r.created_by)}）</span>
    </div>
    <dl class="kv">
      <dt>幂等键</dt><dd class="mono">${esc(r.idempotency_key)}</dd>
      <dt>范围筛选</dt><dd><code>${esc(JSON.stringify(r.filters || {}))}</code>
        ${r.record_count === 0 ? '<span class="pill warn">筛选为空：0 条（仍冻结链锚）</span>' : ""}</dd>
      <dt>截止审计序号</dt><dd class="mono">#${esc(r.cutoff_seq)} · ${fmtTime(r.cutoff_ts)}</dd>
      <dt>链锚 MAC</dt><dd class="mono" style="word-break:break-all;">${esc(r.anchor_mac)}</dd>
      <dt>记录范围</dt><dd class="mono">${esc(r.first_seq ?? "—")} → ${esc(r.last_seq ?? "—")}
        （${esc(r.record_count)} 条）</dd>
      <dt>验真包 CAS / 大小</dt><dd class="mono" style="word-break:break-all;">
        ${esc(r.pack_cas_id)} · ${esc(r.pack_size)} B</dd>
      <dt>清单 / 记录摘要</dt><dd class="mono" style="word-break:break-all;font-size:12px;">
        m:${esc(r.manifest_sha256)}<br>r:${esc(r.records_sha256)}</dd>
    </dl>
    <h3>现场完整性结果（每次打开由服务端重算）</h3>
    <ul class="steps">${checks}</ul>
    ${v.ok ? `<div class="banner">验真通过；截止点之后新增审计事件
      <strong>${esc(v.new_events_after_cutoff ?? 0)}</strong> 条（不影响冻结结果）。</div>`
      : `<div class="banner danger">确定性异常：${esc(v.reason || "verification failed")}` +
        (v.broken_at != null ? `（断裂 seq ${esc(v.broken_at)}）` : "") + `</div>`}
    <h3>创建时冻结结论</h3>
    <pre>${esc(JSON.stringify(r.created_integrity, null, 2))}</pre>
    <div class="dlg-actions"><button class="btn" data-act="modal-close">关闭</button></div>`);
}

// ---------- 启动：恢复会话与真实状态 ----------
(async function boot() {
  if (!S.auth) { showLogin(); return; }
  try {
    S.me = await apiGet("/api/me");
    enterApp();
  } catch (e) {
    if (e.status === 401) return; // sessionExpired 已处理
    showLogin("无法连接服务端恢复会话：" + errText(e));
  }
})();
