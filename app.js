const STORAGE_UPLOAD_AT_KEY = "survey_last_upload_at_v1";
const RULES_KEY = "survey_rules_v1";
const SINGLE_CONFIG_KEY = "survey_single_config_v1";
const OPEN_CONFIG_KEY = "survey_open_config_v1";
const FRAMEWORK_CONFIG_KEY = "survey_framework_config_v1";
const WEIGHT_CONFIG_KEY = "survey_weight_config_v1";
const WEIGHT_HISTORY_KEY = "survey_weight_history_v1";
const ACCESS_PASSWORD = "miyoushe2026";
const ACCESS_SESSION_KEY = "survey_access_granted_v1";
const DB_NAME = "survey_dashboard_db";
const DB_VERSION = 1;
const STORE_NAME = "kv";
const RAW_ROWS_KEY = "survey_raw_rows_v1";
const BAR_COLOR = "#8C9AEB";
const chartResizeObservers = new WeakMap();
let chartWindowResizeBound = false;
const DEFAULT_RULES = {
  terminateField: "",
  terminateValue: "1",
  durationField: "duration",
  enableDuration: false,
  minDuration: 15,
  enableRequired: false,
  requiredFields: [],
};
const FIXED_WEIGHT_DIMS = ["gender", "age", "adventure"];
const FIXED_COMMUNITY_PENETRATION = 0.3687;

const DEFAULT_WEIGHT_PLAN = {
  mode: "two_stage",
  communityField: "近42天内社区活跃",
  communityYesCodes: ["1"],
  communityNoCodes: ["0"],
  genderField: "性别-BI",
  communityPenetration: FIXED_COMMUNITY_PENETRATION,
  groupTargets: {
    community: {
      gender: { 男: 0.5861, 女: 0.4097, 未知: 0.0041 },
      age: { "19-25": 0.3031, "26-30": 0.1377, "31-35": 0.0702, "35+": 0.4762, 未知: 0.0128 },
      adventure: { "0-30": 0.0199, "31-45": 0.0351, "46-60": 0.945 },
      spend: { 大R: 0.0125, 中R: 0.1129, 小R: 0.7405, 无付费: 0.1341 },
      community_active: { 有: 1, 无: 0 },
    },
    non_community: {
      gender: { 男: 0.4887, 女: 0.4845, 未知: 0.0269 },
      age: { "19-25": 0.2137, "26-30": 0.0831, "31-35": 0.0563, "35+": 0.6058, 未知: 0.0411 },
      adventure: { "0-30": 0.1515, "31-45": 0.115, "46-60": 0.7335 },
      spend: { 大R: 0.0035, 中R: 0.0423, 小R: 0.5783, 无付费: 0.3759 },
      community_active: { 有: 0, 无: 1 },
    },
  },
  adventureTierField: "冒险等阶2分级-BI",
  adventureField: "游戏等级-BI",
  spendField: "累计付费",
};

const DEFAULT_CHANNELS = {
  1: "抖音",
  2: "B站",
  3: "小红书",
  4: "知乎",
  5: "lofter",
  6: "微博",
  7: "快手",
  8: "小黑盒",
  9: "taptap",
  10: "米游社",
  11: "NGA",
  12: "百度贴吧",
  13: "QQ频道",
  14: "QQ空间",
  15: "微信、QQ群聊",
};

const Q2_CATEGORIES = {
  1: "抽卡决策/原石/强度对比",
  2: "角色配队/养成/手法",
  3: "探索解谜攻略",
  4: "剧情解析",
  5: "剧情讨论",
  6: "同人",
  7: "游戏外活动",
  8: "周边",
  9: "Cosplay",
};

const OVERVIEW_SCENES = [
  { name: "抽卡规划/原石获取", rank: "q8" },
  { name: "角色养成/操作手法", rank: "q11" },
  { name: "探索解谜", rank: "q14" },
  { name: "剧情解析", rank: "q16" },
  { name: "剧情讨论", rank: "q18" },
  { name: "同人", rank: "q20" },
  { name: "游戏外活动", rank: "q22" },
  { name: "周边", rank: "q24" },
  { name: "Cosplay", rank: "q26" },
];

const Q1_LABELS = {
  1: "几乎每天（6-7天）",
  2: "经常（4-5天）",
  3: "偶尔（1-3天）",
  4: "很少（1-2天）",
  5: "近1个月未登录",
};

const Q27_LABELS = {
  1: "几乎每天都会使用",
  2: "每周2-3次",
  3: "每周1次或更少",
  4: "新版本/活动时使用",
  5: "不使用米游社",
};

const Q29_LABELS = {
  1: "非常满意",
  2: "比较满意",
  3: "一般般",
  4: "比较不满意",
  5: "非常不满意",
};

const Q34_LABELS = {
  1: "男",
  2: "女",
  3: "不方便透露",
};

const DEFAULT_SINGLE_CONFIG = {
  q1: { title: "Q1 活跃度", labels: Q1_LABELS },
  q27: { title: "Q27 米游社使用频次", labels: Q27_LABELS },
  q29: { title: "Q29 米游社满意度", labels: Q29_LABELS },
  q34: { title: "Q34 性别", labels: Q34_LABELS },
  q33: {
    title: "Q33 出生年份",
    labels: {
      1: "1986或更早",
      2: "1987",
      3: "1988",
      4: "1989",
      5: "1990",
      6: "1991",
      7: "1992",
      8: "1993",
      9: "1994",
      10: "1995",
      11: "1996",
      12: "1997",
      13: "1998",
      14: "1999",
      15: "2000",
      16: "2001",
      17: "2002",
      18: "2003",
      19: "2004",
      20: "2005",
      21: "2006",
      22: "2007",
    },
  },
};

let rawRows = [];
let analysisRows = [];
let lastUploadAt = "";
let lastImportStats = null;
let sampleStats = { total: 0, terminateExcluded: 0, invalidExcluded: 0, final: 0 };
let currentRules = { ...DEFAULT_RULES };
let singleConfig = JSON.parse(JSON.stringify(DEFAULT_SINGLE_CONFIG));
let openConfig = { q32: "Q32 平台期待（开放题）" };
let frameworkConfig = { raw: "", items: [], updatedAt: "" };
let weightConfig = {
  enabled: false,
  dims: [...FIXED_WEIGHT_DIMS],
  targetsText: JSON.stringify(DEFAULT_WEIGHT_PLAN, null, 2),
  maxIter: 24,
  capMin: 0.2,
  capMax: 5,
};
let weightState = { applied: false, message: "未启用" };
let weightHistory = [];
let appStarted = false;
let headersCache = [];
let headersCacheRowsRef = null;
let biGenderFieldCache = "";
let biGenderFieldCacheRowsRef = null;
let weightPlanCacheText = "";
let weightPlanCacheObj = null;
let crossRenderTimer = null;
let channelLabelsCache = new Map();
let channelLabelsCacheRowsRef = null;

const WEIGHT_DIM_VALUES = {
  gender: ["男", "女", "未知"],
  age: ["19-25", "26-30", "31-35", "35+", "未知"],
  adventure: ["0-30", "31-45", "46-60", "未知"],
  spend: ["大R", "中R", "小R", "无付费", "未知"],
  community_active: ["有", "无"],
};

const WEIGHT_DIM_LABELS = {
  gender: "性别",
  age: "年龄",
  adventure: "冒险等级2",
  spend: "消费等级",
  community_active: "社区活跃",
};

function clearDerivedCaches() {
  channelLabelsCache = new Map();
  channelLabelsCacheRowsRef = rawRows;
}

function getChannelLabelsForQuestion(qid) {
  if (!qid) return { ...DEFAULT_CHANNELS };
  if (channelLabelsCacheRowsRef !== rawRows) clearDerivedCaches();
  if (channelLabelsCache.has(qid)) return channelLabelsCache.get(qid);

  const headers = getHeaders();
  const re = new RegExp(`^${qid}_(\\d+)_(.+)$`);
  const labels = {};
  for (const h of headers) {
    const m = h.match(re);
    if (!m) continue;
    labels[Number(m[1])] = m[2];
  }

  const sorted = Object.fromEntries(
    Object.entries(labels)
      .sort((a, b) => Number(a[0]) - Number(b[0]))
      .map(([k, v]) => [Number(k), v]),
  );
  const finalLabels = Object.keys(sorted).length ? sorted : { ...DEFAULT_CHANNELS };
  channelLabelsCache.set(qid, finalLabels);
  return finalLabels;
}

function getCommunityPlatformName(labels = {}) {
  const entries = Object.entries(labels || {});
  if (!entries.length) return "米游社";
  const byCode10 = entries.find(([code]) => Number(code) === 10);
  if (byCode10?.[1]) return byCode10[1];

  const keywordRegex = /(米游社|miyoushe|hoyolab|hoYoLAB|官方社区|official community)/i;
  const matched = entries.find(([, name]) => keywordRegex.test(String(name || "")));
  return matched?.[1] || entries[0][1] || "米游社";
}

function isCommunityPlatformName(name, labels) {
  return str(name) !== "" && str(name) === str(getCommunityPlatformName(labels));
}

function getNameHighlightClass(name, labels) {
  return isCommunityPlatformName(name, labels) ? "mys-text" : "";
}

function getFillHighlightClass(name, labels) {
  return isCommunityPlatformName(name, labels) ? "mys-fill" : "";
}

function getPctHighlightClass(name, labels) {
  return isCommunityPlatformName(name, labels) ? "mys-pct" : "";
}

function unlockApp() {
  const gate = document.getElementById("accessGate");
  const shell = document.getElementById("appShell");
  if (gate) gate.style.display = "none";
  if (shell) shell.classList.remove("hidden-before-unlock");
  sessionStorage.setItem(ACCESS_SESSION_KEY, "1");
}

function startApp() {
  if (appStarted) return;
  appStarted = true;
  bootstrap().catch((err) => {
    alert(`初始化失败: ${err.message}`);
  });
}

function initAccessGate() {
  const gate = document.getElementById("accessGate");
  const input = document.getElementById("accessPassword");
  const btn = document.getElementById("btnUnlock");
  const err = document.getElementById("gateError");
  if (!gate || !input || !btn || !err) {
    startApp();
    return;
  }

  if (sessionStorage.getItem(ACCESS_SESSION_KEY) === "1") {
    unlockApp();
    startApp();
    return;
  }

  const tryUnlock = () => {
    if (input.value === ACCESS_PASSWORD) {
      err.textContent = "";
      unlockApp();
      startApp();
      return;
    }
    err.textContent = "密码错误，请重试。";
  };

  btn.addEventListener("click", tryUnlock);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") tryUnlock();
  });
}

function openDb() {
  return new Promise((resolve, reject) => {
    if (!window.indexedDB) {
      reject(new Error("当前浏览器不支持 IndexedDB"));
      return;
    }
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "k" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("打开 IndexedDB 失败"));
  });
}

async function idbGet(key) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readonly");
    const store = tx.objectStore(STORE_NAME);
    const req = store.get(key);
    req.onsuccess = () => resolve(req.result ? req.result.v : null);
    req.onerror = () => reject(req.error || new Error("读取 IndexedDB 失败"));
  });
}

async function idbSet(key, value) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const req = store.put({ k: key, v: value });
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error || new Error("写入 IndexedDB 失败"));
  });
}

async function idbDel(key) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    const store = tx.objectStore(STORE_NAME);
    const req = store.delete(key);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error || new Error("删除 IndexedDB 失败"));
  });
}

function str(v) {
  return (v ?? "").toString().trim();
}

function toInt(v) {
  const n = parseInt(str(v), 10);
  return Number.isNaN(n) ? null : n;
}

function parseDate(v) {
  const t = str(v);
  if (!t) return 0;
  const d = new Date(t.replace(" ", "T"));
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}

function fmtPct(n) {
  return `${(n * 100).toFixed(1)}%`;
}

function isWeightEnabled() {
  return !!weightConfig.enabled;
}

function setWeightEnabled(enabled) {
  weightConfig = {
    ...weightConfig,
    enabled: !!enabled,
  };
  saveWeightConfig();
  recomputeAnalysisRows();
  renderAll();
}

function getRowWeight(row) {
  const w = Number(row?.__w ?? 1);
  if (!Number.isFinite(w) || w <= 0) return 1;
  return w;
}

function sumWeights(rows) {
  return rows.reduce((s, r) => s + getRowWeight(r), 0);
}

function fmtCount(n) {
  const v = Number(n || 0);
  if (isWeightEnabled()) return v.toFixed(1);
  return String(Math.round(v));
}

function fmtTime(ts) {
  if (!ts) return "--";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "--";
  const p = (x) => String(x).padStart(2, "0");
  return `${d.getFullYear()}/${p(d.getMonth() + 1)}/${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function setImportProgress(percent, status) {
  const p = Math.max(0, Math.min(100, percent));
  const bar = document.getElementById("importBar");
  const percentNode = document.getElementById("importPercent");
  const statusNode = document.getElementById("importStatus");
  if (bar) bar.style.width = `${p}%`;
  if (percentNode) percentNode.textContent = `${Math.round(p)}%`;
  if (statusNode) statusNode.textContent = status;
}

function findQ2ColumnSuffix(code, row) {
  const key = Object.keys(row).find((k) => k.startsWith(`q2_${code}_`));
  return key ? key.slice(`q2_${code}_`.length) : "";
}

function hasQ2ByCode(row, code) {
  const direct = str(row[`q2_${code}`]);
  if (direct === "1") return true;
  return Object.keys(row).some((k) => k.startsWith(`q2_${code}_`) && str(row[k]) === "1");
}

function hasQ2AllDiscuss(row) {
  return hasQ2ByCode(row, 10);
}

function isDataRow(row) {
  const ts = parseDate(row?.create_time);
  if (ts > 0) return true;
  const idLike = str(row?.id) || str(row?.account_uid) || str(row?.uid) || str(row?.unique_id);
  return /^\d+$/.test(idLike);
}

function getRowIdentity(row, fallback = "") {
  const id = str(row?.id);
  if (id) return id;
  const accountUid = str(row?.account_uid);
  if (accountUid) return accountUid;
  const uid = str(row?.uid);
  if (uid) return uid;
  const uniqueId = str(row?.unique_id);
  if (uniqueId) return uniqueId;
  const ct = str(row?.create_time);
  const ip = str(row?.ip);
  if (ct || ip) return `${ct}__${ip}`;
  return fallback;
}

function getHeaders() {
  if (!rawRows.length) return [];
  if (headersCacheRowsRef === rawRows && headersCache.length) return headersCache;
  const keys = new Set();
  for (const r of rawRows) {
    for (const k of Object.keys(r || {})) keys.add(k);
  }
  headersCache = [...keys];
  headersCacheRowsRef = rawRows;
  return headersCache;
}

function pickAutoField(headers, preferredPrefix, fallback = "") {
  const found = headers.find((h) => h.startsWith(preferredPrefix));
  return found || fallback;
}

function buildDefaultRules(headers = []) {
  return {
    ...DEFAULT_RULES,
    terminateField: pickAutoField(headers, "q2_11_", ""),
    durationField: headers.includes("duration") ? "duration" : "",
  };
}

function loadRules(headers = []) {
  try {
    const parsed = JSON.parse(localStorage.getItem(RULES_KEY) || "{}");
    currentRules = { ...buildDefaultRules(headers), ...parsed };
  } catch {
    currentRules = buildDefaultRules(headers);
  }
  if (!Array.isArray(currentRules.requiredFields)) currentRules.requiredFields = [];
}

function saveRules() {
  localStorage.setItem(RULES_KEY, JSON.stringify(currentRules));
}

function mergeSingleConfig(base, incoming) {
  const out = JSON.parse(JSON.stringify(base));
  for (const [q, cfg] of Object.entries(incoming || {})) {
    if (!cfg || typeof cfg !== "object") continue;
    const title = str(cfg.title || out[q]?.title || q);
    const labels = {};
    for (const [k, v] of Object.entries(cfg.labels || {})) {
      if (str(k) === "" || str(v) === "") continue;
      labels[String(k)] = String(v);
    }
    out[q] = { title, labels };
  }
  return out;
}

function loadSingleConfig() {
  try {
    const parsed = JSON.parse(localStorage.getItem(SINGLE_CONFIG_KEY) || "{}");
    singleConfig = mergeSingleConfig(DEFAULT_SINGLE_CONFIG, parsed);
  } catch {
    singleConfig = JSON.parse(JSON.stringify(DEFAULT_SINGLE_CONFIG));
  }
}

function saveSingleConfig() {
  localStorage.setItem(SINGLE_CONFIG_KEY, JSON.stringify(singleConfig));
}

function loadOpenConfig() {
  try {
    const parsed = JSON.parse(localStorage.getItem(OPEN_CONFIG_KEY) || "{}");
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      openConfig = { ...openConfig, ...parsed };
    }
  } catch {}
}

function saveOpenConfig() {
  localStorage.setItem(OPEN_CONFIG_KEY, JSON.stringify(openConfig));
}

function loadFrameworkConfig() {
  try {
    const parsed = JSON.parse(localStorage.getItem(FRAMEWORK_CONFIG_KEY) || "{}");
    if (!parsed || typeof parsed !== "object") return;
    frameworkConfig = {
      raw: str(parsed.raw || ""),
      items: Array.isArray(parsed.items) ? parsed.items : [],
      updatedAt: str(parsed.updatedAt || ""),
    };
  } catch {}
}

function saveFrameworkConfig() {
  localStorage.setItem(FRAMEWORK_CONFIG_KEY, JSON.stringify(frameworkConfig));
}

function loadWeightConfig() {
  try {
    const parsed = JSON.parse(localStorage.getItem(WEIGHT_CONFIG_KEY) || "{}");
    if (!parsed || typeof parsed !== "object") return;
    weightConfig = {
      ...weightConfig,
      ...parsed,
      enabled: !!parsed.enabled,
      dims: Array.isArray(parsed.dims) ? parsed.dims : weightConfig.dims,
      targetsText: str(parsed.targetsText || weightConfig.targetsText),
    };
  } catch {}
}

function saveWeightConfig() {
  localStorage.setItem(WEIGHT_CONFIG_KEY, JSON.stringify(weightConfig));
}

function loadWeightHistory() {
  try {
    const parsed = JSON.parse(localStorage.getItem(WEIGHT_HISTORY_KEY) || "[]");
    weightHistory = Array.isArray(parsed) ? parsed : [];
  } catch {
    weightHistory = [];
  }
}

function saveWeightHistory() {
  localStorage.setItem(WEIGHT_HISTORY_KEY, JSON.stringify(weightHistory.slice(0, 10)));
}

function fmtPct1(n) {
  const v = Number(n);
  if (!Number.isFinite(v)) return "--";
  return `${(v * 100).toFixed(1)}%`;
}

function renderWeightModeSwitch() {
  const off = document.getElementById("btnWeightOff");
  const on = document.getElementById("btnWeightOn");
  if (!off || !on) return;
  const enabled = isWeightEnabled();
  off.classList.toggle("active", !enabled);
  on.classList.toggle("active", enabled);
}

function calcSampleDistByGroup(plan, dim, values) {
  const out = {
    community: { total: 0, counts: {} },
    non_community: { total: 0, counts: {} },
  };
  values.forEach((v) => {
    out.community.counts[v] = 0;
    out.non_community.counts[v] = 0;
  });
  for (const row of analysisRows) {
    const group = isCommunityUser(row, plan) ? "community" : "non_community";
    const key = getDimValue(row, dim, plan);
    out[group].total += 1;
    if (key in out[group].counts) out[group].counts[key] += 1;
  }
  return out;
}

function getWeightDimCandidates() {
  return [
    { id: "gender", name: "性别（男 / 女 / 未知）" },
    { id: "age", name: "年龄（19-25 / 26-30 / 31-35 / 35+ / 未知）" },
    { id: "adventure", name: "冒险等级2（0-30 / 31-45 / 46-60 / 未知）" },
  ];
}

function buildManualWeightGrid(plan) {
  const grid = document.getElementById("weightManualGrid");
  if (!grid) return;
  const gt = plan?.groupTargets || {};
  const gC = gt.community || {};
  const gN = gt.non_community || {};

  const blocks = Object.entries(WEIGHT_DIM_VALUES)
    .filter(([dim]) => dim !== "community_active")
    .map(([dim, values]) => {
    const sample = calcSampleDistByGroup(plan, dim, values);
    const rows = values
      .map((v) => {
        const c = Number(gC?.[dim]?.[v] ?? 0);
        const n = Number(gN?.[dim]?.[v] ?? 0);
        const sc = sample.community.total > 0 ? sample.community.counts[v] / sample.community.total : NaN;
        const sn = sample.non_community.total > 0 ? sample.non_community.counts[v] / sample.non_community.total : NaN;
        return `<tr>
          <td>${v}</td>
          <td>${fmtPct1(sc)}</td>
          <td><input data-weight-cell="1" data-dim="${dim}" data-group="community" data-key="${v}" type="number" step="0.0001" value="${Number.isFinite(c) ? c : 0}" /></td>
          <td>${fmtPct1(sn)}</td>
          <td><input data-weight-cell="1" data-dim="${dim}" data-group="non_community" data-key="${v}" type="number" step="0.0001" value="${Number.isFinite(n) ? n : 0}" /></td>
        </tr>`;
      })
      .join("");
    return `
      <div class="table-wrap" style="max-height:none;margin-bottom:8px;">
        <table>
          <thead>
            <tr><th colspan="5">${WEIGHT_DIM_LABELS[dim] || dim}</th></tr>
            <tr><th>分层</th><th>样本(社区)</th><th>大盘(社区)</th><th>样本(非社区)</th><th>大盘(非社区)</th></tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
    });
  grid.innerHTML = blocks.join("");

  const penNode = document.getElementById("weightManualPenetration");
  if (penNode) {
    penNode.value = String(FIXED_COMMUNITY_PENETRATION);
  }
}

function buildPlanFromManualInputs() {
  const plan = {
    mode: "two_stage",
    communityField: "近42天内社区活跃",
    communityYesCodes: ["1"],
    communityNoCodes: ["0"],
    genderField: "性别-BI",
    communityPenetration: FIXED_COMMUNITY_PENETRATION,
    groupTargets: { community: {}, non_community: {} },
    adventureTierField: "冒险等阶2分级-BI",
    adventureField: "游戏等级-BI",
    spendField: "累计付费",
  };
  const cells = [...document.querySelectorAll("[data-weight-cell='1']")];
  for (const cell of cells) {
    const dim = str(cell.getAttribute("data-dim"));
    const group = str(cell.getAttribute("data-group"));
    const key = str(cell.getAttribute("data-key"));
    const val = Number(cell.value || 0);
    if (!dim || !group || !key || !Number.isFinite(val) || val < 0) continue;
    if (!plan.groupTargets[group][dim]) plan.groupTargets[group][dim] = {};
    plan.groupTargets[group][dim][key] = val;
  }
  return plan;
}

function parseWeightTargets(text) {
  const raw = str(text);
  if (!raw) return {};
  const parsed = JSON.parse(raw);
  const out = {};
  for (const [qid, map] of Object.entries(parsed || {})) {
    if (!/^q\d+$/.test(qid) || !map || typeof map !== "object") continue;
    const t = {};
    for (const [code, val] of Object.entries(map)) {
      const num = Number(val);
      if (!Number.isFinite(num) || num <= 0) continue;
      t[String(code)] = num;
    }
    if (Object.keys(t).length) out[qid] = t;
  }
  return out;
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function parseWeightPlan(text) {
  const raw = str(text);
  if (!raw) return {};
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object") return {};
  return parsed;
}

function normKey(v) {
  return str(v).toLowerCase().replace(/\s+/g, "").replace(/[：:]/g, "");
}

function ageBucketFromQ33(code) {
  const n = Number(code);
  if (!Number.isFinite(n)) return "未知";
  if (n >= 16 && n <= 22) return "19-25";
  if (n >= 11 && n <= 15) return "26-30";
  if (n >= 6 && n <= 10) return "31-35";
  if (n <= 5) return "35+";
  return "未知";
}

function adventureBucketFromValue(v) {
  const n = Number(str(v).replace(/[^\d]/g, ""));
  if (!Number.isFinite(n)) return "未知";
  if (n <= 30) return "0-30";
  if (n <= 45) return "31-45";
  if (n <= 60) return "46-60";
  return "未知";
}

function adventureTierBucketFromText(v) {
  const t = str(v);
  if (!t) return "";
  if (t.includes("0-30") || t.includes("0~30")) return "0-30";
  if (t.includes("31-45") || t.includes("31~45") || t.includes("31-44") || t.includes("31~44")) return "31-45";
  if (t.includes("46-60") || t.includes("46~60") || t.includes("45-60") || t.includes("45~60")) return "46-60";
  if (/未知|null|na|n\/a/i.test(t)) return "未知";
  return "";
}

function spendBucketFromValue(v) {
  const t = str(v);
  if (!t) return "未知";
  const n = Number(t);
  if (Number.isFinite(n)) {
    if (n === 1) return "大R";
    if (n === 2) return "中R";
    if (n === 3) return "小R";
    if (n === 4 || n === 0) return "无付费";
  }
  if (/大\s*R|大r|高消费|高付费/.test(t)) return "大R";
  if (/中\s*R|中r|中消费|中付费/.test(t)) return "中R";
  if (/小\s*R|小r|低消费|低付费/.test(t)) return "小R";
  if (/无付费|未付费|无充值|零付费|不付费/.test(t)) return "无付费";
  return "未知";
}

function readFirstExistingField(row, candidates) {
  for (const f of candidates) {
    const key = str(f);
    if (!key) continue;
    if (key in row && str(row[key]) !== "") return row[key];
  }
  return "";
}

function isCommunityUser(row, plan) {
  const normFlag = (v) => {
    const t = str(v).toLowerCase();
    if (t === "") return "";
    if (t === "1" || t === "1.0" || t === "是" || t === "有" || t === "true") return "1";
    if (t === "0" || t === "0.0" || t === "否" || t === "无" || t === "false") return "0";
    return t;
  };
  const preferredField = str(plan.communityField || "近42天内社区活跃");
  const fallbackField = "q27";
  const field = preferredField && preferredField in row ? preferredField : fallbackField;
  const yesDefault = field === "q27" ? ["1", "2", "3", "4"] : ["1"];
  const noDefault = field === "q27" ? ["5"] : ["0"];
  const yes = new Set((plan.communityYesCodes || yesDefault).map((x) => normFlag(x)));
  const no = new Set((plan.communityNoCodes || noDefault).map((x) => normFlag(x)));
  const code = normFlag(row[field]);
  if (yes.has(code)) return true;
  if (no.has(code)) return false;
  return code !== "";
}

function resolvePenetration(plan) {
  if (str(plan?.mode) === "two_stage") {
    return { community: FIXED_COMMUNITY_PENETRATION, non_community: 1 - FIXED_COMMUNITY_PENETRATION };
  }
  const direct = Number(plan?.communityPenetration);
  if (Number.isFinite(direct) && direct > 0 && direct < 1) {
    return { community: direct, non_community: 1 - direct };
  }
  const pen = plan?.penetration || {};
  const c = Number(pen.community);
  const n = Number(pen.non_community);
  if (Number.isFinite(c) && Number.isFinite(n) && c > 0 && n > 0) {
    return { community: c / (c + n), non_community: n / (c + n) };
  }
  return null;
}

function getDimValue(row, dim, plan) {
  const d = str(dim);
  if (d === "gender") {
    const biRaw = readFirstExistingField(row, [plan.genderField, "性别-BI", "性别", "gender_bi", "gender"]);
    const bi = normalizeBiGender(biRaw);
    if (bi) return bi;
    const v = getQuestionnaireGenderValue(row);
    if (v === "男" || v === "女" || v === "未知") return v;
    return "未知";
  }
  if (d === "age") return ageBucketFromQ33(str(row.q33));
  if (d === "community_active") return isCommunityUser(row, plan) ? "有" : "无";
  if (d === "adventure") {
    const tierRaw = readFirstExistingField(row, [plan.adventureTierField, "冒险等阶2分级-BI", "冒险等级2-BI", "冒险等级2"]);
    const tier = adventureTierBucketFromText(tierRaw);
    if (tier) return tier;
    const raw = readFirstExistingField(row, [plan.adventureField, "游戏等级-BI", "游戏等级", "level", "adventure_level", "ar_level", "q36"]);
    return adventureBucketFromValue(raw);
  }
  if (d === "spend") {
    const raw = readFirstExistingField(row, [plan.spendField, "累计付费", "spend_level", "consume_level", "pay_level", "charge_level", "r_level", "q37"]);
    return spendBucketFromValue(raw);
  }
  if (/^q\d+$/.test(d)) {
    const code = str(row[d]);
    const label = getSingleLabel(d, code);
    return label || code;
  }
  return "";
}

function rakeRows(rows, targetsByDim, dims, plan, maxIter, capMin, capMax) {
  if (!rows.length) return;
  for (let iter = 0; iter < maxIter; iter += 1) {
    for (const dim of dims) {
      const targets = targetsByDim[dim];
      if (!targets || typeof targets !== "object") continue;
      const targetKeys = Object.keys(targets);
      if (!targetKeys.length) continue;
      const targetMap = Object.fromEntries(targetKeys.map((k) => [normKey(k), Number(targets[k])]));
      const current = {};
      let currentTotal = 0;
      for (const k of Object.keys(targetMap)) current[k] = 0;

      for (const row of rows) {
        const v = normKey(getDimValue(row, dim, plan));
        if (!(v in current)) continue;
        const w = getRowWeight(row);
        current[v] += w;
        currentTotal += w;
      }
      if (currentTotal <= 0) continue;
      const tSum = Object.values(targetMap).reduce((s, x) => s + (Number.isFinite(x) && x > 0 ? x : 0), 0);
      if (tSum <= 0) continue;

      const factors = {};
      for (const k of Object.keys(targetMap)) {
        const desired = (targetMap[k] / tSum) * currentTotal;
        const cur = current[k];
        factors[k] = cur > 0 ? desired / cur : 1;
      }

      for (const row of rows) {
        const v = normKey(getDimValue(row, dim, plan));
        if (!(v in factors)) continue;
        row.__w = clamp(getRowWeight(row) * factors[v], capMin, capMax);
      }
    }
  }
}

function applyWeighting() {
  for (const r of analysisRows) r.__w = 1;
  if (!analysisRows.length || !weightConfig.enabled) {
    weightState = { applied: false, message: "未启用" };
    return;
  }

  let plan = {};
  try {
    plan = parseWeightPlan(weightConfig.targetsText);
  } catch {
    weightState = { applied: false, message: "目标分布 JSON 解析失败，已回退未加权" };
    return;
  }
  const maxIter = Math.max(1, Number(weightConfig.maxIter || 24));
  const capMin = Math.max(0.01, Number(weightConfig.capMin || 0.2));
  const capMax = Math.max(capMin + 0.01, Number(weightConfig.capMax || 5));

  if (str(plan.mode) !== "two_stage") {
    const targetsByQ = parseWeightTargets(weightConfig.targetsText);
    const dims = [...FIXED_WEIGHT_DIMS].filter((q) => targetsByQ[q]);
    if (!dims.length) {
      weightState = { applied: false, message: "未配置有效加权维度，已回退未加权" };
      return;
    }
    rakeRows(analysisRows, targetsByQ, dims, plan, maxIter, capMin, capMax);
    const totalSimple = sumWeights(analysisRows);
    if (totalSimple > 0) {
      const meanSimple = totalSimple / analysisRows.length;
      for (const r of analysisRows) r.__w = getRowWeight(r) / meanSimple;
    }
    weightState = { applied: true, message: `已加权（Raking）维度：${dims.join(", ")}` };
    return;
  }

  const groupTargets = plan.groupTargets || {};
  const dims = [...FIXED_WEIGHT_DIMS].filter((d) => d in (groupTargets.community || {}) || d in (groupTargets.non_community || {}));
  if (!dims.length) {
    weightState = { applied: false, message: "两阶段加权未配置维度，已回退未加权" };
    return;
  }
  const commRows = analysisRows.filter((r) => isCommunityUser(r, plan));
  const nonRows = analysisRows.filter((r) => !isCommunityUser(r, plan));

  rakeRows(commRows, groupTargets.community || {}, dims, plan, maxIter, capMin, capMax);
  rakeRows(nonRows, groupTargets.non_community || {}, dims, plan, maxIter, capMin, capMax);

  const resolvedPen = resolvePenetration(plan);
  if (resolvedPen) {
    const pComm = resolvedPen.community;
    const pNon = resolvedPen.non_community;
    const total = commRows.length + nonRows.length;
    const desiredComm = pComm * total;
    const desiredNon = pNon * total;
    const curComm = sumWeights(commRows);
    const curNon = sumWeights(nonRows);
    const fComm = curComm > 0 ? desiredComm / curComm : 1;
    const fNon = curNon > 0 ? desiredNon / curNon : 1;
    for (const r of commRows) r.__w = clamp(getRowWeight(r) * fComm, capMin, capMax);
    for (const r of nonRows) r.__w = clamp(getRowWeight(r) * fNon, capMin, capMax);
  }

  const total = sumWeights(analysisRows);
  if (total > 0) {
    const mean = total / analysisRows.length;
    for (const r of analysisRows) r.__w = getRowWeight(r) / mean;
  }
  weightState = { applied: true, message: `已加权（两阶段）维度：${dims.join(", ")}；社区样本 ${commRows.length}，非社区样本 ${nonRows.length}` };
}

function getSingleLabels(qid) {
  return (singleConfig[qid] && singleConfig[qid].labels) || {};
}

function getSingleTitle(qid, fallback = qid) {
  return (singleConfig[qid] && singleConfig[qid].title) || fallback;
}

function normalizeBiGender(raw) {
  const t = str(raw);
  if (!t) return "";
  if (t === "1" || t.includes("男")) return "男";
  if (t === "2" || t.includes("女")) return "女";
  if (t === "0" || t === "3" || /未知|不详|null|na|n\/a/i.test(t)) return "未知";
  return "未知";
}

function normalizeQuestionnaireGender(raw) {
  const t = str(raw).trim();
  if (!t) return "";
  const n = t.toLowerCase().replace(/\s+/g, "");
  if (t === "1" || /^(male|man|m)$/i.test(t) || /^(male|man|m)$/i.test(n) || t.includes("男")) return "男";
  if (t === "2" || /^(female|woman|f)$/i.test(t) || /^(female|woman|f)$/i.test(n) || t.includes("女")) return "女";
  if (
    t === "3" ||
    /不方便透露|保密|未知|其他/.test(t) ||
    /^(unknown|other|others|prefernottosay|rathernotsay|notdisclose|notspecified|unspecified|na|n\/a|null)$/i.test(n)
  ) {
    return "未知";
  }
  return "未知";
}

function getBiGenderField() {
  if (biGenderFieldCacheRowsRef === rawRows && biGenderFieldCache) return biGenderFieldCache;
  const headers = getHeaders();
  if (headers.includes("性别")) {
    biGenderFieldCache = "性别";
    biGenderFieldCacheRowsRef = rawRows;
    return biGenderFieldCache;
  }
  biGenderFieldCache = headers.find((h) => /性别|gender|sex/i.test(h) && h !== "q34") || "";
  biGenderFieldCacheRowsRef = rawRows;
  return biGenderFieldCache;
}

function getBiGenderValue(row) {
  const field = getBiGenderField();
  if (!field) return "";
  return normalizeBiGender(row[field]);
}

function getQuestionnaireGenderValue(row) {
  const raw = str(row.q34);
  const labeled = getSingleLabel("q34", raw);
  return normalizeQuestionnaireGender(labeled || raw);
}

function getCurrentWeightPlan() {
  const raw = str(weightConfig.targetsText || "");
  if (weightPlanCacheText === raw && weightPlanCacheObj) return weightPlanCacheObj;
  try {
    const parsed = parseWeightPlan(raw);
    if (parsed && typeof parsed === "object") {
      weightPlanCacheText = raw;
      weightPlanCacheObj = parsed;
      return parsed;
    }
  } catch {}
  weightPlanCacheText = "";
  weightPlanCacheObj = DEFAULT_WEIGHT_PLAN;
  return DEFAULT_WEIGHT_PLAN;
}

function getBiAdventureValue(row) {
  const plan = getCurrentWeightPlan();
  return getDimValue(row, "adventure", plan);
}

function getBiSpendValue(row) {
  const plan = getCurrentWeightPlan();
  return getDimValue(row, "spend", plan);
}

function qidOrder(id) {
  const m = String(id || "").match(/^q(\d+)/i);
  return m ? Number(m[1]) : Number.POSITIVE_INFINITY;
}

function sortByQid(a, b) {
  const qa = qidOrder(a.id);
  const qb = qidOrder(b.id);
  if (qa !== qb) return qa - qb;
  return String(a.name || a.id).localeCompare(String(b.name || b.id), "zh-CN");
}

function getAnalysisQuestions() {
  const optionDefs = getOptionQuestionDefs();
  const optionIds = new Set(optionDefs.map((x) => x.id));

  const singleDefs = Object.keys(singleConfig)
    .filter((qid) => /^q\d+$/.test(qid))
    .map((qid) => ({
      id: qid,
      name: getSingleTitle(qid, qid),
      type: "single",
      col: qid,
      labels: getSingleLabels(qid),
    }))
    .filter((x) => Object.keys(x.labels || {}).length > 0)
    .filter((x) => !optionIds.has(x.id))
    .sort((a, b) => Number(a.id.replace("q", "")) - Number(b.id.replace("q", "")));

  const derivedDefs = [];
  if (getBiGenderField()) {
    derivedDefs.push({
      id: "bi_gender",
      name: "BI｜性别",
      type: "single_derived",
      labels: { 男: "男", 女: "女" },
      valueFn: (row) => {
        const v = getBiGenderValue(row);
        return v === "男" || v === "女" ? v : "";
      },
    });
  }
  derivedDefs.push(
    {
      id: "bi_adventure",
      name: "BI｜冒险等级2",
      type: "single_derived",
      labels: { "0-30": "0-30", "31-45": "31-45", "46-60": "46-60", 未知: "未知" },
      valueFn: (row) => getBiAdventureValue(row),
    },
    {
      id: "bi_spend",
      name: "BI｜付费等级",
      type: "single_derived",
      labels: { 大R: "大R", 中R: "中R", 小R: "小R", 无付费: "无付费", 未知: "未知" },
      valueFn: (row) => getBiSpendValue(row),
    },
  );

  return [...optionDefs, ...singleDefs, ...derivedDefs].sort(sortByQid);
}

function getAttrQuestions() {
  const base = [];
  const biGenderField = getBiGenderField();
  if (biGenderField) {
    base.push({
      id: "bi_gender",
      name: "用户属性｜性别（BI）",
      type: "single_derived",
      labels: { 男: "男", 女: "女", 未知: "未知" },
      valueFn: (row) => getBiGenderValue(row),
    });
  }
  base.push({
    id: "q34_survey",
    name: "用户属性｜性别（问卷）",
    type: "single_derived",
    labels: { 男: "男", 女: "女", 未知: "未知" },
    valueFn: (row) => getQuestionnaireGenderValue(row),
  });
  base.push(
    { id: "q1", name: `用户属性｜${getSingleTitle("q1", "Q1")}`, type: "single", col: "q1", labels: getSingleLabels("q1") },
    { id: "q27", name: `用户属性｜${getSingleTitle("q27", "Q27")}`, type: "single", col: "q27", labels: getSingleLabels("q27") },
    { id: "q33", name: `用户属性｜${getSingleTitle("q33", "Q33")}`, type: "single", col: "q33", labels: getSingleLabels("q33") },
  );
  const baseIds = new Set(base.map((x) => x.id));
  const questionDims = getAnalysisQuestions()
    .filter((q) => !baseIds.has(q.id))
    .map((q) => ({ ...q, name: `题目｜${q.name}` }));
  return [...base, ...questionDims].sort(sortByQid);
}

function rowMatchGroup(row, def, code) {
  if (!def) return false;
  const codeNum = Number(code);
  if (def.type === "single_derived") {
    return str(def.valueFn ? def.valueFn(row) : "") === String(code);
  }
  if (def.type === "rank_top1") {
    const cols = getOptionColumns(getHeaders(), def.prefix);
    for (const { code: c, col } of cols) {
      const rank = toInt(row[col]);
      if (rank === 1 && c === codeNum) return true;
    }
    return false;
  }
  if (def.type === "multi") {
    const cols = getOptionColumns(getHeaders(), def.prefix, def.id === "q2" ? 9 : Infinity);
    if (def.id === "q2" && codeNum >= 1 && codeNum <= 9 && hasQ2AllDiscuss(row)) {
      return true;
    }
    for (const { code: c, col } of cols) {
      if (c === codeNum && str(row[col]) === "1") return true;
    }
    return false;
  }
  return str(row[def.col]) === String(code);
}

function getFilterOptions() {
  const defs = getAnalysisQuestions();
  const options = [{ id: "all", name: "不过滤", groupKey: "all", mode: "include", fn: () => true }];

  for (const def of defs) {
    const entries = Object.entries(def.labels || {}).sort((a, b) => Number(a[0]) - Number(b[0]));
    for (const [code, label] of entries) {
      if (def.type === "rank_top1") {
        options.push({
          id: `${def.id}:top1=${code}`,
          name: `${def.name} Top1=${label}`,
          groupKey: def.id,
          fn: (r) => rowMatchGroup(r, def, code),
        });
        continue;
      }
      if (def.type === "multi") {
        options.push({
          id: `${def.id}:has=${code}`,
          name: `${def.name} 包含：${label}`,
          groupKey: def.id,
          fn: (r) => rowMatchGroup(r, def, code),
        });
        continue;
      }
      options.push({
        id: `${def.id}=${code}`,
        name: `${def.name}=${label}`,
        groupKey: def.id,
        fn: (r) => rowMatchGroup(r, def, code),
      });
    }
  }

  const q2AnyDiscuss = {
    id: "q2_any=1",
    name: "Q2有任意内容讨论（1-10）",
    groupKey: "q2_any",
    mode: "include",
    fn: (r) => {
      for (let i = 1; i <= 10; i += 1) {
        if (hasQ2ByCode(r, i)) return true;
      }
      return false;
    },
  };
  options.push(q2AnyDiscuss);
  return options;
}

function getOptionQuestionDefs() {
  const headers = getHeaders();
  const questionMap = new Map();
  const re = /^q(\d+)_(\d+)_(.+)$/;

  for (const h of headers) {
    const m = h.match(re);
    if (!m) continue;
    const qid = `q${m[1]}`;
    const code = Number(m[2]);
    const label = m[3];
    if (!questionMap.has(qid)) {
      questionMap.set(qid, { id: qid, options: {}, columns: [] });
    }
    const q = questionMap.get(qid);
    q.options[code] = label;
    q.columns.push(h);
  }

  const defs = [];
  for (const [qid, q] of questionMap.entries()) {
    let labels = Object.fromEntries(
      Object.entries(q.options)
        .sort((a, b) => Number(a[0]) - Number(b[0]))
        .map(([k, v]) => [Number(k), v]),
    );
    // Q2 用于讨论内容分类时，仅保留 1-9 类目；“10=以上都讨论”按展开口径并入 1-9。
    if (qid === "q2") {
      labels = Object.fromEntries(Object.entries(labels).filter(([k]) => Number(k) >= 1 && Number(k) <= 9));
    }
    const isRank = inferRankQuestion(q.columns);
    defs.push({
      id: qid,
      name: getSingleTitle(qid, qid.toUpperCase()),
      type: isRank ? "rank_top1" : "multi",
      prefix: qid,
      labels,
    });
  }

  return defs.sort((a, b) => Number(a.id.replace("q", "")) - Number(b.id.replace("q", "")));
}

function inferRankQuestion(columns) {
  if (!columns.length || !rawRows.length) return false;
  const inspectRows = rawRows.slice(0, 400);
  for (const row of inspectRows) {
    for (const c of columns) {
      const v = str(row[c]);
      if (!v) continue;
      if (v === "0" || v === "1") continue;
      if (v === "-1" || v === " -1") return true;
      const n = toInt(v);
      if (n !== null && n > 1) return true;
    }
  }
  return false;
}

function applySampleRules(rows) {
  const out = [];
  let terminateExcluded = 0;
  let invalidExcluded = 0;

  for (const r of rows) {
    if (
      currentRules.terminateField &&
      str(r[currentRules.terminateField]) !== "" &&
      str(r[currentRules.terminateField]) === str(currentRules.terminateValue)
    ) {
      terminateExcluded += 1;
      continue;
    }

    let invalid = false;
    if (currentRules.enableDuration && currentRules.durationField) {
      const duration = toInt(r[currentRules.durationField]);
      if (duration !== null && duration < Number(currentRules.minDuration || 0)) {
        invalid = true;
      }
    }

    if (!invalid && currentRules.enableRequired && currentRules.requiredFields.length) {
      if (currentRules.requiredFields.some((f) => str(r[f]) === "")) {
        invalid = true;
      }
    }

    if (invalid) {
      invalidExcluded += 1;
      continue;
    }
    out.push(r);
  }

  return {
    rows: out,
    total: rows.length,
    terminateExcluded,
    invalidExcluded,
    final: out.length,
  };
}

function recomputeAnalysisRows() {
  const result = applySampleRules(rawRows);
  analysisRows = result.rows;
  sampleStats = {
    total: result.total,
    terminateExcluded: result.terminateExcluded,
    invalidExcluded: result.invalidExcluded,
    final: result.final,
  };
  applyWeighting();
  if (lastImportStats) {
    lastImportStats = {
      ...lastImportStats,
      dedupRows: rawRows.length,
      terminateExcluded: sampleStats.terminateExcluded,
      invalidExcluded: sampleStats.invalidExcluded,
      analysisRows: isWeightEnabled() ? Number(fmtCount(sumWeights(analysisRows))) : analysisRows.length,
    };
  }
  clearDerivedCaches();
}

function dedupRows(rows) {
  const map = new Map();
  let idx = 0;
  for (const row of rows) {
    if (!isDataRow(row)) continue;
    const id = getRowIdentity(row, `__row_${idx}`);
    idx += 1;
    const prev = map.get(id);
    if (!prev) {
      map.set(id, row);
      continue;
    }
    if (parseDate(row.create_time) >= parseDate(prev.create_time)) {
      map.set(id, row);
    }
  }
  return [...map.values()];
}

function getOptionColumns(headers, prefix, maxCode = Infinity) {
  const result = [];
  const re = new RegExp(`^${prefix}_(\\d+)_`);
  for (const h of headers) {
    const m = h.match(re);
    if (!m) continue;
    const code = Number(m[1]);
    if (code <= maxCode) result.push({ code, col: h });
  }
  return result.sort((a, b) => a.code - b.code);
}

function calcMulti(rows, prefix, labels, maxCode = Infinity) {
  if (!rows.length) return { denominator: 0, items: [] };
  const cols = getOptionColumns(getHeaders(), prefix, maxCode);
  const isQ2 = prefix === "q2";
  const q2AllDiscussCol = isQ2 ? getOptionColumns(getHeaders(), prefix, 10).find((x) => x.code === 10)?.col : "";
  const answered = rows.filter((r) => {
    const hasMain = cols.some((x) => str(r[x.col]) !== "");
    if (!isQ2) return hasMain;
    return hasMain || (q2AllDiscussCol && str(r[q2AllDiscussCol]) !== "");
  });
  const denom = sumWeights(answered);
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;

  for (const r of answered) {
    const w = getRowWeight(r);
    const selected = new Set();
    for (const { code, col } of cols) {
      if (!(code in labels)) continue;
      if (str(r[col]) === "1") selected.add(code);
    }
    // Q2 选“以上都讨论(10)”时，按 1-9 全选口径并入。
    if (isQ2 && q2AllDiscussCol && str(r[q2AllDiscussCol]) === "1") {
      Object.keys(labels).forEach((k) => selected.add(Number(k)));
    }
    for (const code of selected) {
      if (code in labels) counts[code] += w;
    }
  }

  const items = Object.entries(labels).map(([code, name]) => ({
    code: Number(code),
    name,
    count: counts[code] || 0,
    ratio: denom ? (counts[code] || 0) / denom : 0,
  }));
  return { denominator: denom, items };
}

function calcRankTop1(rows, prefix, labels) {
  if (!rows.length) return { denominator: 0, items: [] };
  const cols = getOptionColumns(getHeaders(), prefix);
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;

  let denom = 0;
  for (const r of rows) {
    const ranked = [];
    for (const { code, col } of cols) {
      const rank = toInt(r[col]);
      if (rank && rank > 0) ranked.push({ code, rank });
    }
    if (!ranked.length) continue;
    const w = getRowWeight(r);
    denom += w;
    const top = ranked.find((x) => x.rank === 1);
    if (top && top.code in labels) counts[top.code] += w;
  }

  const items = Object.entries(labels).map(([code, name]) => ({
    code: Number(code),
    name,
    count: counts[code] || 0,
    ratio: denom ? (counts[code] || 0) / denom : 0,
  }));
  return { denominator: denom, items };
}

function calcRankPresence(rows, prefix, labels) {
  if (!rows.length) return { denominator: 0, items: [] };
  const cols = getOptionColumns(getHeaders(), prefix);
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;

  let denom = 0;
  for (const r of rows) {
    let hasAnyRank = false;
    const w = getRowWeight(r);
    for (const { code, col } of cols) {
      const rank = toInt(r[col]);
      if (rank && rank > 0) {
        hasAnyRank = true;
        if (code in labels) counts[code] += w;
      }
    }
    if (hasAnyRank) denom += w;
  }

  const items = Object.entries(labels).map(([code, name]) => ({
    code: Number(code),
    name,
    count: counts[code] || 0,
    ratio: denom ? (counts[code] || 0) / denom : 0,
  }));
  return { denominator: denom, items };
}

function calcSingle(rows, col, labels) {
  const answered = rows.filter((r) => str(r[col]) !== "");
  const denom = sumWeights(answered);
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;
  for (const r of answered) {
    const v = str(r[col]);
    if (v in counts) counts[v] += getRowWeight(r);
  }
  const items = Object.entries(labels).map(([code, name]) => ({
    code: Number(code),
    name,
    count: counts[code] || 0,
    ratio: denom ? (counts[code] || 0) / denom : 0,
  }));
  return { denominator: denom, items };
}

function getQuestionDistribution(rows, def) {
  if (def.type === "single_derived") {
    const labels = def.labels || {};
    const answered = rows.filter((r) => str(def.valueFn ? def.valueFn(r) : "") !== "");
    const denom = sumWeights(answered);
    const counts = {};
    for (const code of Object.keys(labels)) counts[code] = 0;
    for (const r of answered) {
      const v = str(def.valueFn ? def.valueFn(r) : "");
      if (v in counts) counts[v] += getRowWeight(r);
    }
    const items = Object.entries(labels).map(([code, name]) => ({
      code: String(code),
      name,
      count: counts[code] || 0,
      ratio: denom ? (counts[code] || 0) / denom : 0,
    }));
    return { denominator: denom, items };
  }
  if (def.type === "rank_top1") return calcRankTop1(rows, def.prefix, def.labels);
  if (def.type === "multi") {
    const maxCode = def.id === "q2" ? 9 : Infinity;
    return calcMulti(rows, def.prefix, def.labels, maxCode);
  }
  return calcSingle(rows, def.col, def.labels);
}

function drawBarChart(elId, rows, title, opts = {}) {
  const node = document.getElementById(elId);
  if (!node) return;
  const preserveOrder = !!opts.preserveOrder;
  const labels = opts.labels || {};
  const sorted = preserveOrder ? [...rows] : [...rows].sort((a, b) => b.ratio - a.ratio);
  node.classList.add("barlist");
  node.innerHTML = sorted
    .map(
      (x) => `
      <div class="bar-row">
        <div class="bar-name ${getNameHighlightClass(x.name, labels)}" title="${x.name}">${x.name}</div>
        <div class="bar-track"><div class="bar-fill ${getFillHighlightClass(x.name, labels)}" style="width:${(x.ratio * 100).toFixed(1)}%"></div></div>
        <div class="bar-pct ${getPctHighlightClass(x.name, labels)}">${fmtPct(x.ratio)}</div>
      </div>`,
    )
    .join("");
}

function bindChartAutoResize(node, chart) {
  if (chartResizeObservers.has(node)) return;
  const container = node.closest(".chart-card") || node.parentElement || node;
  if (!container || typeof ResizeObserver === "undefined") return;
  const observer = new ResizeObserver(() => {
    requestAnimationFrame(() => chart.resize());
  });
  observer.observe(container);
  chartResizeObservers.set(node, observer);
  bindChartWindowResize();
}

function bindChartWindowResize() {
  if (chartWindowResizeBound || !window.echarts) return;
  chartWindowResizeBound = true;
  window.addEventListener("resize", () => {
    const charts = echarts.getInstanceByDom
      ? [...document.querySelectorAll(".chart")].map((n) => echarts.getInstanceByDom(n)).filter(Boolean)
      : [];
    charts.forEach((c) => c.resize());
  });
}

function topRankLinesHtml(sortedItems, limit = 3, labels = {}) {
  const formatName = (name) => (isCommunityPlatformName(name, labels) ? `<span class="mys-text">${name}</span>` : name || "--");
  return sortedItems
    .slice(0, limit)
    .map(
      (x, idx) => `
      <div class="scene-line top-rank-line rank-${idx + 1}">
        <span class="top-rank-head">
          <span class="top-rank-label" data-rank="${idx + 1}">Top${idx + 1}：</span>
          <span class="top-rank-name">${formatName(x ? x.name : "--")}</span>
        </span>
        <span class="top1-value top-rank-value ${x ? getPctHighlightClass(x.name, labels) : ""}">${x ? fmtPct(x.ratio) : "--"}</span>
      </div>`,
    )
    .join("");
}

function renderOverview() {
  const hintNode = document.getElementById("overviewHint");
  if (hintNode) hintNode.textContent = `数据更新至 ${fmtTime(lastUploadAt)}`;
  const sampleNode = document.getElementById("overviewSampleLine");
  if (sampleNode) sampleNode.textContent = `总样本：${fmtCount(sumWeights(analysisRows))}`;

  const overallLabels = getChannelLabelsForQuestion("q4");
  const communityPlatformName = getCommunityPlatformName(overallLabels);
  const top1 = calcRankTop1(analysisRows, "q4", overallLabels);
  const sortedTop1 = [...top1.items].sort((a, b) => b.ratio - a.ratio);
  const rankNode = document.getElementById("ovOverallTopRanks");
  if (rankNode) rankNode.innerHTML = topRankLinesHtml(sortedTop1, 3, overallLabels);
  const mys = sortedTop1.find((x) => x.name === communityPlatformName);
  const rank = sortedTop1.findIndex((x) => x.name === communityPlatformName);
  const mysExtraNode = document.getElementById("ovOverallMysExtra");
  if (mysExtraNode) {
    if (rank > 2 && mys) {
      mysExtraNode.style.display = "";
      mysExtraNode.innerHTML = `<span class="scene-line top-rank-line rank-extra"><span class="top-rank-head"><span class="top-rank-label">Top${rank + 1}：</span><span class="mys-text">${communityPlatformName}</span></span><span class="top1-value top-rank-value mys-pct">${fmtPct(mys.ratio)}</span></span>`;
    } else {
      mysExtraNode.style.display = "none";
      mysExtraNode.textContent = "";
    }
  }

  drawBarChart("chartTop1", top1.items, "Top1 渠道占比", { labels: overallLabels });
  renderOverviewSceneGrid();
}

function renderOverviewSceneGrid() {
  const grid = document.getElementById("sceneGrid");
  const tooltip = document.getElementById("sceneTooltip");
  if (!grid || !tooltip) return;

  const sceneData = OVERVIEW_SCENES.map((scene) => {
    const channelLabels = getChannelLabelsForQuestion(scene.rank);
    const communityPlatformName = getCommunityPlatformName(channelLabels);
    const top1 = calcRankTop1(analysisRows, scene.rank, channelLabels);
    const top1Sorted = [...top1.items].sort((a, b) => b.ratio - a.ratio);
    const mysRank = top1Sorted.findIndex((x) => x.name === communityPlatformName);
    const mys = top1Sorted.find((x) => x.name === communityPlatformName);
    const channels = calcRankTop1(analysisRows, scene.rank, channelLabels).items.sort((a, b) => b.ratio - a.ratio);
    return {
      ...scene,
      top3: top1Sorted.slice(0, 3),
      mysRank,
      mys,
      channels,
      channelLabels,
      communityPlatformName,
    };
  });

  grid.innerHTML = sceneData
    .map(
      (s, i) => `
      <article class="scene-card" data-scene-index="${i}">
        <div class="scene-title">${s.name}</div>
        ${topRankLinesHtml(s.top3, 3, s.channelLabels)}
        ${s.mys && s.mysRank > 2 ? `<div class="scene-line top-rank-line rank-extra"><span class="top-rank-head"><span class="top-rank-label">Top${s.mysRank + 1}：</span><span class="mys-text">${s.communityPlatformName}</span></span><span class="top1-value top-rank-value mys-pct">${fmtPct(s.mys.ratio)}</span></div>` : ""}
      </article>`,
    )
    .join("");

  const renderTooltip = (scene, x, y) => {
    const bars = scene.channels
      .map(
        (c) => `
        <div class="bar-row">
          <div class="bar-name ${getNameHighlightClass(c.name, scene.channelLabels)}">${c.name}</div>
          <div class="bar-track"><div class="bar-fill ${getFillHighlightClass(c.name, scene.channelLabels)}" style="width:${(c.ratio * 100).toFixed(1)}%"></div></div>
          <div class="bar-pct ${getPctHighlightClass(c.name, scene.channelLabels)}">${fmtPct(c.ratio)}</div>
        </div>`,
      )
      .join("");
    tooltip.innerHTML = `<div class="scene-tip-title">${scene.name} 首选渠道占比</div><div class="bar-rows bar-rows-dark">${bars}</div>`;
    tooltip.style.display = "block";
    const margin = 12;
    const gap = 14;
    const tipW = tooltip.offsetWidth || 320;
    const tipH = tooltip.offsetHeight || 240;
    const vw = window.innerWidth;
    const vh = window.innerHeight;

    let left = x + gap;
    let top = y + gap;

    if (left + tipW + margin > vw) left = x - tipW - gap;
    if (top + tipH + margin > vh) top = y - tipH - gap;

    left = Math.max(margin, Math.min(left, vw - tipW - margin));
    top = Math.max(margin, Math.min(top, vh - tipH - margin));

    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };

  grid.querySelectorAll(".scene-card").forEach((card) => {
    card.addEventListener("mouseenter", (e) => {
      const i = Number(card.dataset.sceneIndex || 0);
      renderTooltip(sceneData[i], e.clientX, e.clientY);
    });
    card.addEventListener("mousemove", (e) => {
      const i = Number(card.dataset.sceneIndex || 0);
      renderTooltip(sceneData[i], e.clientX, e.clientY);
    });
    card.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
    });
  });
}

function setSelectOptions(selectId, options) {
  const node = document.getElementById(selectId);
  node.innerHTML = "";
  options.forEach((opt) => {
    const o = document.createElement("option");
    o.value = opt.id;
    o.textContent = opt.name;
    node.appendChild(o);
  });
}

function getSelectedFilterDefs() {
  const filterOptions = getFilterOptions();
  const node = document.getElementById("analysisFilter");
  const selected = [...node.selectedOptions].map((x) => x.value);
  const mode = getCurrentFilterMode();
  if (!selected.length || (selected.length === 1 && selected[0] === "all")) {
    return [filterOptions[0]];
  }
  return selected
    .filter((id) => id !== "all")
    .map((id) => filterOptions.find((x) => x.id === id))
    .map((x) => (x ? { ...x, mode } : x))
    .filter(Boolean);
}

function getCurrentFilterMode() {
  const active = document.querySelector("#analysisFilterMode .mode-btn.active");
  return active?.dataset.mode === "exclude" ? "exclude" : "include";
}

function setFilterMode(mode) {
  const node = document.getElementById("analysisFilterMode");
  if (!node) return;
  node.querySelectorAll(".mode-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  });
}

function applyGroupedFilters(rows, filterDefs) {
  if (!filterDefs.length || filterDefs[0]?.id === "all") return rows;
  const includeGroups = new Map();
  const excludeGroups = new Map();
  for (const def of filterDefs) {
    const key = def.groupKey || def.id;
    const target = def.mode === "exclude" ? excludeGroups : includeGroups;
    if (!target.has(key)) target.set(key, []);
    target.get(key).push(def);
  }

  // SPSS 多重响应思路：
  // 包含筛选：同一题内 OR，不同题之间 AND
  // 排除筛选：同一题内 OR 后整体取反
  return rows.filter((row) => {
    for (const defs of includeGroups.values()) {
      if (!defs.some((d) => d.fn(row))) return false;
    }
    for (const defs of excludeGroups.values()) {
      if (defs.some((d) => d.fn(row))) return false;
    }
    return true;
  });
}

function getSingleLabel(qid, code) {
  const labels = getSingleLabels(qid);
  return labels[String(code)] || String(code || "");
}

function downloadTextFile(filename, text, mime = "text/plain;charset=utf-8") {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportCrossTableCsv() {
  const table = document.getElementById("crossTable");
  if (!table) return;
  const rows = [...table.querySelectorAll("tr")];
  if (!rows.length) {
    alert("当前没有可导出的交叉结果");
    return;
  }

  const lines = rows.map((tr) => {
    const cells = [...tr.querySelectorAll("th,td")].map((cell) => {
      const val = (cell.textContent || "").replace(/\\r?\\n/g, " ").trim();
      return `"${val.replace(/"/g, '""')}"`;
    });
    return cells.join(",");
  });
  downloadTextFile(`cross-analysis-${Date.now()}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

function csvCell(v) {
  const val = String(v ?? "").replace(/\r?\n/g, " ").trim();
  return `"${val.replace(/"/g, '""')}"`;
}

function buildFullCrossSegments() {
  const segments = [];
  segments.push({ key: "all", name: "全部", fn: () => true });

  const genderSegments = [
    { key: "male_bi", name: "性别(BI)-男", fn: (r) => getBiGenderValue(r) === "男" },
    { key: "female_bi", name: "性别(BI)-女", fn: (r) => getBiGenderValue(r) === "女" },
  ];
  segments.push(...genderSegments);

  const ageSegments = [
    { key: "19_25", name: "年龄-19-25", fn: (r) => ageBucketFromQ33(str(r.q33)) === "19-25" },
    { key: "26_30", name: "年龄-26-30", fn: (r) => ageBucketFromQ33(str(r.q33)) === "26-30" },
    { key: "31_35", name: "年龄-31-35", fn: (r) => ageBucketFromQ33(str(r.q33)) === "31-35" },
    { key: "35_plus", name: "年龄-35+", fn: (r) => ageBucketFromQ33(str(r.q33)) === "35+" },
    { key: "age_unknown", name: "年龄-未知", fn: (r) => ageBucketFromQ33(str(r.q33)) === "未知" },
  ];
  segments.push(...ageSegments);

  const q2Segments = Object.entries(Q2_CATEGORIES).map(([code, label]) => {
    const c = Number(code);
    return {
      key: `q2_${c}`,
      name: `Q2-${label}`,
      fn: (r) => hasQ2ByCode(r, c) || hasQ2AllDiscuss(r),
    };
  });
  segments.push(...q2Segments);

  return segments;
}

function exportFullCrossTableCsv() {
  const questions = getAnalysisQuestions();
  if (!questions.length) {
    alert("暂无可导出的题目");
    return;
  }
  if (!analysisRows.length) {
    alert("暂无样本数据");
    return;
  }

  const segments = buildFullCrossSegments();
  const segRows = segments.map((s) => ({ ...s, rows: analysisRows.filter(s.fn) }));
  const header = ["题目", "选项"];
  segRows.forEach((s) => {
    const n = fmtCount(sumWeights(s.rows));
    header.push(`${s.name}-计数(N=${n})`);
    header.push(`${s.name}-%(N=${n})`);
  });
  const lines = [header.map(csvCell).join(",")];

  const nRow = ["分组样本量", ""];
  segRows.forEach((s) => {
    nRow.push(String(fmtCount(sumWeights(s.rows))));
    nRow.push("");
  });
  lines.push(nRow.map(csvCell).join(","));

  for (const q of questions) {
    const entries = Object.entries(q.labels || {}).sort((a, b) => Number(a[0]) - Number(b[0]));
    if (!entries.length) continue;

    const segDist = segRows.map((s) => {
      const dist = getQuestionDistribution(s.rows, q);
      return {
        denominator: dist.denominator,
        byCode: new Map(dist.items.map((x) => [String(x.code), x])),
      };
    });

    entries.forEach(([code, label], idx) => {
      const row = [
        idx === 0 ? q.name : "",
        label,
      ];
      segDist.forEach((d) => {
        const it = d.byCode.get(String(code));
        row.push(it ? fmtCount(it.count) : "0");
        row.push(it ? fmtPct(it.ratio) : "--");
      });
      lines.push(row.map(csvCell).join(","));
    });

    const qTotalRow = [
      "",
      "总计(作答n)",
    ];
    segDist.forEach((d) => {
      qTotalRow.push(fmtCount(d.denominator));
      qTotalRow.push("");
    });
    lines.push(qTotalRow.map(csvCell).join(","));
  }

  downloadTextFile(`full-cross-${Date.now()}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

function exportOverviewCsv() {
  if (!analysisRows.length) {
    alert("暂无可导出的总览数据");
    return;
  }
  const lines = [];
  lines.push(["模块", "子模块", "指标", "计数", "百分比/值"].map(csvCell).join(","));
  lines.push(["总览", "元信息", "样本量", fmtCount(sumWeights(analysisRows)), ""].map(csvCell).join(","));
  lines.push(["总览", "元信息", "数据更新时间", "", fmtTime(lastUploadAt)].map(csvCell).join(","));
  lines.push(["总览", "元信息", "是否加权", "", isWeightEnabled() ? "是" : "否"].map(csvCell).join(","));

  const overallLabels = getChannelLabelsForQuestion("q4");
  const overall = calcRankTop1(analysisRows, "q4", overallLabels);
  const overallSorted = [...overall.items].sort((a, b) => b.ratio - a.ratio);
  overallSorted.forEach((x, i) => {
    lines.push(["整体讨论心智首选", "Q4", `Top${i + 1}-${x.name}`, fmtCount(x.count), fmtPct(x.ratio)].map(csvCell).join(","));
  });

  OVERVIEW_SCENES.forEach((scene) => {
    const sceneLabels = getChannelLabelsForQuestion(scene.rank);
    const top1 = calcRankTop1(analysisRows, scene.rank, sceneLabels);
    const sorted = [...top1.items].sort((a, b) => b.ratio - a.ratio);
    sorted.forEach((x, i) => {
      lines.push([scene.name, scene.rank.toUpperCase(), `Top${i + 1}-${x.name}`, fmtCount(x.count), fmtPct(x.ratio)].map(csvCell).join(","));
    });
  });

  downloadTextFile(`overview-${Date.now()}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

function exportGenderProfileCsv() {
  if (!analysisRows.length) {
    alert("暂无可导出的样本数据");
    return;
  }

  const rowDefs = [
    { name: "性别-总计", fn: () => true },
    { name: "性别-男", fn: (r) => getBiGenderValue(r) === "男" || str(r.q34) === "1" },
    { name: "性别-女", fn: (r) => getBiGenderValue(r) === "女" || str(r.q34) === "2" },
  ];

  const colDefs = [
    { name: "总计", fn: () => true },
    { name: "性别-男", fn: (r) => getBiGenderValue(r) === "男" || str(r.q34) === "1" },
    { name: "性别-女", fn: (r) => getBiGenderValue(r) === "女" || str(r.q34) === "2" },
    { name: "年龄-19-25", fn: (r) => ageBucketFromQ33(str(r.q33)) === "19-25" },
    { name: "年龄-26-30", fn: (r) => ageBucketFromQ33(str(r.q33)) === "26-30" },
    { name: "年龄-31-35", fn: (r) => ageBucketFromQ33(str(r.q33)) === "31-35" },
    { name: "年龄-35+", fn: (r) => ageBucketFromQ33(str(r.q33)) === "35+" },
    { name: "抽卡决策/原石/强度对比", fn: (r) => hasQ2ByCode(r, 1) || hasQ2AllDiscuss(r) },
    { name: "角色配队/养成/手法", fn: (r) => hasQ2ByCode(r, 2) || hasQ2AllDiscuss(r) },
    { name: "探索解谜攻略", fn: (r) => hasQ2ByCode(r, 3) || hasQ2AllDiscuss(r) },
    { name: "剧情解析", fn: (r) => hasQ2ByCode(r, 4) || hasQ2AllDiscuss(r) },
    { name: "剧情讨论", fn: (r) => hasQ2ByCode(r, 5) || hasQ2AllDiscuss(r) },
    { name: "同人", fn: (r) => hasQ2ByCode(r, 6) || hasQ2AllDiscuss(r) },
    { name: "游戏外活动", fn: (r) => hasQ2ByCode(r, 7) || hasQ2AllDiscuss(r) },
    { name: "周边", fn: (r) => hasQ2ByCode(r, 8) || hasQ2AllDiscuss(r) },
    { name: "Cosplay", fn: (r) => hasQ2ByCode(r, 9) || hasQ2AllDiscuss(r) },
  ];

  const colRows = colDefs.map((c) => ({ ...c, rows: analysisRows.filter(c.fn) }));
  const colTotals = colRows.map((c) => sumWeights(c.rows));

  const header1 = ["行维度"];
  const header2 = [""];
  colDefs.forEach((c) => {
    header1.push(c.name, c.name);
    header2.push("计数", "百分比");
  });
  const lines = [
    header1.map(csvCell).join(","),
    header2.map(csvCell).join(","),
  ];

  rowDefs.forEach((rowDef) => {
    const row = [rowDef.name];
    colRows.forEach((c, idx) => {
      const inter = c.rows.filter(rowDef.fn);
      const cnt = sumWeights(inter);
      const denom = colTotals[idx];
      row.push(fmtCount(cnt));
      row.push(denom > 0 ? fmtPct(cnt / denom) : "--");
    });
    lines.push(row.map(csvCell).join(","));
  });

  downloadTextFile(`gender-profile-${Date.now()}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

function exportAgeProfileCsv() {
  if (!analysisRows.length) {
    alert("暂无可导出的样本数据");
    return;
  }

  const rowDefs = [
    { name: "年龄-19-25", fn: (r) => ageBucketFromQ33(str(r.q33)) === "19-25" },
    { name: "年龄-26-30", fn: (r) => ageBucketFromQ33(str(r.q33)) === "26-30" },
    { name: "年龄-31-35", fn: (r) => ageBucketFromQ33(str(r.q33)) === "31-35" },
    { name: "年龄-35+", fn: (r) => ageBucketFromQ33(str(r.q33)) === "35+" },
  ];

  const colDefs = [
    { name: "总计", fn: () => true },
    { name: "性别-男", fn: (r) => getBiGenderValue(r) === "男" || str(r.q34) === "1" },
    { name: "性别-女", fn: (r) => getBiGenderValue(r) === "女" || str(r.q34) === "2" },
    { name: "年龄-19-25", fn: (r) => ageBucketFromQ33(str(r.q33)) === "19-25" },
    { name: "年龄-26-30", fn: (r) => ageBucketFromQ33(str(r.q33)) === "26-30" },
    { name: "年龄-31-35", fn: (r) => ageBucketFromQ33(str(r.q33)) === "31-35" },
    { name: "年龄-35+", fn: (r) => ageBucketFromQ33(str(r.q33)) === "35+" },
    { name: "抽卡决策/原石/强度对比", fn: (r) => hasQ2ByCode(r, 1) || hasQ2AllDiscuss(r) },
    { name: "角色配队/养成/手法", fn: (r) => hasQ2ByCode(r, 2) || hasQ2AllDiscuss(r) },
    { name: "探索解谜攻略", fn: (r) => hasQ2ByCode(r, 3) || hasQ2AllDiscuss(r) },
    { name: "剧情解析", fn: (r) => hasQ2ByCode(r, 4) || hasQ2AllDiscuss(r) },
    { name: "剧情讨论", fn: (r) => hasQ2ByCode(r, 5) || hasQ2AllDiscuss(r) },
    { name: "同人", fn: (r) => hasQ2ByCode(r, 6) || hasQ2AllDiscuss(r) },
    { name: "游戏外活动", fn: (r) => hasQ2ByCode(r, 7) || hasQ2AllDiscuss(r) },
    { name: "周边", fn: (r) => hasQ2ByCode(r, 8) || hasQ2AllDiscuss(r) },
    { name: "Cosplay", fn: (r) => hasQ2ByCode(r, 9) || hasQ2AllDiscuss(r) },
  ];

  const colRows = colDefs.map((c) => ({ ...c, rows: analysisRows.filter(c.fn) }));
  const colTotals = colRows.map((c) => sumWeights(c.rows));

  const header1 = ["行维度"];
  const header2 = [""];
  colDefs.forEach((c) => {
    header1.push(c.name, c.name);
    header2.push("计数", "百分比");
  });
  const lines = [
    header1.map(csvCell).join(","),
    header2.map(csvCell).join(","),
  ];

  rowDefs.forEach((rowDef) => {
    const row = [rowDef.name];
    colRows.forEach((c, idx) => {
      const inter = c.rows.filter(rowDef.fn);
      const cnt = sumWeights(inter);
      const denom = colTotals[idx];
      row.push(fmtCount(cnt));
      row.push(denom > 0 ? fmtPct(cnt / denom) : "--");
    });
    lines.push(row.map(csvCell).join(","));
  });

  const totalRow = ["总计"];
  colTotals.forEach((denom) => {
    totalRow.push(fmtCount(denom));
    totalRow.push(denom > 0 ? "100.0%" : "--");
  });
  lines.push(totalRow.map(csvCell).join(","));

  downloadTextFile(`age-profile-${Date.now()}.csv`, lines.join("\n"), "text/csv;charset=utf-8");
}

function renderCross() {
  const analysisQuestions = getAnalysisQuestions();
  const attrQuestions = getAttrQuestions();
  const qId = document.getElementById("analysisQuestion").value;
  const attrId = document.getElementById("analysisAttr").value;

  const qDef = analysisQuestions.find((x) => x.id === qId) || analysisQuestions[0];
  const attrDef = attrQuestions.find((x) => x.id === attrId) || attrQuestions[0];
  const filterDefs = getSelectedFilterDefs();

  const filtered = applyGroupedFilters(analysisRows, filterDefs);
  const overall = getQuestionDistribution(filtered, qDef);
  drawBarChart("chartAnalysis", overall.items, `${qDef.name}（样本: ${fmtCount(sumWeights(filtered))}）`);

  const sortedCols = [...overall.items].sort((a, b) => b.ratio - a.ratio);
  const thead = document.querySelector("#crossTable thead");
  const tbody = document.querySelector("#crossTable tbody");

  thead.innerHTML = "";
  tbody.innerHTML = "";

  const header = document.createElement("tr");
  header.innerHTML = `<th>${attrDef.name}</th><th>样本量</th>${sortedCols.map((x) => `<th>${x.name}</th>`).join("")}`;
  thead.appendChild(header);

  const attrEntries = Object.entries(attrDef.labels);
  for (const [code, label] of attrEntries) {
    const groupRows = filtered.filter((r) => rowMatchGroup(r, attrDef, code));
    if (!groupRows.length) continue;
    const dist = getQuestionDistribution(groupRows, qDef);
    const byCode = new Map(dist.items.map((x) => [x.code, x]));

    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${label}</td><td>${fmtCount(sumWeights(groupRows))}</td>${sortedCols
      .map((col) => `<td>${fmtPct((byCode.get(col.code) || { ratio: 0 }).ratio)}</td>`)
      .join("")}`;
    tbody.appendChild(tr);
  }
}

function scheduleRenderCross() {
  if (crossRenderTimer) clearTimeout(crossRenderTimer);
  crossRenderTimer = setTimeout(() => {
    crossRenderTimer = null;
    renderCross();
  }, 0);
}

function renderUploadMeta() {
  const msg = [
    `分析样本（继续作答）: ${fmtCount(sumWeights(analysisRows))}${isWeightEnabled() ? "（加权）" : ""}`,
    `数据更新至: ${fmtTime(lastUploadAt)}`,
  ].join("\n");
  document.getElementById("sidebarMeta").textContent = msg;
  document.getElementById("uploadLog").textContent = msg;

  const statsNode = document.getElementById("importStats");
  if (!statsNode) return;
  if (!lastImportStats) {
    statsNode.innerHTML = "暂无导入统计。";
    return;
  }
  statsNode.innerHTML = [
    `导入模式：${lastImportStats.mode === "append" ? "追加" : "覆盖"}`,
    `导入文件数：${lastImportStats.files}`,
    `读取总行数：${lastImportStats.readRows}`,
    `去重后总样本：${lastImportStats.dedupRows}`,
    `终止样本：${lastImportStats.terminateExcluded}`,
    `废样本：${lastImportStats.invalidExcluded}`,
    `最终分析样本：${lastImportStats.analysisRows}`,
  ].join("<br/>");
}

function parseOptionLines(text) {
  const lines = String(text || "").split(/\r?\n/);
  const labels = {};
  for (const line of lines) {
    const raw = line.trim();
    if (!raw) continue;
    const idx = raw.indexOf("=");
    if (idx <= 0) continue;
    const code = raw.slice(0, idx).trim();
    const label = raw.slice(idx + 1).trim();
    if (!code || !label) continue;
    labels[code] = label;
  }
  return labels;
}

function toOptionLines(labels) {
  return Object.entries(labels || {})
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
}

function isLikelySingleQuestion(qid) {
  const samples = rawRows.slice(0, 1500);
  let nonEmpty = 0;
  const uniq = new Set();
  let longTextCount = 0;
  for (const row of samples) {
    const v = str(row[qid]);
    if (!v) continue;
    nonEmpty += 1;
    uniq.add(v);
    if (v.length > 16) longTextCount += 1;
    if (uniq.size > 80) return false;
  }
  if (!nonEmpty) return false;
  if (longTextCount / nonEmpty > 0.35 && uniq.size / nonEmpty > 0.4) return false;
  return true;
}

function getSingleQuestionCandidates() {
  const qHeaders = getHeaders().filter((h) => /^q\d+$/.test(h));
  const optionIds = new Set(getOptionQuestionDefs().map((x) => x.id));
  const openIds = new Set(Object.keys(openConfig || {}));
  const configured = Object.keys(singleConfig || {}).filter((qid) => Object.keys(singleConfig[qid]?.labels || {}).length > 0);

  const inferred = qHeaders.filter((qid) => !optionIds.has(qid) && !openIds.has(qid) && isLikelySingleQuestion(qid));
  const merged = Array.from(new Set([...configured, ...inferred]));
  return merged.sort((a, b) => Number(a.replace("q", "")) - Number(b.replace("q", "")));
}

function renderSingleConfigPanel() {
  const fieldSelect = document.getElementById("singleConfigField");
  const titleInput = document.getElementById("singleConfigTitle");
  const optionsInput = document.getElementById("singleConfigOptions");
  const preview = document.getElementById("singleConfigPreview");
  if (!fieldSelect || !titleInput || !optionsInput || !preview) return;

  const singleKeys = getSingleQuestionCandidates();

  const prev = fieldSelect.value;
  setSelectOptionsWithRaw("singleConfigField", singleKeys);
  if (prev && singleKeys.includes(prev)) fieldSelect.value = prev;
  if (!fieldSelect.value && singleKeys.length) fieldSelect.value = singleKeys[0];
  const key = fieldSelect.value;
  const cfg = singleConfig[key] || { title: key, labels: {} };
  titleInput.value = cfg.title || key;
  optionsInput.value = toOptionLines(cfg.labels);

  preview.innerHTML = singleKeys
    .map((k) => `${k}：${(singleConfig[k]?.title || k)}（${Object.keys(singleConfig[k]?.labels || {}).length}项）`)
    .join("<br/>");
}

function bindSingleConfigEditor() {
  const fieldSelect = document.getElementById("singleConfigField");
  const titleInput = document.getElementById("singleConfigTitle");
  const optionsInput = document.getElementById("singleConfigOptions");
  const saveBtn = document.getElementById("btnSaveSingleConfig");
  if (!fieldSelect || !titleInput || !optionsInput || !saveBtn) return;

  fieldSelect.addEventListener("change", renderSingleConfigPanel);
  saveBtn.addEventListener("click", () => {
    const qid = fieldSelect.value;
    if (!qid) return;
    const labels = parseOptionLines(optionsInput.value);
    if (!Object.keys(labels).length) {
      alert("选项配置不能为空，至少填写一行：编码=文案");
      return;
    }
    singleConfig[qid] = {
      title: str(titleInput.value) || qid,
      labels,
    };
    saveSingleConfig();
    renderSingleConfigPanel();

    const currentQ = document.getElementById("analysisQuestion").value;
    const currentA = document.getElementById("analysisAttr").value;
    const currentFilter = [...document.getElementById("analysisFilter").selectedOptions].map((x) => x.value);
    setSelectOptions("analysisQuestion", getAnalysisQuestions().map((x) => ({ id: x.id, name: x.name })));
    setSelectOptions("analysisAttr", getAttrQuestions().map((x) => ({ id: x.id, name: x.name })));
    setSelectOptions("analysisFilter", getFilterOptions().map((x) => ({ id: x.id, name: x.name })));
    if (currentQ) document.getElementById("analysisQuestion").value = currentQ;
    if (currentA) document.getElementById("analysisAttr").value = currentA;
    const filterNode = document.getElementById("analysisFilter");
    [...filterNode.options].forEach((o) => {
      o.selected = currentFilter.includes(o.value);
    });
    if (![...filterNode.selectedOptions].length && filterNode.options.length) {
      filterNode.options[0].selected = true;
    }
    renderCross();
    alert("单选题配置已保存");
  });
}

function guessOpenQuestionFields() {
  const headers = getHeaders();
  return headers.filter((h) => /^q\d+$/.test(h) || /^qc\d+/.test(h));
}

function renderOpenConfigPanel() {
  const fieldSelect = document.getElementById("openConfigField");
  const titleInput = document.getElementById("openConfigTitle");
  const preview = document.getElementById("openConfigPreview");
  if (!fieldSelect || !titleInput || !preview) return;

  const candidates = Array.from(new Set([...Object.keys(openConfig), ...guessOpenQuestionFields()]))
    .sort((a, b) => {
      const na = Number(a.replace(/\D/g, "")) || 0;
      const nb = Number(b.replace(/\D/g, "")) || 0;
      return na - nb;
    });

  const prev = fieldSelect.value;
  setSelectOptionsWithRaw("openConfigField", candidates);
  if (prev && candidates.includes(prev)) fieldSelect.value = prev;
  if (!fieldSelect.value && candidates.length) fieldSelect.value = candidates[0];

  const key = fieldSelect.value;
  titleInput.value = openConfig[key] || key;

  preview.innerHTML = Object.entries(openConfig)
    .sort((a, b) => {
      const na = Number(a[0].replace(/\D/g, "")) || 0;
      const nb = Number(b[0].replace(/\D/g, "")) || 0;
      return na - nb;
    })
    .map(([k, v]) => `${k}：${v}`)
    .join("<br/>");
}

function bindOpenConfigEditor() {
  const fieldSelect = document.getElementById("openConfigField");
  const titleInput = document.getElementById("openConfigTitle");
  const saveBtn = document.getElementById("btnSaveOpenConfig");
  if (!fieldSelect || !titleInput) return;

  const persistCurrent = () => {
    const key = fieldSelect.value;
    if (!key) return;
    const title = str(titleInput.value) || key;
    openConfig[key] = title;
    saveOpenConfig();
    renderOpenConfigPanel();
  };

  fieldSelect.addEventListener("change", () => {
    persistCurrent();
    renderOpenConfigPanel();
  });

  let timer = null;
  titleInput.addEventListener("input", () => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      persistCurrent();
      timer = null;
    }, 280);
  });
  titleInput.addEventListener("blur", persistCurrent);

  if (saveBtn) {
    saveBtn.addEventListener("click", () => {
      persistCurrent();
      alert("开放题配置已保存");
    });
  }
}

function setSelectOptionsWithRaw(selectId, values) {
  const node = document.getElementById(selectId);
  if (!node) return;
  node.innerHTML = "";
  values.forEach((v) => {
    const o = document.createElement("option");
    o.value = v;
    o.textContent = v;
    node.appendChild(o);
  });
}

function normalizeTargetMap(map, cats) {
  const out = {};
  let sum = 0;
  cats.forEach((c) => {
    const v = Number(map?.[c] ?? 0);
    if (Number.isFinite(v) && v > 0) {
      out[c] = v;
      sum += v;
    } else {
      out[c] = 0;
    }
  });
  if (sum <= 0) return Object.fromEntries(cats.map((c) => [c, 0]));
  return Object.fromEntries(cats.map((c) => [c, out[c] / sum]));
}

function calcDistForRows(rows, dim, plan, useWeight) {
  const cats = WEIGHT_DIM_VALUES[dim] || [];
  const counts = Object.fromEntries(cats.map((c) => [c, 0]));
  let total = 0;
  for (const row of rows) {
    const key = getDimValue(row, dim, plan);
    if (!(key in counts)) continue;
    const w = useWeight ? getRowWeight(row) : 1;
    counts[key] += w;
    total += w;
  }
  const ratios = Object.fromEntries(cats.map((c) => [c, total > 0 ? counts[c] / total : 0]));
  return { counts, ratios, total };
}

function renderWeightMappingAndDiagnostics(plan) {
  const mapNode = document.getElementById("weightMapping");
  const diagNode = document.getElementById("weightDiagnostics");
  if (!mapNode || !diagNode) return;

  const communityField = str(plan.communityField || "近42天内社区活跃");
  const genderField = str(plan.genderField || getBiGenderField() || "q34");
  const ageField = "q33";
  const adventureField = str(plan.adventureTierField || plan.adventureField || "冒险等阶2分级-BI");

  mapNode.innerHTML = [
    "锁定字段：",
    `社区分组字段：${communityField}`,
    `性别字段：${genderField}`,
    `年龄字段：${ageField}`,
    `冒险字段：${adventureField}`,
  ].join("<br/>");

  const commRows = analysisRows.filter((r) => isCommunityUser(r, plan));
  const nonRows = analysisRows.filter((r) => !isCommunityUser(r, plan));
  const weightedComm = sumWeights(commRows);
  const weightedNon = sumWeights(nonRows);
  const weightedTotal = weightedComm + weightedNon;
  const p = resolvePenetration(plan);
  const dims = [...FIXED_WEIGHT_DIMS].filter((d) => d in WEIGHT_DIM_VALUES);

  const tables = dims
    .map((dim) => {
      const cats = WEIGHT_DIM_VALUES[dim];
      const tComm = normalizeTargetMap(plan?.groupTargets?.community?.[dim], cats);
      const tNon = normalizeTargetMap(plan?.groupTargets?.non_community?.[dim], cats);
      const preComm = calcDistForRows(commRows, dim, plan, false).ratios;
      const preNon = calcDistForRows(nonRows, dim, plan, false).ratios;
      const postComm = calcDistForRows(commRows, dim, plan, true).ratios;
      const postNon = calcDistForRows(nonRows, dim, plan, true).ratios;
      const rows = cats
        .map(
          (c) => `<tr>
            <td>${c}</td>
            <td>${fmtPct1(preComm[c])}</td><td>${fmtPct1(postComm[c])}</td><td>${fmtPct1(tComm[c])}</td>
            <td>${fmtPct1(preNon[c])}</td><td>${fmtPct1(postNon[c])}</td><td>${fmtPct1(tNon[c])}</td>
          </tr>`,
        )
        .join("");
      return `<div class="table-wrap" style="max-height:none;margin-top:8px;">
        <table>
          <thead>
            <tr><th colspan="7">${WEIGHT_DIM_LABELS[dim] || dim}</th></tr>
            <tr><th>类别</th><th>社前</th><th>社后</th><th>社目标</th><th>非前</th><th>非后</th><th>非目标</th></tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
    })
    .join("");

  diagNode.innerHTML = [
    `加权诊断：`,
    `样本量(未加权)：社区 ${commRows.length}，非社区 ${nonRows.length}，总体 ${analysisRows.length}`,
    `样本量(当前权重)：社区 ${weightedComm.toFixed(1)}，非社区 ${weightedNon.toFixed(1)}，总体 ${weightedTotal.toFixed(1)}`,
    `目标渗透率：社区 ${p ? fmtPct1(p.community) : "--"}，非社区 ${p ? fmtPct1(p.non_community) : "--"}`,
    tables,
  ].join("<br/>");
}

function renderRulesPanel() {
  const headers = getHeaders();
  const qHeaders = headers.filter((h) => /^q\d+/.test(h));

  setSelectOptionsWithRaw("ruleTerminateField", ["", ...headers]);
  setSelectOptionsWithRaw("ruleDurationField", ["", ...headers]);
  setSelectOptionsWithRaw("ruleRequiredFields", qHeaders);

  const terminateField = document.getElementById("ruleTerminateField");
  const terminateValue = document.getElementById("ruleTerminateValue");
  const durationField = document.getElementById("ruleDurationField");
  const enableDuration = document.getElementById("ruleEnableDuration");
  const minDuration = document.getElementById("ruleMinDuration");
  const enableRequired = document.getElementById("ruleEnableRequired");
  const requiredFields = document.getElementById("ruleRequiredFields");
  const autoHint = document.getElementById("ruleAutoHint");
  const ruleStats = document.getElementById("ruleStats");
  const weightEnable = document.getElementById("weightEnable");
  const weightStats = document.getElementById("weightStats");
  const dimBox = document.getElementById("weightDimsManual");
  const historySelect = document.getElementById("weightHistorySelect");

  if (!terminateField || !terminateValue || !durationField || !enableDuration || !minDuration || !enableRequired || !requiredFields || !autoHint || !ruleStats) return;

  terminateField.value = currentRules.terminateField || "";
  terminateValue.value = currentRules.terminateValue || "1";
  durationField.value = currentRules.durationField || "";
  enableDuration.checked = !!currentRules.enableDuration;
  minDuration.value = String(currentRules.minDuration ?? 15);
  enableRequired.checked = !!currentRules.enableRequired;
  [...requiredFields.options].forEach((o) => {
    o.selected = currentRules.requiredFields.includes(o.value);
  });

  autoHint.textContent = `已识别表头 ${headers.length} 个；自动建议终止题字段：${buildDefaultRules(headers).terminateField || "未识别"}`;
  ruleStats.innerHTML = [
    `规则试跑结果：`,
    `总样本：${sampleStats.total}`,
    `终止样本：${sampleStats.terminateExcluded}`,
    `废样本：${sampleStats.invalidExcluded}`,
    `最终分析样本：${sampleStats.final}`,
  ].join("<br/>");

  if (weightEnable && weightStats) {
    weightEnable.checked = !!weightConfig.enabled;
    if (dimBox) {
      const dims = new Set(FIXED_WEIGHT_DIMS);
      dimBox.querySelectorAll("input[type='checkbox']").forEach((cb) => {
        cb.checked = dims.has(cb.value);
        cb.disabled = true;
      });
    }
    weightStats.innerHTML = `状态：${weightState.message}`;
    try {
      const parsedPlan = parseWeightPlan(weightConfig.targetsText || "");
      buildManualWeightGrid(parsedPlan);
      renderWeightMappingAndDiagnostics(parsedPlan);
    } catch {
      buildManualWeightGrid({});
      renderWeightMappingAndDiagnostics({});
    }
    if (historySelect) {
      historySelect.innerHTML = "";
      const options = weightHistory.map((h) => ({
        id: h.id,
        name: `${fmtTime(h.savedAt)}｜${(h.dims || []).join("+")}${h.enabled ? "｜启用" : "｜停用"}`,
      }));
      setSelectOptions("weightHistorySelect", options);
    }
  }

  renderFrameworkBulkPanel();
}

function saveWeightFromPanel() {
  const weightEnable = document.getElementById("weightEnable");
  if (!weightEnable) return;
  const parsed = buildPlanFromManualInputs();
  weightConfig = {
    ...weightConfig,
    enabled: !!weightEnable.checked,
    dims: [...FIXED_WEIGHT_DIMS],
    targetsText: JSON.stringify(parsed, null, 2),
  };
  saveWeightConfig();
  weightHistory.unshift({
    id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    savedAt: new Date().toISOString(),
    enabled: weightConfig.enabled,
    dims: [...weightConfig.dims],
    targetsText: weightConfig.targetsText,
  });
  saveWeightHistory();
  recomputeAnalysisRows();
  renderAll();
  alert("加权配置已保存并重算");
}

function toggleWeightEnabledFromPanel() {
  const weightEnable = document.getElementById("weightEnable");
  if (!weightEnable) return;
  setWeightEnabled(!!weightEnable.checked);
}

function applyBiWeightFromPanel() {
  const input = document.getElementById("weightBiInput");
  if (!input) return;
  const text = str(input.value);
  if (!text) {
    alert("请先粘贴大盘表文本");
    return;
  }
  let parsed = null;
  try {
    parsed = parseBiWeightTable(text);
  } catch (err) {
    alert(`解析失败：${err.message}`);
    return;
  }
  if (!parsed) {
    alert("未识别到可用的大盘表数据，请检查是否包含“维度 / 社区活跃 / 非社区活跃”列");
    return;
  }

  const normalizedPlan = { ...parsed, communityPenetration: FIXED_COMMUNITY_PENETRATION };
  weightConfig = {
    ...weightConfig,
    dims: [...FIXED_WEIGHT_DIMS],
    targetsText: JSON.stringify(normalizedPlan, null, 2),
  };
  saveWeightConfig();
  buildManualWeightGrid(normalizedPlan);
  const weightStats = document.getElementById("weightStats");
  if (weightStats) {
    const dimsText = FIXED_WEIGHT_DIMS.map((d) => WEIGHT_DIM_LABELS[d] || d).join("、");
    const pen = resolvePenetration(normalizedPlan);
    weightStats.innerHTML = `状态：已从粘贴文本填充。识别维度：${dimsText}；社区渗透率：${pen ? fmtPct1(pen.community) : "--"}`;
  }
  renderRulesPanel();
  alert("大盘表已解析并填充到加权表格，可直接点“保存加权并重算”");
}

function apply64PresetWeight() {
  const plan = JSON.parse(JSON.stringify(DEFAULT_WEIGHT_PLAN));
  weightConfig = {
    ...weightConfig,
    dims: [...FIXED_WEIGHT_DIMS],
    targetsText: JSON.stringify(plan, null, 2),
  };
  saveWeightConfig();
  buildManualWeightGrid(plan);
  const weightStats = document.getElementById("weightStats");
  if (weightStats) {
    const pen = resolvePenetration(plan);
    weightStats.innerHTML = `状态：已一键填入6.4大盘；社区渗透率：${pen ? fmtPct1(pen.community) : "--"}`;
  }
  renderRulesPanel();
  alert("已一键填入6.4大盘参数，可直接勾选启用加权");
}

function loadAndApplyWeightHistory() {
  const sel = document.getElementById("weightHistorySelect");
  if (!sel || !sel.value) return;
  const item = weightHistory.find((x) => x.id === sel.value);
  if (!item) return;
  weightConfig = {
    ...weightConfig,
    enabled: !!item.enabled,
    dims: Array.isArray(item.dims) ? item.dims : [...weightConfig.dims],
    targetsText: str(item.targetsText || weightConfig.targetsText),
  };
  saveWeightConfig();
  recomputeAnalysisRows();
  renderAll();
}

function deleteWeightHistoryItem() {
  const sel = document.getElementById("weightHistorySelect");
  if (!sel || !sel.value) return;
  weightHistory = weightHistory.filter((x) => x.id !== sel.value);
  saveWeightHistory();
  renderRulesPanel();
}

function splitBiLine(line) {
  const raw = String(line || "");
  if (!raw.trim()) return [];
  // 这里不能先 trim：粘贴自 Excel 的续行会用首列空白表示“沿用上一维度”，
  // 先 trim 会丢失这个空列，导致“维度枚举/数值列”错位。
  if (raw.includes("\t")) return raw.split("\t").map((x) => x.trim());
  const multi = raw.trim().split(/\s{2,}/).map((x) => x.trim());
  if (multi.length >= 3) return multi;
  return raw.trim().split(/\s+/).map((x) => x.trim());
}

function toNum(v) {
  const n = Number(String(v || "").replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
}

function parseBiWeightTable(text) {
  const lines = String(text || "")
    .replace(/\u00a0/g, " ")
    .replace(/[；;]\s*加权识别.*$/gim, "")
    .split(/\r?\n/)
    .map((x) => String(x || ""))
    .filter((x) => x.trim() !== "");
  if (!lines.length) return null;

  const KNOWN_DIMS = new Set(["整体", "性别", "年龄", "冒险等级1", "冒险等级2", "消费等级", "社区信息"]);
  const normalizeDimName = (raw) => {
    const s = str(raw).replace(/\s+/g, "");
    if (!s) return "";
    if (s.includes("整体")) return "整体";
    if (s.includes("性别")) return "性别";
    if (s.includes("年龄")) return "年龄";
    if (s.includes("冒险") && (s.includes("等级2") || s.includes("等阶2"))) return "冒险等级2";
    if (s.includes("冒险") && (s.includes("等级1") || s.includes("等阶1"))) return "冒险等级1";
    if (s.includes("消费")) return "消费等级";
    if (s.includes("社区信息")) return "社区信息";
    return "";
  };
  const normalizeRangeText = (raw) =>
    str(raw)
      .replace(/[–—~～至]/g, "-")
      .replace(/\s+/g, "")
      .replace(/岁/g, "");
  const data = [];
  let currentDim = "";

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || /^(维度|dimension)\b/i.test(line) || /维度枚举|grand\s*total/i.test(line)) continue;

    // 兼容 Tab/空格混排：先按“行尾数字列”识别，再反推左侧文案列
    const numMatches = [...line.matchAll(/-?\d[\d,]*/g)];
    if (numMatches.length < 2) continue;
    const c = toNum(numMatches[0][0]);
    const n = toNum(numMatches[1][0]);
    const left = line.slice(0, numMatches[0].index).trim();
    if (!left) continue;

    const tokens = left.split(/\s+/).filter(Boolean);
    if (!tokens.length) continue;

    let dim = "";
    let label = "";

    const maybeDim = normalizeDimName(tokens[0]);
    if (tokens.length >= 2 && (KNOWN_DIMS.has(tokens[0]) || maybeDim)) {
      dim = maybeDim || tokens[0];
      label = tokens.slice(1).join("");
      currentDim = dim;
    } else if (KNOWN_DIMS.has(tokens[0]) || maybeDim) {
      dim = maybeDim || tokens[0];
      label = "";
      currentDim = dim;
    } else {
      dim = currentDim;
      label = tokens.join("");
    }

    if (!dim || /row labels/i.test(dim)) continue;
    if (!label && dim !== "整体") continue;
    data.push({ dim, label: normalizeRangeText(label), community: c, non: n });
  }
  if (!data.length) return null;

  const getRows = (dimName) => data.filter((r) => r.dim === dimName);
  const mapDist = (rowsIn, mapFn) => {
    const outC = {};
    const outN = {};
    for (const r of rowsIn) {
      const key = mapFn(r.label);
      if (!key) continue;
      outC[key] = (outC[key] || 0) + r.community;
      outN[key] = (outN[key] || 0) + r.non;
    }
    const sumC = Object.values(outC).reduce((s, x) => s + x, 0) || 1;
    const sumN = Object.values(outN).reduce((s, x) => s + x, 0) || 1;
    const norm = (obj, sum) => Object.fromEntries(Object.entries(obj).map(([k, v]) => [k, v / sum]));
    return { community: norm(outC, sumC), non_community: norm(outN, sumN) };
  };

  const overallRow = getRows("整体")[0];
  const totalC = overallRow ? overallRow.community : data.reduce((s, x) => s + x.community, 0);
  const totalN = overallRow ? overallRow.non : data.reduce((s, x) => s + x.non, 0);
  const communityPenetration = totalC + totalN > 0 ? totalC / (totalC + totalN) : 0.5;

  const genderDist = mapDist(getRows("性别"), (l) => {
    const s = str(l);
    if (s.includes("男")) return "男";
    if (s.includes("女")) return "女";
    if (s.includes("未知")) return "未知";
    return "";
  });
  const ageDist = mapDist(getRows("年龄"), (l) => {
    const s = str(l);
    if (s.includes("19-25")) return "19-25";
    if (s.includes("26-30")) return "26-30";
    if (s.includes("31-35")) return "31-35";
    if (s.includes("35")) return "35+";
    if (s.includes("未知")) return "未知";
    return "";
  });
  const advDist = mapDist(getRows("冒险等级2"), (l) => {
    const s = str(l);
    if (s.includes("0-30")) return "0-30";
    if (s.includes("31-45") || s.includes("31-44")) return "31-45";
    if (s.includes("46-60") || s.includes("45-60")) return "46-60";
    if (s.includes("未知")) return "未知";
    return "";
  });
  const spendDist = mapDist(getRows("消费等级"), (l) => {
    const s = str(l);
    if (s.includes("大R")) return "大R";
    if (s.includes("中R")) return "中R";
    if (s.includes("小R")) return "小R";
    if (s.includes("无付费")) return "无付费";
    if (s.includes("未知")) return "未知";
    return "";
  });

  return {
    mode: "two_stage",
    communityField: "近42天内社区活跃",
    communityYesCodes: ["1"],
    communityNoCodes: ["0"],
    genderField: "性别-BI",
    communityPenetration,
    groupTargets: {
      community: {
        gender: genderDist.community,
        age: ageDist.community,
        adventure: advDist.community,
        spend: spendDist.community,
        community_active: { 有: 1, 无: 0 },
      },
      non_community: {
        gender: genderDist.non_community,
        age: ageDist.non_community,
        adventure: advDist.non_community,
        spend: spendDist.non_community,
        community_active: { 有: 0, 无: 1 },
      },
    },
    adventureTierField: "冒险等阶2分级-BI",
    adventureField: "游戏等级-BI",
    spendField: "累计付费",
  };
}

function parseFrameworkBulkText(text) {
  const raw = String(text || "").trim();
  if (!raw) return [];
  const rows = raw
    .split(/\r?\n/)
    .map((line) => line.split("\t").map((c) => c.trim()))
    .filter((cells) => cells.some((c) => c));
  if (!rows.length) return [];

  const header = rows[0].join("|");
  const hasHeader = /题号|题目属性|题目类型|题干|具体问题|详细题目|跳转逻辑|问卷逻辑/.test(header);
  const body = hasHeader ? rows.slice(1) : rows;
  const headCells = hasHeader ? rows[0] : [];

  const findIdx = (keys, fallback) => {
    if (!hasHeader) return fallback;
    const idx = headCells.findIndex((h) => keys.some((k) => h.includes(k)));
    return idx >= 0 ? idx : fallback;
  };

  const idxQ = findIdx(["题号", "编号"], 0);
  const idxType = findIdx(["题目属性", "题目类型", "题型", "类型"], 1);
  const idxTitle = findIdx(["具体问题", "详细题目", "题干", "题目"], 2);
  const idxLogic = findIdx(["跳转逻辑", "问卷逻辑", "逻辑"], 3);

  const byQid = new Map();
  for (const cells of body) {
    const joined = cells.join(" ");
    const qCell = cells[idxQ] || joined;
    const m = String(qCell).match(/q?\s*(\d{1,3})/i);
    if (!m) continue;
    const qid = `q${Number(m[1])}`;
    const type = str(cells[idxType] || "");
    const title = str(cells[idxTitle] || "");
    const logic = str(cells[idxLogic] || "");
    if (!type && !title && !logic) continue;
    byQid.set(qid, { qid, type, title, logic });
  }
  return [...byQid.values()].sort((a, b) => Number(a.qid.slice(1)) - Number(b.qid.slice(1)));
}

function applyFrameworkItems(items) {
  let titleUpdated = 0;
  let openUpdated = 0;
  for (const item of items) {
    const qid = item.qid;
    const titleText = str(item.title || "");
    const fullTitle = titleText ? `Q${Number(qid.slice(1))} ${titleText}` : `Q${Number(qid.slice(1))}`;
    const t = str(item.type || "");
    const isOpen = /开放|填空|文本|主观/.test(t);
    const prev = singleConfig[qid] || { labels: {} };
    singleConfig[qid] = {
      title: fullTitle,
      labels: prev.labels || {},
    };
    titleUpdated += 1;
    if (isOpen) {
      openConfig[qid] = fullTitle;
      openUpdated += 1;
    }
  }
  return { titleUpdated, openUpdated };
}

function renderFrameworkBulkPanel() {
  const input = document.getElementById("frameworkBulkInput");
  const preview = document.getElementById("frameworkBulkPreview");
  if (!input || !preview) return;
  if (frameworkConfig.raw && !input.value) input.value = frameworkConfig.raw;
  const items = Array.isArray(frameworkConfig.items) ? frameworkConfig.items : [];
  const head = `已保存 ${items.length} 道题${frameworkConfig.updatedAt ? `；更新于 ${fmtTime(frameworkConfig.updatedAt)}` : ""}`;
  const lines = items.slice(0, 20).map((x) => `${x.qid}｜${x.type || "-"}｜${x.title || "-"}｜逻辑：${x.logic || "-"}`);
  preview.innerHTML = [head, ...lines, items.length > 20 ? `... 其余 ${items.length - 20} 道题` : ""].filter(Boolean).join("<br/>");
}

function bindFrameworkBulkEditor() {
  const input = document.getElementById("frameworkBulkInput");
  const btn = document.getElementById("btnApplyFrameworkBulk");
  if (!input || !btn) return;
  btn.addEventListener("click", () => {
    const items = parseFrameworkBulkText(input.value);
    if (!items.length) {
      alert("未识别到有效题目，请检查是否包含题号和题干列。");
      return;
    }
    const applied = applyFrameworkItems(items);
    frameworkConfig = {
      raw: str(input.value || ""),
      items,
      updatedAt: new Date().toISOString(),
    };
    saveFrameworkConfig();
    saveSingleConfig();
    saveOpenConfig();
    renderAll();
    alert(`框架导入完成：识别 ${items.length} 题；更新题目标题 ${applied.titleUpdated} 题；更新开放题 ${applied.openUpdated} 题。`);
  });
}

function saveRulesFromPanel() {
  const terminateField = document.getElementById("ruleTerminateField");
  const terminateValue = document.getElementById("ruleTerminateValue");
  const durationField = document.getElementById("ruleDurationField");
  const enableDuration = document.getElementById("ruleEnableDuration");
  const minDuration = document.getElementById("ruleMinDuration");
  const enableRequired = document.getElementById("ruleEnableRequired");
  const requiredFields = document.getElementById("ruleRequiredFields");
  if (!terminateField || !terminateValue || !durationField || !enableDuration || !minDuration || !enableRequired || !requiredFields) return;

  currentRules = {
    terminateField: terminateField.value,
    terminateValue: terminateValue.value || "1",
    durationField: durationField.value,
    enableDuration: enableDuration.checked,
    minDuration: Number(minDuration.value || 0),
    enableRequired: enableRequired.checked,
    requiredFields: [...requiredFields.selectedOptions].map((o) => o.value),
  };
  saveRules();
  recomputeAnalysisRows();
  renderAll();
  alert("规则已保存并重算完成");
}

function resetRulesToDefault() {
  currentRules = buildDefaultRules(getHeaders());
  saveRules();
  recomputeAnalysisRows();
  renderAll();
}

async function saveLocal() {
  await idbSet(RAW_ROWS_KEY, rawRows);
  localStorage.setItem(STORAGE_UPLOAD_AT_KEY, String(lastUploadAt || ""));
}

async function loadLocal() {
  try {
    const rows = await idbGet(RAW_ROWS_KEY);
    rawRows = Array.isArray(rows) ? rows : [];
  } catch {
    // Fallback: IndexedDB 不可用时，降级为仅内存模式，避免 localStorage 超配额
    rawRows = [];
  }
  try {
    if (!Array.isArray(rawRows)) rawRows = [];
  } catch {}
  const rawTime = localStorage.getItem(STORAGE_UPLOAD_AT_KEY) || "";
  if (!rawTime) {
    lastUploadAt = "";
    return;
  }
  const parsed = new Date(rawTime);
  lastUploadAt = Number.isNaN(parsed.getTime()) ? "" : rawTime;
}

async function readFileRows(file, csvEncoding = "auto") {
  const ext = file.name.toLowerCase().split(".").pop();
  const buf = await file.arrayBuffer();

  if (ext === "csv") {
    const text = decodeCsvText(buf, csvEncoding);
    const wb = XLSX.read(text, { type: "string" });
    const sheet = wb.Sheets[wb.SheetNames[0]];
    return XLSX.utils.sheet_to_json(sheet, { defval: "" });
  }

  const wb = XLSX.read(buf, { type: "array" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: "" });
}

function decodeCsvText(buf, preferEncoding = "auto") {
  const encodings = ["utf-8", "gb18030", "gbk", "utf-16le"];
  if (preferEncoding !== "auto") {
    try {
      return new TextDecoder(preferEncoding).decode(buf);
    } catch {
      return new TextDecoder("utf-8").decode(buf);
    }
  }

  const tryDecode = (encoding) => {
    try {
      const text = new TextDecoder(encoding).decode(buf);
      const head = (text.split(/\r?\n/, 1)[0] || "").slice(0, 4000);
      // 评分：替换符、常见乱码字符越少越好；中文关键词越多越好
      const badCharCount = (text.match(/\uFFFD/g) || []).length;
      const mojibakeCount = (text.match(/[ÃÕÐÂË¼ÎÊÏÂµ]/g) || []).length;
      const cjkCount = (head.match(/[\u4e00-\u9fff]/g) || []).length;
      const keywordCount = ["原神", "米游社", "请问", "剧情", "探索", "渠道", "性别", "q1", "q2", "q3"]
        .map((k) => (head.includes(k) ? 1 : 0))
        .reduce((a, b) => a + b, 0);
      const score = keywordCount * 8 + Math.min(cjkCount, 80) - badCharCount * 10 - mojibakeCount * 6;
      return { encoding, text, score, badCharCount, mojibakeCount };
    } catch {
      return { encoding, text: "", score: Number.NEGATIVE_INFINITY };
    }
  };

  const candidates = encodings.map(tryDecode).sort((a, b) => b.score - a.score);
  return candidates[0]?.text || new TextDecoder("utf-8").decode(buf);
}

async function importFiles() {
  const files = [...document.getElementById("fileInput").files];
  const appendMode = !!document.getElementById("appendMode")?.checked;
  const csvEncoding = document.getElementById("csvEncoding")?.value || "auto";
  if (!files.length) {
    alert("请先选择文件");
    return;
  }

  setImportProgress(0, "开始读取文件...");
  let merged = appendMode ? [...rawRows] : [];
  let readRows = 0;
  for (let i = 0; i < files.length; i += 1) {
    const file = files[i];
    setImportProgress((i / files.length) * 100, `读取中：${file.name}`);
    const rows = await readFileRows(file, csvEncoding);
    readRows += rows.length;
    merged = merged.concat(rows);
  }
  setImportProgress(80, "去重与样本筛选中...");

  rawRows = dedupRows(merged);
  recomputeAnalysisRows();
  lastUploadAt = Date.now();
  await saveLocal();
  lastImportStats = {
    files: files.length,
    readRows,
    dedupRows: rawRows.length,
    terminateExcluded: sampleStats.terminateExcluded,
    invalidExcluded: sampleStats.invalidExcluded,
    analysisRows: analysisRows.length,
    mode: appendMode ? "append" : "replace",
  };
  setImportProgress(100, "导入完成");
  renderAll();
}

async function clearLocalData() {
  if (!confirm("确认清空本地导入数据吗？")) return;
  rawRows = [];
  analysisRows = [];
  sampleStats = { total: 0, terminateExcluded: 0, invalidExcluded: 0, final: 0 };
  lastUploadAt = "";
  lastImportStats = null;
  try {
    await idbDel(RAW_ROWS_KEY);
  } catch {}
  localStorage.removeItem(STORAGE_UPLOAD_AT_KEY);
  setImportProgress(0, "未开始导入");
  renderAll();
}

function bindTabs() {
  document.getElementById("tabs").addEventListener("click", (e) => {
    const btn = e.target.closest(".tab");
    if (!btn) return;
    const tab = btn.dataset.tab;

    document.querySelectorAll(".tab").forEach((x) => x.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((x) => x.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(`panel-${tab}`).classList.add("active");

    if (tab === "cross") {
      scheduleRenderCross();
      setTimeout(scheduleRenderCross, 80);
    }
    if (tab === "rules") renderRulesPanel();
    if (tab === "wordcloud") {
      renderWordcloudPanel();
      setTimeout(renderWordcloudPanel, 80);
    }
  });
}

function bindActions() {
  document.getElementById("btnImport").addEventListener("click", () => {
    importFiles().catch((err) => {
      alert(`导入失败: ${err.message}`);
    });
  });
  document.getElementById("btnClear").addEventListener("click", () => {
    clearLocalData().catch((err) => {
      alert(`清空失败: ${err.message}`);
    });
  });

  ["analysisQuestion", "analysisAttr", "analysisFilter"].forEach((id) => {
    document.getElementById(id).addEventListener("change", scheduleRenderCross);
  });
  const modeNode = document.getElementById("analysisFilterMode");
  if (modeNode) {
    modeNode.addEventListener("click", (e) => {
      const btn = e.target.closest(".mode-btn");
      if (!btn) return;
      setFilterMode(btn.dataset.mode === "exclude" ? "exclude" : "include");
      scheduleRenderCross();
    });
  }
  document.getElementById("btnExportCross").addEventListener("click", exportCrossTableCsv);
  const btnExportFullCross = document.getElementById("btnExportFullCross");
  if (btnExportFullCross) btnExportFullCross.addEventListener("click", exportFullCrossTableCsv);
  const btnExportGenderProfile = document.getElementById("btnExportGenderProfile");
  if (btnExportGenderProfile) btnExportGenderProfile.addEventListener("click", exportGenderProfileCsv);
  const btnExportAgeProfile = document.getElementById("btnExportAgeProfile");
  if (btnExportAgeProfile) btnExportAgeProfile.addEventListener("click", exportAgeProfileCsv);
  const btnExportOverview = document.getElementById("btnExportOverview");
  if (btnExportOverview) btnExportOverview.addEventListener("click", exportOverviewCsv);
  document.getElementById("btnSaveRules").addEventListener("click", saveRulesFromPanel);
  document.getElementById("btnResetRules").addEventListener("click", resetRulesToDefault);
  const btnSaveWeight = document.getElementById("btnSaveWeight");
  if (btnSaveWeight) btnSaveWeight.addEventListener("click", saveWeightFromPanel);
  const weightEnable = document.getElementById("weightEnable");
  if (weightEnable) weightEnable.addEventListener("change", toggleWeightEnabledFromPanel);
  const btnWeightOff = document.getElementById("btnWeightOff");
  const btnWeightOn = document.getElementById("btnWeightOn");
  if (btnWeightOff) btnWeightOff.addEventListener("click", () => setWeightEnabled(false));
  if (btnWeightOn) btnWeightOn.addEventListener("click", () => setWeightEnabled(true));
  const btnApplyBiWeight = document.getElementById("btnApplyBiWeight");
  if (btnApplyBiWeight) btnApplyBiWeight.addEventListener("click", applyBiWeightFromPanel);
  const btnApply64Preset = document.getElementById("btnApply64Preset");
  if (btnApply64Preset) btnApply64Preset.addEventListener("click", apply64PresetWeight);
  const btnLoadWeightHistory = document.getElementById("btnLoadWeightHistory");
  if (btnLoadWeightHistory) btnLoadWeightHistory.addEventListener("click", loadAndApplyWeightHistory);
  const btnDeleteWeightHistory = document.getElementById("btnDeleteWeightHistory");
  if (btnDeleteWeightHistory) btnDeleteWeightHistory.addEventListener("click", deleteWeightHistoryItem);
  document.getElementById("btnGenerateWordcloud").addEventListener("click", renderWordcloudPanel);
}

function renderAll() {
  renderWeightModeSwitch();
  renderUploadMeta();
  renderOverview();
  renderRulesPanel();
  renderSingleConfigPanel();
  renderOpenConfigPanel();
  if (document.getElementById("panel-cross").classList.contains("active")) {
    renderCross();
  }
  if (document.getElementById("panel-wordcloud").classList.contains("active")) {
    renderWordcloudPanel();
  }
}

function splitStopwords(text) {
  return new Set(
    String(text || "")
      .split(/[\s,，、;；]+/)
      .map((x) => x.trim())
      .filter(Boolean),
  );
}

function tokenizeZh(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  if (typeof Intl !== "undefined" && Intl.Segmenter) {
    const seg = new Intl.Segmenter("zh", { granularity: "word" });
    return [...seg.segment(t)]
      .map((x) => x.segment.trim())
      .filter(Boolean);
  }
  // Fallback: 按中文连续串和英文词拆分
  return (t.match(/[\u4e00-\u9fff]{1,}|[A-Za-z0-9_]+/g) || []).map((x) => x.trim()).filter(Boolean);
}

function extractContinuousPhrases(text, minLen, maxLen) {
  const t = String(text || "");
  // 保留连续中文/英文数字短语，确保“提瓦特小助手”这类完整词可直接入词云
  return (t.match(/[\u4e00-\u9fffA-Za-z0-9_]{2,}/g) || [])
    .map((x) => x.trim())
    .filter((x) => x.length >= minLen && x.length <= maxLen);
}

function getOpenConfigEntries() {
  return Object.entries(openConfig)
    .filter(([k]) => k)
    .sort((a, b) => {
      const na = Number(a[0].replace(/\D/g, "")) || 0;
      const nb = Number(b[0].replace(/\D/g, "")) || 0;
      return na - nb;
    });
}

function renderWordcloudPanel() {
  const fieldNode = document.getElementById("wordcloudField");
  const minNode = document.getElementById("wordcloudMinFreq");
  const minLenNode = document.getElementById("wordcloudMinLen");
  const maxLenNode = document.getElementById("wordcloudMaxLen");
  const stopNode = document.getElementById("wordcloudStopwords");
  const topNode = document.getElementById("wordcloudTopWords");
  const chartNode = document.getElementById("chartWordcloud");
  const detailTbody = document.querySelector("#wordcloudDetailTable tbody");
  if (!fieldNode || !minNode || !minLenNode || !maxLenNode || !stopNode || !topNode || !chartNode) return;

  const entries = getOpenConfigEntries();
  const prevSelected = [...fieldNode.selectedOptions].map((o) => o.value);
  setSelectOptions("wordcloudField", entries.map(([k, v]) => ({ id: k, name: `${k} ${v}` })));
  [...fieldNode.options].forEach((opt) => {
    opt.selected = prevSelected.includes(opt.value);
  });
  if (![...fieldNode.selectedOptions].length && fieldNode.options.length) fieldNode.options[0].selected = true;
  const selectedFields = [...fieldNode.selectedOptions].map((o) => o.value).filter(Boolean);
  if (!selectedFields.length) {
    topNode.innerHTML = "请先在规则配置中维护开放题字段。";
    chartNode.innerHTML = "";
    if (detailTbody) detailTbody.innerHTML = "";
    return;
  }

  const minFreq = Math.max(1, Number(minNode.value || 2));
  let minLen = Math.max(1, Number(minLenNode.value || 2));
  let maxLen = Math.max(1, Number(maxLenNode.value || 8));
  if (maxLen < minLen) {
    const t = maxLen;
    maxLen = minLen;
    minLen = t;
    minLenNode.value = String(minLen);
    maxLenNode.value = String(maxLen);
  }
  const stopwords = splitStopwords(stopNode.value);

  const counter = new Map();
  let sourceCount = 0; // 有效文本条数（按字段粒度）
  for (const row of analysisRows) {
    const rw = getRowWeight(row);
    for (const field of selectedFields) {
      const raw = str(row[field]);
      if (!raw) continue;
      sourceCount += rw;
      const words = [
        ...tokenizeZh(raw),
        ...extractContinuousPhrases(raw, minLen, maxLen),
      ];
      for (const w0 of words) {
        const w = w0.trim();
        if (!w) continue;
        if (w.length < minLen || w.length > maxLen) continue;
        if (/^\d+$/.test(w)) continue;
        if (stopwords.has(w)) continue;
        counter.set(w, (counter.get(w) || 0) + rw);
      }
    }
  }

  const words = [...counter.entries()]
    .map(([name, value]) => ({ name, value }))
    .filter((x) => x.value >= minFreq)
    .sort((a, b) => b.value - a.value)
    .slice(0, 180);

  if (!words.length) {
    const fieldNames = selectedFields.map((f) => `${f}（${openConfig[f] || f}）`).join("、");
    topNode.innerHTML = `题目：${fieldNames}<br/>无可展示词汇，请降低最小词频或减少停用词。`;
    chartNode.innerHTML = "";
    if (detailTbody) detailTbody.innerHTML = "";
    return;
  }

  const fieldNames = selectedFields.map((f) => `${f}（${openConfig[f] || f}）`).join("、");
  topNode.innerHTML = [
    `题目：${fieldNames}`,
    `样本量：${fmtCount(sourceCount)}`,
    `词长范围：${minLen}-${maxLen}字`,
    `词数：${words.length}`,
    `Top20：${words.slice(0, 20).map((x) => `${x.name}(${x.value})`).join("、")}`,
  ].join("<br/>");

  if (!window.echarts) return;
  const chart = echarts.getInstanceByDom(chartNode) || echarts.init(chartNode);
  chart.clear();
  chart.setOption({
    tooltip: {
      trigger: "item",
      formatter: (p) => `${p.name}<br/>频次: ${p.value}`,
    },
    series: [
      {
        type: "wordCloud",
        shape: "circle",
        left: "center",
        top: "center",
        width: "100%",
        height: "100%",
        sizeRange: [12, 56],
        rotationRange: [-45, 45],
        gridSize: 8,
        drawOutOfBound: false,
        textStyle: {
          color: () => {
            const palette = ["#0e7490", "#2563eb", "#0f766e", "#7c3aed", "#b45309", "#be123c"];
            return palette[Math.floor(Math.random() * palette.length)];
          },
        },
        emphasis: {
          focus: "self",
          textStyle: { shadowBlur: 8, shadowColor: "#94a3b8" },
        },
        data: words,
      },
    ],
  });
  requestAnimationFrame(() => chart.resize());

  if (!detailTbody) return;
  const rowsForTable = [];
  for (const row of analysisRows) {
    const parts = [];
    for (const f of selectedFields) {
      const text = str(row[f]);
      if (!text) continue;
      parts.push(`${openConfig[f] || f}: ${text}`);
    }
    if (!parts.length) continue;
    rowsForTable.push({
      id: str(row.id),
      age: getSingleLabel("q33", str(row.q33)),
      gender: getQuestionnaireGenderValue(row),
      answer: parts.join(" | "),
    });
  }
  detailTbody.innerHTML = rowsForTable
    .slice(0, 500)
    .map(
      (r) =>
        `<tr><td>${r.id || "-"}</td><td>${r.age || "-"}</td><td>${r.gender || "-"}</td><td style="white-space:normal;word-break:break-word;">${r.answer}</td></tr>`,
    )
    .join("");
}

async function bootstrap() {
  await loadLocal();
  rawRows = dedupRows(rawRows);
  loadSingleConfig();
  loadOpenConfig();
  loadFrameworkConfig();
  loadWeightConfig();
  loadWeightHistory();
  loadRules(getHeaders());
  recomputeAnalysisRows();

  setSelectOptions("analysisQuestion", getAnalysisQuestions().map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisAttr", getAttrQuestions().map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisFilter", getFilterOptions().map((x) => ({ id: x.id, name: x.name })));
  const filterNode = document.getElementById("analysisFilter");
  if (filterNode && filterNode.options.length) filterNode.options[0].selected = true;
  setFilterMode("include");

  bindTabs();
  bindActions();
  bindSingleConfigEditor();
  bindOpenConfigEditor();
  bindFrameworkBulkEditor();
  setImportProgress(0, "未开始导入");
  document.getElementById("wordcloudMinLen").value = "2";
  document.getElementById("wordcloudMaxLen").value = "8";
  document.getElementById("wordcloudStopwords").value = "原神 米游社 感觉 觉得 就是 一个 可以 还是 这个 那个";
  renderAll();
}

initAccessGate();
