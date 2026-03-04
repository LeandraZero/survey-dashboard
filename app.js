const STORAGE_UPLOAD_AT_KEY = "survey_last_upload_at_v1";
const RULES_KEY = "survey_rules_v1";
const SINGLE_CONFIG_KEY = "survey_single_config_v1";
const OPEN_CONFIG_KEY = "survey_open_config_v1";
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

const CHANNELS = {
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
let appStarted = false;

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

function isDataRow(row) {
  return /^\d+$/.test(str(row.id));
}

function getHeaders() {
  if (!rawRows.length) return [];
  const keys = new Set();
  for (const r of rawRows) {
    for (const k of Object.keys(r || {})) keys.add(k);
  }
  return [...keys];
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

function getSingleLabels(qid) {
  return (singleConfig[qid] && singleConfig[qid].labels) || {};
}

function getSingleTitle(qid, fallback = qid) {
  return (singleConfig[qid] && singleConfig[qid].title) || fallback;
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

  return [...optionDefs, ...singleDefs];
}

function getAttrQuestions() {
  return [
    { id: "q34", name: getSingleTitle("q34", "Q34"), col: "q34", labels: getSingleLabels("q34") },
    { id: "q1", name: getSingleTitle("q1", "Q1"), col: "q1", labels: getSingleLabels("q1") },
    { id: "q27", name: getSingleTitle("q27", "Q27"), col: "q27", labels: getSingleLabels("q27") },
    { id: "q33", name: getSingleTitle("q33", "Q33"), col: "q33", labels: getSingleLabels("q33") },
  ];
}

function getFilterOptions() {
  const q34Labels = getSingleLabels("q34");
  const q1Labels = getSingleLabels("q1");
  const q29Labels = getSingleLabels("q29");
  const q27Labels = getSingleLabels("q27");
  return [
    { id: "all", name: "不过滤", groupKey: "all", fn: () => true },
    ...Object.entries(q34Labels).map(([code, label]) => ({
      id: `q34=${code}`,
      name: `${getSingleTitle("q34", "Q34")}=${label}`,
      groupKey: "q34",
      fn: (r) => str(r.q34) === String(code),
    })),
    ...Object.entries(q1Labels).map(([code, label]) => ({
      id: `q1=${code}`,
      name: `${getSingleTitle("q1", "Q1")}=${label}`,
      groupKey: "q1",
      fn: (r) => str(r.q1) === String(code),
    })),
    ...Object.entries(q27Labels).map(([code, label]) => ({
      id: `q27=${code}`,
      name: `${getSingleTitle("q27", "Q27")}=${label}`,
      groupKey: "q27",
      fn: (r) => str(r.q27) === String(code),
    })),
    ...Object.entries(q29Labels).map(([code, label]) => ({
      id: `q29=${code}`,
      name: `${getSingleTitle("q29", "Q29")}=${label}`,
      groupKey: "q29",
      fn: (r) => str(r.q29) === String(code),
    })),
    ...Object.entries(Q2_CATEGORIES).map(([code, label]) => ({
      id: `q2_${code}=1`,
      name: `Q2包含：${label}`,
      groupKey: "q2",
      fn: (r) => str(r[`q2_${code}_${findQ2ColumnSuffix(code, r)}`] || r[`q2_${code}`]) === "1" || hasQ2ByCode(r, Number(code)),
    })),
  ];
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
    const labels = Object.fromEntries(
      Object.entries(q.options)
        .sort((a, b) => Number(a[0]) - Number(b[0]))
        .map(([k, v]) => [Number(k), v]),
    );
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
  if (lastImportStats) {
    lastImportStats = {
      ...lastImportStats,
      dedupRows: rawRows.length,
      terminateExcluded: sampleStats.terminateExcluded,
      invalidExcluded: sampleStats.invalidExcluded,
      analysisRows: analysisRows.length,
    };
  }
}

function dedupRows(rows) {
  const map = new Map();
  for (const row of rows) {
    if (!isDataRow(row)) continue;
    const id = str(row.id);
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
  const cols = getOptionColumns(Object.keys(rows[0]), prefix, maxCode);
  const answered = rows.filter((r) => cols.some((x) => str(r[x.col]) !== ""));
  const denom = answered.length;
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;

  for (const r of answered) {
    for (const { code, col } of cols) {
      if (!(code in labels)) continue;
      if (str(r[col]) === "1") counts[code] += 1;
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
  const cols = getOptionColumns(Object.keys(rows[0]), prefix);
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
    denom += 1;
    const top = ranked.find((x) => x.rank === 1);
    if (top && top.code in labels) counts[top.code] += 1;
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
  const cols = getOptionColumns(Object.keys(rows[0]), prefix);
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;

  let denom = 0;
  for (const r of rows) {
    let hasAnyRank = false;
    for (const { code, col } of cols) {
      const rank = toInt(r[col]);
      if (rank && rank > 0) {
        hasAnyRank = true;
        if (code in labels) counts[code] += 1;
      }
    }
    if (hasAnyRank) denom += 1;
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
  const denom = answered.length;
  const counts = {};
  for (const code of Object.keys(labels)) counts[code] = 0;
  for (const r of answered) {
    const v = str(r[col]);
    if (v in counts) counts[v] += 1;
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
  const sorted = preserveOrder ? [...rows] : [...rows].sort((a, b) => b.ratio - a.ratio);
  node.classList.add("barlist");
  node.innerHTML = sorted
    .map(
      (x) => `
      <div class="bar-row">
        <div class="bar-name ${x.name === "米游社" ? "mys-text" : ""}" title="${x.name}">${x.name}</div>
        <div class="bar-track"><div class="bar-fill ${x.name === "米游社" ? "mys-fill" : ""}" style="width:${(x.ratio * 100).toFixed(1)}%"></div></div>
        <div class="bar-pct">${fmtPct(x.ratio)}</div>
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

function topRankBarsHtml(sortedItems, limit = 3) {
  const formatName = (name) => (name === "米游社" ? `<span class="mys-text">米游社</span>` : name || "--");
  return sortedItems
    .slice(0, limit)
    .map(
      (x, idx) => `
      <div class="bar-row top-rank-row rank-${idx + 1}">
        <div class="bar-name" title="${x ? x.name : "--"}"><span class="top-rank-label" data-rank="${idx + 1}">Top${idx + 1}：</span>${formatName(x ? x.name : "--")}</div>
        <div class="bar-track"><div class="bar-fill ${x && x.name === "米游社" ? "mys-fill" : ""}" style="width:${x ? (x.ratio * 100).toFixed(1) : 0}%"></div></div>
        <div class="bar-pct">${x ? fmtPct(x.ratio) : "--"}</div>
      </div>`,
    )
    .join("");
}

function renderOverview() {
  const hintNode = document.getElementById("overviewHint");
  if (hintNode) hintNode.textContent = `数据更新至 ${fmtTime(lastUploadAt)}`;
  const sampleNode = document.getElementById("overviewSampleLine");
  if (sampleNode) sampleNode.textContent = `总样本：${analysisRows.length}`;

  const top1 = calcRankTop1(analysisRows, "q4", CHANNELS);
  const sortedTop1 = [...top1.items].sort((a, b) => b.ratio - a.ratio);
  const rankNode = document.getElementById("ovOverallTopRanks");
  if (rankNode) {
    const top3Html = topRankBarsHtml(sortedTop1, 3);
    rankNode.innerHTML = `<div class="bar-rows">${top3Html}</div>`;
  }
  const mys = sortedTop1.find((x) => x.name === "米游社");
  const rank = sortedTop1.findIndex((x) => x.name === "米游社");
  const mysExtraNode = document.getElementById("ovOverallMysExtra");
  if (mysExtraNode) {
    if (rank > 2 && mys) {
      mysExtraNode.style.display = "";
      mysExtraNode.innerHTML = `<div class="bar-row top-rank-row rank-extra"><div class="bar-name"><span class="top-rank-label">Top${rank + 1}：</span><span class="mys-text">米游社</span></div><div class="bar-track"><div class="bar-fill mys-fill" style="width:${(mys.ratio * 100).toFixed(1)}%"></div></div><div class="bar-pct">${fmtPct(mys.ratio)}</div></div>`;
    } else {
      mysExtraNode.style.display = "none";
      mysExtraNode.textContent = "";
    }
  }

  drawBarChart("chartTop1", top1.items, "Top1 渠道占比");
  renderOverviewSceneGrid();
}

function renderOverviewSceneGrid() {
  const grid = document.getElementById("sceneGrid");
  const tooltip = document.getElementById("sceneTooltip");
  if (!grid || !tooltip) return;

  const sceneData = OVERVIEW_SCENES.map((scene) => {
    const top1 = calcRankTop1(analysisRows, scene.rank, CHANNELS);
    const top1Sorted = [...top1.items].sort((a, b) => b.ratio - a.ratio);
    const mysRank = top1Sorted.findIndex((x) => x.name === "米游社");
    const mys = top1Sorted.find((x) => x.name === "米游社");
    const channels = calcRankPresence(analysisRows, scene.rank, CHANNELS).items.sort((a, b) => b.ratio - a.ratio);
    return {
      ...scene,
      top3: top1Sorted.slice(0, 3),
      mysRank,
      mys,
      channels,
    };
  });

  grid.innerHTML = sceneData
    .map(
      (s, i) => `
      <article class="scene-card" data-scene-index="${i}">
        <div class="scene-title">${s.name}</div>
        <div class="bar-rows">${topRankBarsHtml(s.top3, 3)}</div>
        ${s.mys && s.mysRank > 2 ? `<div class="bar-row top-rank-row rank-extra"><div class="bar-name"><span class="top-rank-label">Top${s.mysRank + 1}：</span><span class="mys-text">米游社</span></div><div class="bar-track"><div class="bar-fill mys-fill" style="width:${(s.mys.ratio * 100).toFixed(1)}%"></div></div><div class="bar-pct">${fmtPct(s.mys.ratio)}</div></div>` : ""}
      </article>`,
    )
    .join("");

  const renderTooltip = (scene, x, y) => {
    const bars = scene.channels
      .map(
        (c) => `
        <div class="bar-row">
          <div class="bar-name ${c.name === "米游社" ? "mys-text" : ""}">${c.name}</div>
          <div class="bar-track"><div class="bar-fill ${c.name === "米游社" ? "mys-fill" : ""}" style="width:${(c.ratio * 100).toFixed(1)}%"></div></div>
          <div class="bar-pct">${fmtPct(c.ratio)}</div>
        </div>`,
      )
      .join("");
    tooltip.innerHTML = `<div class="scene-tip-title">${scene.name} 渠道占比</div><div class="bar-rows bar-rows-dark">${bars}</div>`;
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
  if (!selected.length || (selected.length === 1 && selected[0] === "all")) {
    return [filterOptions[0]];
  }
  return selected
    .filter((id) => id !== "all")
    .map((id) => filterOptions.find((x) => x.id === id))
    .filter(Boolean);
}

function applyGroupedFilters(rows, filterDefs) {
  if (!filterDefs.length || filterDefs[0]?.id === "all") return rows;
  const groups = new Map();
  for (const def of filterDefs) {
    const key = def.groupKey || def.id;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(def);
  }

  // SPSS 多重响应思路：同一题内 OR，不同题之间 AND
  return rows.filter((row) => {
    for (const defs of groups.values()) {
      if (!defs.some((d) => d.fn(row))) return false;
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
  drawBarChart("chartAnalysis", overall.items, `${qDef.name}（样本: ${filtered.length}）`, {
    preserveOrder: qDef.id === "q29",
  });

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
    const groupRows = filtered.filter((r) => str(r[attrDef.col]) === String(code));
    if (!groupRows.length) continue;
    const dist = getQuestionDistribution(groupRows, qDef);
    const byCode = new Map(dist.items.map((x) => [x.code, x]));

    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${label}</td><td>${groupRows.length}</td>${sortedCols
      .map((col) => `<td>${fmtPct((byCode.get(col.code) || { ratio: 0 }).ratio)}</td>`)
      .join("")}`;
    tbody.appendChild(tr);
  }
}

function renderUploadMeta() {
  const msg = [
    `分析样本（继续作答）: ${analysisRows.length}`,
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

function renderSingleConfigPanel() {
  const fieldSelect = document.getElementById("singleConfigField");
  const titleInput = document.getElementById("singleConfigTitle");
  const optionsInput = document.getElementById("singleConfigOptions");
  const preview = document.getElementById("singleConfigPreview");
  if (!fieldSelect || !titleInput || !optionsInput || !preview) return;

  const headerCandidates = getHeaders().filter((h) => /^q\d+$/.test(h));
  const singleKeys = Array.from(new Set([...Object.keys(singleConfig), ...headerCandidates]))
    .sort((a, b) => Number(a.replace("q", "")) - Number(b.replace("q", "")));

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
  if (!fieldSelect || !titleInput || !saveBtn) return;

  fieldSelect.addEventListener("change", renderOpenConfigPanel);
  saveBtn.addEventListener("click", () => {
    const key = fieldSelect.value;
    if (!key) return;
    const title = str(titleInput.value) || key;
    openConfig[key] = title;
    saveOpenConfig();
    renderOpenConfigPanel();
    alert("开放题配置已保存");
  });
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
      renderCross();
      setTimeout(renderCross, 80);
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
    document.getElementById(id).addEventListener("change", renderCross);
  });
  document.getElementById("btnExportCross").addEventListener("click", exportCrossTableCsv);
  document.getElementById("btnSaveRules").addEventListener("click", saveRulesFromPanel);
  document.getElementById("btnResetRules").addEventListener("click", resetRulesToDefault);
  document.getElementById("btnGenerateWordcloud").addEventListener("click", renderWordcloudPanel);
}

function renderAll() {
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
    for (const field of selectedFields) {
      const raw = str(row[field]);
      if (!raw) continue;
      sourceCount += 1;
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
        counter.set(w, (counter.get(w) || 0) + 1);
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
    `样本量：${sourceCount}`,
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
      gender: getSingleLabel("q34", str(row.q34)),
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
  loadRules(getHeaders());
  recomputeAnalysisRows();

  setSelectOptions("analysisQuestion", getAnalysisQuestions().map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisAttr", getAttrQuestions().map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisFilter", getFilterOptions().map((x) => ({ id: x.id, name: x.name })));
  const filterNode = document.getElementById("analysisFilter");
  if (filterNode && filterNode.options.length) filterNode.options[0].selected = true;

  bindTabs();
  bindActions();
  bindSingleConfigEditor();
  bindOpenConfigEditor();
  setImportProgress(0, "未开始导入");
  document.getElementById("wordcloudMinLen").value = "2";
  document.getElementById("wordcloudMaxLen").value = "8";
  document.getElementById("wordcloudStopwords").value = "原神 米游社 感觉 觉得 就是 一个 可以 还是 这个 那个";
  renderAll();
}

initAccessGate();
