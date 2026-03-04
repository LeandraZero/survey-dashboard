const STORAGE_UPLOAD_AT_KEY = "survey_last_upload_at_v1";
const RULES_KEY = "survey_rules_v1";
const ACCESS_PASSWORD = "miyoushe2026";
const ACCESS_SESSION_KEY = "survey_access_granted_v1";
const DB_NAME = "survey_dashboard_db";
const DB_VERSION = 1;
const STORE_NAME = "kv";
const RAW_ROWS_KEY = "survey_raw_rows_v1";
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

const ANALYSIS_QUESTIONS = [
  { id: "q4", name: "Q4 整体第一心智", type: "rank_top1", prefix: "q4", labels: CHANNELS },
  { id: "q3", name: "Q3 整体渠道渗透", type: "multi", prefix: "q3", labels: CHANNELS },
  { id: "q2", name: "Q2 内容心智", type: "multi", prefix: "q2", labels: Q2_CATEGORIES },
  { id: "q29", name: "Q29 米游社满意度", type: "single", col: "q29", labels: Q29_LABELS },
  { id: "q27", name: "Q27 米游社使用频次", type: "single", col: "q27", labels: Q27_LABELS },
];

const ATTR_QUESTIONS = [
  { id: "q34", name: "Q34 性别", col: "q34", labels: Q34_LABELS },
  { id: "q1", name: "Q1 活跃度", col: "q1", labels: Q1_LABELS },
  { id: "q27", name: "Q27 米游社使用频次", col: "q27", labels: Q27_LABELS },
  {
    id: "q33",
    name: "Q33 出生年份",
    col: "q33",
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
];

const FILTER_OPTIONS = [
  { id: "all", name: "不过滤", fn: () => true },
  ...Object.entries(Q34_LABELS).map(([code, label]) => ({
    id: `q34=${code}`,
    name: `性别=${label}`,
    fn: (r) => str(r.q34) === String(code),
  })),
  ...Object.entries(Q1_LABELS).map(([code, label]) => ({
    id: `q1=${code}`,
    name: `活跃度=${label}`,
    fn: (r) => str(r.q1) === String(code),
  })),
  ...Object.entries(Q29_LABELS).map(([code, label]) => ({
    id: `q29=${code}`,
    name: `满意度=${label}`,
    fn: (r) => str(r.q29) === String(code),
  })),
  ...Object.entries(Q2_CATEGORIES).map(([code, label]) => ({
    id: `q2_${code}=1`,
    name: `Q2包含：${label}`,
    fn: (r) => str(r[`q2_${code}_${findQ2ColumnSuffix(code, r)}`] || r[`q2_${code}`]) === "1" || hasQ2ByCode(r, Number(code)),
  })),
];

let rawRows = [];
let analysisRows = [];
let lastUploadAt = "";
let lastImportStats = null;
let sampleStats = { total: 0, terminateExcluded: 0, invalidExcluded: 0, final: 0 };
let currentRules = { ...DEFAULT_RULES };
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
  return Object.keys(rawRows[0]);
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

function drawBarChart(elId, rows, title) {
  const node = document.getElementById(elId);
  if (!node) return;
  const sorted = [...rows].sort((a, b) => b.ratio - a.ratio);

  if (!window.echarts) {
    node.innerHTML = sorted
      .map((x) => `<div style="margin:8px 0;display:grid;grid-template-columns:150px 1fr 60px;gap:8px;align-items:center;">
        <span>${x.name}</span>
        <span style="background:#e6eef7;height:10px;border-radius:8px;overflow:hidden;"><span style="display:block;width:${(x.ratio * 100).toFixed(1)}%;background:#0e7490;height:100%;"></span></span>
        <span>${fmtPct(x.ratio)}</span>
      </div>`)
      .join("");
    return;
  }

  const chart = echarts.getInstanceByDom(node) || echarts.init(node);
  chart.clear();
  chart.setOption({
    title: { text: title, textStyle: { fontSize: 13, fontWeight: 500 } },
    tooltip: {
      trigger: "item",
      formatter: (p) => `${p.name}<br/>样本量: ${p.data.count}`,
    },
    grid: { left: 110, right: 20, top: 40, bottom: 20 },
    xAxis: {
      type: "value",
      axisLabel: { formatter: (v) => `${v}%` },
    },
    yAxis: {
      type: "category",
      data: sorted.map((x) => x.name),
    },
    series: [
      {
        type: "bar",
        data: sorted.map((x) => ({ value: +(x.ratio * 100).toFixed(2), count: x.count })),
        itemStyle: { color: "#0e7490" },
        label: { show: true, position: "right", formatter: (p) => `${p.value}%` },
      },
    ],
  });
  // 防止在隐藏面板初始化时出现 0 宽高导致空白
  requestAnimationFrame(() => chart.resize());
}

function renderOverview() {
  document.getElementById("overviewHint").textContent = `数据更新至 ${fmtTime(lastUploadAt)}`;
  document.getElementById("kpiSample").textContent = String(analysisRows.length);

  const top1 = calcRankTop1(analysisRows, "q4", CHANNELS);
  const sortedTop1 = [...top1.items].sort((a, b) => b.ratio - a.ratio);
  const best = sortedTop1[0];
  document.getElementById("kpiTop1").textContent = best ? best.name : "--";
  document.getElementById("kpiTop1Ratio").textContent = best ? `${fmtPct(best.ratio)}（${best.count}）` : "--";

  const mys = sortedTop1.find((x) => x.name === "米游社");
  const rank = sortedTop1.findIndex((x) => x.name === "米游社");
  document.getElementById("kpiMysRank").textContent = rank >= 0 ? `Top${rank + 1}` : "--";
  document.getElementById("kpiMysRatio").textContent = mys ? `${fmtPct(mys.ratio)}（${mys.count}）` : "--";

  const sat = calcSingle(analysisRows, "q29", Q29_LABELS);
  const satPositive = sat.items.filter((x) => x.code === 1 || x.code === 2).reduce((a, b) => a + b.count, 0);
  const satRatio = sat.denominator ? satPositive / sat.denominator : 0;
  document.getElementById("kpiSat").textContent = fmtPct(satRatio);
  document.getElementById("kpiSatDetail").textContent = `满意（非常+比较）${satPositive}/${sat.denominator}`;

  drawBarChart("chartTop1", top1.items, "Top1 渠道占比");
  drawBarChart("chartSat", sat.items, "满意度占比");
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
  const node = document.getElementById("analysisFilter");
  const selected = [...node.selectedOptions].map((x) => x.value);
  if (!selected.length || (selected.length === 1 && selected[0] === "all")) {
    return [FILTER_OPTIONS[0]];
  }
  return selected
    .filter((id) => id !== "all")
    .map((id) => FILTER_OPTIONS.find((x) => x.id === id))
    .filter(Boolean);
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
  const qId = document.getElementById("analysisQuestion").value;
  const attrId = document.getElementById("analysisAttr").value;

  const qDef = ANALYSIS_QUESTIONS.find((x) => x.id === qId) || ANALYSIS_QUESTIONS[0];
  const attrDef = ATTR_QUESTIONS.find((x) => x.id === attrId) || ATTR_QUESTIONS[0];
  const filterDefs = getSelectedFilterDefs();

  const filtered = analysisRows.filter((r) => filterDefs.every((f) => f.fn(r)));
  const overall = getQuestionDistribution(filtered, qDef);
  drawBarChart("chartAnalysis", overall.items, `${qDef.name}（样本: ${filtered.length}）`);

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
}

function renderAll() {
  renderUploadMeta();
  renderOverview();
  renderRulesPanel();
  if (document.getElementById("panel-cross").classList.contains("active")) {
    renderCross();
  }
}

async function bootstrap() {
  await loadLocal();
  rawRows = dedupRows(rawRows);
  loadRules(getHeaders());
  recomputeAnalysisRows();

  setSelectOptions("analysisQuestion", ANALYSIS_QUESTIONS.map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisAttr", ATTR_QUESTIONS.map((x) => ({ id: x.id, name: x.name })));
  setSelectOptions("analysisFilter", FILTER_OPTIONS.map((x) => ({ id: x.id, name: x.name })));
  const filterNode = document.getElementById("analysisFilter");
  if (filterNode && filterNode.options.length) {
    filterNode.options[0].selected = true;
  }

  bindTabs();
  bindActions();
  setImportProgress(0, "未开始导入");
  renderAll();
}

initAccessGate();
