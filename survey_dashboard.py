#!/usr/bin/env python3
import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

CHANNELS = {
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
}

Q2_CATEGORIES = {
    1: "抽卡决策/原石/强度对比",
    2: "角色配队/养成/手法",
    3: "探索解谜攻略",
    4: "剧情解析",
    5: "剧情讨论",
    6: "同人",
    7: "游戏外活动",
    8: "周边",
    9: "Cosplay",
}

SCENE_MAPPING = [
    ("抽卡与原石", "q7", "q8"),
    ("配队养成", "q10", "q11"),
    ("探索解谜", "q13", "q14"),
    ("剧情解析", "q15", "q16"),
    ("剧情讨论", "q17", "q18"),
    ("同人", "q19", "q20"),
    ("游戏外活动", "q21", "q22"),
    ("周边", "q23", "q24"),
    ("Cosplay", "q25", "q26"),
]

GENDER_MAP = {"1": "男", "2": "女", "3": "不方便透露"}


def parse_args():
    parser = argparse.ArgumentParser(description="问卷导入与统计")
    parser.add_argument("input", help="CSV 路径")
    parser.add_argument(
        "--out-dir",
        default="output",
        help="输出目录（默认: output）",
    )
    return parser.parse_args()


def to_int(text):
    if text is None:
        return None
    t = str(text).strip()
    if not t:
        return None
    try:
        return int(t)
    except ValueError:
        return None


def parse_time(text):
    t = str(text).strip()
    if not t:
        return datetime.min
    try:
        return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.min


def load_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        raw = list(reader)

    valid = []
    for row in raw:
        rid = str(row.get("id", "")).strip()
        if not rid.isdigit():
            continue
        valid.append(row)

    dedup = {}
    for row in valid:
        rid = str(row.get("id", "")).strip()
        if rid not in dedup:
            dedup[rid] = row
            continue
        if parse_time(row.get("create_time")) >= parse_time(dedup[rid].get("create_time")):
            dedup[rid] = row

    return list(dedup.values())


def is_continue_respondent(row):
    # q2 选了“我不交流/讨论《原神》相关的任何内容”即终止问卷，不纳入继续作答样本
    return str(row.get("q2_11_我不交流/讨论《原神》相关的任何内容", "")).strip() != "1"


def find_option_columns(headers, qprefix):
    pattern = re.compile(rf"^{re.escape(qprefix)}_(\d+)_")
    found = []
    for h in headers:
        if h.startswith(f"qc{qprefix[1:]}_"):
            continue
        m = pattern.match(h)
        if not m:
            continue
        found.append((int(m.group(1)), h))
    return sorted(found, key=lambda x: x[0])


def answered_multi(row, cols):
    for _, c in cols:
        if str(row.get(c, "")).strip() != "":
            return True
    return False


def selected_multi(row, col):
    return str(row.get(col, "")).strip() == "1"


def calc_multi(rows, cols, code_to_name):
    answered_rows = [r for r in rows if answered_multi(r, cols)]
    denom = len(answered_rows)
    counts = Counter()
    for r in answered_rows:
        for code, col in cols:
            if selected_multi(r, col):
                counts[code] += 1

    items = []
    for code, _ in cols:
        if code not in code_to_name:
            continue
        cnt = counts.get(code, 0)
        pct = (cnt / denom) if denom else 0.0
        items.append(
            {
                "code": code,
                "name": code_to_name[code],
                "count": cnt,
                "ratio": round(pct, 4),
            }
        )
    return {"denominator": denom, "items": items}


def calc_ranking_first(rows, cols, code_to_name):
    answered_rows = []
    first_counts = Counter()
    for r in rows:
        ranks = {}
        for code, col in cols:
            v = to_int(r.get(col, ""))
            if v is None or v <= 0:
                continue
            ranks[code] = v
        if not ranks:
            continue
        answered_rows.append(r)
        for code, rank in ranks.items():
            if rank == 1:
                first_counts[code] += 1

    denom = len(answered_rows)
    items = []
    for code, _ in cols:
        if code not in code_to_name:
            continue
        cnt = first_counts.get(code, 0)
        pct = (cnt / denom) if denom else 0.0
        items.append(
            {
                "code": code,
                "name": code_to_name[code],
                "count": cnt,
                "ratio": round(pct, 4),
            }
        )
    return {"denominator": denom, "items": items}


def calc_gender(rows):
    answered = [r for r in rows if str(r.get("q34", "")).strip() != ""]
    denom = len(answered)
    cnt = Counter(str(r.get("q34", "")).strip() for r in answered)
    items = []
    for code in ["1", "2", "3"]:
        count = cnt.get(code, 0)
        items.append(
            {
                "code": code,
                "name": GENDER_MAP[code],
                "count": count,
                "ratio": round((count / denom) if denom else 0.0, 4),
            }
        )
    return {"denominator": denom, "items": items}


def calc_q2(rows, headers):
    cols = [x for x in find_option_columns(headers, "q2") if x[0] <= 11]
    answered_rows = [r for r in rows if answered_multi(r, cols)]
    strict_denom = len(answered_rows)

    strict_counts = Counter()
    expanded_counts = Counter()

    for r in answered_rows:
        selected = {code for code, col in cols if selected_multi(r, col)}
        for code in range(1, 10):
            if code in selected:
                strict_counts[code] += 1
        if 10 in selected:
            for code in range(1, 10):
                expanded_counts[code] += 1
        for code in range(1, 10):
            if code in selected:
                expanded_counts[code] += 1

    strict_items = []
    expanded_items = []
    for code, name in Q2_CATEGORIES.items():
        s = strict_counts.get(code, 0)
        e = expanded_counts.get(code, 0)
        strict_items.append(
            {
                "code": code,
                "name": name,
                "count": s,
                "ratio": round((s / strict_denom) if strict_denom else 0.0, 4),
            }
        )
        expanded_items.append(
            {
                "code": code,
                "name": name,
                "count": e,
                "ratio": round((e / strict_denom) if strict_denom else 0.0, 4),
            }
        )

    return {
        "denominator": strict_denom,
        "strict": strict_items,
        "expanded": expanded_items,
    }


def calc_single_choice(rows, qcode, labels):
    answered = [r for r in rows if str(r.get(qcode, "")).strip() != ""]
    denom = len(answered)
    cnt = Counter(str(r.get(qcode, "")).strip() for r in answered)
    items = []
    for code, name in labels.items():
        n = cnt.get(str(code), 0)
        items.append(
            {
                "code": int(code),
                "name": name,
                "count": n,
                "ratio": round((n / denom) if denom else 0.0, 4),
            }
        )
    return {"denominator": denom, "items": items}


def write_csv(path, rows, headers):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def render_dashboard_html(summary):
    payload = json.dumps(summary, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>问卷看板</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    body {{
      margin: 0;
      padding: 20px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      background: #f7f8fa;
      color: #222;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
      gap: 16px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #e8ebf0;
      border-radius: 10px;
      padding: 14px;
    }}
    h1 {{ margin: 0 0 10px 0; font-size: 22px; }}
    .meta {{ color: #5f6b7a; font-size: 13px; margin-bottom: 14px; }}
    .chart {{ height: 320px; }}
    .fallback {{ padding: 8px 4px; }}
    .f-row {{
      display: grid;
      grid-template-columns: 140px 1fr 56px;
      gap: 8px;
      align-items: center;
      margin: 8px 0;
      font-size: 12px;
    }}
    .f-bar {{
      height: 10px;
      background: #e6ecff;
      border-radius: 6px;
      position: relative;
      overflow: hidden;
    }}
    .f-fill {{ height: 100%; background: #3f7cff; border-radius: 6px; }}
    .table-wrap {{ overflow: auto; max-height: 320px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef1f5; padding: 8px 6px; text-align: left; }}
    th {{ background: #fafbfd; position: sticky; top: 0; }}
  </style>
</head>
<body>
  <h1>原神问卷数据面板（继续作答样本口径）</h1>
  <div class="meta" id="meta"></div>
  <div class="grid">
    <div class="card">
      <h3>男女占比（Q34）</h3>
      <div id="genderChart" class="chart"></div>
    </div>
    <div class="card">
      <h3>整体渠道渗透（Q3）</h3>
      <div id="q3Chart" class="chart"></div>
    </div>
    <div class="card">
      <h3>整体第一心智（Q4，Top1）</h3>
      <div id="q4Chart" class="chart"></div>
    </div>
    <div class="card">
      <h3>9类内容心智占比（Q2 Strict）</h3>
      <div id="q2Chart" class="chart"></div>
    </div>
    <div class="card" style="grid-column: 1 / -1;">
      <h3>分场景第一心智（Top1）</h3>
      <div style="margin-bottom: 10px;">
        <label for="sceneSelect">选择场景：</label>
        <select id="sceneSelect"></select>
      </div>
      <div id="sceneTop1Chart" class="chart"></div>
      <div class="table-wrap">
        <table id="sceneTable">
          <thead>
            <tr><th>排名</th><th>渠道</th><th>占比</th><th>样本</th></tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>
<script>
const data = {payload};
const pct = v => (v * 100).toFixed(1) + '%';
document.getElementById('meta').textContent =
  '口径: 已排除 q2=11 终止问卷样本 | 分析样本: ' + data.sample_size + ' | 排除样本: ' + data.excluded_q2_11_count;

function barChart(el, rows, title) {{
  const elNode = document.getElementById(el);
  const sorted = [...rows].sort((a,b) => b.ratio - a.ratio);
  if (window.echarts) {{
    const chart = echarts.init(elNode);
    chart.setOption({{
      grid: {{ left: 80, right: 20, top: 30, bottom: 20 }},
      tooltip: {{
        trigger: 'item',
        formatter: (p) => `${{p.name}}<br/>样本量: ${{p.data.count}}`
      }},
      xAxis: {{
        type: 'value',
        axisLabel: {{ formatter: v => v + '%' }}
      }},
      yAxis: {{
        type: 'category',
        data: sorted.map(x => x.name)
      }},
      series: [{{
        type: 'bar',
        data: sorted.map(x => ({{
          value: +(x.ratio * 100).toFixed(2),
          count: x.count
        }})),
        label: {{ show: true, position: 'right', formatter: p => p.value + '%' }},
        itemStyle: {{ color: '#3f7cff' }}
      }}]
    }});
    return;
  }}

  elNode.innerHTML = '<div class="fallback"></div>';
  const wrap = elNode.firstElementChild;
  for (const r of sorted) {{
    const row = document.createElement('div');
    row.className = 'f-row';
    row.innerHTML = `<div>${{r.name}}</div><div class="f-bar"><div class="f-fill" style="width:${{(r.ratio*100).toFixed(1)}}%"></div></div><div>${{pct(r.ratio)}}</div>`;
    wrap.appendChild(row);
  }}
}}

try {{
  barChart('genderChart', [...data.gender.items].sort((a,b) => b.ratio - a.ratio), '男女占比');
  barChart('q3Chart', [...data.overall_channel_penetration.items].sort((a,b) => b.ratio - a.ratio), '整体渠道渗透');
  barChart('q4Chart', [...data.overall_first_mindshare.items].sort((a,b) => b.ratio - a.ratio), '第一心智');
  barChart('q2Chart', [...data.q2_content_mindshare.strict].sort((a,b) => b.ratio - a.ratio), '9类内容');

  const sceneSelect = document.getElementById('sceneSelect');
  data.scenes.forEach((scene, idx) => {{
    const opt = document.createElement('option');
    opt.value = String(idx);
    opt.textContent = scene.scene;
    sceneSelect.appendChild(opt);
  }});

  const tbody = document.querySelector('#sceneTable tbody');

  function renderSceneTop1(index) {{
    const scene = data.scenes[index] || data.scenes[0];
    const sorted = [...scene.first_mindshare.items].sort((a,b) => b.ratio - a.ratio);
    barChart('sceneTop1Chart', sorted, scene.scene + ' 第一心智');
    tbody.innerHTML = '';
    sorted.forEach((row, idx) => {{
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${{idx + 1}}</td><td>${{row.name}}</td><td>${{pct(row.ratio)}}</td><td>${{scene.first_mindshare.denominator}}</td>`;
      tbody.appendChild(tr);
    }});
  }}

  sceneSelect.addEventListener('change', (e) => {{
    renderSceneTop1(Number(e.target.value || 0));
  }});
  renderSceneTop1(0);
}} catch (e) {{
  document.getElementById('meta').textContent += ' | 页面渲染异常: ' + e.message;
}}
</script>
</body>
</html>"""


def flatten_for_csv(summary):
    records = []

    for item in summary["gender"]["items"]:
        records.append(
            {
                "metric_group": "gender",
                "segment": "overall",
                "option": item["name"],
                "count": item["count"],
                "ratio": item["ratio"],
                "denominator": summary["gender"]["denominator"],
            }
        )

    for mode in ["strict", "expanded"]:
        for item in summary["q2_content_mindshare"][mode]:
            records.append(
                {
                    "metric_group": f"q2_content_mindshare_{mode}",
                    "segment": "overall",
                    "option": item["name"],
                    "count": item["count"],
                    "ratio": item["ratio"],
                    "denominator": summary["q2_content_mindshare"]["denominator"],
                }
            )

    for item in summary["overall_channel_penetration"]["items"]:
        records.append(
            {
                "metric_group": "overall_channel_penetration",
                "segment": "overall",
                "option": item["name"],
                "count": item["count"],
                "ratio": item["ratio"],
                "denominator": summary["overall_channel_penetration"]["denominator"],
            }
        )

    for item in summary["overall_first_mindshare"]["items"]:
        records.append(
            {
                "metric_group": "overall_first_mindshare",
                "segment": "overall",
                "option": item["name"],
                "count": item["count"],
                "ratio": item["ratio"],
                "denominator": summary["overall_first_mindshare"]["denominator"],
            }
        )

    for scene in summary["scenes"]:
        for item in scene["penetration"]["items"]:
            records.append(
                {
                    "metric_group": "scene_channel_penetration",
                    "segment": scene["scene"],
                    "option": item["name"],
                    "count": item["count"],
                    "ratio": item["ratio"],
                    "denominator": scene["penetration"]["denominator"],
                }
            )
        for item in scene["first_mindshare"]["items"]:
            records.append(
                {
                    "metric_group": "scene_first_mindshare",
                    "segment": scene["scene"],
                    "option": item["name"],
                    "count": item["count"],
                    "ratio": item["ratio"],
                    "denominator": scene["first_mindshare"]["denominator"],
                }
            )

    for metric_name in ["q27_usage_frequency", "q29_satisfaction"]:
        block = summary[metric_name]
        for item in block["items"]:
            records.append(
                {
                    "metric_group": metric_name,
                    "segment": "overall",
                    "option": item["name"],
                    "count": item["count"],
                    "ratio": item["ratio"],
                    "denominator": block["denominator"],
                }
            )

    return records


def main():
    args = parse_args()
    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_rows = load_rows(input_path)
    if not raw_rows:
        raise SystemExit("没有可用数据行")
    rows = [r for r in raw_rows if is_continue_respondent(r)]
    if not rows:
        raise SystemExit("没有继续作答样本")

    headers = list(rows[0].keys())

    summary = {
        "source_file": str(input_path),
        "raw_sample_size": len(raw_rows),
        "excluded_q2_11_count": len(raw_rows) - len(rows),
        "sample_size": len(rows),
        "gender": calc_gender(rows),
        "q2_content_mindshare": calc_q2(rows, headers),
        "overall_channel_penetration": calc_multi(rows, find_option_columns(headers, "q3"), CHANNELS),
        "overall_first_mindshare": calc_ranking_first(rows, find_option_columns(headers, "q4"), CHANNELS),
        "scenes": [],
        "q27_usage_frequency": calc_single_choice(
            rows,
            "q27",
            {
                1: "几乎每天都会使用",
                2: "每周2-3次",
                3: "每周1次或更少",
                4: "只在新版本/重大活动时使用",
                5: "我不使用米游社",
            },
        ),
        "q29_satisfaction": calc_single_choice(
            rows,
            "q29",
            {
                1: "非常满意",
                2: "比较满意",
                3: "一般般",
                4: "比较不满意",
                5: "非常不满意",
            },
        ),
    }

    for scene_name, multi_q, rank_q in SCENE_MAPPING:
        summary["scenes"].append(
            {
                "scene": scene_name,
                "penetration": calc_multi(rows, find_option_columns(headers, multi_q), CHANNELS),
                "first_mindshare": calc_ranking_first(rows, find_option_columns(headers, rank_q), CHANNELS),
            }
        )

    json_path = out_dir / "metrics_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    flat_rows = flatten_for_csv(summary)
    csv_path = out_dir / "metrics_flat.csv"
    write_csv(
        csv_path,
        flat_rows,
        ["metric_group", "segment", "option", "count", "ratio", "denominator"],
    )

    html_path = out_dir / "dashboard.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(render_dashboard_html(summary))

    print(f"valid_sample={summary['sample_size']}")
    print(f"json={json_path}")
    print(f"csv={csv_path}")
    print(f"html={html_path}")


if __name__ == "__main__":
    main()
