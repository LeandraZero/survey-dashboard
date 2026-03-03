# 问卷导入与看板

## 已实现
- 导入米哈游导出的宽表 CSV（按选项拆列）
- 自动清洗（移除非数据行、按 `id` 去重保留最新）
- 自动筛选分析样本：仅保留“继续作答样本”（排除 `q2=11` 终止问卷人群）
- 输出核心指标：
  - 男女占比（Q34）
  - 9类内容心智占比（Q2，strict/expanded 双口径）
  - 整体渠道渗透（Q3）
  - 整体第一心智（Q4）
  - 分场景渠道渗透/第一心智（Q7-26）
  - 米游社使用频次（Q27）与满意度（Q29）
- 产出文件：
  - `metrics_summary.json`
  - `metrics_flat.csv`
  - `dashboard.html`

## 运行
```bash
cd /Users/zexuan.ling/Documents/问卷看板
./survey_dashboard.py '/Users/zexuan.ling/Library/Application Support/miHoYo/HoYowave/Shell/File/Downloads/26141_米游社玩家心智摸底问卷_26948_1.csv' --out-dir ./output
```

## 口径说明
- 分析样本：只统计继续作答样本（`q2_11 != 1`）
- 多选题占比：`选择该选项人数 / 该题作答人数`
- 排序题第一心智：仅统计名次 `=1`，分母为该排序题有效作答人数
- 排序题中的 `-1`、`0` 视为未进入有效排名
- Q2 双口径：
  - `strict`：仅按 1-9 的显式勾选统计
  - `expanded`：若选了 `q2_10(以上内容我都会)`，会扩展计入 1-9

## 面板查看
直接打开：`/Users/zexuan.ling/Documents/问卷看板/output/dashboard.html`
