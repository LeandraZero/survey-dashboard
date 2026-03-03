# 问卷网站（数据总览 / 交叉分析 / 数据录入）

## 页面结构
- 数据总览：首页标题为“游戏讨论心智”，展示总样本、游戏讨论心智首选Top1、米游社首选心智TopX、米游社满意度，并显示“数据更新至 YYYY/MM/DD HH:mm:ss”。
- 交叉分析：3个筛选框（筛选题目、筛选用户属性、筛选某道题答案），输出分布图和交叉表。
- 数据录入：上传 Excel/CSV 读取问卷源数据，自动更新全站。

## 数据口径
- 仅统计继续作答样本（排除 `q2=11` 终止问卷人群）。
- 同 `id` 自动去重，保留 `create_time` 最新记录。
- 图表悬浮提示显示样本量（count），排序均按占比从高到低。

## 本地运行
直接打开根目录的 `index.html`。

## GitHub Pages
1. 仓库 `Settings -> Pages`
2. Source 选 `Deploy from a branch`
3. Branch 选 `main`，Folder 选 `/ (root)`
4. 访问：`https://leandrazero.github.io/survey-dashboard/`

## 旧脚本
`survey_dashboard.py` 与 `output/` 仍保留，可继续用于离线批处理产出 JSON/CSV。
