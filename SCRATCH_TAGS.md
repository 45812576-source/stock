# 股票标签体系实施

## 需求
三类标签全显示：
1. 选股标签：stock_rule_tags（L1/L2/L3引擎已有）
2. 行业标签：KG belongs_to_industry 关系
3. 投资主题标签：KG theme 实体关系

分层更新规则 + 系统设置触发按钮

## 新增文件
- `tagging/stock_tag_service.py` — 统一标签服务
- `tagging/batch_updater.py` — 分层批量更新
- 修改 `routers/stock.py` — 改用新服务
- 修改 `routers/settings.py` — 添加批量更新触发
- 修改前端模板

## 进度
- [ ] stock_tag_service.py
- [ ] batch_updater.py
- [ ] 修改 routers
- [ ] 修改前端
