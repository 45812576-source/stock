import request from './request'

// ---- 页面上下文 ----
// tab: api | structured | skills | strategy
export const getPageContext = (tab = 'api') =>
  request.get('/settings/api/page-context', { params: { tab } })

// ---- 多模型配置 ----
export const getModelConfigs = () => request.get('/settings/api/model-configs')
export const saveModelConfig = (payload) =>
  request.post('/settings/api/save-model-config', payload)
export const saveKey = (keyName, keyValue) =>
  request.post('/settings/api/save-key', { key_name: keyName, key_value: keyValue })
export const saveConfig = (payload) => request.post('/settings/api/save-config', payload)
export const testClaude = () => request.post('/settings/api/test-claude', {})

// ---- Skill 编辑器 ----
export const getSkillContent = (skillName) =>
  request.get(`/settings/api/skill-content/${encodeURIComponent(skillName)}`)
export const saveSkillContent = (skillName, content) =>
  request.post(`/settings/api/skill-save/${encodeURIComponent(skillName)}`, { content })

// ---- 选股规则库 ----
export const getSelectionRules = () => request.get('/settings/api/selection-rules')
export const seedSelectionRules = () => request.post('/settings/api/selection-rules/seed', {})
export const addSelectionRule = (payload) =>
  request.post('/settings/api/selection-rules', payload)
export const updateSelectionRule = (id, payload) =>
  request.put(`/settings/api/selection-rules/${id}`, payload)
export const deleteSelectionRule = (id) =>
  request.delete(`/settings/api/selection-rules/${id}`)

// ---- 数据监控规则（供数据管理页 DataStructured 复用）----
export const saveMonitorRule = (payload) =>
  request.post('/settings/api/save-monitor-rule', payload)
export const triggerMonitor = (ruleId) =>
  request.post(`/settings/api/trigger-monitor/${ruleId}`, {})
export const deleteMonitor = (ruleId) =>
  request.delete(`/settings/api/delete-monitor/${ruleId}`)

// ---- 结构化数据批量下载（供数据管理页 DataStructured 复用）----
export const structBatch = (payload) => request.post('/settings/api/struct-batch', payload)
export const getStructTask = (taskId) => request.get(`/settings/api/struct-task/${taskId}`)

// ---- 标签计算引擎（供数据管理页 DataStrategy 复用）----
export const runTagging = (payload) => request.post('/settings/api/run-tagging', payload)
export const getTaggingStatus = () => request.get('/settings/api/tagging-status')
export const getTaggingTask = (taskId) => request.get(`/settings/api/tagging-task/${taskId}`)
export const runBatchTagUpdate = (payload) =>
  request.post('/settings/api/run-batch-tag-update', payload)
export const getBatchTagStatus = () => request.get('/settings/api/batch-tag-status')
export const getBatchTagTask = (taskId) => request.get(`/settings/api/batch-tag-task/${taskId}`)
export const getStockTagStats = () => request.get('/settings/api/stock-tag-stats')
