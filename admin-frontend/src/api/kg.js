import request from './request'

// ---- 元数据 / 图谱数据 ----
export const getMeta = () => request.get('/kg/api/meta')
export const getGraphData = (centerId = 0, depth = 2) =>
  request.get('/kg/api/graph-data', { params: { center_id: centerId, depth } })
export const getEntityDetail = (entityId) => request.get(`/kg/api/entity/${entityId}`)
export const searchEntities = (q, entityType = '') =>
  request.get('/kg/api/search', { params: { q, entity_type: entityType } })

// ---- 实体 CRUD ----
export const addEntity = (payload) => request.post('/kg/api/entity', payload)
export const updateEntity = (id, payload) => request.put(`/kg/api/entity/${id}`, payload)
export const deleteEntity = (id) => request.delete(`/kg/api/entity/${id}`)

// ---- 关系查询 ----
export const listRelationships = (relationType = '', limit = 100, offset = 0) =>
  request.get('/kg/api/relationships', { params: { relation_type: relationType, limit, offset } })

// ---- 关系 CRUD ----
export const addRelationship = (payload) => request.post('/kg/api/relationship', payload)
export const deleteRelationship = (id) => request.delete(`/kg/api/relationship/${id}`)

// ---- 推理 / 构建 ----
export const runInference = (ruleType) => request.post('/kg/api/inference', { rule_type: ruleType })
export const acceptInference = (payload) => request.post('/kg/api/accept-inference', payload)
export const identifyThemes = (days = 7, confidenceThreshold = 0.6) =>
  request.post('/kg/api/identify-themes', null, { params: { days, confidence_threshold: confidenceThreshold } })
export const updateKg = (useClaude = false) =>
  request.post('/kg/api/update-kg', null, { params: { use_claude: useClaude } })
export const getTaskStatus = (taskId) => request.get(`/kg/api/task-status/${taskId}`)

// ---- 巡检 / 语义校验 ----
export const runKgInspect = (days = 7, limit = 20, dryRun = false) =>
  request.post('/kg/api/kg-inspect', null, { params: { days, limit, dry_run: dryRun } })
export const getKgInspectStatus = (taskId) => request.get(`/kg/api/kg-inspect/${taskId}`)
export const getKgInspectInfo = () => request.get('/kg/api/kg-inspect-info')
export const runSemanticValidate = (dryRun = false, entityTypes = '') =>
  request.post('/kg/api/kg-semantic-validate', null, { params: { dry_run: dryRun, entity_types: entityTypes } })
export const getSemanticStatus = (taskId) => request.get(`/kg/api/kg-semantic-validate/${taskId}`)
export const getSemanticLatest = () => request.get('/kg/api/kg-semantic-latest')

// ---- 审核工作台 ----
export const getReviewStats = () => request.get('/kg/api/review/stats')
export const getReviewQueue = (params) => request.get('/kg/api/review/queue', { params })
export const getReviewDetail = (targetType, targetId) =>
  request.get(`/kg/api/review/detail/${targetType}/${targetId}`)
export const reviewApprove = (payload) => request.post('/kg/api/review/approve', payload)
export const reviewReject = (payload) => request.post('/kg/api/review/reject', payload)
export const reviewMarkPending = (payload) => request.post('/kg/api/review/mark-pending', payload)
export const reviewRevert = (payload) => request.post('/kg/api/review/revert', payload)
export const reviewBatch = (payload) => request.post('/kg/api/review/batch', payload)
export const editEntityReview = (payload) => request.post('/kg/api/review/edit-entity', payload)
export const editRelationshipReview = (payload) => request.post('/kg/api/review/edit-relationship', payload)
