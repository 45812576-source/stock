import request from './request'

// ---- 概况 / 统计 ----
export const getPageContext = () => request.get('/data/api/page-context')
export const getDocStats = (docType = '') =>
  request.get('/data/api/doc-stats', { params: docType ? { doc_type: docType } : {} })
export const getSourceDocStats = () => request.get('/data/api/source-documents/stats')

// ---- 文档列表 ----
export const listSourceDocuments = (params) =>
  request.get('/data/api/source-documents', { params })

// ---- 清洗管线 ----
export function runPipeline(pipeline, limit) {
  const fd = new FormData()
  fd.append('pipeline', pipeline)
  if (limit != null && limit !== '') fd.append('limit', limit)
  return request.post('/data/api/run-pipeline', fd)
}
export function backfillChunks(limit) {
  const fd = new FormData()
  if (limit != null && limit !== '') fd.append('limit', limit)
  return request.post('/data/api/backfill-chunks', fd)
}
export function backfillSummaryChunks(batchSize, dryRun) {
  const fd = new FormData()
  if (batchSize != null && batchSize !== '') fd.append('batch_size', batchSize)
  fd.append('dry_run', dryRun ? 'true' : 'false')
  return request.post('/data/api/backfill-summary-chunks', fd)
}
export function syncStockDb(limit) {
  const fd = new FormData()
  if (limit != null && limit !== '') fd.append('limit', limit)
  return request.post('/data/api/sync-stockdb', fd)
}
export const fetchZsxq = (payload) => request.post('/data/api/fetch-zsxq', payload)

// ---- 任务轮询 ----
export const getTaskStatus = (taskId) => request.get(`/data/task-status/${taskId}`)
export const getActiveTasks = () => request.get('/data/api/active-tasks')
export const cancelTask = (taskId) => request.post('/data/api/cancel-task', { task_id: taskId })
export const pauseTask = (taskId) => request.post('/data/api/pause-task', { task_id: taskId })
export const resumeTask = (taskId) => request.post('/data/api/resume-task', { task_id: taskId })

// ---- 手动触发一次性任务 ----
export const runManualTask = (taskType, limit = 500) =>
  request.post('/data/api/run-manual-task', { task_type: taskType, limit })

// ---- 审核台 ----
export const extractPreview = (docIds) =>
  request.post('/data/api/extract-preview', { doc_ids: docIds })
export const getExtractPreviewResult = (taskId) =>
  request.get('/data/api/extract-preview-result', { params: { task_id: taskId } })
export const approveDocs = (docs) => request.post('/data/api/approve-docs', { docs })
export const rejectDocs = (docIds) => request.post('/data/api/reject-docs', { doc_ids: docIds })

// ---- Source Documents 状态操作 ----
export const reviewDocs = (docIds) =>
  request.post('/data/api/source-documents/review', { doc_ids: docIds })
export const pipeDocs = (docIds) =>
  request.post('/data/api/source-documents/pipe', { doc_ids: docIds })
export const retryDocs = (docIds) =>
  request.post('/data/api/source-documents/retry', { doc_ids: docIds })

// ---- 信息源配置 ----
export const getAvailableSources = () => request.get('/data/api/available-sources')
export const saveSources = (payload) => request.post('/data/api/save-sources', payload)
export const getCleaningLogs = () => request.get('/data/api/cleaning-logs')
export const getZsxqTokenStatus = () => request.get('/data/api/zsxq-token-status')

// ---- 定时任务管理 ----
export const getSchedulerJobs = () => request.get('/data/api/scheduler-jobs')
export const getSchedulerRuns = (params) => request.get('/data/api/scheduler-runs', { params })
export const getSchedulerPipelines = () => request.get('/data/api/scheduler-pipelines')
export const createSchedulerJob = (payload) => request.post('/data/api/scheduler-job/create', payload)
export const updateSchedulerJob = (payload) => request.post('/data/api/scheduler-job/update', payload)
export const deleteSchedulerJob = (payload) => request.post('/data/api/scheduler-job/delete', payload)

// ---- 数据来源配置 ----
export const getSourceConfig = () => request.get('/data/api/source-config')
export const addSource = (payload) => request.post('/data/api/source/add', payload)
export const updateSource = (payload) => request.post('/data/api/source/update', payload)
export const deleteSource = (payload) => request.post('/data/api/source/delete', payload)

// 代理文件预览 URL
export const proxyFileUrl = (url) =>
  `/data/api/proxy-file?url=${encodeURIComponent(url)}`

// ---- 任务历史与诊断 ----
export const getTaskHistory = (limit = 30) => request.get('/data/api/task-history', { params: { limit } })
export const diagnoseTask = (taskId) => request.get(`/data/api/diagnose-task/${taskId}`)
