<template>
  <div class="data-overview">
    <!-- 统计看板 -->
    <el-row :gutter="16" class="stat-row">
      <el-col :span="6" v-for="c in summaryCards" :key="c.key">
        <el-card shadow="never" class="stat-card">
          <div class="stat-value">{{ formatNum(c.value) }}</div>
          <div class="stat-label">{{ c.label }}</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 定时任务列表（分组展示） -->
    <el-card shadow="never" class="block-card">
      <template #header>
        <div class="card-header">
          <span>定时任务</span>
          <div class="header-actions">
            <el-button type="primary" size="small" @click="openCreateDialog">新建任务</el-button>
            <el-button link type="primary" @click="loadJobs">
              <el-icon><Refresh /></el-icon> 刷新
            </el-button>
          </div>
        </div>
      </template>

      <div v-loading="loadingJobs">
        <div v-for="g in jobGrouped" :key="g.key" class="job-group">
          <div class="job-group-title">
            <span class="job-group-dot" :style="{ background: groupDotColor(g.key) }"></span>
            {{ g.label }}
            <span class="job-group-count">{{ g.jobs.length }}</span>
          </div>
          <el-table :data="g.jobs" size="small" border class="job-group-table">
            <el-table-column prop="batch" label="批次" width="60" />
            <el-table-column label="任务名称" min-width="220">
              <template #default="{ row }">
                <span>{{ row.name }}</span>
                <el-tooltip :content="row.description" placement="top" :show-after="300">
                  <el-icon style="margin-left:4px;cursor:help;color:#909399;vertical-align:middle"><InfoFilled /></el-icon>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column label="调度" width="160">
              <template #default="{ row }">
                <span class="schedule-text">{{ formatSchedule(row.schedule) }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="next_run" label="下次执行" width="170" />
            <el-table-column label="状态" width="80">
              <template #default="{ row }">
                <el-tag size="small" :type="row.paused ? 'danger' : 'success'">
                  {{ row.paused ? '已暂停' : '运行中' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="180" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" size="small" @click="openEditDialog(row)">编辑</el-button>
                <el-button link :type="row.paused ? 'success' : 'warning'" size="small" @click="toggleJob(row)">
                  {{ row.paused ? '启用' : '暂停' }}
                </el-button>
                <el-button link type="primary" size="small" @click="filterRuns(row.job_id)">历史</el-button>
                <el-button v-if="row.is_custom" link type="danger" size="small" @click="deleteJob(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-card>

    <!-- 一键运行任务 -->
    <el-card shadow="never" class="block-card">
      <template #header>
        <div class="card-header">
          <span>一键运行</span>
          <span class="header-tip">手动触发一次性管线任务，进度将实时显示在任务中心</span>
        </div>
      </template>
      <div class="manual-tasks">
        <div v-for="t in manualTasks" :key="t.key" class="manual-task-item">
          <div class="task-info">
            <span class="task-name">{{ t.label }}</span>
            <span class="task-desc">{{ t.desc }}</span>
          </div>
          <div class="task-actions">
            <el-input-number
              v-model="t.limit"
              :min="10" :max="5000" :step="100"
              size="small"
              style="width: 120px"
              controls-position="right"
            />
            <el-button
              type="primary"
              size="small"
              :loading="t.loading"
              @click="triggerManualTask(t)"
            >
              运行
            </el-button>
          </div>
        </div>
      </div>
    </el-card>

    <!-- 批次运行详情 -->
    <el-card shadow="never" class="block-card">
      <template #header>
        <div class="card-header">
          <span>批次运行详情</span>
          <div class="header-actions">
            <el-select
              v-model="runFilter.job_id"
              placeholder="筛选任务"
              clearable
              style="width: 200px"
              @change="loadRuns"
            >
              <el-option v-for="j in jobs" :key="j.job_id" :label="j.name" :value="j.job_id" />
            </el-select>
            <el-button link type="primary" @click="loadRuns">
              <el-icon><Refresh /></el-icon> 刷新
            </el-button>
          </div>
        </div>
      </template>
      <el-table :data="runs" v-loading="loadingRuns" size="small" border>
        <el-table-column prop="batch" label="批次" width="60" />
        <el-table-column label="任务" min-width="180">
          <template #default="{ row }">
            <span>{{ row.job_name }}</span>
            <el-tooltip v-if="row.desc" :content="row.desc" placement="top" :show-after="300">
              <el-icon style="margin-left:4px;cursor:help;color:#909399;vertical-align:middle"><InfoFilled /></el-icon>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column prop="started_at" label="开始时间" width="170" />
        <el-table-column label="耗时" width="80">
          <template #default="{ row }">{{ duration(row) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)">{{ statusLabel(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="执行摘要" min-width="280">
          <template #default="{ row }">
            <div v-if="row.result_summary" class="run-summary" :class="{ 'run-warning': row.status === 'partial' }">
              {{ formatSummary(row.result_summary) }}
            </div>
            <div v-else-if="row.status === 'failed' && row.error_msg" class="run-error">
              {{ row.error_msg.slice(0, 200) }}
            </div>
            <div v-else-if="row.status === 'orphaned'" class="run-error">⚠️ 服务重启中断</div>
            <span v-else-if="row.status === 'running'" class="run-running">执行中...</span>
            <span v-else class="run-empty">—</span>
          </template>
        </el-table-column>
        <el-table-column label="预期" min-width="200" show-overflow-tooltip>
          <template #default="{ row }">
            <span class="run-expect">{{ row.expect || '—' }}</span>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="runs.length >= runFilter.limit" class="load-more">
        <el-button text @click="loadMoreRuns">加载更多</el-button>
      </div>
    </el-card>

    <!-- 创建/编辑任务对话框 -->
    <el-dialog v-model="dialogVisible" :title="isEdit ? '编辑任务' : '新建定时任务'" width="520px">
      <el-form :model="jobForm" label-width="100px">
        <el-form-item label="任务名称">
          <el-input v-model="jobForm.job_name" placeholder="例: 每日午间KG构建" />
        </el-form-item>
        <el-form-item v-if="!isEdit" label="执行管线">
          <el-select v-model="jobForm.pipeline_key" placeholder="选择管线" style="width: 100%">
            <el-option
              v-for="p in pipelines"
              :key="p.key"
              :label="p.label"
              :value="p.key"
            >
              <div>{{ p.label }}</div>
              <div style="font-size: 12px; color: #999">{{ p.desc }}</div>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Cron 表达式">
          <el-input v-model="jobForm.cron_expr" placeholder="分 时 日 月 星期  如: 30 12 * * *" />
          <div class="cron-hint">格式: minute hour day month day_of_week（* 表示任意）</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submitJob">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { Refresh, InfoFilled } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as dataApi from '@/api/data'

// ---------- 手动一次性任务 ----------
const manualTasks = reactive([
  { key: 'batch_push', label: '批量入库', desc: '将已提取文档推入 extracted_texts', limit: 500, loading: false },
  { key: 'summarize', label: '摘要生成', desc: '对 pending 文档生成分族摘要', limit: 1000, loading: false },
  { key: 'chunk_index', label: 'Chunk索引', desc: '对 family=2 摘要建向量索引', limit: 2000, loading: false },
  { key: 'diagnose', label: '诊断重试', desc: '诊断 failed 文档并智能重试', limit: 50, loading: false },
  { key: 'pending_sweep', label: 'Pending Sweep', desc: '补处理近7天遗漏的 pending 文档', limit: 200, loading: false },
])

async function triggerManualTask(t) {
  t.loading = true
  try {
    const res = await dataApi.runManualTask(t.key, t.limit)
    ElMessage.success(`已触发「${t.label}」，可在任务中心查看进度`)
  } catch (e) {
    ElMessage.error(`触发失败: ${e.message || e}`)
  } finally {
    t.loading = false
  }
}

// ---------- 统计看板 ----------
const summary = ref({})

const summaryCards = computed(() => {
  const s = summary.value || {}
  return [
    { key: 'total', label: '文档总数', value: s.total },
    { key: 'extracted', label: '已提取', value: s.extracted },
    { key: 'source_count', label: '信息源数', value: s.source_count },
    { key: 'chunks_total', label: '向量切片', value: s.chunks_total },
    { key: 'pipeline_a', label: 'A 摘要', value: s.pipeline_a },
    { key: 'pipeline_b', label: 'B 结构化', value: s.pipeline_b },
    { key: 'pipeline_c', label: 'C 图谱', value: s.pipeline_c },
    { key: 'et_total', label: 'ET 总量', value: s.et_total },
  ]
})

async function loadOverview() {
  try {
    const ds = await dataApi.getDocStats()
    summary.value = ds.summary || {}
  } catch {}
}

// ---------- 定时任务 CRUD ----------
const jobs = ref([])
const jobGroups = ref([])
const loadingJobs = ref(false)
const pipelines = ref([])
const dialogVisible = ref(false)
const isEdit = ref(false)
const saving = ref(false)
const jobForm = reactive({ job_id: '', job_name: '', pipeline_key: '', cron_expr: '' })

// 分组后的任务列表
const jobGrouped = computed(() => {
  if (!jobGroups.value.length) return []
  return jobGroups.value
    .map(g => ({
      ...g,
      jobs: jobs.value.filter(j => j.group === g.key)
    }))
    .filter(g => g.jobs.length > 0)
})

const _GROUP_COLORS = {
  collect: '#409eff',
  kg: '#67c23a',
  analysis: '#e6a23c',
  maintain: '#909399',
  custom: '#f56c6c',
}
function groupDotColor(key) {
  return _GROUP_COLORS[key] || '#909399'
}

function formatSchedule(raw) {
  // cron[hour='16', minute='0'] => 每天 16:00
  const m = raw.match(/hour='([^']+)'.*?minute='([^']+)'/);
  if (!m) return raw;
  const hour = m[1], min = m[2].padStart(2, '0');
  const dow = raw.match(/day_of_week='([^']+)'/);
  const day = raw.match(/day='([^']+)'/);
  let prefix = '每天';
  if (day) prefix = `每月${day[1]}日`;
  else if (dow) prefix = `工作日`;
  return `${prefix} ${hour}:${min}`;
}

async function loadJobs() {
  loadingJobs.value = true
  try {
    const res = await dataApi.getSchedulerJobs()
    jobs.value = res.jobs || []
    jobGroups.value = res.groups || []
  } finally {
    loadingJobs.value = false
  }
}

async function loadPipelines() {
  try {
    const res = await dataApi.getSchedulerPipelines()
    pipelines.value = res.pipelines || []
  } catch {}
}

function openCreateDialog() {
  isEdit.value = false
  jobForm.job_id = ''
  jobForm.job_name = ''
  jobForm.pipeline_key = ''
  jobForm.cron_expr = ''
  dialogVisible.value = true
}

function openEditDialog(row) {
  isEdit.value = true
  jobForm.job_id = row.job_id
  jobForm.job_name = row.name
  jobForm.pipeline_key = ''
  // 从 schedule 尝试提取 cron (best effort display)
  jobForm.cron_expr = ''
  dialogVisible.value = true
}

async function submitJob() {
  saving.value = true
  try {
    if (isEdit.value) {
      const payload = { job_id: jobForm.job_id }
      if (jobForm.job_name) payload.job_name = jobForm.job_name
      if (jobForm.cron_expr) payload.cron_expr = jobForm.cron_expr
      const res = await dataApi.updateSchedulerJob(payload)
      if (res.ok === false) { ElMessage.error(res.error || '更新失败'); return }
      ElMessage.success('已更新')
    } else {
      if (!jobForm.pipeline_key || !jobForm.cron_expr) {
        ElMessage.warning('请填写管线和 Cron 表达式')
        return
      }
      const res = await dataApi.createSchedulerJob({
        job_name: jobForm.job_name || pipelines.value.find(p => p.key === jobForm.pipeline_key)?.label || '',
        pipeline_key: jobForm.pipeline_key,
        cron_expr: jobForm.cron_expr,
      })
      if (res.ok === false) { ElMessage.error(res.error || '创建失败'); return }
      ElMessage.success('已创建')
    }
    dialogVisible.value = false
    loadJobs()
  } finally {
    saving.value = false
  }
}

async function toggleJob(row) {
  const res = await dataApi.updateSchedulerJob({ job_id: row.job_id, enabled: row.paused })
  if (res.ok === false) { ElMessage.error(res.error); return }
  ElMessage.success(row.paused ? '已启用' : '已暂停')
  loadJobs()
}

async function deleteJob(row) {
  await ElMessageBox.confirm(`确定删除任务「${row.name}」？`, '删除确认', { type: 'warning' })
  const res = await dataApi.deleteSchedulerJob({ job_id: row.job_id })
  if (res.ok === false) { ElMessage.error(res.error); return }
  ElMessage.success('已删除')
  loadJobs()
}

// ---------- 批次运行 ----------
const runs = ref([])
const loadingRuns = ref(false)
const runFilter = reactive({ job_id: '', limit: 50 })

async function loadRuns() {
  loadingRuns.value = true
  try {
    const params = { limit: runFilter.limit }
    if (runFilter.job_id) params.job_id = runFilter.job_id
    const res = await dataApi.getSchedulerRuns(params)
    runs.value = res.runs || []
  } finally {
    loadingRuns.value = false
  }
}

function loadMoreRuns() { runFilter.limit += 50; loadRuns() }
function filterRuns(jobId) { runFilter.job_id = jobId; loadRuns() }

// ---------- 工具 ----------
function formatNum(n) {
  if (n == null) return '0'
  return Number(n).toLocaleString('en-US')
}
function statusType(s) {
  if (s === 'success') return 'success'
  if (s === 'failed') return 'danger'
  if (s === 'running') return 'warning'
  if (s === 'partial') return 'warning'
  if (s === 'orphaned') return 'info'
  return 'info'
}
function statusLabel(s) {
  const map = { success: '成功', failed: '失败', running: '运行中', partial: '部分成功', orphaned: '中断' }
  return map[s] || s
}
function duration(row) {
  if (!row.started_at || !row.finished_at) return '—'
  const sec = Math.round((new Date(row.finished_at) - new Date(row.started_at)) / 1000)
  if (sec < 60) return `${sec}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m${sec % 60}s`
  return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`
}
function formatSummary(raw) {
  try {
    const obj = typeof raw === 'string' ? JSON.parse(raw) : raw
    if (!obj || typeof obj !== 'object') return raw
    const parts = []
    for (const [k, v] of Object.entries(obj)) {
      if (k === 'error') parts.push(`❌ ${v}`)
      else if (typeof v === 'object' && v !== null) {
        parts.push(`${k}(${Object.entries(v).map(([sk, sv]) => `${sk}:${sv}`).join(' ')})`)
      } else parts.push(`${k}: ${v}`)
    }
    return parts.join(' | ')
  } catch { return raw?.slice?.(0, 200) || '—' }
}

onMounted(() => {
  loadOverview()
  loadJobs()
  loadPipelines()
  loadRuns()
})
</script>

<style scoped>
.stat-row { margin-bottom: 16px; }
.stat-card { text-align: center; }
.stat-value { font-size: 24px; font-weight: 700; color: var(--admin-text); }
.stat-label { font-size: 13px; color: var(--admin-text-dim); margin-top: 4px; }
.block-card { margin-bottom: 16px; }
.card-header { display: flex; align-items: center; justify-content: space-between; }
.header-actions { display: flex; align-items: center; gap: 12px; }
.run-summary { font-size: 12px; color: var(--admin-text); line-height: 1.5; word-break: break-all; }
.run-error { font-size: 12px; color: var(--el-color-danger); line-height: 1.5; word-break: break-all; }
.run-running { color: var(--el-color-warning); font-size: 12px; }
.run-empty { color: var(--admin-text-dim); }
.run-warning { color: var(--el-color-warning) !important; }
.run-expect { font-size: 11px; color: #999; }
.load-more { text-align: center; padding: 12px 0; }
.cron-hint { font-size: 12px; color: var(--admin-text-dim); margin-top: 4px; }

/* 任务分组样式 */
.job-group { margin-bottom: 20px; }
.job-group:last-child { margin-bottom: 0; }
.job-group-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  font-weight: 600;
  color: var(--admin-text);
  margin-bottom: 10px;
  padding-left: 2px;
}
.job-group-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.job-group-count {
  font-size: 12px;
  font-weight: 400;
  color: var(--admin-text-dim);
  background: var(--admin-bg-card, #f5f5f5);
  padding: 1px 7px;
  border-radius: 10px;
}
.job-group-table { margin-bottom: 0; }
.schedule-text { font-size: 12px; color: var(--admin-text-dim); }

/* 一键运行样式 */
.header-tip { font-size: 12px; color: var(--admin-text-dim); font-weight: 400; }
.manual-tasks { display: flex; flex-direction: column; gap: 12px; }
.manual-task-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 14px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  transition: background 0.2s;
}
.manual-task-item:hover { background: var(--admin-bg-card, #fafafa); }
.task-info { display: flex; flex-direction: column; gap: 2px; }
.task-name { font-size: 14px; font-weight: 500; color: var(--admin-text); }
.task-desc { font-size: 12px; color: var(--admin-text-dim); }
.task-actions { display: flex; align-items: center; gap: 10px; }
</style>
