<template>
  <div class="data-manage">
    <el-tabs v-model="activeTab" class="dm-tabs" @tab-change="onTabChange">
      <!-- ============ 数据管理 ============ -->
      <el-tab-pane label="数据管理" name="overview">
        <DataOverview v-if="loaded.overview" />
      </el-tab-pane>

      <!-- ============ 数据来源 ============ -->
      <el-tab-pane label="数据来源" name="sources">
        <DataSources v-if="loaded.sources" />
      </el-tab-pane>

      <!-- ============ 源文档 ============ -->
      <el-tab-pane label="源文档" name="docs">
        <div class="status-cards">
          <div
            v-for="s in statusList"
            :key="s.key"
            class="status-chip"
            :class="{ active: docFilter.status === s.key }"
            @click="filterByStatus(s.key)"
          >
            <div class="chip-value">{{ formatNum(docStatusStats[s.key] || 0) }}</div>
            <div class="chip-label">{{ s.label }}</div>
          </div>
        </div>

        <el-card shadow="never" class="block-card">
          <div class="filter-bar">
            <el-input v-model="docFilter.search" placeholder="标题/关键词" clearable style="width: 200px" @keyup.enter="loadDocs(1)
  loadTaskHistory()" />
            <el-select v-model="docFilter.doc_type" placeholder="文档类型" clearable style="width: 150px">
              <el-option v-for="dt in docTypes" :key="dt.key" :label="dt.label" :value="dt.key" />
            </el-select>
            <el-input v-model="docFilter.source" placeholder="信息源" clearable style="width: 150px" />
            <el-button type="primary" @click="loadDocs(1)">查询</el-button>
            <el-button @click="resetDocFilter">重置</el-button>
            <div class="spacer" />
            <el-button
              type="success"
              :disabled="!selectedDocIds.length"
              @click="doReview(selectedDocIds)"
            >送审 ({{ selectedDocIds.length }})</el-button>
            <el-button
              type="warning"
              :disabled="!selectedDocIds.length"
              @click="doPipe(selectedDocIds)"
            >入库</el-button>
            <el-button
              :disabled="!selectedDocIds.length"
              @click="doRetry(selectedDocIds)"
            >重试</el-button>
          </div>

          <el-table
            :data="docList"
            v-loading="loadingDocs"
            size="small"
            border
            @selection-change="onSelectionChange"
          >
            <el-table-column type="selection" width="46" />
            <el-table-column prop="title" label="标题" min-width="240" show-overflow-tooltip />
            <el-table-column prop="source" label="信息源" width="130" />
            <el-table-column prop="doc_type" label="类型" width="110" />
            <el-table-column prop="file_type" label="文件" width="80" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag size="small" :type="statusTagType(row.status_color)">{{ row.status_label }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="publish_date" label="发布日期" width="110" />
            <el-table-column label="操作" width="90" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" @click="openReviewDialog(row)">审核</el-button>
              </template>
            </el-table-column>
          </el-table>

          <el-pagination
            class="pager"
            layout="total, sizes, prev, pager, next"
            :total="docTotal"
            :page-size="docFilter.page_size"
            :current-page="docFilter.page"
            :page-sizes="[20, 50, 100]"
            @current-change="loadDocs"
            @size-change="onSizeChange"
          />
        </el-card>
      </el-tab-pane>

      <!-- ============ 清洗管线 ============ -->
      <el-tab-pane label="清洗管线" name="pipeline">
        <el-card shadow="never" class="block-card">
          <template #header>清洗管线触发</template>
          <div class="pipeline-form">
            <el-input-number v-model="pipelineLimit" :min="1" :max="5000" :step="50" />
            <span class="hint">处理条数上限</span>
          </div>
          <el-space wrap style="margin-top: 12px">
            <el-button type="primary" :loading="running" @click="triggerPipeline('a')">A · 摘要</el-button>
            <el-button type="primary" :loading="running" @click="triggerPipeline('c')">C · 知识图谱</el-button>
            <el-button type="primary" :loading="running" @click="triggerPipeline('d')">D · 行业指标</el-button>
            <el-button type="success" :loading="running" @click="triggerPipeline('acd')">A+C+D 并发</el-button>
          </el-space>
        </el-card>

        <el-card shadow="never" class="block-card">
          <template #header>回填工具</template>
          <el-space wrap>
            <el-button :loading="running" @click="triggerBackfillChunks">向量切片回填</el-button>
            <el-button :loading="running" @click="triggerBackfillSummary">摘要 Chunk 回填</el-button>
            <el-button :loading="running" @click="triggerSyncStockDb">同步个股库</el-button>
          </el-space>
        </el-card>

        <el-card v-if="taskProgress" shadow="never" class="block-card">
          <template #header>
            当前任务：{{ taskProgress.label || currentTaskId }}
            <el-tooltip
              v-if="pausable && taskProgress.status === 'running' && !paused"
              content="软暂停：停止推进新任务，在途批次会跑完"
              placement="top"
            >
              <el-button link type="warning" @click="doPauseTask">暂停</el-button>
            </el-tooltip>
            <el-button
              v-if="pausable && taskProgress.status === 'running' && paused"
              link type="primary" @click="doResumeTask"
            >恢复</el-button>
            <el-button v-if="taskProgress.status === 'running'" link type="danger" @click="doCancelTask">取消</el-button>
          </template>
          <el-progress
            :percentage="taskPercent"
            :status="taskProgress.status === 'done' ? 'success' : (taskProgress.status === 'cancelled' ? 'exception' : '')"
          />
          <div class="task-meta">
            {{ taskProgress.current || '' }} · {{ taskProgress.progress || 0 }}/{{ taskProgress.total || 0 }} · {{ taskProgress.status }}
          </div>
        </el-card>

        <el-card shadow="never" class="block-card">
          <template #header>
            任务运行记录 <el-button link type="primary" @click="loadTaskHistory">刷新</el-button>
          </template>
          <el-table :data="taskHistory" size="small" stripe max-height="400" v-if="taskHistory.length">
            <el-table-column label="任务" prop="label" width="120" />
            <el-table-column label="开始时间" width="160">
              <template #default="{ row }">{{ row.started_at ? row.started_at.slice(0,19).replace('T',' ') : '' }}</template>
            </el-table-column>
            <el-table-column label="状态" width="80">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status === 'done' ? 'success' : 'danger'">{{ row.status === 'done' ? '完成' : '取消' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="结果" min-width="200">
              <template #default="{ row }">
                <span :style="{ color: row.has_failures ? '#f56c6c' : '#67c23a' }">{{ row.result_summary || '—' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="80">
              <template #default="{ row }">
                <el-button v-if="row.has_failures" link type="warning" size="small" @click="doDiagnose(row.task_id)">诊断</el-button>
              </template>
            </el-table-column>
          </el-table>
          <div v-else style="color:#909399; padding:12px 0">（暂无任务记录）</div>
        </el-card>

        <!-- 诊断结果弹窗 -->
        <el-dialog v-model="diagVisible" title="失败诊断" width="600" destroy-on-close>
          <div v-if="diagData">
            <p><b>任务：</b>{{ diagData.label }} | <b>状态：</b>{{ diagData.status }}</p>
            <p><b>结果摘要：</b>{{ diagData.result_summary }}</p>
            <el-divider />
            <div v-if="diagData.errors && diagData.errors.length">
              <h4 style="margin:0 0 8px">错误信息</h4>
              <div v-for="(e, i) in diagData.errors" :key="i" class="diag-error-item">
                <el-tag size="small" type="danger">{{ e.source }}</el-tag> {{ e.error }}
              </div>
            </div>
            <div v-if="diagData.milvus_status" style="margin-top:12px">
              <b>Milvus 状态：</b><el-tag size="small" :type="diagData.milvus_status === '正常' ? 'success' : 'danger'">{{ diagData.milvus_status }}</el-tag>
            </div>
            <div v-if="diagData.failed_ids && diagData.failed_ids.length" style="margin-top:12px">
              <h4 style="margin:0 0 8px">失败 ID 列表（前20条）</h4>
              <el-tag v-for="id in diagData.failed_ids.slice(0, 20)" :key="id" size="small" style="margin:2px 4px">{{ id }}</el-tag>
            </div>
            <div v-if="diagData.failed_details && diagData.failed_details.length" style="margin-top:12px">
              <h4 style="margin:0 0 8px">失败文档详情</h4>
              <el-table :data="diagData.failed_details" size="small" max-height="200">
                <el-table-column prop="id" label="ID" width="80" />
                <el-table-column prop="source" label="来源" width="120" />
                <el-table-column prop="source_format" label="格式" width="80" />
                <el-table-column prop="summary_status" label="摘要状态" width="100" />
              </el-table>
            </div>
          </div>
          <div v-else style="color:#909399">加载中...</div>
        </el-dialog>
      </el-tab-pane>

      <!-- ============ 结构化数据 ============ -->
      <el-tab-pane label="结构化数据" name="structured">
        <DataStructured v-if="loaded.structured" :ctx="subCtx.structured" @refresh="() => loadSubTab('structured', true)" />
      </el-tab-pane>

      <!-- ============ 选股策略 ============ -->
      <el-tab-pane label="选股策略" name="strategy">
        <DataStrategy v-if="loaded.strategy" :ctx="subCtx.strategy" @refresh="() => loadSubTab('strategy', true)" />
      </el-tab-pane>
    </el-tabs>

    <!-- ============ 审核对话框 ============ -->
    <el-dialog v-model="reviewDialog" title="对照审核" width="90%" top="4vh" class="review-dialog">
      <div class="review-body" v-if="reviewDoc">
        <div class="review-left">
          <div class="review-sub">原始文件</div>
          <iframe v-if="previewUrl" :src="previewUrl" class="preview-frame" />
          <div v-else class="preview-empty">无可预览文件</div>
        </div>
        <div class="review-right">
          <div class="review-sub">
            提取文本
            <el-button size="small" :loading="extracting" @click="runExtract">重新提取</el-button>
          </div>
          <el-input
            v-model="reviewText"
            type="textarea"
            :rows="24"
            placeholder="提取文本内容"
          />
        </div>
      </div>
      <template #footer>
        <el-button @click="reviewDialog = false">取消</el-button>
        <el-button type="danger" @click="doRejectSingle">拒绝</el-button>
        <el-button type="success" @click="doApproveSingle">通过并保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onBeforeUnmount, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as dataApi from '@/api/data'
import * as settingsApi from '@/api/settings'
import DataStructured from './data/DataStructured.vue'
import DataStrategy from './data/DataStrategy.vue'
import DataOverview from './data/DataOverview.vue'
import DataSources from './data/DataSources.vue'

const activeTab = ref('overview')
const route = useRoute()
const router = useRouter()
watch(
  () => route.query.tab,
  (t) => { if (t && t !== activeTab.value) activeTab.value = String(t) },
  { immediate: true },
)

// ---------- 子 tab 懒加载（结构化数据 / 选股策略 / 数据管理 / 数据来源）----------
const subCtx = reactive({ structured: null, strategy: null })
const loaded = reactive({ structured: false, strategy: false, overview: false, sources: false })
async function loadSubTab(tab, force = false) {
  if (loaded[tab] && !force) return
  try {
    subCtx[tab] = await settingsApi.getPageContext(tab)
    loaded[tab] = true
  } catch (e) {
    ElMessage.error('加载数据失败')
  }
}
function onTabChange(name) {
  router.replace({ query: { ...route.query, tab: name } })
  if (name === 'overview' || name === 'sources') {
    loaded[name] = true
  }
  if (name === 'structured' || name === 'strategy') loadSubTab(name)
}

// ---------- 概况 ----------
const loadingOverview = ref(false)
const docTypes = ref([])

async function loadOverview() {
  loadingOverview.value = true
  try {
    const ctx = await dataApi.getPageContext()
    docTypes.value = ctx.doc_types || []
  } finally {
    loadingOverview.value = false
  }
}

// ---------- 源文档 ----------
const statusList = [
  { key: '', label: '全部' },
  { key: 'pending', label: '待提取' },
  { key: 'extracted', label: '已提取' },
  { key: 'ready_to_pipe', label: '待入库' },
  { key: 'processing', label: '处理中' },
  { key: 'done', label: '已完成' },
  { key: 'failed', label: '失败' },
  { key: 'rejected', label: '已拒绝' },
  { key: 'url_expired', label: 'URL失效' },
  { key: 'remix', label: 'Remix' },
]
const docStatusStats = ref({})
const docList = ref([])
const docTotal = ref(0)
const loadingDocs = ref(false)
const selectedDocIds = ref([])
const docFilter = reactive({ page: 1, page_size: 50, status: '', doc_type: '', source: '', search: '' })

async function loadDocStatusStats() {
  try {
    docStatusStats.value = await dataApi.getSourceDocStats()
  } catch (e) { /* ignore */ }
}

async function loadDocs(page = 1) {
  docFilter.page = page
  loadingDocs.value = true
  try {
    const params = { page: docFilter.page, page_size: docFilter.page_size }
    if (docFilter.status) params.status = docFilter.status
    if (docFilter.doc_type) params.doc_type = docFilter.doc_type
    if (docFilter.source) params.source = docFilter.source
    if (docFilter.search) params.search = docFilter.search
    const res = await dataApi.listSourceDocuments(params)
    docList.value = res.items || []
    docTotal.value = res.total || 0
  } finally {
    loadingDocs.value = false
  }
}

function filterByStatus(status) {
  docFilter.status = status
  loadDocs(1)
}
function resetDocFilter() {
  docFilter.status = ''
  docFilter.doc_type = ''
  docFilter.source = ''
  docFilter.search = ''
  loadDocs(1)
}
function onSizeChange(size) {
  docFilter.page_size = size
  loadDocs(1)
}
function onSelectionChange(rows) {
  selectedDocIds.value = rows.map((r) => r.id)
}

function statusTagType(color) {
  const map = { green: 'success', red: 'danger', orange: 'warning', gray: 'info', blue: '' }
  return map[color] || 'info'
}

async function doReview(ids) {
  await dataApi.reviewDocs(ids)
  ElMessage.success('已送审')
  refreshDocs()
}
async function doPipe(ids) {
  await dataApi.pipeDocs(ids)
  ElMessage.success('已提交入库')
  refreshDocs()
}
async function doRetry(ids) {
  await dataApi.retryDocs(ids)
  ElMessage.success('已重试')
  refreshDocs()
}
function refreshDocs() {
  loadDocs(docFilter.page)
  loadDocStatusStats()
}

// ---------- 审核对话框 ----------
const reviewDialog = ref(false)
const reviewDoc = ref(null)
const reviewText = ref('')
const previewUrl = ref('')
const extracting = ref(false)

function openReviewDialog(row) {
  reviewDoc.value = row
  reviewText.value = row.extracted_text || ''
  previewUrl.value = row.oss_url ? dataApi.proxyFileUrl(row.oss_url) : ''
  reviewDialog.value = true
}

async function runExtract() {
  if (!reviewDoc.value) return
  extracting.value = true
  try {
    const { task_id } = await dataApi.extractPreview([reviewDoc.value.id])
    const poll = async () => {
      const res = await dataApi.getExtractPreviewResult(task_id)
      const st = res.status
      if (st === 'done' || st === 'cancelled') {
        const item = (res.items || res.results || [])[0]
        if (item && (item.extracted_text || item.text)) {
          reviewText.value = item.extracted_text || item.text
        }
        extracting.value = false
        ElMessage.success('提取完成')
        return
      }
      setTimeout(poll, 1500)
    }
    poll()
  } catch (e) {
    extracting.value = false
  }
}

async function doApproveSingle() {
  await dataApi.approveDocs([{ id: reviewDoc.value.id, extracted_text: reviewText.value }])
  ElMessage.success('已通过')
  reviewDialog.value = false
  refreshDocs()
}
async function doRejectSingle() {
  await dataApi.rejectDocs([reviewDoc.value.id])
  ElMessage.success('已拒绝')
  reviewDialog.value = false
  refreshDocs()
}

// ---------- 清洗管线 ----------
const pipelineLimit = ref(100)
const running = ref(false)
const paused = ref(false)
const pausable = ref(false)
const currentTaskId = ref('')
const taskProgress = ref(null)
let pollTimer = null

const taskPercent = computed(() => {
  const t = taskProgress.value
  if (!t || !t.total) return 0
  return Math.min(100, Math.round(((t.progress || 0) / t.total) * 100))
})

function startPolling(taskId, label) {
  currentTaskId.value = taskId
  taskProgress.value = { status: 'running', progress: 0, total: 0, label }
  clearInterval(pollTimer)
  pollTimer = setInterval(async () => {
    try {
      const res = await dataApi.getTaskStatus(taskId)
      taskProgress.value = { ...res, label: res.label || label }
      paused.value = !!res.paused
      if (res.status === 'done' || res.status === 'cancelled') {
        clearInterval(pollTimer)
        running.value = false
        pausable.value = false
        refreshDocs()
      }
    } catch (e) {
      clearInterval(pollTimer)
      running.value = false
      pausable.value = false
    }
  }, 2000)
}

async function triggerPipeline(pipeline) {
  running.value = true
  paused.value = false
  pausable.value = true
  try {
    const { task_id } = await dataApi.runPipeline(pipeline, pipelineLimit.value)
    startPolling(task_id, `管线 ${pipeline.toUpperCase()}`)
  } catch (e) {
    running.value = false
    pausable.value = false
  }
}
async function triggerBackfillChunks() {
  running.value = true
  pausable.value = false
  try {
    const { task_id } = await dataApi.backfillChunks(pipelineLimit.value)
    startPolling(task_id, '向量切片回填')
  } catch (e) { running.value = false }
}
async function triggerBackfillSummary() {
  running.value = true
  pausable.value = false
  try {
    const { task_id } = await dataApi.backfillSummaryChunks(pipelineLimit.value, false)
    startPolling(task_id, '摘要 Chunk 回填')
  } catch (e) { running.value = false }
}
async function triggerSyncStockDb() {
  running.value = true
  pausable.value = false
  try {
    const { task_id } = await dataApi.syncStockDb(pipelineLimit.value)
    startPolling(task_id, '同步个股库')
  } catch (e) { running.value = false }
}
async function doCancelTask() {
  if (!currentTaskId.value) return
  await dataApi.cancelTask(currentTaskId.value)
  ElMessage.info('已请求取消')
}
async function doPauseTask() {
  if (!currentTaskId.value) return
  await dataApi.pauseTask(currentTaskId.value)
  paused.value = true
  ElMessage.info('已暂停')
}
async function doResumeTask() {
  if (!currentTaskId.value) return
  await dataApi.resumeTask(currentTaskId.value)
  paused.value = false
  ElMessage.success('已恢复')
}

// ---------- 任务历史 & 诊断 ----------
const taskHistory = ref([])
const diagVisible = ref(false)
const diagData = ref(null)

async function loadTaskHistory() {
  try {
    taskHistory.value = await dataApi.getTaskHistory(30)
  } catch (e) { /* ignore */ }
}

async function doDiagnose(taskId) {
  diagVisible.value = true
  diagData.value = null
  try {
    diagData.value = await dataApi.diagnoseTask(taskId)
  } catch (e) {
    diagData.value = { label: '错误', status: 'error', result_summary: '无法获取诊断信息', errors: [{ source: 'API', error: String(e) }] }
  }
}

// ---------- 工具 ----------
function formatNum(n) {
  if (n == null) return '0'
  return Number(n).toLocaleString('en-US')
}

onMounted(() => {
  loaded.overview = true  // 默认加载第一个 tab
  loaded[activeTab.value] = true
  loadOverview()
  loadDocStatusStats()
  loadDocs(1)
})
onBeforeUnmount(() => clearInterval(pollTimer))
</script>

<style scoped>
.data-manage { color: var(--admin-text); }
.block-card { margin-bottom: 16px; }
.status-cards { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 16px; }
.status-chip {
  min-width: 92px; padding: 10px 14px; border-radius: 8px; cursor: pointer;
  background: var(--admin-panel); border: 1px solid var(--admin-border); text-align: center;
}
.status-chip.active { border-color: var(--admin-primary); background: var(--admin-panel-2); }
.chip-value { font-size: 18px; font-weight: 700; }
.chip-label { font-size: 12px; color: var(--admin-text-dim); }
.filter-bar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; flex-wrap: wrap; }
.filter-bar .spacer { flex: 1; }
.pager { margin-top: 14px; justify-content: flex-end; }
.pipeline-form { display: flex; align-items: center; gap: 10px; }
.pipeline-form .hint { color: var(--admin-text-dim); font-size: 13px; }
.task-meta { margin-top: 8px; font-size: 13px; color: var(--admin-text-dim); }
.log-box {
  max-height: 320px; overflow: auto; background: var(--admin-bg); padding: 12px;
  border-radius: 6px; font-size: 12px; white-space: pre-wrap; margin: 0;
}
.review-body { display: flex; gap: 16px; height: 68vh; }
.review-left, .review-right { flex: 1; display: flex; flex-direction: column; }
.review-sub { font-weight: 600; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }
.preview-frame { flex: 1; width: 100%; border: 1px solid var(--admin-border); border-radius: 6px; background: #fff; }
.preview-empty { flex: 1; display: flex; align-items: center; justify-content: center; color: var(--admin-text-dim); border: 1px dashed var(--admin-border); border-radius: 6px; }
.diag-error-item {
  padding: 4px 0;
  font-size: 13px;
  color: #606266;
}
</style>
