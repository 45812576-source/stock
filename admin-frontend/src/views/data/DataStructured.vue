<template>
  <div v-if="ctx">
    <!-- 数据新鲜度 -->
    <el-card shadow="never" class="block">
      <template #header><span>数据新鲜度</span></template>
      <el-descriptions :column="4" border size="small">
        <el-descriptions-item label="日线">{{ ctx.data_freshness?.daily || '—' }}</el-descriptions-item>
        <el-descriptions-item label="资金流">{{ ctx.data_freshness?.capital || '—' }}</el-descriptions-item>
        <el-descriptions-item label="财报">{{ ctx.data_freshness?.financial || '—' }}</el-descriptions-item>
        <el-descriptions-item label="北向">{{ ctx.data_freshness?.northbound || '—' }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <!-- 结构化数据批量下载 -->
    <el-card shadow="never" class="block">
      <template #header><span>结构化数据批量下载</span></template>
      <el-form inline @submit.prevent>
        <el-form-item label="股票池">
          <el-select v-model="batch.pool" style="width: 140px">
            <el-option label="自选池" value="watchlist" />
            <el-option label="全市场" value="all" />
            <el-option label="自定义" value="custom" />
          </el-select>
        </el-form-item>
        <el-form-item label="自定义代码" v-if="batch.pool === 'custom'">
          <el-input v-model="batch.customCodes" placeholder="逗号分隔，如 600519,000001" style="width: 240px" />
        </el-form-item>
        <el-form-item label="起始">
          <el-input v-model="batch.start" placeholder="20240101" style="width: 120px" />
        </el-form-item>
        <el-form-item label="结束">
          <el-input v-model="batch.end" placeholder="20241231" style="width: 120px" />
        </el-form-item>
        <el-form-item label="类型">
          <el-checkbox-group v-model="batch.types">
            <el-checkbox value="daily">日线</el-checkbox>
            <el-checkbox value="capital">资金流</el-checkbox>
            <el-checkbox value="financial">财报</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="batch.running" @click="startBatch">开始下载</el-button>
        </el-form-item>
      </el-form>
      <div v-if="batch.task" class="task-progress">
        <el-progress :percentage="batchPercent" :status="batch.task.status === 'done' ? 'success' : ''" />
        <span class="task-current">{{ batch.task.current }} ({{ batch.task.progress }}/{{ batch.task.total }})</span>
      </div>
    </el-card>

    <!-- 监控规则 -->
    <el-card shadow="never" class="block">
      <template #header>
        <div class="block-header">
          <span>数据监控规则</span>
          <el-button size="small" @click="openAddRule">新增规则</el-button>
        </div>
      </template>
      <el-table :data="ctx.monitor_rules || []" size="small" border>
        <el-table-column prop="module_name" label="模块" width="130" />
        <el-table-column prop="data_type" label="数据类型" width="120" />
        <el-table-column prop="stock_pool" label="股票池" width="110" />
        <el-table-column prop="lookback_days" label="回溯天数" width="90" />
        <el-table-column prop="schedule_cron" label="Cron" width="140" />
        <el-table-column prop="enabled" label="启用" width="70">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">{{ row.enabled ? '是' : '否' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="last_status" label="上次状态" width="100" />
        <el-table-column label="操作" width="180">
          <template #default="{ row }">
            <el-button size="small" text @click="triggerRule(row)">触发</el-button>
            <el-button size="small" text @click="editRule(row)">编辑</el-button>
            <el-button size="small" text type="danger" @click="removeRule(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="ruleDialog" :title="ruleEditing ? '编辑监控规则' : '新增监控规则'" width="480px">
      <el-form :model="ruleForm" label-width="100px">
        <el-form-item label="模块名"><el-input v-model="ruleForm.module_name" /></el-form-item>
        <el-form-item label="数据类型"><el-input v-model="ruleForm.data_type" /></el-form-item>
        <el-form-item label="股票池">
          <el-select v-model="ruleForm.stock_pool" style="width: 100%">
            <el-option label="自选池" value="watchlist" />
            <el-option label="全市场" value="all" />
            <el-option label="自定义" value="custom" />
          </el-select>
        </el-form-item>
        <el-form-item label="自定义代码" v-if="ruleForm.stock_pool === 'custom'">
          <el-input v-model="ruleForm.customCodes" placeholder="逗号分隔" />
        </el-form-item>
        <el-form-item label="回溯天数"><el-input-number v-model="ruleForm.lookback_days" :min="1" /></el-form-item>
        <el-form-item label="Cron"><el-input v-model="ruleForm.schedule_cron" placeholder="如 0 18 * * *" /></el-form-item>
        <el-form-item label="启用"><el-switch v-model="ruleForm.enabled" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="ruleDialog = false">取消</el-button>
        <el-button type="primary" :loading="savingRule" @click="doSaveRule">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onBeforeUnmount } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as settingsApi from '@/api/settings'

const props = defineProps({ ctx: Object })
const emit = defineEmits(['refresh'])

let batchTimer = null

// ---- 批量下载 ----
const batch = reactive({
  pool: 'watchlist', customCodes: '', start: '20240101',
  end: new Date().toISOString().slice(0, 10).replace(/-/g, ''),
  types: ['daily', 'capital', 'financial'], running: false, task: null,
})
const batchPercent = computed(() =>
  batch.task && batch.task.total ? Math.round((batch.task.progress / batch.task.total) * 100) : 0)

async function startBatch() {
  batch.running = true
  try {
    const payload = {
      pool: batch.pool,
      custom_codes: batch.customCodes ? batch.customCodes.split(',').map((s) => s.trim()) : [],
      start_date: batch.start, end_date: batch.end, data_types: batch.types,
    }
    const r = await settingsApi.structBatch(payload)
    if (!r.ok) return ElMessage.error(r.error || '触发失败')
    ElMessage.success(`已触发，共 ${r.codes_count} 只股票`)
    pollBatch(r.task_id)
  } finally {
    batch.running = false
  }
}
function pollBatch(taskId) {
  clearInterval(batchTimer)
  batchTimer = setInterval(async () => {
    try {
      const t = await settingsApi.getStructTask(taskId)
      batch.task = t
      if (t.status === 'done') clearInterval(batchTimer)
    } catch { clearInterval(batchTimer) }
  }, 1500)
}

// ---- 监控规则 ----
const ruleDialog = ref(false)
const ruleEditing = ref(false)
const savingRule = ref(false)
const ruleForm = reactive({
  id: null, module_name: '', data_type: '', stock_pool: 'watchlist',
  customCodes: '', lookback_days: 7, schedule_cron: '', enabled: true,
})
function resetRule() {
  Object.assign(ruleForm, {
    id: null, module_name: '', data_type: '', stock_pool: 'watchlist',
    customCodes: '', lookback_days: 7, schedule_cron: '', enabled: true,
  })
}
function openAddRule() { ruleEditing.value = false; resetRule(); ruleDialog.value = true }
function editRule(row) {
  ruleEditing.value = true
  let codes = ''
  try { codes = (JSON.parse(row.custom_codes_json || '[]') || []).join(',') } catch { codes = '' }
  Object.assign(ruleForm, {
    id: row.id, module_name: row.module_name, data_type: row.data_type,
    stock_pool: row.stock_pool || 'watchlist', customCodes: codes,
    lookback_days: row.lookback_days || 7, schedule_cron: row.schedule_cron || '',
    enabled: !!row.enabled,
  })
  ruleDialog.value = true
}
async function doSaveRule() {
  if (!ruleForm.module_name || !ruleForm.data_type) return ElMessage.warning('模块名和数据类型必填')
  savingRule.value = true
  try {
    const payload = {
      module_name: ruleForm.module_name, data_type: ruleForm.data_type,
      stock_pool: ruleForm.stock_pool,
      custom_codes: ruleForm.customCodes ? ruleForm.customCodes.split(',').map((s) => s.trim()) : [],
      lookback_days: ruleForm.lookback_days, schedule_cron: ruleForm.schedule_cron,
      enabled: ruleForm.enabled,
    }
    if (ruleForm.id) payload.id = ruleForm.id
    const r = await settingsApi.saveMonitorRule(payload)
    if (r.ok) { ElMessage.success('已保存'); ruleDialog.value = false; emit('refresh') }
    else ElMessage.error(r.error || '保存失败')
  } finally {
    savingRule.value = false
  }
}
async function triggerRule(row) {
  try {
    const r = await settingsApi.triggerMonitor(row.id)
    if (r.ok !== false) ElMessage.success('已触发')
    else ElMessage.error(r.error || '触发失败')
  } catch { /* handled by interceptor */ }
}
async function removeRule(row) {
  await ElMessageBox.confirm('确认删除此监控规则？', '提示', { type: 'warning' })
  const r = await settingsApi.deleteMonitor(row.id)
  if (r.ok !== false) { ElMessage.success('已删除'); emit('refresh') }
}

onBeforeUnmount(() => { clearInterval(batchTimer) })
</script>

<style scoped>
.block { margin-bottom: 12px; }
.block-header { display: flex; justify-content: space-between; align-items: center; }
.task-progress { margin-top: 10px; }
.task-current { color: #909399; font-size: 12px; margin-left: 8px; }
</style>
