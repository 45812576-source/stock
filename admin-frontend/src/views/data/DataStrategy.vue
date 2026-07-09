<template>
  <div v-if="ctx">
    <!-- 选股规则库 -->
    <el-card shadow="never" class="block">
      <template #header>
        <div class="block-header">
          <span>选股规则库（系统 {{ ctx.system_count || 0 }} / 自定义 {{ ctx.custom_count || 0 }}）</span>
          <div>
            <el-button size="small" @click="doSeed">补充预置规则</el-button>
            <el-button size="small" type="primary" @click="openAddRule">新增规则</el-button>
          </div>
        </div>
      </template>
      <el-table :data="ctx.rules || []" size="small" border max-height="480">
        <el-table-column prop="rule_name" label="规则名" width="200" />
        <el-table-column label="分类" width="130">
          <template #default="{ row }">{{ catLabel(row.category) }}</template>
        </el-table-column>
        <el-table-column prop="definition" label="定义" show-overflow-tooltip />
        <el-table-column prop="layer" label="层" width="60" />
        <el-table-column label="类型" width="90">
          <template #default="{ row }">
            <el-tag :type="row.is_system ? 'warning' : 'success'" size="small">
              {{ row.is_system ? '系统' : '自定义' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="130">
          <template #default="{ row }">
            <el-button size="small" text :disabled="!!row.is_system" @click="editRule(row)">编辑</el-button>
            <el-button size="small" text type="danger" :disabled="!!row.is_system" @click="removeRule(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 标签计算引擎 -->
    <el-card shadow="never" class="block">
      <template #header><span>标签计算引擎</span></template>
      <el-form inline @submit.prevent>
        <el-form-item label="层级">
          <el-select v-model="tag.layer" style="width: 120px">
            <el-option label="L1" :value="1" />
            <el-option label="L2" :value="2" />
            <el-option label="L3" :value="3" />
          </el-select>
        </el-form-item>
        <el-form-item label="股票代码">
          <el-input v-model="tag.stockCode" placeholder="留空=全量" style="width: 160px" />
        </el-form-item>
        <el-form-item label="模式">
          <el-select v-model="tag.mode" style="width: 120px">
            <el-option label="测试" value="test" />
            <el-option label="全量" value="full" />
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="tag.running" @click="startTagging">运行标签计算</el-button>
        </el-form-item>
      </el-form>
      <div v-if="tag.task" class="task-progress">
        <el-tag :type="tag.task.status === 'done' ? 'success' : tag.task.status === 'error' ? 'danger' : 'info'">
          {{ tag.task.status }}
        </el-tag>
        <pre v-if="tag.task.result" class="task-result">{{ formatResult(tag.task.result) }}</pre>
      </div>
    </el-card>

    <!-- 规则编辑对话框 -->
    <el-dialog v-model="ruleDialog" :title="ruleEditing ? '编辑规则' : '新增规则'" width="520px">
      <el-form :model="ruleForm" label-width="90px">
        <el-form-item label="规则名"><el-input v-model="ruleForm.rule_name" /></el-form-item>
        <el-form-item label="分类">
          <el-select v-model="ruleForm.category" filterable allow-create style="width: 100%">
            <el-option v-for="(meta, key) in ctx.rule_categories" :key="key" :label="meta.label" :value="key" />
          </el-select>
        </el-form-item>
        <el-form-item label="定义">
          <el-input v-model="ruleForm.definition" type="textarea" :rows="4" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="ruleDialog = false">取消</el-button>
        <el-button type="primary" :loading="savingRule" @click="doSaveRule">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onBeforeUnmount } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as settingsApi from '@/api/settings'

const props = defineProps({ ctx: Object })
const emit = defineEmits(['refresh'])

let tagTimer = null

const catLabel = (key) => props.ctx?.rule_categories?.[key]?.label || key

// ---- 规则 CRUD ----
const ruleDialog = ref(false)
const ruleEditing = ref(false)
const savingRule = ref(false)
const ruleForm = reactive({ id: null, rule_name: '', category: '', definition: '' })

function resetRule() { Object.assign(ruleForm, { id: null, rule_name: '', category: '', definition: '' }) }
function openAddRule() { ruleEditing.value = false; resetRule(); ruleDialog.value = true }
function editRule(row) {
  ruleEditing.value = true
  Object.assign(ruleForm, { id: row.id, rule_name: row.rule_name, category: row.category, definition: row.definition })
  ruleDialog.value = true
}
async function doSaveRule() {
  if (!ruleForm.rule_name || !ruleForm.definition) return ElMessage.warning('规则名和定义必填')
  savingRule.value = true
  try {
    const payload = { rule_name: ruleForm.rule_name, category: ruleForm.category || 'custom', definition: ruleForm.definition }
    const r = ruleForm.id
      ? await settingsApi.updateSelectionRule(ruleForm.id, payload)
      : await settingsApi.addSelectionRule(payload)
    if (r.ok) { ElMessage.success('已保存'); ruleDialog.value = false; emit('refresh') }
    else ElMessage.error(r.error || '保存失败')
  } finally {
    savingRule.value = false
  }
}
async function removeRule(row) {
  await ElMessageBox.confirm(`确认删除规则「${row.rule_name}」？`, '提示', { type: 'warning' })
  const r = await settingsApi.deleteSelectionRule(row.id)
  if (r.ok) { ElMessage.success('已删除'); emit('refresh') }
  else ElMessage.error(r.error || '删除失败')
}
async function doSeed() {
  const r = await settingsApi.seedSelectionRules()
  if (r.ok) { ElMessage.success(`已补充 ${r.added}/${r.total}`); emit('refresh') }
}

// ---- 标签引擎 ----
const tag = reactive({ layer: 1, stockCode: '', mode: 'test', running: false, task: null })

function formatResult(r) {
  try { return typeof r === 'string' ? r : JSON.stringify(r, null, 2) } catch { return String(r) }
}

async function startTagging() {
  tag.running = true
  try {
    const r = await settingsApi.runTagging({
      layer: tag.layer, stock_code: tag.stockCode || undefined, mode: tag.mode,
    })
    if (r.ok === false) return ElMessage.error(r.error || '触发失败')
    ElMessage.success('已启动')
    pollTagging()
  } finally {
    tag.running = false
  }
}
function pollTagging() {
  clearInterval(tagTimer)
  tagTimer = setInterval(async () => {
    try {
      const s = await settingsApi.getTaggingStatus()
      tag.task = s
      if (s.status === 'done' || s.status === 'error' || s.status === 'idle') clearInterval(tagTimer)
    } catch { clearInterval(tagTimer) }
  }, 2000)
}

onBeforeUnmount(() => clearInterval(tagTimer))
</script>

<style scoped>
.block { margin-bottom: 12px; }
.block-header { display: flex; justify-content: space-between; align-items: center; }
.task-progress { margin-top: 10px; }
.task-result { background: #f5f7fa; padding: 8px; border-radius: 4px; font-size: 12px; max-height: 200px; overflow: auto; }
</style>
