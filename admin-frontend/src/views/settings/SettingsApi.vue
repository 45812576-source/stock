<template>
  <div v-if="ctx">
    <!-- 数据库统计 -->
    <el-row :gutter="12" class="stat-row">
      <el-col :span="6" v-for="(v, k) in ctx.db_stats" :key="k">
        <el-card shadow="never" class="stat-card">
          <div class="stat-num">{{ v }}</div>
          <div class="stat-label">{{ dbStatLabel(k) }}</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 多模型配置 -->
    <el-card shadow="never" class="block">
      <template #header>
        <div class="block-header">
          <span>多模型配置（按阶段）</span>
          <el-button size="small" @click="openAddConfig">新增/编辑阶段</el-button>
        </div>
      </template>
      <el-table :data="ctx.model_configs || []" size="small" border>
        <el-table-column prop="stage" label="阶段" width="130" />
        <el-table-column prop="provider" label="Provider" width="130" />
        <el-table-column prop="model_name" label="模型" />
        <el-table-column prop="base_url" label="Base URL" show-overflow-tooltip />
        <el-table-column prop="api_key_ref" label="Key 引用" width="150" />
        <el-table-column prop="enabled" label="启用" width="70">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
              {{ row.enabled ? '是' : '否' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="90">
          <template #default="{ row }">
            <el-button size="small" text @click="editConfig(row)">编辑</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 已存 Key + 连通性测试 -->
    <el-card shadow="never" class="block">
      <template #header><span>API Key 管理</span></template>
      <el-form inline @submit.prevent>
        <el-form-item label="Key 名称">
          <el-input v-model="keyForm.name" placeholder="如 openai_api_key" style="width: 200px" />
        </el-form-item>
        <el-form-item label="Key 值">
          <el-input v-model="keyForm.value" type="password" show-password placeholder="sk-..." style="width: 260px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="savingKey" @click="doSaveKey">保存 Key</el-button>
          <el-button :loading="testing" @click="doTestClaude">测试 Chat 模型</el-button>
        </el-form-item>
      </el-form>
      <div class="stored-keys" v-if="ctx.stored_keys && Object.keys(ctx.stored_keys).length">
        <el-tag v-for="(masked, name) in ctx.stored_keys" :key="name" class="key-tag">
          {{ name }}: {{ masked }}
        </el-tag>
      </div>
    </el-card>

    <!-- API 用量 -->
    <el-card shadow="never" class="block">
      <template #header><span>API 用量（近 30 条）</span></template>
      <el-table :data="ctx.api_usage || []" size="small" border max-height="300">
        <el-table-column prop="api_name" label="API" width="150" />
        <el-table-column prop="call_date" label="日期" width="120" />
        <el-table-column prop="call_count" label="次数" width="80" />
        <el-table-column prop="input_tokens" label="输入 tokens" width="110" />
        <el-table-column prop="output_tokens" label="输出 tokens" width="110" />
        <el-table-column prop="cost_usd" label="费用 (USD)" />
      </el-table>
    </el-card>

    <!-- 管线日志 -->
    <el-card shadow="never" class="block">
      <template #header><span>管线运行日志（近 20 条）</span></template>
      <el-table :data="ctx.pipeline_logs || []" size="small" border max-height="300">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="started_at" label="开始时间" width="180" />
        <el-table-column prop="pipeline_name" label="管线" width="140" />
        <el-table-column prop="stage" label="阶段" width="100" />
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.status === 'success' ? 'success' : row.status === 'error' ? 'danger' : 'info'" size="small">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="items_processed" label="处理数" width="90" />
        <el-table-column prop="error_message" label="错误" show-overflow-tooltip />
      </el-table>
    </el-card>

    <!-- 阶段配置对话框 -->
    <el-dialog v-model="dialogVisible" :title="editing ? '编辑阶段配置' : '新增阶段配置'" width="520px">
      <el-form :model="form" label-width="110px">
        <el-form-item label="阶段 stage">
          <el-select v-model="form.stage" filterable allow-create placeholder="选择或输入" style="width: 100%">
            <el-option v-for="s in STAGES" :key="s" :label="s" :value="s" />
          </el-select>
        </el-form-item>
        <el-form-item label="Provider">
          <el-select v-model="form.provider" style="width: 100%">
            <el-option v-for="p in PROVIDERS" :key="p" :label="p" :value="p" />
          </el-select>
        </el-form-item>
        <el-form-item label="模型名">
          <el-input v-model="form.model_name" placeholder="如 sonnet / gpt-4o" />
        </el-form-item>
        <el-form-item label="Base URL">
          <el-input v-model="form.base_url" placeholder="可选" />
        </el-form-item>
        <el-form-item label="API Key">
          <el-input v-model="form.api_key" type="password" show-password placeholder="留空则不更新" />
        </el-form-item>
        <el-form-item label="Key 引用名">
          <el-input v-model="form.api_key_ref" placeholder="可选，默认 {provider}_api_key" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="savingConfig" @click="doSaveConfig">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive } from 'vue'
import { ElMessage } from 'element-plus'
import * as settingsApi from '@/api/settings'

const props = defineProps({ ctx: Object })
const emit = defineEmits(['refresh'])

const STAGES = ['extraction', 'cleaning', 'kg', 'research', 'hotspot', 'ai_recommend', 'chat', 'vision', 'audio']
const PROVIDERS = ['claude_cli', 'anthropic', 'openai', 'gemini', 'groq', 'deepseek', 'minimax']

const DB_LABELS = {
  raw_items: '原始条目', cleaned_items: '已清洗', stock_info: '股票信息',
  stock_daily: '日线', capital_flow: '资金流', deep_research: '深度研报',
  kg_entities: 'KG 实体', kg_relationships: 'KG 关系',
}
const dbStatLabel = (k) => DB_LABELS[k] || k

const keyForm = reactive({ name: '', value: '' })
const savingKey = ref(false)
const testing = ref(false)

async function doSaveKey() {
  if (!keyForm.name || !keyForm.value) return ElMessage.warning('请填写 Key 名称和值')
  savingKey.value = true
  try {
    const r = await settingsApi.saveKey(keyForm.name, keyForm.value)
    if (r.ok) {
      ElMessage.success('已保存')
      keyForm.value = ''
      emit('refresh')
    } else ElMessage.error(r.error || '保存失败')
  } finally {
    savingKey.value = false
  }
}

async function doTestClaude() {
  testing.value = true
  try {
    const r = await settingsApi.testClaude()
    if (r.ok) ElMessage.success('连通正常：' + r.response)
    else ElMessage.error(r.error || '测试失败')
  } finally {
    testing.value = false
  }
}

const dialogVisible = ref(false)
const editing = ref(false)
const savingConfig = ref(false)
const form = reactive({ stage: '', provider: 'claude_cli', model_name: '', base_url: '', api_key: '', api_key_ref: '' })

function resetForm() {
  Object.assign(form, { stage: '', provider: 'claude_cli', model_name: '', base_url: '', api_key: '', api_key_ref: '' })
}
function openAddConfig() {
  editing.value = false
  resetForm()
  dialogVisible.value = true
}
function editConfig(row) {
  editing.value = true
  Object.assign(form, {
    stage: row.stage, provider: row.provider, model_name: row.model_name,
    base_url: row.base_url || '', api_key: '', api_key_ref: row.api_key_ref || '',
  })
  dialogVisible.value = true
}
async function doSaveConfig() {
  if (!form.stage) return ElMessage.warning('请填写阶段')
  savingConfig.value = true
  try {
    const r = await settingsApi.saveModelConfig({ ...form })
    if (r.ok) {
      ElMessage.success('已保存')
      dialogVisible.value = false
      emit('refresh')
    } else ElMessage.error(r.error || '保存失败')
  } finally {
    savingConfig.value = false
  }
}
</script>

<style scoped>
.stat-row { margin-bottom: 12px; }
.stat-card { text-align: center; }
.stat-num { font-size: 22px; font-weight: 600; }
.stat-label { color: #909399; font-size: 12px; margin-top: 4px; }
.block { margin-bottom: 12px; }
.block-header { display: flex; justify-content: space-between; align-items: center; }
.stored-keys { margin-top: 10px; }
.key-tag { margin: 4px 6px 4px 0; }
</style>
