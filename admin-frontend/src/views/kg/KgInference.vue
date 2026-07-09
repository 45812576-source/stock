<template>
  <div class="kg-inference">
    <el-row :gutter="16">
      <el-col :span="8">
        <div class="action-card">
          <div class="ac-title">识别投资主题</div>
          <div class="ac-desc">从近 N 天清洗数据中，纯数据驱动聚类识别投资主题（不调用大模型）。</div>
          <div class="ac-params">
            <span>天数</span>
            <el-input-number v-model="themeDays" :min="1" :max="90" size="small" />
            <span>置信度</span>
            <el-input-number v-model="themeConf" :min="0.1" :max="1" :step="0.1" size="small" />
          </div>
          <el-button type="primary" :loading="busy === 'theme'" @click="runTheme">开始识别</el-button>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="action-card">
          <div class="ac-title">构建 / 更新知识图谱</div>
          <div class="ac-desc">从清洗后的语料中提取实体与关系写入图谱，完成后自动触发推理。</div>
          <div class="ac-params">
            <el-checkbox v-model="useClaude">使用 Claude 增强抽取</el-checkbox>
          </div>
          <el-button type="warning" :loading="busy === 'update'" @click="runUpdate">开始构建</el-button>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="action-card">
          <div class="ac-title">运行推理引擎</div>
          <div class="ac-desc">基于现有关系发现隐含关系（链式/同业竞争），结果可人工采纳入库。</div>
          <div class="ac-params">
            <span>规则</span>
            <el-select v-model="ruleType" size="small" style="width: 120px">
              <el-option label="全部" value="all" />
              <el-option label="链式推理" value="chain" />
              <el-option label="同业相似" value="similarity" />
            </el-select>
          </div>
          <el-button type="success" :loading="busy === 'infer'" @click="runInfer">开始推理</el-button>
        </div>
      </el-col>
    </el-row>

    <div v-if="progressMsg" class="progress-box">
      <el-icon class="is-loading" v-if="busy"><Loading /></el-icon>
      <span>{{ progressMsg }}</span>
    </div>

    <div v-if="discovered.length" class="discovered">
      <div class="dv-head">
        <span>发现 {{ discovered.length }} 条隐含关系</span>
      </div>
      <el-table :data="discovered" size="small" border max-height="440">
        <el-table-column label="源实体" min-width="140">
          <template #default="{ row }">
            <span class="dot" :style="{ background: colorOf(row.source_type) }" />{{ row.source_name }}
          </template>
        </el-table-column>
        <el-table-column label="目标实体" min-width="140">
          <template #default="{ row }">
            <span class="dot" :style="{ background: colorOf(row.target_type) }" />{{ row.target_name }}
          </template>
        </el-table-column>
        <el-table-column label="推理逻辑" min-width="280" show-overflow-tooltip>
          <template #default="{ row }">{{ row.logic }}</template>
        </el-table-column>
        <el-table-column label="置信度" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.confidence >= 0.5 ? 'success' : 'info'">{{ row.confidence }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row, $index }">
            <el-button link type="primary" :disabled="row._accepted" @click="accept(row, $index)">
              {{ row._accepted ? '已采纳' : '采纳' }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Loading } from '@element-plus/icons-vue'
import * as kgApi from '@/api/kg'

const props = defineProps({ meta: { type: Object, required: true } })
function colorOf(t) { return props.meta.entity_colors?.[t]?.bg || '#64748b' }

const themeDays = ref(7)
const themeConf = ref(0.6)
const useClaude = ref(false)
const ruleType = ref('all')

const busy = ref('')
const progressMsg = ref('')
const discovered = ref([])

let timer = null
function poll(taskId, onDone) {
  clearInterval(timer)
  timer = setInterval(async () => {
    try {
      const t = await kgApi.getTaskStatus(taskId)
      progressMsg.value = t.progress || progressMsg.value
      if (t.status === 'done' || t.status === 'failed') {
        clearInterval(timer)
        busy.value = ''
        onDone(t)
      }
    } catch (e) {
      clearInterval(timer)
      busy.value = ''
    }
  }, 1500)
}

async function runTheme() {
  busy.value = 'theme'
  progressMsg.value = '正在识别投资主题...'
  discovered.value = []
  const { task_id } = await kgApi.identifyThemes(themeDays.value, themeConf.value)
  poll(task_id, (t) => {
    if (t.status === 'done') {
      const r = t.result || {}
      progressMsg.value = `识别完成：新增/更新主题 ${r.themes_count ?? r.total ?? '若干'} 个`
      ElMessage.success('主题识别完成')
    } else {
      progressMsg.value = `失败：${t.result?.error || '未知错误'}`
      ElMessage.error('主题识别失败')
    }
  })
}

async function runUpdate() {
  busy.value = 'update'
  progressMsg.value = '正在构建知识图谱...'
  discovered.value = []
  const { task_id } = await kgApi.updateKg(useClaude.value)
  poll(task_id, (t) => {
    if (t.status === 'done') {
      const r = t.result || {}
      progressMsg.value = `构建完成：实体 ${r.entities ?? '?'} · 关系 ${r.relationships ?? '?'} · 推理 ${r.inferred ?? 0}`
      ElMessage.success('知识图谱构建完成')
    } else {
      progressMsg.value = `失败：${t.result?.error || '未知错误'}`
      ElMessage.error('构建失败')
    }
  })
}

async function runInfer() {
  busy.value = 'infer'
  progressMsg.value = '正在分析实体关系模式...'
  discovered.value = []
  const { task_id } = await kgApi.runInference(ruleType.value)
  poll(task_id, (t) => {
    if (t.status === 'done') {
      const r = t.result || {}
      discovered.value = (r.discovered || []).map((x) => ({ ...x, _accepted: false }))
      progressMsg.value = `推理完成：发现 ${r.total ?? discovered.value.length} 条隐含关系`
    } else {
      progressMsg.value = `失败：${t.result?.error || '未知错误'}`
      ElMessage.error('推理失败')
    }
  })
}

async function accept(row, idx) {
  await kgApi.acceptInference({
    source_id: row.source_id,
    target_id: row.target_id,
    relation_type: row.relation_type || 'related',
    confidence: row.confidence,
    logic: row.logic,
  })
  discovered.value[idx]._accepted = true
  ElMessage.success('已采纳并写入图谱')
}
</script>

<style scoped>
.action-card {
  background: #ffffff;
  border: 1px solid var(--divider, #f0f0f0);
  border-radius: 8px;
  padding: 20px;
  height: 100%;
  display: flex;
  flex-direction: column;
  transition: box-shadow 0.2s ease;
}
.action-card:hover {
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
}
.ac-title { font-weight: 600; font-size: 15px; color: var(--text-main, rgba(0,0,0,0.88)); margin-bottom: 8px; }
.ac-desc { font-size: 13px; color: var(--text-secondary, rgba(0,0,0,0.65)); flex: 1; line-height: 20px; }
.ac-params { display: flex; align-items: center; gap: 10px; margin: 14px 0; font-size: 13px; color: var(--text-secondary); flex-wrap: wrap; }
.progress-box {
  margin-top: 16px;
  padding: 12px 16px;
  background: rgba(22, 119, 255, 0.06);
  border: 1px solid rgba(22, 119, 255, 0.15);
  border-radius: 6px;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: var(--text-main);
}
.discovered { margin-top: 16px; }
.dv-head { margin-bottom: 8px; font-weight: 600; font-size: 14px; color: var(--text-main); }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
</style>
