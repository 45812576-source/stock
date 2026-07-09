<template>
  <div class="kg-inspect">
    <el-row :gutter="16">
      <el-col :span="12">
        <div class="action-card">
          <div class="ac-title">KG 巡检（冲突清理 + 交叉补全）</div>
          <div class="ac-desc">扫描 Schema 违规、清洗实体名、清理冲突关系，并对近 N 天数据做交叉补全。</div>
          <div class="ac-params">
            <span>天数</span><el-input-number v-model="days" :min="1" :max="90" size="small" />
            <span>数量</span><el-input-number v-model="limit" :min="1" :max="200" size="small" />
            <el-checkbox v-model="dryRun">仅试运行（不写库）</el-checkbox>
          </div>
          <el-button type="primary" :loading="inspecting" @click="runInspect">开始巡检</el-button>
          <span v-if="info.last_inspect" class="last-time">上次巡检：{{ info.last_inspect }}</span>
        </div>
      </el-col>
      <el-col :span="12">
        <div class="action-card">
          <div class="ac-title">深度语义校验</div>
          <div class="ac-desc">按实体类型逐类深度校验语义一致性（调用大模型，耗时较长）。</div>
          <div class="ac-params">
            <el-checkbox v-model="semDryRun">仅试运行</el-checkbox>
            <el-input v-model="semTypes" placeholder="实体类型(逗号分隔,空=全量)" size="small" style="width: 240px" />
          </div>
          <el-button type="warning" :loading="validating" @click="runSemantic">开始校验</el-button>
        </div>
      </el-col>
    </el-row>

    <div v-if="inspecting || inspectResult" class="result-panel">
      <div class="rp-title">巡检进度</div>
      <el-steps :active="phaseIndex" finish-status="success" simple>
        <el-step title="Schema 扫描" />
        <el-step title="实体名清洗" />
        <el-step title="冲突清理" />
        <el-step title="交叉补全" />
      </el-steps>
      <div class="phase-label">{{ phaseLabel }}</div>
      <el-progress
        v-if="crossTotal > 0"
        :percentage="Math.round((crossProgress / crossTotal) * 100)"
        :stroke-width="12"
      />
      <el-descriptions v-if="scan" :column="3" border size="small" class="scan-desc">
        <el-descriptions-item label="非法实体类型">{{ scan.invalid_entity_types }}</el-descriptions-item>
        <el-descriptions-item label="非法关系类型">{{ scan.invalid_relation_types }}</el-descriptions-item>
        <el-descriptions-item label="非法组合">{{ scan.invalid_combinations }}</el-descriptions-item>
        <el-descriptions-item label="冲突关系">{{ scan.conflicting_relations }}</el-descriptions-item>
        <el-descriptions-item label="非法实体名">{{ scan.invalid_entity_names }}</el-descriptions-item>
        <el-descriptions-item label="related 占比">{{ scan.related_ratio }}</el-descriptions-item>
      </el-descriptions>
    </div>

    <div v-if="validating || semanticMsg" class="result-panel">
      <div class="rp-title">语义校验</div>
      <div class="phase-label">
        <el-icon class="is-loading" v-if="validating"><Loading /></el-icon>
        {{ semanticMsg }}
      </div>
      <el-progress
        v-if="semTotalTypes > 0"
        :percentage="Math.round((semTypeIdx / semTotalTypes) * 100)"
        :stroke-width="12"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { ElMessage } from 'element-plus'
import { Loading } from '@element-plus/icons-vue'
import * as kgApi from '@/api/kg'

const days = ref(7)
const limit = ref(20)
const dryRun = ref(false)
const semDryRun = ref(false)
const semTypes = ref('')

const info = ref({})
const inspecting = ref(false)
const inspectResult = ref(null)
const phase = ref('')
const phaseLabel = ref('')
const scan = ref(null)
const crossProgress = ref(0)
const crossTotal = ref(0)

const PHASES = ['schema_validate', 'name_cleanup', 'cleanup', 'cross_complete', 'done']
const phaseIndex = computed(() => Math.max(0, PHASES.indexOf(phase.value)))

const validating = ref(false)
const semanticMsg = ref('')
const semTypeIdx = ref(0)
const semTotalTypes = ref(0)

let timer = null
let semTimer = null

async function loadInfo() {
  try { info.value = await kgApi.getKgInspectInfo() } catch (e) { /* ignore */ }
}

async function runInspect() {
  inspecting.value = true
  inspectResult.value = null
  scan.value = null
  crossProgress.value = 0
  crossTotal.value = 0
  phase.value = 'schema_validate'
  phaseLabel.value = '正在扫描 Schema...'
  const { task_id } = await kgApi.runKgInspect(days.value, limit.value, dryRun.value)
  clearInterval(timer)
  timer = setInterval(async () => {
    try {
      const t = await kgApi.getKgInspectStatus(task_id)
      phase.value = t.phase || phase.value
      phaseLabel.value = t.phase_label || phaseLabel.value
      if (t.scan) scan.value = t.scan
      crossProgress.value = t.cross_progress || 0
      crossTotal.value = t.cross_total || 0
      if (t.status === 'done' || t.status === 'failed') {
        clearInterval(timer)
        inspecting.value = false
        inspectResult.value = t.result
        if (t.status === 'done') { ElMessage.success('巡检完成'); loadInfo() }
        else ElMessage.error('巡检失败')
      }
    } catch (e) {
      clearInterval(timer); inspecting.value = false
    }
  }, 1500)
}

async function runSemantic() {
  validating.value = true
  semanticMsg.value = '正在启动语义校验...'
  semTypeIdx.value = 0
  semTotalTypes.value = 0
  const { task_id } = await kgApi.runSemanticValidate(semDryRun.value, semTypes.value)
  clearInterval(semTimer)
  semTimer = setInterval(async () => {
    try {
      const t = await kgApi.getSemanticStatus(task_id)
      semTypeIdx.value = t.type_idx || 0
      semTotalTypes.value = t.total_types || 0
      semanticMsg.value = t.current_type
        ? `正在校验 [${t.current_type}] ${t.batch_msg || ''}`
        : (t.batch_msg || '校验中...')
      if (t.status === 'done' || t.status === 'failed') {
        clearInterval(semTimer)
        validating.value = false
        semanticMsg.value = t.status === 'done' ? '语义校验完成' : `失败：${t.result?.error || ''}`
        ElMessage[t.status === 'done' ? 'success' : 'error'](semanticMsg.value)
      }
    } catch (e) {
      clearInterval(semTimer); validating.value = false
    }
  }, 2000)
}

onMounted(loadInfo)
onBeforeUnmount(() => { clearInterval(timer); clearInterval(semTimer) })
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
.ac-title { font-weight: 600; font-size: 15px; color: var(--text-main); margin-bottom: 8px; }
.ac-desc { font-size: 13px; color: var(--text-secondary); flex: 1; line-height: 20px; }
.ac-params { display: flex; align-items: center; gap: 10px; margin: 14px 0; font-size: 13px; color: var(--text-secondary); flex-wrap: wrap; }
.last-time { font-size: 12px; color: var(--text-tertiary); margin-top: 8px; }
.result-panel { margin-top: 18px; padding: 16px; border: 1px solid var(--divider, #f0f0f0); border-radius: 8px; background: #fff; }
.rp-title { font-weight: 600; margin-bottom: 14px; color: var(--text-main); }
.phase-label { margin: 12px 0; font-size: 13px; color: var(--text-tertiary); display: flex; align-items: center; gap: 6px; }
.scan-desc { margin-top: 12px; }
</style>
