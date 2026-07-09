<template>
  <el-drawer
    :model-value="visible"
    :size="420"
    :with-header="false"
    direction="rtl"
    @update:model-value="onVisibleUpdate"
  >
    <div class="task-drawer">
      <!-- Header -->
      <div class="drawer-header">
        <div class="header-title">
          <span class="material-symbols-outlined text-primary">task_alt</span>
          <span>任务中心</span>
          <span v-if="tasks.length" class="count-pill">{{ tasks.length }}</span>
        </div>
        <button class="icon-close" @click="onVisibleUpdate(false)">
          <span class="material-symbols-outlined">close</span>
        </button>
      </div>

      <div class="drawer-tip">
        软暂停语义：停止推进新任务，在途批次会跑完。
      </div>

      <!-- Empty -->
      <div v-if="!tasks.length" class="empty-block">
        <span class="material-symbols-outlined text-[48px] text-text-secondary">inbox</span>
        <div class="empty-text">暂无运行中的任务</div>
        <div class="empty-sub">批量任务启动后会在此实时显示进度</div>
      </div>

      <!-- Task List -->
      <div v-else class="task-list">
        <div v-for="t in tasks" :key="t.task_id" class="task-item">
          <div class="task-head">
            <div class="task-label">
              <span
                class="task-status-dot"
                :class="dotClass(t)"
              ></span>
              {{ t.label }}
            </div>
            <div class="task-count">{{ t.done }}/{{ t.total }}</div>
          </div>

          <div class="task-progress">
            <div class="progress-outer">
              <div
                class="progress-inner"
                :class="progressClass(t)"
                :style="{ width: (t.progress || 0) + '%' }"
              ></div>
            </div>
            <span class="progress-pct">{{ Math.round(t.progress || 0) }}%</span>
          </div>

          <div v-if="t.current" class="task-current" :title="t.current">{{ t.current }}</div>

          <div class="task-actions">
            <button
              v-if="!t.paused"
              class="action-btn"
              :disabled="loading[t.task_id]"
              @click="handlePause(t)"
            >
              <span class="material-symbols-outlined">pause_circle</span> 暂停
            </button>
            <button
              v-else
              class="action-btn"
              :disabled="loading[t.task_id]"
              @click="handleResume(t)"
            >
              <span class="material-symbols-outlined">play_circle</span> 恢复
            </button>
            <button
              class="action-btn action-danger"
              :disabled="loading[t.task_id]"
              @click="handleCancel(t)"
            >
              <span class="material-symbols-outlined">cancel</span> 取消
            </button>
          </div>
        </div>
      </div>
    </div>
  </el-drawer>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getActiveTasks, pauseTask, resumeTask, cancelTask } from '@/api/data'

const props = defineProps({
  visible: { type: Boolean, default: false },
})
const emit = defineEmits(['update:visible', 'count-change'])

const tasks = ref([])
const loading = ref({})
let timer = null

function onVisibleUpdate(v) {
  emit('update:visible', v)
}

async function poll() {
  try {
    const r = await getActiveTasks()
    tasks.value = Array.isArray(r) ? r : []
    emit('count-change', tasks.value.length)
  } catch {
    // 静默失败
  }
}

function dotClass(t) {
  if (t.paused) return 'dot-warning'
  if (t.done >= t.total && t.total > 0) return 'dot-success'
  return 'dot-active'
}

function progressClass(t) {
  if (t.paused) return 'progress-warning'
  return 'progress-primary'
}

async function handlePause(t) {
  loading.value[t.task_id] = true
  try {
    await pauseTask(t.task_id)
    ElMessage.success('已提交软暂停请求')
    poll()
  } catch {
    // request 拦截器已提示
  } finally {
    loading.value[t.task_id] = false
  }
}

async function handleResume(t) {
  loading.value[t.task_id] = true
  try {
    await resumeTask(t.task_id)
    ElMessage.success('已恢复任务')
    poll()
  } catch {
    // ignore
  } finally {
    loading.value[t.task_id] = false
  }
}

async function handleCancel(t) {
  try {
    await ElMessageBox.confirm(`确认取消任务「${t.label}」？在途批次会尽快中止。`, '危险操作', {
      type: 'warning',
      confirmButtonText: '确认取消',
      cancelButtonText: '再想想',
      confirmButtonClass: 'el-button--danger',
    })
  } catch {
    return
  }
  loading.value[t.task_id] = true
  try {
    await cancelTask(t.task_id)
    ElMessage.success('已提交取消请求')
    poll()
  } catch {
    // ignore
  } finally {
    loading.value[t.task_id] = false
  }
}

onMounted(() => {
  poll()
  timer = setInterval(poll, 3000)
})
onBeforeUnmount(() => clearInterval(timer))

// 打开时立即刷新一次
watch(
  () => props.visible,
  (v) => {
    if (v) poll()
  }
)
</script>

<style scoped>
.task-drawer {
  display: flex;
  flex-direction: column;
  height: 100%;
  padding: 20px;
}
.drawer-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}
.header-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  font-weight: 600;
  color: var(--text-main);
}
.count-pill {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  height: 20px;
  padding: 0 8px;
  background: var(--primary-fixed);
  color: var(--primary);
  font-size: 12px;
  font-weight: 600;
  border-radius: 10px;
  margin-left: 4px;
}
.icon-close {
  background: transparent;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 4px;
  border-radius: 4px;
  display: inline-flex;
}
.icon-close:hover {
  background: #f5f5f5;
  color: var(--text-main);
}

.drawer-tip {
  font-size: 12px;
  color: var(--text-tertiary);
  padding: 8px 12px;
  background: #fafafa;
  border-radius: 4px;
  margin-bottom: 16px;
  border-left: 3px solid var(--primary);
}

.empty-block {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--text-tertiary);
  gap: 6px;
}
.empty-text {
  color: var(--text-secondary);
  font-size: 14px;
  margin-top: 8px;
}
.empty-sub {
  color: var(--text-tertiary);
  font-size: 12px;
}

.task-list {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.task-item {
  background: #fff;
  border: 1px solid var(--divider);
  border-radius: 4px;
  padding: 12px;
  transition: box-shadow 0.15s;
}
.task-item:hover {
  box-shadow: var(--shadow-card);
}
.task-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}
.task-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  font-weight: 500;
  color: var(--text-main);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 240px;
}
.task-status-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  flex-shrink: 0;
}
.dot-active {
  background: var(--primary);
  box-shadow: 0 0 0 2px rgba(22, 119, 255, 0.15);
  animation: pulse 1.5s ease-in-out infinite;
}
.dot-warning {
  background: var(--warning);
}
.dot-success {
  background: var(--success);
}
@keyframes pulse {
  0%, 100% {
    box-shadow: 0 0 0 2px rgba(22, 119, 255, 0.15);
  }
  50% {
    box-shadow: 0 0 0 4px rgba(22, 119, 255, 0.25);
  }
}
.task-count {
  font-size: 12px;
  color: var(--text-tertiary);
  font-variant-numeric: tabular-nums;
}
.task-progress {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.progress-outer {
  flex: 1;
  height: 6px;
  background: #f0f0f0;
  border-radius: 3px;
  overflow: hidden;
}
.progress-inner {
  height: 100%;
  transition: width 0.3s;
  border-radius: 3px;
}
.progress-primary {
  background: var(--primary);
}
.progress-warning {
  background: var(--warning);
}
.progress-pct {
  font-size: 12px;
  color: var(--text-secondary);
  min-width: 40px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.task-current {
  font-size: 12px;
  color: var(--text-tertiary);
  margin-bottom: 8px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.task-actions {
  display: flex;
  gap: 8px;
  padding-top: 8px;
  border-top: 1px dashed var(--divider);
}
.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 10px;
  background: #fff;
  border: 1px solid var(--divider);
  border-radius: 2px;
  color: var(--text-main);
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}
.action-btn .material-symbols-outlined {
  font-size: 16px !important;
}
.action-btn:hover:not(:disabled) {
  border-color: var(--primary);
  color: var(--primary);
}
.action-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.action-danger:hover:not(:disabled) {
  border-color: var(--error);
  color: var(--error);
}
</style>
