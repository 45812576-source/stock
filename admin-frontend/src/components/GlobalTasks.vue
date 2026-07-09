<template>
  <el-popover placement="bottom-end" :width="360" trigger="click">
    <template #reference>
      <el-badge :value="tasks.length" :hidden="!tasks.length" type="warning">
        <el-button link>
          <el-icon :class="{ spinning: tasks.length }"><Loading v-if="tasks.length" /><Bell v-else /></el-icon>
          <span class="badge-text">任务</span>
        </el-button>
      </el-badge>
    </template>

    <div class="task-panel">
      <div class="panel-title">后台任务</div>
      <el-empty v-if="!tasks.length" description="暂无运行中的任务" :image-size="60" />
      <div v-else class="task-list">
        <div v-for="t in tasks" :key="t.task_id" class="task-item">
          <div class="task-head">
            <span class="task-label">{{ t.label }}</span>
            <span class="task-count">{{ t.done }}/{{ t.total }}</span>
          </div>
          <el-progress :percentage="t.progress" :stroke-width="6" />
          <div class="task-current" v-if="t.current">{{ t.current }}</div>
        </div>
      </div>
    </div>
  </el-popover>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { Loading, Bell } from '@element-plus/icons-vue'
import { getActiveTasks } from '@/api/data'

const tasks = ref([])
let timer = null

async function poll() {
  try {
    const r = await getActiveTasks()
    tasks.value = Array.isArray(r) ? r : []
  } catch {
    // 静默失败，不打断 UI
  }
}

onMounted(() => {
  poll()
  timer = setInterval(poll, 3000)
})
onBeforeUnmount(() => clearInterval(timer))
</script>

<style scoped>
.badge-text { margin-left: 4px; }
.spinning { animation: rotate 1.2s linear infinite; }
@keyframes rotate { from { transform: rotate(0); } to { transform: rotate(360deg); } }
.task-panel { max-height: 400px; overflow-y: auto; }
.panel-title { font-weight: 600; margin-bottom: 10px; }
.task-item { padding: 8px 0; border-bottom: 1px solid var(--el-border-color-lighter); }
.task-item:last-child { border-bottom: none; }
.task-head { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 4px; }
.task-count { color: #909399; }
.task-current { font-size: 12px; color: #909399; margin-top: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
