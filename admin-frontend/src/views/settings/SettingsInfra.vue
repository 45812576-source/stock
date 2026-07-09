<template>
  <div class="infra-status">
    <div class="header-row">
      <h3 style="margin:0">基础设施状态</h3>
      <el-button size="small" :loading="loading" @click="refresh">刷新</el-button>
    </div>

    <div v-if="loading && !data" class="loading-tip">检测中...</div>

    <div v-if="data" class="status-grid">
      <!-- Milvus -->
      <div class="status-card" :class="data.milvus?.ok ? 'ok' : 'fail'">
        <div class="card-header">
          <span class="dot" :class="data.milvus?.ok ? 'green' : 'red'"></span>
          <span class="card-title">Milvus 向量数据库</span>
          <el-tag size="small" :type="data.milvus?.ok ? 'success' : 'danger'">
            {{ data.milvus?.ok ? '运行中' : '离线' }}
          </el-tag>
        </div>
        <div v-if="data.milvus?.ok && data.milvus.collections" class="card-body">
          <div v-for="(count, name) in data.milvus.collections" :key="name" class="coll-item">
            <span class="coll-name">{{ name }}</span>
            <span class="coll-count">{{ count.toLocaleString() }} 条</span>
          </div>
        </div>
        <div v-else-if="!data.milvus?.ok" class="card-body error-msg">
          {{ data.milvus?.error }}
        </div>
      </div>

      <!-- 本地 MySQL -->
      <div class="status-card" :class="data.local_mysql?.ok ? 'ok' : 'fail'">
        <div class="card-header">
          <span class="dot" :class="data.local_mysql?.ok ? 'green' : 'red'"></span>
          <span class="card-title">本地 MySQL</span>
          <el-tag size="small" :type="data.local_mysql?.ok ? 'success' : 'danger'">
            {{ data.local_mysql?.ok ? '正常' : '异常' }}
          </el-tag>
        </div>
        <div v-if="!data.local_mysql?.ok" class="card-body error-msg">
          {{ data.local_mysql?.error }}
        </div>
      </div>

      <!-- 云端 MySQL -->
      <div class="status-card" :class="data.cloud_mysql?.ok ? 'ok' : 'fail'">
        <div class="card-header">
          <span class="dot" :class="data.cloud_mysql?.ok ? 'green' : 'red'"></span>
          <span class="card-title">云端 MySQL</span>
          <el-tag size="small" :type="data.cloud_mysql?.ok ? 'success' : 'danger'">
            {{ data.cloud_mysql?.ok ? '正常' : '异常' }}
          </el-tag>
        </div>
        <div v-if="!data.cloud_mysql?.ok" class="card-body error-msg">
          {{ data.cloud_mysql?.error }}
        </div>
      </div>

      <!-- Embedding 模型 -->
      <div class="status-card" :class="data.embedding?.ok ? 'ok' : 'fail'">
        <div class="card-header">
          <span class="dot" :class="data.embedding?.ok ? 'green' : 'red'"></span>
          <span class="card-title">Embedding 模型</span>
          <el-tag size="small" :type="data.embedding?.ok ? 'success' : 'danger'">
            {{ data.embedding?.ok ? `正常 (dim=${data.embedding.dim})` : '异常' }}
          </el-tag>
        </div>
        <div v-if="!data.embedding?.ok" class="card-body error-msg">
          {{ data.embedding?.error }}
        </div>
      </div>
    </div>

    <div v-if="checkedAt" class="checked-at">上次检查: {{ checkedAt }}</div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import request from '@/api/request'

const loading = ref(false)
const data = ref(null)
const checkedAt = ref('')

async function refresh() {
  loading.value = true
  try {
    const res = await request.get('/api/infra-status')
    data.value = res
    checkedAt.value = new Date().toLocaleTimeString()
  } catch (e) {
    data.value = {
      milvus: { ok: false, error: '请求失败' },
      local_mysql: { ok: false, error: '请求失败' },
      cloud_mysql: { ok: false, error: '请求失败' },
      embedding: { ok: false, error: '请求失败' },
    }
  } finally {
    loading.value = false
  }
}

onMounted(() => refresh())
</script>

<style scoped>
.infra-status { padding: 8px 0; }
.header-row { display: flex; align-items: center; gap: 16px; margin-bottom: 16px; }
.loading-tip { color: #909399; padding: 20px 0; }
.status-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 12px; }
.status-card {
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  padding: 12px 16px;
  transition: border-color .2s;
}
.status-card.ok { border-left: 3px solid #67c23a; }
.status-card.fail { border-left: 3px solid #f56c6c; }
.card-header { display: flex; align-items: center; gap: 8px; }
.card-title { font-weight: 500; flex: 1; }
.dot {
  width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0;
}
.dot.green { background: #67c23a; }
.dot.red { background: #f56c6c; }
.card-body { margin-top: 8px; font-size: 13px; color: #606266; }
.coll-item { display: flex; justify-content: space-between; padding: 2px 0; }
.coll-name { font-family: monospace; }
.coll-count { color: #909399; }
.error-msg { color: #f56c6c; word-break: break-all; }
.checked-at { margin-top: 12px; font-size: 12px; color: #909399; }
</style>
