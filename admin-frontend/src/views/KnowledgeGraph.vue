<template>
  <div class="kg-page" v-loading="loading">
    <div class="kg-header">
      <div class="kg-stat">
        <span class="dot" :class="{ ok: stats.total_entities > 0 }" />
        {{ stats.total_entities || 0 }} 实体 · {{ stats.total_relationships || 0 }} 关系
      </div>
    </div>

    <el-tabs v-model="activeTab" class="kg-tabs" @tab-change="onTabChange">
      <el-tab-pane label="Schema" name="schema">
        <KgSchema v-if="meta" :meta="meta" />
      </el-tab-pane>
      <el-tab-pane label="可视化" name="visualization">
        <KgVisualization v-if="meta && activeTab === 'visualization'" :meta="meta" />
      </el-tab-pane>
      <el-tab-pane label="实体管理" name="entities">
        <KgEntities v-if="meta && activeTab === 'entities'" :meta="meta" />
      </el-tab-pane>
      <el-tab-pane label="推理引擎" name="inference">
        <KgInference v-if="meta && activeTab === 'inference'" :meta="meta" />
      </el-tab-pane>
      <el-tab-pane label="巡检" name="inspect">
        <KgInspect v-if="activeTab === 'inspect'" :meta="meta" />
      </el-tab-pane>
      <el-tab-pane label="审核工作台" name="annotate">
        <KgReview v-if="meta && activeTab === 'annotate'" :meta="meta" />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import * as kgApi from '@/api/kg'
import KgSchema from './kg/KgSchema.vue'
import KgVisualization from './kg/KgVisualization.vue'
import KgEntities from './kg/KgEntities.vue'
import KgInference from './kg/KgInference.vue'
import KgInspect from './kg/KgInspect.vue'
import KgReview from './kg/KgReview.vue'

const activeTab = ref('schema')
const route = useRoute()
const router = useRouter()
watch(
  () => route.query.tab,
  (t) => { if (t && t !== activeTab.value) activeTab.value = String(t) },
  { immediate: true },
)
function onTabChange(name) {
  router.replace({ query: { ...route.query, tab: name } })
}
const loading = ref(false)
const meta = ref(null)
const stats = computed(() => meta.value?.stats || {})

async function loadMeta() {
  loading.value = true
  try {
    meta.value = await kgApi.getMeta()
  } finally {
    loading.value = false
  }
}

onMounted(loadMeta)
</script>

<style scoped>
.kg-page { color: var(--admin-text); }
.kg-header { display: flex; justify-content: flex-end; margin-bottom: 8px; }
.kg-stat { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--admin-text-dim); }
.kg-stat .dot { width: 8px; height: 8px; border-radius: 50%; background: #f59e0b; }
.kg-stat .dot.ok { background: #10b981; }
</style>
