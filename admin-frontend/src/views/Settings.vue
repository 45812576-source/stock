<template>
  <div class="settings-page">
    <el-tabs v-model="activeTab" @tab-change="onTabChange">
      <el-tab-pane label="API / 模型配置" name="api">
        <SettingsApi v-if="loaded.api" :ctx="ctx.api" @refresh="() => loadTab('api', true)" />
      </el-tab-pane>
      <el-tab-pane label="Skill 编辑器" name="skills">
        <SettingsSkills v-if="loaded.skills" :ctx="ctx.skills" @refresh="() => loadTab('skills', true)" />
      </el-tab-pane>
      <el-tab-pane label="基础设施状态" name="infra">
        <SettingsInfra v-if="activeTab === 'infra'" />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import * as settingsApi from '@/api/settings'
import SettingsApi from './settings/SettingsApi.vue'
import SettingsSkills from './settings/SettingsSkills.vue'
import SettingsInfra from './settings/SettingsInfra.vue'

const activeTab = ref('api')
const route = useRoute()
const router = useRouter()
const ctx = reactive({ api: null, skills: null })
const loaded = reactive({ api: false, skills: false })

async function loadTab(tab, force = false) {
  if (loaded[tab] && !force) return
  try {
    ctx[tab] = await settingsApi.getPageContext(tab)
    loaded[tab] = true
  } catch (e) {
    ElMessage.error('加载设置数据失败')
  }
}

watch(
  () => route.query.tab,
  (t) => {
    if (t && t !== activeTab.value) {
      activeTab.value = String(t)
      loadTab(String(t))
    }
  },
  { immediate: true },
)

function onTabChange(name) {
  router.replace({ query: { ...route.query, tab: name } })
  loadTab(name)
}

onMounted(() => loadTab(activeTab.value))
</script>

<style scoped>
.settings-page {
  padding: 4px;
}
</style>
