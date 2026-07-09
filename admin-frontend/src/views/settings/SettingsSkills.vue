<template>
  <div v-if="ctx">
    <el-alert type="info" :closable="false" show-icon class="tip">
      点击「编辑」加载 Skill 文件内容，可直接修改并保存。
    </el-alert>

    <el-collapse v-model="activeModules">
      <el-collapse-item v-for="(mod, modKey) in ctx.registry" :key="modKey" :name="modKey">
        <template #title>
          <span class="mod-title">{{ mod.label || modKey }}</span>
          <el-tag size="small" class="mod-count">{{ (mod.entries || []).length }} 项</el-tag>
        </template>
        <el-table :data="mod.entries || []" size="small" border>
          <el-table-column prop="label" label="名称" width="180" />
          <el-table-column prop="desc" label="描述" show-overflow-tooltip />
          <el-table-column prop="skill_name" label="Skill" width="180" />
          <el-table-column label="状态" width="110">
            <template #default="{ row }">
              <el-tag v-if="row.skill_status?.exists" type="success" size="small">
                {{ row.skill_status.chars }} 字符
              </el-tag>
              <el-tag v-else type="info" size="small">未创建</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="90">
            <template #default="{ row }">
              <el-button size="small" text :disabled="!row.skill_name" @click="openEditor(row)">编辑</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-collapse-item>
    </el-collapse>

    <el-dialog v-model="editorVisible" :title="`编辑 Skill: ${current.skill_name}`" width="70%" top="5vh">
      <div v-loading="loading">
        <el-input
          v-model="content"
          type="textarea"
          :rows="24"
          placeholder="Skill 内容"
          style="font-family: monospace"
        />
        <div class="editor-meta">字符数：{{ content.length }}</div>
      </div>
      <template #footer>
        <el-button @click="editorVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="doSave">保存</el-button>
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

const activeModules = ref([])
const editorVisible = ref(false)
const loading = ref(false)
const saving = ref(false)
const content = ref('')
const current = reactive({ skill_name: '' })

async function openEditor(row) {
  current.skill_name = row.skill_name
  content.value = ''
  editorVisible.value = true
  loading.value = true
  try {
    const r = await settingsApi.getSkillContent(row.skill_name)
    content.value = r.ok ? r.content : ''
    if (!r.ok) ElMessage.warning(r.error || '文件不存在，将新建')
  } catch {
    content.value = ''
  } finally {
    loading.value = false
  }
}

async function doSave() {
  saving.value = true
  try {
    const r = await settingsApi.saveSkillContent(current.skill_name, content.value)
    if (r.ok) {
      ElMessage.success(`已保存（${r.chars} 字符）`)
      editorVisible.value = false
      emit('refresh')
    } else ElMessage.error(r.error || '保存失败')
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.tip { margin-bottom: 12px; }
.mod-title { font-weight: 600; margin-right: 8px; }
.mod-count { margin-left: 4px; }
.editor-meta { text-align: right; color: #909399; font-size: 12px; margin-top: 6px; }
</style>
