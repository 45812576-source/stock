<template>
  <div class="data-sources">
    <!-- 信息源明细统计 -->
    <el-card shadow="never" class="block-card">
      <template #header>
        <div class="card-header">
          <span>信息源明细</span>
          <el-button link type="primary" @click="loadStats">
            <el-icon><Refresh /></el-icon> 刷新
          </el-button>
        </div>
      </template>
      <el-table :data="sourceDocStats" v-loading="loadingStats" size="small" border>
        <el-table-column prop="source" label="信息源" min-width="140" />
        <el-table-column prop="doc_count" label="文档数" width="100" />
        <el-table-column prop="extracted_count" label="已提取" width="100" />
        <el-table-column prop="pending_count" label="待提取" width="100" />
        <el-table-column prop="latest_date" label="最新日期" width="120" />
        <el-table-column prop="file_types" label="文件类型" min-width="160" />
      </el-table>
    </el-card>

    <!-- Token 配置 -->
    <el-card shadow="never" class="block-card">
      <template #header>Token / 凭据配置</template>
      <el-form label-width="140px" size="default" class="token-form">
        <el-form-item label="知识星球 Cookie">
          <div class="token-row">
            <el-tag v-if="config.zsxq_cookie_set" type="success" size="small">已配置</el-tag>
            <el-tag v-else type="danger" size="small">未配置</el-tag>
            <el-input v-model="tokenForm.zsxq_cookie" type="password" show-password placeholder="粘贴完整 Cookie" style="width: 400px" />
          </div>
        </el-form-item>
        <el-form-item label="星球 Group IDs">
          <el-input v-model="tokenForm.zsxq_group_ids" placeholder="多个用逗号分隔" style="width: 400px" />
        </el-form-item>
        <el-form-item label="新闻有效期(小时)">
          <el-input-number v-model="tokenForm.news_hours" :min="1" :max="168" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="saving" @click="saveTokenConfig">保存凭据</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- 获取规则配置 -->
    <el-card shadow="never" class="block-card">
      <template #header>
        <div class="card-header">
          <span>渠道获取规则</span>
          <div class="header-actions">
            <el-button type="primary" size="small" @click="openAddDialog">新增来源</el-button>
            <el-button link type="primary" @click="loadConfig">
              <el-icon><Refresh /></el-icon> 刷新
            </el-button>
          </div>
        </div>
      </template>
      <div v-for="group in config.source_groups" :key="group.key" class="source-group">
        <div class="group-title">
          <span class="material-symbols-outlined group-icon" :style="{ color: groupColor(group.color) }">{{ group.icon }}</span>
          {{ group.label }}
        </div>
        <el-table :data="group.sources" size="small" border class="group-table">
          <el-table-column label="状态" width="70">
            <template #default="{ row }">
              <el-switch v-model="row.enabled" size="small" @change="quickSave(row)" />
            </template>
          </el-table-column>
          <el-table-column prop="label" label="名称" width="160" />
          <el-table-column prop="desc" label="说明" min-width="240" show-overflow-tooltip />
          <el-table-column prop="fetcher_type" label="采集器" width="120" />
          <el-table-column label="限额" width="100">
            <template #default="{ row }">
              <span v-if="row.limit != null">{{ row.limit }}</span>
              <span v-else-if="row.max_pages != null">{{ row.max_pages }}页</span>
              <span v-else class="text-dim">—</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="120" fixed="right">
            <template #default="{ row }">
              <el-button link type="primary" size="small" @click="openEditDialog(row)">编辑</el-button>
              <el-button link type="danger" size="small" @click="doDelete(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-card>

    <!-- 新增/编辑来源对话框 -->
    <el-dialog v-model="srcDialogVisible" :title="isEditSrc ? '编辑数据来源' : '新增数据来源'" width="520px">
      <el-form :model="srcForm" label-width="100px">
        <el-form-item label="标识 Key" :disabled="isEditSrc">
          <el-input v-model="srcForm.key" :disabled="isEditSrc" placeholder="唯一标识，如 my_source" />
        </el-form-item>
        <el-form-item label="显示名称">
          <el-input v-model="srcForm.label" placeholder="如：我的数据源" />
        </el-form-item>
        <el-form-item label="分组">
          <el-select v-model="srcForm.group" style="width: 100%">
            <el-option label="新闻资讯" value="news" />
            <el-option label="研报数据" value="report" />
            <el-option label="社群舆情" value="community" />
            <el-option label="行情数据" value="market" />
          </el-select>
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="srcForm.desc" placeholder="数据源简要描述" />
        </el-form-item>
        <el-form-item label="采集器类型">
          <el-select v-model="srcForm.fetcher_type" style="width: 100%">
            <el-option label="Jasper (通用新闻)" value="jasper" />
            <el-option label="洞见研报" value="djyanbao" />
            <el-option label="发现报告" value="fxbaogao" />
            <el-option label="东财研报" value="em_report" />
            <el-option label="知识星球" value="zsxq" />
            <el-option label="巨潮公告" value="cninfo_notice" />
            <el-option label="业绩预告" value="earnings" />
            <el-option label="stock_db" value="stock_db" />
          </el-select>
        </el-form-item>
        <el-form-item label="限额">
          <el-input-number v-model="srcForm.limit" :min="0" placeholder="每次采集上限" />
          <span class="form-hint">条数上限（0=不限）</span>
        </el-form-item>
        <el-form-item label="图标">
          <el-input v-model="srcForm.icon" placeholder="Material Symbol 名称" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="srcDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submitSource">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as dataApi from '@/api/data'

// ---------- 信息源统计 ----------
const sourceDocStats = ref([])
const loadingStats = ref(false)

async function loadStats() {
  loadingStats.value = true
  try {
    const ds = await dataApi.getDocStats()
    sourceDocStats.value = ds.source_doc_stats || []
  } finally {
    loadingStats.value = false
  }
}

// ---------- 配置 ----------
const config = ref({ source_groups: [], zsxq_cookie_set: false, zsxq_group_ids: '', news_hours: 24 })
const tokenForm = reactive({ zsxq_cookie: '', zsxq_group_ids: '', news_hours: 24 })
const saving = ref(false)

async function loadConfig() {
  try {
    const res = await dataApi.getSourceConfig()
    config.value = res
    tokenForm.zsxq_group_ids = res.zsxq_group_ids || ''
    tokenForm.news_hours = res.news_hours || 24
  } catch {
    ElMessage.error('加载配置失败')
  }
}

async function saveTokenConfig() {
  saving.value = true
  try {
    const payload = { news_hours: tokenForm.news_hours }
    if (tokenForm.zsxq_cookie) payload.zsxq_cookie = tokenForm.zsxq_cookie
    if (tokenForm.zsxq_group_ids) payload.zsxq_group_ids = tokenForm.zsxq_group_ids
    await dataApi.saveSources(payload)
    ElMessage.success('凭据已保存')
    tokenForm.zsxq_cookie = ''
    loadConfig()
  } finally {
    saving.value = false
  }
}

// 快速保存启用状态
async function quickSave(row) {
  await dataApi.updateSource({ key: row.key, enabled: row.enabled })
}

// ---------- 来源 CRUD ----------
const srcDialogVisible = ref(false)
const isEditSrc = ref(false)
const srcForm = reactive({ key: '', label: '', group: 'news', desc: '', fetcher_type: 'jasper', limit: null, icon: 'article' })

function openAddDialog() {
  isEditSrc.value = false
  Object.assign(srcForm, { key: '', label: '', group: 'news', desc: '', fetcher_type: 'jasper', limit: null, icon: 'article' })
  srcDialogVisible.value = true
}

function openEditDialog(row) {
  isEditSrc.value = true
  Object.assign(srcForm, {
    key: row.key,
    label: row.label,
    group: '', // will be filled from parent group
    desc: row.desc,
    fetcher_type: row.fetcher_type,
    limit: row.limit,
    icon: row.icon || 'article',
  })
  // find group
  for (const g of config.value.source_groups) {
    if (g.sources.some(s => s.key === row.key)) {
      srcForm.group = g.key
      break
    }
  }
  srcDialogVisible.value = true
}

async function submitSource() {
  saving.value = true
  try {
    if (isEditSrc.value) {
      const res = await dataApi.updateSource({
        key: srcForm.key,
        label: srcForm.label,
        group: srcForm.group,
        desc: srcForm.desc,
        fetcher_type: srcForm.fetcher_type,
        limit: srcForm.limit || null,
        icon: srcForm.icon,
      })
      if (res.ok === false) { ElMessage.error(res.msg || '更新失败'); return }
      ElMessage.success('已更新')
    } else {
      if (!srcForm.key || !srcForm.label) { ElMessage.warning('Key 和名称必填'); return }
      const res = await dataApi.addSource({
        key: srcForm.key,
        label: srcForm.label,
        group: srcForm.group,
        desc: srcForm.desc,
        fetcher_type: srcForm.fetcher_type,
        limit: srcForm.limit || null,
        icon: srcForm.icon,
      })
      if (res.ok === false) { ElMessage.error(res.msg || '新增失败'); return }
      ElMessage.success('已新增')
    }
    srcDialogVisible.value = false
    loadConfig()
  } finally {
    saving.value = false
  }
}

async function doDelete(row) {
  await ElMessageBox.confirm(`确定删除来源「${row.label}」？删除后不可恢复。`, '删除确认', { type: 'warning' })
  const res = await dataApi.deleteSource({ key: row.key })
  if (res.ok === false) { ElMessage.error(res.msg || '删除失败'); return }
  ElMessage.success('已删除')
  loadConfig()
}

// ---------- 工具 ----------
function groupColor(color) {
  const map = { blue: '#1677ff', orange: '#fa8c16', purple: '#722ed1', green: '#52c41a' }
  return map[color] || '#666'
}

onMounted(() => {
  loadStats()
  loadConfig()
})
</script>

<style scoped>
.block-card { margin-bottom: 16px; }
.card-header { display: flex; align-items: center; justify-content: space-between; }
.header-actions { display: flex; align-items: center; gap: 12px; }
.token-form { max-width: 700px; }
.token-row { display: flex; align-items: center; gap: 12px; }
.source-group { margin-bottom: 20px; }
.group-title {
  display: flex; align-items: center; gap: 8px;
  font-weight: 600; font-size: 14px; margin-bottom: 8px;
}
.group-icon { font-size: 20px !important; }
.group-table { margin-left: 28px; }
.text-dim { color: var(--admin-text-dim); }
.form-hint { font-size: 12px; color: var(--admin-text-dim); margin-left: 8px; }
</style>
