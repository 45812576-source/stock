<template>
  <div class="kg-entities">
    <div class="filter-bar">
      <el-select v-model="filterType" placeholder="实体类型" clearable style="width: 160px" @change="load">
        <el-option v-for="et in entityTypeList" :key="et.key" :label="et.cfg.label" :value="et.key" />
      </el-select>
      <el-input v-model="keyword" placeholder="搜索实体名称" clearable style="width: 220px" @keyup.enter="load" />
      <el-button type="primary" @click="load">查询</el-button>
      <div class="spacer" />
      <el-button type="success" @click="openAdd">新增实体</el-button>
    </div>

    <el-table :data="list" v-loading="loading" size="small" border>
      <el-table-column label="实体" min-width="200">
        <template #default="{ row }">
          <span class="dot" :style="{ background: colorOf(row.entity_type) }" />
          <span class="ename">{{ row.entity_name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="类型" width="130">
        <template #default="{ row }">
          <el-tag size="small" :style="{ color: colorOf(row.entity_type), borderColor: colorOf(row.entity_type) }">
            {{ typeLabel(row.entity_type) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="description" label="描述" min-width="240" show-overflow-tooltip />
      <el-table-column label="操作" width="120" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openEdit(row)">编辑</el-button>
          <el-button link type="danger" @click="doDelete(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="total"
      :page-size="perPage"
      :current-page="page"
      @current-change="onPage"
    />

    <el-dialog v-model="dialogVisible" :title="editId ? '编辑实体' : '新增实体'" width="520px">
      <el-form :model="form" label-width="90px">
        <el-form-item label="实体类型">
          <el-select v-model="form.entity_type" :disabled="!!editId" style="width: 100%">
            <el-option v-for="et in entityTypeList" :key="et.key" :label="et.cfg.label" :value="et.key" />
          </el-select>
        </el-form-item>
        <el-form-item label="实体名称">
          <el-input v-model="form.entity_name" :disabled="!!editId" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" type="textarea" :rows="3" />
        </el-form-item>
        <el-form-item label="投资逻辑">
          <el-input v-model="form.investment_logic" type="textarea" :rows="3" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as kgApi from '@/api/kg'

const props = defineProps({ meta: { type: Object, required: true } })

const entityTypeList = computed(() =>
  Object.entries(props.meta.entity_colors || {}).map(([key, cfg]) => ({ key, cfg })),
)
function colorOf(t) { return props.meta.entity_colors?.[t]?.bg || '#64748b' }
function typeLabel(t) { return props.meta.entity_colors?.[t]?.label || t }

const list = ref([])
const total = ref(0)
const loading = ref(false)
const filterType = ref('')
const keyword = ref('')
const page = ref(1)
const perPage = 50

async function load() {
  loading.value = true
  try {
    if (keyword.value) {
      const results = await kgApi.searchEntities(keyword.value, filterType.value)
      list.value = results || []
    } else {
      // 无独立实体分页 JSON API：空搜索时回退用 graph-data 节点浏览（label=名称, group=类型）
      const data = await kgApi.getGraphData(0, 1).catch(() => ({ nodes: [] }))
      let nodes = (data.nodes || []).map((n) => ({
        id: n.id, entity_name: n.label, entity_type: n.group, description: '',
      }))
      if (filterType.value) nodes = nodes.filter((n) => n.entity_type === filterType.value)
      list.value = nodes
    }
    total.value = list.value.length
  } finally {
    loading.value = false
  }
}
function onPage(p) { page.value = p; load() }

// ---- 增改删 ----
const dialogVisible = ref(false)
const editId = ref(null)
const form = reactive({ entity_type: 'company', entity_name: '', description: '', investment_logic: '' })

function openAdd() {
  editId.value = null
  Object.assign(form, { entity_type: 'company', entity_name: '', description: '', investment_logic: '' })
  dialogVisible.value = true
}
async function openEdit(row) {
  editId.value = row.id
  const d = await kgApi.getEntityDetail(row.id)
  Object.assign(form, {
    entity_type: d.entity.entity_type,
    entity_name: d.entity.entity_name,
    description: d.entity.description || '',
    investment_logic: d.entity.investment_logic || '',
  })
  dialogVisible.value = true
}
async function save() {
  if (!form.entity_name) { ElMessage.warning('请输入实体名称'); return }
  if (editId.value) {
    await kgApi.updateEntity(editId.value, {
      description: form.description || null,
      investment_logic: form.investment_logic || null,
    })
    ElMessage.success('更新成功')
  } else {
    await kgApi.addEntity({
      entity_type: form.entity_type,
      entity_name: form.entity_name,
      description: form.description || null,
      investment_logic: form.investment_logic || null,
    })
    ElMessage.success('创建成功')
  }
  dialogVisible.value = false
  load()
}
async function doDelete(row) {
  await ElMessageBox.confirm(`确定删除实体「${row.entity_name}」及其所有关系？`, '确认删除', { type: 'warning' })
  await kgApi.deleteEntity(row.id)
  ElMessage.success('已删除')
  load()
}

onMounted(load)
</script>

<style scoped>
.filter-bar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }
.filter-bar .spacer { flex: 1; }
.dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px; }
.ename { font-weight: 600; }
.pager { margin-top: 14px; justify-content: flex-end; }
</style>
