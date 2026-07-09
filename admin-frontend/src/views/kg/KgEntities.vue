<template>
  <div class="kg-entities-merged">
    <div class="left-panel">
      <div class="panel-title">实体类型（{{ entityTypeList.length }}）</div>
      <div
        v-for="et in entityTypeList"
        :key="et.key"
        class="type-item"
        :class="{ active: filterType === et.key }"
        @click="onTypeClick(et.key)"
      >
        <span class="type-dot" :style="{ background: et.cfg.bg }" />
        <span class="type-label">{{ et.cfg.label }}</span>
        <span class="type-count">{{ (stats.entity_by_type || {})[et.key] || 0 }}</span>
      </div>

      <template v-if="filterType">
        <div class="panel-title mt-12">属性定义</div>
        <div class="prop-section">
          <el-tag v-for="p in commonProps" :key="p" size="small" type="info" class="ptag">{{ p }}</el-tag>
          <template v-if="specificProps.length">
            <el-tag v-for="p in specificProps" :key="p" size="small" class="ptag">{{ p }}</el-tag>
          </template>
        </div>
      </template>

      <div class="panel-title mt-12">关系类型（{{ relationList.length }}）</div>
      <div
        v-for="rt in relationList"
        :key="rt.key"
        class="type-item"
        :class="{ active: filterRelType === rt.key }"
        @click="onRelTypeClick(rt.key)"
      >
        <span class="type-dot" :style="{ background: rt.cfg.color }" />
        <span class="type-label">{{ rt.cfg.label }}</span>
        <span class="type-count">{{ (stats.rel_by_type || {})[rt.key] || 0 }}</span>
      </div>
    </div>

    <div class="right-panel">
      <div class="filter-bar">
        <el-input v-model="keyword" placeholder="搜索实体名称" clearable style="width: 240px" @keyup.enter="load" />
        <el-button type="primary" @click="load">查询</el-button>
        <div class="spacer" />
        <el-button type="success" @click="openAdd">新增实体</el-button>
      </div>

      <!-- 关系列表视图 -->
      <template v-if="filterRelType">
        <div class="filter-bar">
          <el-tag type="warning">关系类型: {{ relLabel(filterRelType) }}</el-tag>
          <span class="rel-total">共 {{ relListTotal }} 条</span>
          <div class="spacer" />
          <el-button size="small" @click="filterRelType = ''">返回实体列表</el-button>
        </div>
        <el-table :data="relList" v-loading="relListLoading" size="small" border>
          <el-table-column label="源实体" min-width="180">
            <template #default="{ row }">
              <span class="dot" :style="{ background: colorOf(row.source_type) }" />
              {{ row.source_name }}
            </template>
          </el-table-column>
          <el-table-column label="关系" width="140">
            <template #default="{ row }">{{ relLabel(row.relation_type) }}</template>
          </el-table-column>
          <el-table-column label="目标实体" min-width="180">
            <template #default="{ row }">
              <span class="dot" :style="{ background: colorOf(row.target_type) }" />
              {{ row.target_name }}
            </template>
          </el-table-column>
          <el-table-column label="操作" width="70">
            <template #default="{ row }">
              <el-button link type="danger" size="small" @click="deleteRelFromList(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          class="pager"
          layout="total, prev, pager, next"
          :total="relListTotal"
          :page-size="100"
          :current-page="relListPage"
          @current-change="onRelPage"
        />
      </template>

      <!-- 实体列表视图 -->
      <template v-else>
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
      </template>
    </div>

    <!-- 实体编辑 Dialog（含关系管理） -->
    <el-dialog v-model="dialogVisible" :title="editId ? `编辑: ${form.entity_name}` : '新增实体'" width="680px" top="5vh">
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
          <el-input v-model="form.description" type="textarea" :rows="2" />
        </el-form-item>
        <el-form-item label="投资逻辑">
          <el-input v-model="form.investment_logic" type="textarea" :rows="2" />
        </el-form-item>
      </el-form>

      <!-- 关系管理区域（仅编辑模式） -->
      <template v-if="editId">
        <el-divider content-position="left">关系（{{ relations.length }}）</el-divider>
        <div class="rel-section">
          <el-table :data="relations" size="small" max-height="200" border>
            <el-table-column label="方向" width="60">
              <template #default="{ row }">
                <el-tag size="small" :type="row._dir === 'out' ? 'warning' : 'success'">
                  {{ row._dir === 'out' ? '→' : '←' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="关系类型" width="120">
              <template #default="{ row }">{{ relLabel(row.relation_type) }}</template>
            </el-table-column>
            <el-table-column label="关联实体" min-width="160">
              <template #default="{ row }">
                <span class="dot" :style="{ background: colorOf(row._other_type) }" />
                {{ row._other_name }}
              </template>
            </el-table-column>
            <el-table-column label="操作" width="70">
              <template #default="{ row }">
                <el-button link type="danger" size="small" @click="removeRelation(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>

          <div class="add-rel-bar">
            <el-select v-model="newRel.relation_type" placeholder="关系类型" style="width: 140px" size="small">
              <el-option v-for="rt in relationList" :key="rt.key" :label="rt.cfg.label" :value="rt.key" />
            </el-select>
            <el-select
              v-model="newRel.target_id"
              placeholder="搜索目标实体"
              filterable
              remote
              :remote-method="searchTarget"
              :loading="targetLoading"
              style="flex: 1"
              size="small"
            >
              <el-option v-for="t in targetOptions" :key="t.id" :label="`${t.entity_name} (${typeLabel(t.entity_type)})`" :value="t.id" />
            </el-select>
            <el-button type="primary" size="small" :disabled="!newRel.relation_type || !newRel.target_id" @click="addRelation">添加</el-button>
          </div>
        </div>
      </template>

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

// ---- Schema 数据 ----
const stats = computed(() => props.meta.stats || {})
const entityTypeList = computed(() =>
  Object.entries(props.meta.entity_colors || {}).map(([key, cfg]) => ({ key, cfg })),
)
const relationList = computed(() =>
  Object.entries(props.meta.relation_labels || {}).map(([key, cfg]) => ({ key, cfg })),
)
const commonProps = ['entity_name', 'description', 'properties_json', 'investment_logic', 'created_at', 'updated_at']
const specificProps = computed(() =>
  filterType.value ? (props.meta.entity_schema?.[filterType.value] || []) : [],
)

function colorOf(t) { return props.meta.entity_colors?.[t]?.bg || '#64748b' }
function typeLabel(t) { return props.meta.entity_colors?.[t]?.label || t }
function relLabel(t) { return props.meta.relation_labels?.[t]?.label || t }

// ---- 实体列表 ----
const list = ref([])
const total = ref(0)
const loading = ref(false)
const filterType = ref('')
const keyword = ref('')
const page = ref(1)
const perPage = 50

function onTypeClick(key) {
  filterType.value = filterType.value === key ? '' : key
  filterRelType.value = ''  // 取消关系类型选中
  page.value = 1
  load()
}

async function load() {
  loading.value = true
  try {
    if (keyword.value) {
      const results = await kgApi.searchEntities(keyword.value, filterType.value)
      list.value = results || []
    } else {
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
  relations.value = []
  Object.assign(form, { entity_type: filterType.value || 'company', entity_name: '', description: '', investment_logic: '' })
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
  // 组装关系列表
  const out = (d.outgoing || []).map(r => ({ ...r, _dir: 'out', _other_name: r.target_name || r.target_entity_name || `#${r.target_entity_id}`, _other_type: r.target_type || '' }))
  const inc = (d.incoming || []).map(r => ({ ...r, _dir: 'in', _other_name: r.source_name || r.source_entity_name || `#${r.source_entity_id}`, _other_type: r.source_type || '' }))
  relations.value = [...out, ...inc]
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

// ---- 关系列表（按类型浏览） ----
const filterRelType = ref('')
const relList = ref([])
const relListTotal = ref(0)
const relListLoading = ref(false)
const relListPage = ref(1)

function onRelTypeClick(key) {
  filterRelType.value = filterRelType.value === key ? '' : key
  filterType.value = ''  // 取消实体类型选中
  if (filterRelType.value) {
    relListPage.value = 1
    loadRelList()
  }
}

async function loadRelList() {
  relListLoading.value = true
  try {
    const offset = (relListPage.value - 1) * 100
    const data = await kgApi.listRelationships(filterRelType.value, 100, offset)
    relList.value = data.items || []
    relListTotal.value = data.total || 0
  } finally {
    relListLoading.value = false
  }
}
function onRelPage(p) { relListPage.value = p; loadRelList() }

async function deleteRelFromList(row) {
  await ElMessageBox.confirm(`确定删除「${row.source_name} → ${row.target_name}」的关系？`, '确认', { type: 'warning' })
  await kgApi.deleteRelationship(row.id)
  ElMessage.success('关系已删除')
  loadRelList()
}

// ---- 关系管理 ----
const relations = ref([])
const newRel = reactive({ relation_type: '', target_id: null })
const targetOptions = ref([])
const targetLoading = ref(false)

async function searchTarget(q) {
  if (!q || q.length < 1) { targetOptions.value = []; return }
  targetLoading.value = true
  try {
    const results = await kgApi.searchEntities(q, '')
    targetOptions.value = (results || []).filter(e => e.id !== editId.value)
  } finally {
    targetLoading.value = false
  }
}

async function addRelation() {
  if (!newRel.relation_type || !newRel.target_id) return
  try {
    await kgApi.addRelationship({
      source_entity_id: editId.value,
      target_entity_id: newRel.target_id,
      relation_type: newRel.relation_type,
    })
    ElMessage.success('关系已添加')
    // 刷新关系列表
    const d = await kgApi.getEntityDetail(editId.value)
    const out = (d.outgoing || []).map(r => ({ ...r, _dir: 'out', _other_name: r.target_name || r.target_entity_name || `#${r.target_entity_id}`, _other_type: r.target_type || '' }))
    const inc = (d.incoming || []).map(r => ({ ...r, _dir: 'in', _other_name: r.source_name || r.source_entity_name || `#${r.source_entity_id}`, _other_type: r.source_type || '' }))
    relations.value = [...out, ...inc]
    newRel.relation_type = ''
    newRel.target_id = null
  } catch (e) {
    ElMessage.error('添加关系失败')
  }
}

async function removeRelation(row) {
  await ElMessageBox.confirm(`确定删除与「${row._other_name}」的关系？`, '确认', { type: 'warning' })
  await kgApi.deleteRelationship(row.id)
  relations.value = relations.value.filter(r => r.id !== row.id)
  ElMessage.success('关系已删除')
}

onMounted(load)
</script>

<style scoped>
.kg-entities-merged {
  display: flex;
  gap: 16px;
  min-height: 500px;
}
.left-panel {
  width: 240px;
  flex-shrink: 0;
  border-right: 1px solid var(--divider, #f0f0f0);
  padding-right: 16px;
  max-height: 70vh;
  overflow-y: auto;
}
.right-panel {
  flex: 1;
  min-width: 0;
}
.panel-title {
  font-size: 12px;
  font-weight: 700;
  color: var(--text-tertiary, #909399);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  margin-bottom: 8px;
}
.mt-12 { margin-top: 12px; }
.type-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.15s;
}
.type-item:hover { background: var(--secondary-bg, #fafafa); }
.type-item.active { background: rgba(22, 119, 255, 0.08); }
.type-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
.type-label { font-size: 13px; font-weight: 500; color: var(--text-main); }
.type-count { margin-left: auto; font-size: 12px; font-weight: 700; font-variant-numeric: tabular-nums; color: var(--text-main); }
.prop-section { padding: 4px 10px; }
.ptag { margin: 0 4px 4px 0; }
.filter-bar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }
.filter-bar .spacer { flex: 1; }
.dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 8px; }
.ename { font-weight: 600; }
.pager { margin-top: 14px; justify-content: flex-end; }
.rel-section { margin-top: 8px; }
.rel-total { font-size: 13px; color: var(--text-tertiary, #909399); margin-left: 8px; }
.add-rel-bar {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-top: 10px;
}
</style>
