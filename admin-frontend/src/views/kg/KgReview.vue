<template>
  <div class="kg-review">
    <div class="stat-cards">
      <div class="stat-card"><div class="sc-num warn">{{ stats.pending || 0 }}</div><div class="sc-label">待审批</div></div>
      <div class="stat-card"><div class="sc-num ok">{{ stats.approved || 0 }}</div><div class="sc-label">已批准</div></div>
      <div class="stat-card"><div class="sc-num danger">{{ stats.rejected || 0 }}</div><div class="sc-label">已驳回</div></div>
      <div class="stat-card"><div class="sc-num">{{ stats.today_reviewed || 0 }}</div><div class="sc-label">今日审核</div></div>
    </div>

    <div class="filter-bar">
      <el-select v-model="filters.target_type" size="small" style="width: 120px" @change="reload">
        <el-option label="全部对象" value="all" />
        <el-option label="实体" value="entity" />
        <el-option label="关系" value="relationship" />
        <el-option label="佐证来源" value="triple_source" />
      </el-select>
      <el-select v-if="filters.target_type === 'entity'" v-model="filters.entity_type" size="small" placeholder="全部实体类型" clearable style="width: 130px" @change="reload">
        <el-option v-for="(cfg, key) in (meta.entity_colors || {})" :key="key" :label="cfg.label" :value="key" />
      </el-select>
      <el-select v-if="filters.target_type === 'relationship'" v-model="filters.relation_type" size="small" placeholder="全部关系类型" clearable style="width: 140px" @change="reload">
        <el-option v-for="(cfg, key) in (meta.relation_labels || {})" :key="key" :label="cfg.label" :value="key" />
      </el-select>
      <el-select v-model="filters.status" size="small" style="width: 130px" @change="reload">
        <el-option label="全部状态" value="all" />
        <el-option label="待审批" value="pending_approval" />
        <el-option label="未审核" value="unreviewed" />
        <el-option label="已批准" value="approved" />
        <el-option label="已驳回" value="rejected" />
      </el-select>
      <el-input v-model="filters.keyword" placeholder="搜索名称/关系" clearable size="small" style="width: 200px" @keyup.enter="reload" />
      <el-button type="primary" size="small" @click="reload">查询</el-button>
      <div class="spacer" />
      <el-button type="success" size="small" :disabled="!multipleSelection.length" @click="batchReview('approve')">批量批准</el-button>
      <el-button type="danger" size="small" :disabled="!multipleSelection.length" @click="batchReview('reject')">批量驳回</el-button>
    </div>

    <el-table :data="list" v-loading="loading" size="small" border @selection-change="onSelect">
      <el-table-column type="selection" width="42" />
      <el-table-column label="对象" min-width="260">
        <template #default="{ row }">
          <template v-if="row.target_type === 'entity'">
            <el-tag size="small" effect="plain" :color="colorOf(row.entity_type)" style="color:#fff; border:none;">{{ entityLabel(row.entity_type) }}</el-tag>
            <span style="margin-left:6px;">{{ row.entity_name }}</span>
          </template>
          <template v-else-if="row.target_type === 'relationship'">
            <el-tag size="small" effect="plain" :color="relColor(row.relation_type)" style="color:#fff; border:none;">{{ relLabel(row.relation_type) }}</el-tag>
            <span style="margin-left:6px;">{{ row.src_name }} → {{ row.tgt_name }}</span>
          </template>
          <template v-else>
            <el-tag size="small" effect="plain">佐证</el-tag>
            <span style="margin-left:6px;">{{ row.src_name || row.entity_name }}</span>
          </template>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag size="small" :type="statusType(row.review_status)">{{ statusLabel(row.review_status) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="review_note" label="备注" min-width="160" show-overflow-tooltip />
      <el-table-column label="操作" width="200" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openDetail(row)">详情</el-button>
          <el-button link type="success" @click="doReview(row, 'approve')">批准</el-button>
          <el-button link type="danger" @click="doReview(row, 'reject')">驳回</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      class="pager"
      layout="total, prev, pager, next"
      :total="total"
      :page-size="pageSize"
      :current-page="page"
      @current-change="onPage"
    />

    <el-drawer v-model="drawerVisible" title="审核详情" size="480px">
      <div v-if="detail" class="detail-body">
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="对象类型">{{ typeName(curRow?.target_type) }}</el-descriptions-item>
          <el-descriptions-item label="状态">{{ statusLabel(curRow?.review_status) }}</el-descriptions-item>
          <el-descriptions-item v-if="curRow?.entity_name" label="实体名">{{ curRow.entity_name }}</el-descriptions-item>
          <el-descriptions-item v-if="curRow?.relation_type" label="关系">
            {{ curRow.src_name }} —[{{ curRow.relation_type }}]→ {{ curRow.tgt_name }}
          </el-descriptions-item>
          <el-descriptions-item v-if="curRow?.description" label="描述">{{ curRow.description }}</el-descriptions-item>
          <el-descriptions-item v-if="curRow?.evidence" label="证据">{{ curRow.evidence }}</el-descriptions-item>
        </el-descriptions>

        <div v-if="detail.log?.length" class="log-section">
          <div class="ls-title">审核记录</div>
          <el-timeline>
            <el-timeline-item v-for="lg in detail.log" :key="lg.id" :timestamp="lg.created_at" size="small">
              {{ lg.action }} · {{ lg.note || '—' }}
            </el-timeline-item>
          </el-timeline>
        </div>

        <div class="drawer-actions">
          <el-input v-model="reviewNote" placeholder="审核备注（可选）" size="small" />
          <div class="da-btns">
            <el-button type="success" @click="doReview(curRow, 'approve')">批准</el-button>
            <el-button type="danger" @click="doReview(curRow, 'reject')">驳回</el-button>
            <el-button @click="doReview(curRow, 'mark-pending')">标记待审</el-button>
          </div>
        </div>
      </div>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as kgApi from '@/api/kg'

const props = defineProps({ meta: { type: Object, required: true } })
const meta = computed(() => props.meta || {})
function colorOf(t) { return props.meta.entity_colors?.[t]?.bg || '#64748b' }
function entityLabel(t) { return props.meta.entity_colors?.[t]?.label || t }
function relLabel(t) { return props.meta.relation_labels?.[t]?.label || t }
function relColor(t) { return props.meta.relation_labels?.[t]?.color || '#64748b' }

const TYPE_NAMES = { entity: '实体', relationship: '关系', triple_source: '佐证' }
function typeName(t) { return TYPE_NAMES[t] || t }
const STATUS_LABELS = { pending_approval: '待审批', unreviewed: '未审核', approved: '已批准', rejected: '已驳回' }
function statusLabel(s) { return STATUS_LABELS[s] || s }
function statusType(s) {
  return { pending_approval: 'warning', approved: 'success', rejected: 'danger', unreviewed: 'info' }[s] || 'info'
}

const stats = ref({})
const list = ref([])
const total = ref(0)
const loading = ref(false)
const page = ref(1)
const pageSize = 50
const filters = reactive({ target_type: 'all', status: 'all', keyword: '', entity_type: '', relation_type: '' })
const multipleSelection = ref([])

async function loadStats() {
  try { stats.value = await kgApi.getReviewStats() } catch (e) { /* ignore */ }
}

async function load() {
  loading.value = true
  try {
    const res = await kgApi.getReviewQueue({
      target_type: filters.target_type,
      status: filters.status,
      entity_type: filters.entity_type,
      relation_type: filters.relation_type,
      keyword: filters.keyword,
      limit: pageSize,
      offset: (page.value - 1) * pageSize,
    })
    list.value = res.items || []
    total.value = res.total || 0
  } finally {
    loading.value = false
  }
}
function reload() { page.value = 1; load() }
function onPage(p) { page.value = p; load() }
function onSelect(rows) { multipleSelection.value = rows }

// ---- 详情 ----
const drawerVisible = ref(false)
const detail = ref(null)
const curRow = ref(null)
const reviewNote = ref('')

async function openDetail(row) {
  curRow.value = row
  reviewNote.value = ''
  drawerVisible.value = true
  detail.value = null
  detail.value = await kgApi.getReviewDetail(row.target_type, row.id)
}

async function doReview(row, action) {
  if (!row) return
  const payload = { target_type: row.target_type, target_id: row.id, note: reviewNote.value }
  const fn = { approve: kgApi.reviewApprove, reject: kgApi.reviewReject, 'mark-pending': kgApi.reviewMarkPending }[action]
  await fn(payload)
  ElMessage.success('操作成功')
  drawerVisible.value = false
  loadStats()
  load()
}

async function batchReview(action) {
  await ElMessageBox.confirm(`确定${action === 'approve' ? '批准' : '驳回'}选中的 ${multipleSelection.value.length} 项？`, '批量审核', { type: 'warning' })
  const items = multipleSelection.value.map((r) => ({ target_type: r.target_type, target_id: r.id }))
  await kgApi.reviewBatch({ items, action })
  ElMessage.success('批量操作完成')
  loadStats()
  load()
}

onMounted(() => { loadStats(); load() })
</script>

<style scoped>
.stat-cards { display: flex; gap: 14px; margin-bottom: 16px; }
.stat-card {
  flex: 1;
  background: #ffffff;
  border: 1px solid var(--divider, #f0f0f0);
  border-radius: 8px;
  padding: 16px 20px;
  text-align: center;
  transition: box-shadow 0.2s ease;
}
.stat-card:hover { box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06); }
.sc-num { font-size: 26px; font-weight: 700; font-variant-numeric: tabular-nums; color: var(--text-main); }
.sc-num.warn { color: var(--warning, #faad14); }
.sc-num.ok { color: var(--success, #52c41a); }
.sc-num.danger { color: var(--error, #ff4d4f); }
.sc-label { font-size: 12px; color: var(--text-tertiary); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.04em; }
.filter-bar { display: flex; gap: 12px; align-items: center; margin-bottom: 14px; }
.filter-bar .spacer { flex: 1; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin: 0 6px; }
.rel { color: var(--text-tertiary); font-size: 12px; }
.pager { margin-top: 14px; justify-content: flex-end; }
.detail-body { display: flex; flex-direction: column; gap: 16px; }
.log-section .ls-title { font-weight: 600; margin-bottom: 8px; color: var(--text-main); }
.drawer-actions { display: flex; flex-direction: column; gap: 10px; }
.da-btns { display: flex; gap: 8px; }
</style>
