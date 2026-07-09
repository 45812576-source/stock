<template>
  <div class="kg-vis" v-loading="loading">
    <div class="vis-toolbar">
      <el-input
        v-model="searchQ"
        placeholder="搜索实体聚焦"
        clearable
        style="width: 220px"
        @keyup.enter="doSearch"
      >
        <template #append>
          <el-button @click="doSearch">搜索</el-button>
        </template>
      </el-input>
      <span class="depth-label">深度</span>
      <el-slider v-model="depth" :min="1" :max="3" :step="1" style="width: 120px" @change="reload" />
      <el-button @click="reload">重新布局</el-button>
      <el-button @click="fit">适配视图</el-button>
      <div class="spacer" />
      <span class="hint">节点数：{{ nodeCount }} · 边数：{{ edgeCount }}</span>
    </div>

    <div class="vis-body">
      <div ref="canvasRef" class="vis-canvas" />

      <transition name="panel-slide">
        <div v-if="detail" class="detail-panel">
          <div class="panel-header">
            <span class="panel-title">{{ detail.entity.entity_name }}</span>
            <div class="panel-actions">
              <el-button text size="small" @click="enterEdit" v-if="!editing">
                <span class="material-symbols-outlined" style="font-size:16px">edit</span>
              </el-button>
              <el-button text size="small" @click="detail = null; editing = false">
                <span class="material-symbols-outlined" style="font-size:18px">close</span>
              </el-button>
            </div>
          </div>

          <!-- 查看模式 -->
          <template v-if="!editing">
            <div class="panel-meta">
              <span class="meta-tag">{{ typeLabel(detail.entity.entity_type) }}</span>
              <span class="meta-stat">{{ detail.degree }} 关系</span>
              <span v-if="detail.entity.review_status" class="meta-status" :class="detail.entity.review_status">
                {{ statusLabel(detail.entity.review_status) }}
              </span>
            </div>
            <p v-if="detail.entity.description" class="panel-desc">{{ detail.entity.description }}</p>
            <div v-if="detail.entity.investment_logic" class="detail-logic">
              <div class="detail-sub">投资逻辑</div>
              {{ detail.entity.investment_logic }}
            </div>
            <div class="detail-sub">关系列表</div>
            <div class="rel-list">
              <div v-for="r in detail.outgoing" :key="'o' + r.id" class="rel-line">
                <span class="arrow out">→</span>
                <span class="rel-type">{{ relLabel(r.relation_type) }}</span>
                <b>{{ r.target_name || '#' + r.target_entity_id }}</b>
                <el-button link size="small" class="rel-edit-btn" @click="editRel(r, 'outgoing')">
                  <span class="material-symbols-outlined" style="font-size:14px">edit</span>
                </el-button>
              </div>
              <div v-for="r in detail.incoming" :key="'i' + r.id" class="rel-line">
                <span class="arrow in">←</span>
                <span class="rel-type">{{ relLabel(r.relation_type) }}</span>
                <b>{{ r.source_name || '#' + r.source_entity_id }}</b>
                <el-button link size="small" class="rel-edit-btn" @click="editRel(r, 'incoming')">
                  <span class="material-symbols-outlined" style="font-size:14px">edit</span>
                </el-button>
              </div>
            </div>
          </template>

          <!-- 编辑模式 -->
          <template v-else>
            <div class="edit-form">
              <div class="ef-row">
                <label>实体名</label>
                <el-input v-model="editForm.entity_name" size="small" />
              </div>
              <div class="ef-row">
                <label>类型</label>
                <el-select v-model="editForm.entity_type" size="small" style="width:100%">
                  <el-option v-for="(cfg, key) in props.meta.entity_colors" :key="key" :label="cfg.label || key" :value="key" />
                </el-select>
              </div>
              <div class="ef-row">
                <label>描述</label>
                <el-input v-model="editForm.description" type="textarea" :rows="3" size="small" />
              </div>
              <div class="ef-row">
                <label>备注</label>
                <el-input v-model="editForm.note" size="small" placeholder="修改原因（可选）" />
              </div>
              <div class="ef-btns">
                <el-button size="small" @click="editing = false">取消</el-button>
                <el-button type="primary" size="small" :loading="saving" @click="saveEntity">保存并提审</el-button>
              </div>
            </div>
          </template>

          <!-- 审核操作栏 -->
          <div v-if="!editing" class="review-bar">
            <el-input v-model="reviewNote" size="small" placeholder="审核备注" />
            <div class="rb-btns">
              <el-button type="success" size="small" @click="doEntityReview('approve')">批准</el-button>
              <el-button type="danger" size="small" @click="doEntityReview('reject')">驳回</el-button>
            </div>
          </div>
        </div>
      </transition>

      <!-- 关系编辑弹窗 -->
      <el-dialog v-model="relEditVisible" title="编辑关系" width="420px">
        <div class="ef-row">
          <label>关系类型</label>
          <el-select v-model="relForm.relation_type" size="small" style="width:100%" filterable allow-create>
            <el-option v-for="(cfg, key) in (props.meta.relation_labels || {})" :key="key" :label="cfg.label || key" :value="key" />
          </el-select>
        </div>
        <div class="ef-row">
          <label>强度 (0-1)</label>
          <el-input-number v-model="relForm.strength" :min="0" :max="1" :step="0.1" size="small" />
        </div>
        <div class="ef-row">
          <label>编辑备注</label>
          <el-input v-model="relForm.note" size="small" placeholder="修改原因（可选）" />
        </div>
        <template #footer>
          <el-button @click="relEditVisible = false">取消</el-button>
          <el-button type="primary" :loading="saving" @click="saveRelationship">保存并提审</el-button>
        </template>
      </el-dialog>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, shallowRef } from 'vue'
import { Network, DataSet } from 'vis-network/standalone'
import { ElMessage } from 'element-plus'
import * as kgApi from '@/api/kg'

const props = defineProps({ meta: { type: Object, required: true } })

const canvasRef = ref(null)
const loading = ref(false)
const depth = ref(2)
const searchQ = ref('')
const nodeCount = ref(0)
const edgeCount = ref(0)
const detailVisible = ref(false) // kept for compat but panel uses detail directly
const detail = ref(null)
const network = shallowRef(null)
const nodesDS = shallowRef(null)
const edgesDS = shallowRef(null)

// 原始样式存储（用于重置）
const originalStyles = new Map()

const SHAPE_MAP = {
  market: 'hexagon', theme: 'star', industry: 'dot', industry_chain: 'dot',
  company: 'diamond', macro_indicator: 'triangle', commodity: 'square',
  energy: 'triangleDown', intermediate: 'square', consumer_good: 'dot', policy: 'triangle',
}

function typeLabel(t) { return props.meta.entity_colors?.[t]?.label || t }
function relLabel(t) { return props.meta.relation_labels?.[t]?.label || t }

function buildGroups() {
  const groups = {}
  for (const [etype, cfg] of Object.entries(props.meta.entity_colors || {})) {
    groups[etype] = {
      color: { background: cfg.bg, border: cfg.bg, highlight: { background: cfg.bg, border: '#0057c2' } },
      shape: SHAPE_MAP[etype] || 'dot',
      font: { color: 'rgba(0,0,0,0.88)', size: 13, face: 'Inter' },
      borderWidth: 2,
    }
  }
  return groups
}

async function loadGraph(centerId = 0) {
  loading.value = true
  try {
    const data = await kgApi.getGraphData(centerId, depth.value)
    nodeCount.value = (data.nodes || []).length
    edgeCount.value = (data.edges || []).length
    render(data)
  } finally {
    loading.value = false
  }
}

function render(data) {
  if (!canvasRef.value) return

  // 边上显示关系名称
  const edges = (data.edges || []).map(e => ({
    ...e,
    label: e.label || relLabel(e.relation_type || ''),
    font: { size: 11, color: 'rgba(0,0,0,0.65)', strokeWidth: 3, strokeColor: '#ffffff', face: 'Inter', align: 'middle' },
    color: { color: '#b0b8c4', highlight: '#1677ff', hover: '#1677ff', opacity: 0.6 },
    width: 1.5,
    smooth: { type: 'continuous' },
  }))

  const nodes = (data.nodes || []).map(n => ({
    ...n,
    font: { color: 'rgba(0,0,0,0.88)', size: 13, face: 'Inter' },
  }))

  nodesDS.value = new DataSet(nodes)
  edgesDS.value = new DataSet(edges)

  // 存储原始样式
  originalStyles.clear()
  nodes.forEach(n => originalStyles.set('n_' + n.id, { opacity: 1, font: n.font }))
  edges.forEach(e => originalStyles.set('e_' + e.id, { opacity: 0.6, width: 1.5, font: e.font, color: e.color }))

  const options = {
    groups: buildGroups(),
    layout: { improvedLayout: true },
    physics: {
      solver: 'forceAtlas2Based',
      forceAtlas2Based: { gravitationalConstant: -100, springLength: 180, springConstant: 0.03, damping: 0.3 },
      stabilization: { iterations: 100 },
    },
    interaction: { hover: true, tooltipDelay: 100, selectConnectedEdges: true, multiselect: false },
    nodes: { borderWidth: 2, size: 18 },
    edges: {
      arrows: { to: { enabled: true, scaleFactor: 0.5 } },
    },
  }

  if (network.value) network.value.destroy()
  network.value = new Network(canvasRef.value, { nodes: nodesDS.value, edges: edgesDS.value }, options)

  // 点击节点：高亮邻居 + 淡化其他
  network.value.on('click', (params) => {
    if (params.nodes.length > 0) {
      highlightNeighbors(params.nodes[0])
      showDetail(params.nodes[0])
    } else {
      resetHighlight()
    }
  })

  // 点击画布空白处重置
  network.value.on('deselectNode', () => {
    resetHighlight()
  })
}

// 高亮策略：BFS N 跳（按 depth 滑块），所有层级节点+边全量展示名称，自动 zoom
function highlightNeighbors(nodeId) {
  if (!network.value || !nodesDS.value || !edgesDS.value) return

  // BFS 收集 N 跳内的所有节点，并记录每个节点的跳数
  const hops = depth.value  // 用户选的深度就是高亮跳数
  const visited = new Map()  // nodeId -> hop level
  visited.set(nodeId, 0)
  let frontier = [nodeId]

  for (let hop = 1; hop <= hops; hop++) {
    const nextFrontier = []
    for (const nid of frontier) {
      const neighbors = network.value.getConnectedNodes(nid)
      for (const nb of neighbors) {
        if (!visited.has(nb)) {
          visited.set(nb, hop)
          nextFrontier.push(nb)
        }
      }
    }
    frontier = nextFrontier
    if (frontier.length === 0) break
  }

  // 收集高亮节点之间的所有边
  const highlightedNodes = new Set(visited.keys())
  const highlightedEdges = new Set()
  edgesDS.value.forEach(edge => {
    if (highlightedNodes.has(edge.from) && highlightedNodes.has(edge.to)) {
      highlightedEdges.add(edge.id)
    }
  })

  // 节点样式：中心节点最大，逐级递减字号
  const fontSizes = [16, 14, 13, 12]  // hop 0/1/2/3
  const nodeUpdates = nodesDS.value.getIds().map(id => {
    if (visited.has(id)) {
      const hop = visited.get(id)
      const sz = fontSizes[Math.min(hop, fontSizes.length - 1)]
      return {
        id,
        opacity: 1,
        font: { color: 'rgba(0,0,0,0.88)', size: sz, bold: hop === 0, face: 'Inter' },
      }
    }
    return { id, opacity: 0.08, font: { color: 'transparent', size: 0, face: 'Inter' } }
  })
  nodesDS.value.update(nodeUpdates)

  // 边样式：高亮范围内的边显示关系名，其余隐形
  const edgeUpdates = edgesDS.value.getIds().map(id => {
    if (highlightedEdges.has(id)) {
      return {
        id,
        width: 2,
        color: { color: '#1677ff', highlight: '#1677ff', opacity: 0.9 },
        font: { size: 12, color: '#1677ff', strokeWidth: 4, strokeColor: '#ffffff', bold: true, face: 'Inter', align: 'middle' },
      }
    }
    return {
      id,
      width: 0.3,
      color: { color: '#e5e5e5', opacity: 0.08 },
      font: { size: 0, color: 'transparent' },
    }
  })
  edgesDS.value.update(edgeUpdates)

  // 自动 zoom-fit 到高亮子图，留 padding 让文字不被裁
  const fitNodes = Array.from(highlightedNodes)
  network.value.fit({ nodes: fitNodes, animation: { duration: 400, easingFunction: 'easeInOutQuad' } })
}

// 重置高亮
function resetHighlight() {
  if (!nodesDS.value || !edgesDS.value) return

  const nodeUpdates = nodesDS.value.getIds().map(id => ({
    id,
    opacity: 1,
    font: { color: 'rgba(0,0,0,0.88)', size: 13, face: 'Inter' },
  }))
  nodesDS.value.update(nodeUpdates)

  const edgeUpdates = edgesDS.value.getIds().map(id => ({
    id,
    width: 1.5,
    color: { color: '#b0b8c4', highlight: '#1677ff', hover: '#1677ff', opacity: 0.6 },
    font: { size: 11, color: 'rgba(0,0,0,0.65)', strokeWidth: 3, strokeColor: '#ffffff', face: 'Inter', align: 'middle' },
  }))
  edgesDS.value.update(edgeUpdates)
}

async function showDetail(entityId) {
  try {
    detail.value = await kgApi.getEntityDetail(entityId)
    editing.value = false
  } catch (e) { /* ignore */ }
}

// ---- 编辑 & 审核联动 ----
const editing = ref(false)
const saving = ref(false)
const reviewNote = ref('')
const editForm = reactive({ entity_name: '', entity_type: '', description: '', note: '' })
const relEditVisible = ref(false)
const relForm = reactive({ rel_id: null, relation_type: '', strength: 0.5, note: '' })

const STATUS_LABELS = { pending_approval: '待审批', unreviewed: '未审核', approved: '已批准', rejected: '已驳回' }
function statusLabel(s) { return STATUS_LABELS[s] || s }

function enterEdit() {
  if (!detail.value) return
  const e = detail.value.entity
  editForm.entity_name = e.entity_name || ''
  editForm.entity_type = e.entity_type || ''
  editForm.description = e.description || ''
  editForm.note = ''
  editing.value = true
}

async function saveEntity() {
  saving.value = true
  try {
    const res = await kgApi.editEntityReview({
      entity_id: detail.value.entity.id,
      new_name: editForm.entity_name,
      new_type: editForm.entity_type,
      new_description: editForm.description,
      note: editForm.note,
    })
    if (res.ok) {
      ElMessage.success('已保存，状态转为待审批')
      editing.value = false
      // 刷新详情
      showDetail(detail.value.entity.id)
    } else {
      ElMessage.error(res.error || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

function editRel(r, direction) {
  relForm.rel_id = r.id
  relForm.relation_type = r.relation_type || ''
  relForm.strength = r.strength ?? 0.5
  relForm.note = ''
  relEditVisible.value = true
}

async function saveRelationship() {
  saving.value = true
  try {
    const res = await kgApi.editRelationshipReview({
      rel_id: relForm.rel_id,
      new_relation_type: relForm.relation_type,
      new_strength: relForm.strength,
      note: relForm.note,
    })
    if (res.ok) {
      ElMessage.success('关系已修改，状态转为待审批')
      relEditVisible.value = false
      showDetail(detail.value.entity.id)
    } else {
      ElMessage.error(res.error || '保存失败')
    }
  } catch (e) {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

async function doEntityReview(action) {
  if (!detail.value) return
  const payload = {
    target_type: 'entity',
    target_id: detail.value.entity.id,
    note: reviewNote.value,
  }
  try {
    const fn = action === 'approve' ? kgApi.reviewApprove : kgApi.reviewReject
    await fn(payload)
    ElMessage.success(action === 'approve' ? '已批准' : '已驳回')
    reviewNote.value = ''
    showDetail(detail.value.entity.id)
  } catch (e) {
    ElMessage.error('操作失败')
  }
}

async function doSearch() {
  if (!searchQ.value) return
  const results = await kgApi.searchEntities(searchQ.value)
  if (results.length && network.value) {
    const id = results[0].id
    network.value.focus(id, { scale: 1.5, animation: true })
    network.value.selectNodes([id])
    highlightNeighbors(id)
    showDetail(id)
  }
}

function reload() { loadGraph(0) }
function fit() { if (network.value) network.value.fit() }

onMounted(() => loadGraph(0))
onBeforeUnmount(() => { if (network.value) network.value.destroy() })
</script>

<style scoped>
.kg-vis { display: flex; flex-direction: column; height: 100%; }
.vis-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
  flex-wrap: wrap;
  padding: 10px 16px;
  background: #fff;
  border-radius: 8px;
  border: 1px solid var(--divider, #f0f0f0);
}
.vis-toolbar .depth-label { font-size: 13px; color: var(--text-tertiary); font-weight: 500; }
.vis-toolbar .spacer { flex: 1; }
.vis-toolbar .hint { font-size: 12px; color: var(--text-tertiary); font-variant-numeric: tabular-nums; }
.vis-body { display: flex; gap: 12px; flex: 1; min-height: 0; }
.vis-canvas {
  height: calc(100vh - 240px);
  min-height: 420px;
  flex: 1;
  background: #F0F2F5;
  border: 1px solid var(--divider, #f0f0f0);
  border-radius: 8px;
  background-image: radial-gradient(#d1d5db 1px, transparent 1px);
  background-size: 32px 32px;
}
/* 右侧详情面板 */
.detail-panel {
  width: 320px;
  min-width: 320px;
  height: calc(100vh - 240px);
  min-height: 420px;
  background: #fff;
  border: 1px solid var(--divider, #f0f0f0);
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 16px 10px;
  border-bottom: 1px solid var(--divider, #f0f0f0);
}
.panel-title { font-size: 15px; font-weight: 600; color: rgba(0,0,0,0.88); }
.panel-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 16px;
}
.meta-tag {
  background: rgba(22,119,255,0.08);
  color: #1677ff;
  font-size: 12px;
  padding: 2px 8px;
  border-radius: 4px;
  font-weight: 500;
}
.meta-stat { font-size: 12px; color: var(--text-tertiary); }
.panel-desc {
  color: var(--text-secondary);
  font-size: 13px;
  line-height: 20px;
  margin: 0;
  padding: 0 16px 10px;
}
.detail-logic {
  background: rgba(22, 119, 255, 0.06);
  border: 1px solid rgba(22, 119, 255, 0.15);
  border-radius: 6px;
  padding: 10px 12px;
  font-size: 13px;
  line-height: 20px;
  margin: 0 16px 10px;
}
.detail-sub { font-size: 12px; color: var(--text-tertiary); text-transform: uppercase; letter-spacing: 0.04em; padding: 0 16px; margin: 10px 0 6px; font-weight: 600; }
.rel-list {
  flex: 1;
  overflow-y: auto;
  padding: 0 16px 16px;
}
.rel-line { display: flex; align-items: center; gap: 6px; padding: 5px 0; font-size: 13px; border-bottom: 1px solid var(--divider, #f5f5f5); }
.rel-line:last-child { border-bottom: none; }
.rel-line .rel-edit-btn { opacity: 0; transition: opacity 0.15s; margin-left: auto; }
.rel-line:hover .rel-edit-btn { opacity: 1; }
.rel-type { color: var(--text-tertiary); font-size: 12px; min-width: 56px; }
.arrow.out { color: var(--success, #52c41a); font-weight: 600; font-size: 14px; }
.arrow.in { color: var(--primary, #1677ff); font-weight: 600; font-size: 14px; }
/* 审核状态标签 */
.meta-status { font-size: 11px; padding: 1px 6px; border-radius: 3px; font-weight: 500; }
.meta-status.pending_approval { background: #fff7e6; color: #d48806; }
.meta-status.approved { background: #f6ffed; color: #389e0d; }
.meta-status.rejected { background: #fff2f0; color: #cf1322; }
.meta-status.unreviewed { background: #f5f5f5; color: #8c8c8c; }
/* 编辑表单 */
.panel-actions { display: flex; align-items: center; gap: 4px; }
.edit-form { padding: 12px 16px; display: flex; flex-direction: column; gap: 12px; flex: 1; overflow-y: auto; }
.ef-row { display: flex; flex-direction: column; gap: 4px; }
.ef-row label { font-size: 12px; color: var(--text-tertiary); font-weight: 500; }
.ef-btns { display: flex; gap: 8px; justify-content: flex-end; padding-top: 4px; }
/* 审核操作栏 */
.review-bar {
  border-top: 1px solid var(--divider, #f0f0f0);
  padding: 12px 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.rb-btns { display: flex; gap: 8px; }
/* 面板滑入动画 */
.panel-slide-enter-active, .panel-slide-leave-active { transition: all 0.25s ease; }
.panel-slide-enter-from, .panel-slide-leave-to { opacity: 0; transform: translateX(20px); width: 0; min-width: 0; padding: 0; margin: 0; }
</style>
