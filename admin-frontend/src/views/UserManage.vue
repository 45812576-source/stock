<template>
  <div class="user-manage">
    <!-- 概览统计 -->
    <el-row :gutter="12" class="stat-row">
      <el-col :span="6">
        <el-card shadow="never" class="stat-card">
          <div class="stat-num">{{ overview.total_users ?? '—' }}</div>
          <div class="stat-label">总用户数</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never" class="stat-card">
          <div class="stat-num">{{ overview.total_points ?? '—' }}</div>
          <div class="stat-label">总积分</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never" class="stat-card">
          <div class="stat-num">{{ overview.active_today ?? '—' }}</div>
          <div class="stat-label">今日活跃</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="never" class="stat-card">
          <div class="stat-num">{{ roleCount }}</div>
          <div class="stat-label">角色分布</div>
          <div class="role-dist">
            <el-tag v-for="(v, r) in overview.role_distribution" :key="r" size="small" class="role-tag">
              {{ r }}: {{ v }}
            </el-tag>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-tabs v-model="activeTab" @tab-change="onTabChange">
      <!-- 用户列表 -->
      <el-tab-pane label="用户管理" name="users">
        <el-table :data="users" v-loading="loading" size="small" border>
          <el-table-column prop="id" label="ID" width="70" />
          <el-table-column prop="username" label="用户名" width="140" />
          <el-table-column prop="role" label="角色" width="120">
            <template #default="{ row }">
              <el-tag :type="roleType(row.role)" size="small">{{ row.role }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="is_active" label="状态" width="80">
            <template #default="{ row }">
              <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                {{ row.is_active ? '启用' : '禁用' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="points_balance" label="积分" width="90" />
          <el-table-column label="AI对话" width="100">
            <template #default="{ row }">{{ row.ai_chat_used }}/{{ row.ai_chat_monthly_limit }}</template>
          </el-table-column>
          <el-table-column label="研究" width="90">
            <template #default="{ row }">{{ row.research_used }}/{{ row.research_limit }}</template>
          </el-table-column>
          <el-table-column prop="created_at" label="注册时间" width="170" />
          <el-table-column label="操作" width="200" fixed="right">
            <template #default="{ row }">
              <el-button size="small" text @click="editUser(row)">编辑</el-button>
              <el-button size="small" text @click="doResetUsage(row)">重置用量</el-button>
              <el-button size="small" text type="danger" @click="removeUser(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          class="pager"
          layout="total, prev, pager, next"
          :total="total"
          :page-size="pageSize"
          :current-page="page"
          @current-change="onPageChange"
        />
      </el-tab-pane>

      <!-- 积分包管理 -->
      <el-tab-pane label="积分包管理" name="packages">
        <div class="pkg-header">
          <el-button size="small" type="primary" @click="openAddPkg">新增积分包</el-button>
        </div>
        <el-table :data="packages" v-loading="pkgLoading" size="small" border>
          <el-table-column prop="id" label="ID" width="70" />
          <el-table-column prop="name" label="名称" width="160" />
          <el-table-column prop="points" label="积分" width="100" />
          <el-table-column prop="bonus_points" label="赠送" width="100" />
          <el-table-column prop="price" label="价格" width="100" />
          <el-table-column prop="description" label="描述" show-overflow-tooltip />
          <el-table-column prop="is_active" label="状态" width="80">
            <template #default="{ row }">
              <el-tag :type="row.is_active ? 'success' : 'info'" size="small">
                {{ row.is_active ? '上架' : '下架' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="90">
            <template #default="{ row }">
              <el-button size="small" text @click="editPkg(row)">编辑</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>

    <!-- 用户编辑对话框 -->
    <el-dialog v-model="userDialog" :title="`编辑用户: ${userForm.username}`" width="480px">
      <el-form :model="userForm" label-width="110px">
        <el-form-item label="角色">
          <el-select v-model="userForm.role" style="width: 100%">
            <el-option label="超级管理员" value="super_admin" />
            <el-option label="数据管理员" value="data_admin" />
            <el-option label="订阅用户" value="subscriber" />
            <el-option label="免费用户" value="free_user" />
          </el-select>
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="userForm.is_active" />
        </el-form-item>
        <el-form-item label="组合上限">
          <el-input-number v-model="userForm.portfolio_limit" :min="0" />
        </el-form-item>
        <el-form-item label="AI对话月限">
          <el-input-number v-model="userForm.ai_chat_monthly_limit" :min="0" />
        </el-form-item>
        <el-form-item label="标签组上限">
          <el-input-number v-model="userForm.tag_group_limit" :min="0" />
        </el-form-item>
        <el-form-item label="研究上限">
          <el-input-number v-model="userForm.research_limit" :min="0" />
        </el-form-item>
        <el-form-item label="增减积分">
          <el-input-number v-model="userForm.points_delta" :step="100" />
          <span class="hint">正数增加/负数扣减</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="userDialog = false">取消</el-button>
        <el-button type="primary" :loading="savingUser" @click="doSaveUser">保存</el-button>
      </template>
    </el-dialog>

    <!-- 积分包编辑对话框 -->
    <el-dialog v-model="pkgDialog" :title="pkgEditing ? '编辑积分包' : '新增积分包'" width="440px">
      <el-form :model="pkgForm" label-width="90px">
        <el-form-item label="名称"><el-input v-model="pkgForm.name" /></el-form-item>
        <el-form-item label="积分"><el-input-number v-model="pkgForm.points" :min="0" /></el-form-item>
        <el-form-item label="赠送"><el-input-number v-model="pkgForm.bonus_points" :min="0" /></el-form-item>
        <el-form-item label="价格"><el-input-number v-model="pkgForm.price" :min="0" :precision="2" /></el-form-item>
        <el-form-item label="描述"><el-input v-model="pkgForm.description" type="textarea" :rows="2" /></el-form-item>
        <el-form-item label="上架" v-if="pkgEditing"><el-switch v-model="pkgForm.is_active" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="pkgDialog = false">取消</el-button>
        <el-button type="primary" :loading="savingPkg" @click="doSavePkg">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as adminApi from '@/api/admin'

const activeTab = ref('users')
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
const overview = reactive({})
const roleCount = computed(() => Object.keys(overview.role_distribution || {}).length)

function roleType(role) {
  return { super_admin: 'danger', data_admin: 'warning', subscriber: 'success', free_user: 'info' }[role] || 'info'
}

async function loadOverview() {
  try { Object.assign(overview, await adminApi.getOverview()) } catch { /* handled */ }
}

// ---- 用户列表 ----
const users = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = 20
const loading = ref(false)

async function loadUsers() {
  loading.value = true
  try {
    const r = await adminApi.listUsers(page.value, pageSize)
    users.value = r.users || []
    total.value = r.total || 0
  } finally {
    loading.value = false
  }
}
function onPageChange(p) { page.value = p; loadUsers() }

// ---- 用户编辑 ----
const userDialog = ref(false)
const savingUser = ref(false)
const userForm = reactive({
  id: null, username: '', role: 'free_user', is_active: true,
  portfolio_limit: 0, ai_chat_monthly_limit: 0, tag_group_limit: 0,
  research_limit: 0, points_delta: 0,
})
function editUser(row) {
  Object.assign(userForm, {
    id: row.id, username: row.username, role: row.role, is_active: !!row.is_active,
    portfolio_limit: row.portfolio_limit || 0, ai_chat_monthly_limit: row.ai_chat_monthly_limit || 0,
    tag_group_limit: row.tag_group_limit || 0, research_limit: row.research_limit || 0, points_delta: 0,
  })
  userDialog.value = true
}
async function doSaveUser() {
  savingUser.value = true
  try {
    const payload = {
      role: userForm.role, is_active: userForm.is_active,
      portfolio_limit: userForm.portfolio_limit,
      ai_chat_monthly_limit: userForm.ai_chat_monthly_limit,
      tag_group_limit: userForm.tag_group_limit,
      research_limit: userForm.research_limit,
    }
    if (userForm.points_delta) payload.points_balance = userForm.points_delta
    const r = await adminApi.updateUser(userForm.id, payload)
    if (r.success) { ElMessage.success('已保存'); userDialog.value = false; loadUsers(); loadOverview() }
  } finally {
    savingUser.value = false
  }
}
async function doResetUsage(row) {
  await ElMessageBox.confirm(`确认重置「${row.username}」的用量？`, '提示', { type: 'warning' })
  const r = await adminApi.resetUsage(row.id)
  if (r.success) { ElMessage.success('已重置'); loadUsers() }
}
async function removeUser(row) {
  await ElMessageBox.confirm(`确认删除用户「${row.username}」？`, '警告', { type: 'warning' })
  const r = await adminApi.deleteUser(row.id)
  if (r.success) { ElMessage.success('已删除'); loadUsers(); loadOverview() }
}

// ---- 积分包 ----
const packages = ref([])
const pkgLoading = ref(false)
async function loadPackages() {
  pkgLoading.value = true
  try {
    const r = await adminApi.listPackages()
    packages.value = r.packages || []
  } finally {
    pkgLoading.value = false
  }
}

const pkgDialog = ref(false)
const pkgEditing = ref(false)
const savingPkg = ref(false)
const pkgForm = reactive({ id: null, name: '', points: 0, bonus_points: 0, price: 0, description: '', is_active: true })
function resetPkg() { Object.assign(pkgForm, { id: null, name: '', points: 0, bonus_points: 0, price: 0, description: '', is_active: true }) }
function openAddPkg() { pkgEditing.value = false; resetPkg(); pkgDialog.value = true }
function editPkg(row) {
  pkgEditing.value = true
  Object.assign(pkgForm, {
    id: row.id, name: row.name, points: row.points, bonus_points: row.bonus_points,
    price: row.price, description: row.description || '', is_active: !!row.is_active,
  })
  pkgDialog.value = true
}
async function doSavePkg() {
  if (!pkgForm.name) return ElMessage.warning('名称必填')
  savingPkg.value = true
  try {
    let r
    if (pkgEditing.value) {
      r = await adminApi.updatePackage(pkgForm.id, {
        name: pkgForm.name, points: pkgForm.points, price: pkgForm.price,
        bonus_points: pkgForm.bonus_points, is_active: pkgForm.is_active,
      })
    } else {
      r = await adminApi.createPackage({
        name: pkgForm.name, points: pkgForm.points, price: pkgForm.price,
        bonus_points: pkgForm.bonus_points, description: pkgForm.description,
      })
    }
    if (r.success) { ElMessage.success('已保存'); pkgDialog.value = false; loadPackages() }
  } finally {
    savingPkg.value = false
  }
}

watch(activeTab, (t) => { if (t === 'packages' && !packages.value.length) loadPackages() })

onMounted(() => { loadOverview(); loadUsers() })
</script>

<style scoped>
.stat-row { margin-bottom: 12px; }
.stat-card { text-align: center; }
.stat-num { font-size: 22px; font-weight: 600; }
.stat-label { color: #909399; font-size: 12px; margin-top: 4px; }
.role-dist { margin-top: 6px; }
.role-tag { margin: 2px; }
.pager { margin-top: 12px; text-align: right; justify-content: flex-end; }
.pkg-header { margin-bottom: 10px; }
.hint { color: #909399; font-size: 12px; margin-left: 8px; }
</style>
