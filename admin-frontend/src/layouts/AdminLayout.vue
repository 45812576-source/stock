<template>
  <div class="admin-shell flex min-h-screen">
    <!-- 侧栏 -->
    <aside
      class="admin-sidebar"
      :class="{ collapsed }"
    >
      <div class="brand" :class="{ 'is-collapsed': collapsed }">
        <div class="brand-logo">
          <span class="material-symbols-outlined filled">insights</span>
        </div>
        <div v-if="!collapsed" class="brand-text">
          <div class="brand-title">运营后台</div>
          <div class="brand-subtitle">Enterprise Console</div>
        </div>
      </div>

      <nav class="nav-scroll">
        <div v-for="group in visibleGroups" :key="group.title" class="nav-group">
          <div v-if="!collapsed" class="nav-group-title">{{ group.title }}</div>
          <div v-else class="nav-group-divider"></div>
          <router-link
            v-for="item in group.items"
            :key="item.key"
            :to="item.to"
            class="nav-item"
            :class="{ active: isActive(item) }"
            :title="collapsed ? item.title : ''"
          >
            <span class="material-symbols-outlined nav-icon">{{ item.icon }}</span>
            <span v-if="!collapsed" class="nav-label">{{ item.title }}</span>
          </router-link>
        </div>
      </nav>

      <div class="sidebar-footer">
        <button class="collapse-btn" @click="collapsed = !collapsed">
          <span class="material-symbols-outlined">
            {{ collapsed ? 'chevron_right' : 'chevron_left' }}
          </span>
        </button>
      </div>
    </aside>

    <!-- 右侧内容 -->
    <div class="flex-1 flex flex-col min-w-0">
      <!-- 顶栏 -->
      <header class="admin-header">
        <div class="breadcrumbs">
          <span class="crumb-root">
            <span class="material-symbols-outlined text-base align-middle">home</span>
          </span>
          <template v-for="(c, idx) in breadcrumbs" :key="idx">
            <span class="crumb-sep">/</span>
            <span
              class="crumb"
              :class="{ 'crumb-current': idx === breadcrumbs.length - 1 }"
            >{{ c }}</span>
          </template>
        </div>

        <div class="header-actions">
          <button class="icon-btn" title="任务中心" @click="taskDrawerVisible = true">
            <span class="material-symbols-outlined">task_alt</span>
            <span v-if="activeTaskCount" class="badge">{{ activeTaskCount }}</span>
          </button>
          <div class="header-divider"></div>
          <el-dropdown trigger="click" @command="onUserCommand">
            <div class="user-chip">
              <div class="user-avatar">{{ avatarText }}</div>
              <div class="user-meta">
                <div class="user-name">{{ userStore.user?.username || '—' }}</div>
                <div class="user-role">{{ userStore.role || 'user' }}</div>
              </div>
              <span class="material-symbols-outlined text-text-secondary">arrow_drop_down</span>
            </div>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="logout">
                  <span class="material-symbols-outlined text-base align-middle mr-1">logout</span>
                  登出
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </header>

      <!-- 主内容 -->
      <main class="admin-main">
        <router-view />
      </main>
    </div>

    <!-- 全局任务中心 Drawer -->
    <TaskCenterDrawer v-model:visible="taskDrawerVisible" @count-change="onTaskCountChange" />
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import TaskCenterDrawer from '@/components/TaskCenterDrawer.vue'

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()

const collapsed = ref(false)
const taskDrawerVisible = ref(false)
const activeTaskCount = ref(0)

// 导航结构（二级分组）
const navGroups = [
  {
    title: '数据管理',
    items: [
      { key: 'overview',   title: '数据管理',   icon: 'dashboard',     to: { path: '/data', query: { tab: 'overview' } },   pathPrefix: '/data', tab: 'overview' },
      { key: 'sources',    title: '数据来源',   icon: 'cloud_download', to: { path: '/data', query: { tab: 'sources' } },    pathPrefix: '/data', tab: 'sources' },
      { key: 'docs',       title: '源文档',     icon: 'description',   to: { path: '/data', query: { tab: 'docs' } },       pathPrefix: '/data', tab: 'docs' },
      { key: 'pipeline',   title: '清洗管线',   icon: 'account_tree',  to: { path: '/data', query: { tab: 'pipeline' } },   pathPrefix: '/data', tab: 'pipeline' },
      { key: 'structured', title: '结构化数据', icon: 'table_chart',   to: { path: '/data', query: { tab: 'structured' } }, pathPrefix: '/data', tab: 'structured' },
      { key: 'strategy',   title: '选股策略',   icon: 'insights',      to: { path: '/data', query: { tab: 'strategy' } },   pathPrefix: '/data', tab: 'strategy' },
    ],
  },
  {
    title: '知识图谱',
    items: [
      { key: 'kg-entities', title: '实体管理',  icon: 'category',      to: { path: '/kg', query: { tab: 'entities' } },      pathPrefix: '/kg', tab: 'entities' },
      { key: 'kg-vis',      title: '可视化',    icon: 'hub',           to: { path: '/kg', query: { tab: 'visualization' } }, pathPrefix: '/kg', tab: 'visualization' },
      { key: 'kg-infer',    title: '推理引擎',  icon: 'psychology',    to: { path: '/kg', query: { tab: 'inference' } },     pathPrefix: '/kg', tab: 'inference' },
      { key: 'kg-inspect',  title: '巡检',      icon: 'monitor_heart', to: { path: '/kg', query: { tab: 'inspect' } },       pathPrefix: '/kg', tab: 'inspect' },
      { key: 'kg-review',   title: '审核工作台',icon: 'rule',          to: { path: '/kg', query: { tab: 'annotate' } },      pathPrefix: '/kg', tab: 'annotate' },
    ],
  },
  {
    title: '系统设置',
    items: [
      { key: 'set-api',    title: 'API·模型',    icon: 'tune', to: { path: '/settings', query: { tab: 'api' } },    pathPrefix: '/settings', tab: 'api' },
      { key: 'set-skills', title: 'Skill 编辑器', icon: 'code', to: { path: '/settings', query: { tab: 'skills' } }, pathPrefix: '/settings', tab: 'skills' },
    ],
  },
  {
    title: '用户管理',
    role: 'super_admin',
    items: [
      { key: 'users',        title: '用户',   icon: 'group',  to: { path: '/users', query: { tab: 'users' } },    pathPrefix: '/users', tab: 'users' },
      { key: 'users-points', title: '积分包', icon: 'redeem', to: { path: '/users', query: { tab: 'packages' } }, pathPrefix: '/users', tab: 'packages' },
    ],
  },
]

const visibleGroups = computed(() =>
  navGroups.filter((g) => !g.role || (g.role === 'super_admin' && userStore.isSuperAdmin))
)

function isActive(item) {
  if (!route.path.startsWith(item.pathPrefix)) return false
  const tab = route.query.tab || item.__firstTabInPrefix
  // 该 prefix 下第一个 item 且当前无 query.tab 时高亮为默认
  const first = navGroups
    .flatMap((g) => g.items)
    .find((it) => it.pathPrefix === item.pathPrefix)
  if (!route.query.tab && item === first) return true
  return tab === item.tab
}

const breadcrumbs = computed(() => {
  const item = navGroups
    .flatMap((g) => g.items.map((i) => ({ ...i, group: g.title })))
    .find((i) => isActive(i))
  if (!item) return ['首页']
  return [item.group, item.title]
})

const avatarText = computed(() => {
  const name = userStore.user?.username || 'U'
  return name.slice(0, 2).toUpperCase()
})

function onTaskCountChange(n) {
  activeTaskCount.value = n || 0
}

async function onUserCommand(cmd) {
  if (cmd === 'logout') {
    await userStore.logout()
    router.push('/login')
  }
}
</script>

<style scoped>
.admin-shell {
  background: var(--page-bg);
  min-height: 100vh;
}

/* ============ 侧栏 ============ */
.admin-sidebar {
  width: 240px;
  height: 100vh;
  position: sticky;
  top: 0;
  left: 0;
  background: #ffffff;
  border-right: 1px solid var(--divider);
  box-shadow: var(--shadow-pro-nav, 0 1px 2px rgba(0, 0, 0, 0.03));
  display: flex;
  flex-direction: column;
  z-index: 60;
  transition: width 0.25s cubic-bezier(0.645, 0.045, 0.355, 1);
}
.admin-sidebar.collapsed {
  width: 72px;
}
.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 20px 24px;
  border-bottom: 1px solid var(--divider);
}
.brand.is-collapsed {
  justify-content: center;
  padding: 20px 12px;
}
.brand-logo {
  width: 36px;
  height: 36px;
  border-radius: 4px;
  background: linear-gradient(135deg, #1677ff 0%, #0958d9 100%);
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}
.brand-title {
  font-size: 16px;
  font-weight: 600;
  line-height: 22px;
  color: var(--primary);
}
.brand-subtitle {
  font-size: 12px;
  color: var(--text-tertiary);
  line-height: 16px;
  letter-spacing: 0.5px;
  text-transform: uppercase;
}
.nav-scroll {
  flex: 1;
  overflow-y: auto;
  padding: 12px 8px;
}
.nav-group + .nav-group {
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--divider);
}
.nav-group-title {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.06em;
  color: var(--text-tertiary);
  text-transform: uppercase;
  padding: 8px 12px 6px;
}
.nav-group-divider {
  height: 0;
  margin: 4px 8px;
}
.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 12px;
  border-radius: 4px;
  color: var(--text-main);
  text-decoration: none;
  font-size: 14px;
  line-height: 22px;
  transition: background 0.15s;
  white-space: nowrap;
  overflow: hidden;
}
.nav-item:hover {
  background: #f5f5f5;
}
.nav-item.active {
  background: #e6f4ff;
  color: var(--primary);
  font-weight: 600;
}
.nav-item.active .nav-icon {
  color: var(--primary);
}
.nav-icon {
  color: var(--text-tertiary);
  flex-shrink: 0;
  font-size: 20px !important;
}
.admin-sidebar.collapsed .nav-item {
  justify-content: center;
  padding: 8px 0;
}
.sidebar-footer {
  border-top: 1px solid var(--divider);
  padding: 8px;
  display: flex;
  justify-content: flex-end;
}
.admin-sidebar.collapsed .sidebar-footer {
  justify-content: center;
}
.collapse-btn {
  background: transparent;
  border: none;
  color: var(--text-tertiary);
  cursor: pointer;
  padding: 6px;
  border-radius: 4px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}
.collapse-btn:hover {
  background: #f5f5f5;
  color: var(--text-main);
}

/* ============ 顶栏 ============ */
.admin-header {
  height: 56px;
  background: #ffffff;
  border-bottom: 1px solid var(--divider);
  padding: 0 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 50;
  box-shadow: var(--shadow-pro-nav, 0 1px 2px rgba(0, 0, 0, 0.03));
}
.breadcrumbs {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  color: var(--text-secondary);
}
.crumb-sep {
  color: var(--text-tertiary);
}
.crumb-current {
  color: var(--text-main);
  font-weight: 500;
}
.header-actions {
  display: flex;
  align-items: center;
  gap: 16px;
}
.icon-btn {
  position: relative;
  background: transparent;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 6px 8px;
  border-radius: 4px;
  display: inline-flex;
  align-items: center;
}
.icon-btn:hover {
  background: #f5f5f5;
  color: var(--text-main);
}
.icon-btn .badge {
  position: absolute;
  top: 0;
  right: 2px;
  min-width: 16px;
  height: 16px;
  padding: 0 4px;
  background: var(--error);
  color: #fff;
  font-size: 10px;
  line-height: 16px;
  text-align: center;
  border-radius: 8px;
  font-weight: 600;
}
.header-divider {
  width: 1px;
  height: 20px;
  background: var(--divider);
}
.user-chip {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 8px;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.15s;
}
.user-chip:hover {
  background: #f5f5f5;
}
.user-avatar {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  background: var(--primary-fixed);
  color: var(--primary);
  font-size: 11px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
  letter-spacing: 0.02em;
}
.user-name {
  font-size: 13px;
  color: var(--text-main);
  line-height: 16px;
  font-weight: 500;
}
.user-role {
  font-size: 11px;
  color: var(--text-tertiary);
  line-height: 14px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

/* ============ 主内容 ============ */
.admin-main {
  flex: 1;
  padding: 24px;
  overflow-y: auto;
  background: var(--page-bg);
}
</style>
