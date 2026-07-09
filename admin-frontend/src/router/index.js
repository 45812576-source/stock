import { createRouter, createWebHashHistory } from 'vue-router'
import { useUserStore } from '@/stores/user'

const routes = [
  { path: '/login', name: 'login', component: () => import('@/views/Login.vue'), meta: { public: true } },
  {
    path: '/',
    component: () => import('@/layouts/AdminLayout.vue'),
    redirect: '/data',
    children: [
      { path: 'data', name: 'data', component: () => import('@/views/DataManage.vue'), meta: { title: '数据管理' } },
      { path: 'kg', name: 'kg', component: () => import('@/views/KnowledgeGraph.vue'), meta: { title: '知识图谱' } },
      { path: 'settings', name: 'settings', component: () => import('@/views/Settings.vue'), meta: { title: '系统设置' } },
      { path: 'users', name: 'users', component: () => import('@/views/UserManage.vue'), meta: { title: '用户管理', role: 'super_admin' } },
    ],
  },
  { path: '/:pathMatch(.*)*', redirect: '/data' },
]

const router = createRouter({
  history: createWebHashHistory('/admin-app/'),
  routes,
})

router.beforeEach(async (to) => {
  if (to.meta.public) return true
  const store = useUserStore()
  if (!store.loaded) {
    try {
      await store.fetchMe()
    } catch (e) {
      return { path: '/login', query: { redirect: to.fullPath } }
    }
  }
  if (!store.isLoggedIn) {
    return { path: '/login', query: { redirect: to.fullPath } }
  }
  if (to.meta.role === 'super_admin' && !store.isSuperAdmin) {
    return { path: '/data' }
  }
  return true
})

export default router
