import { defineStore } from 'pinia'
import request from '@/api/request'

export const useUserStore = defineStore('user', {
  state: () => ({
    user: null,
    loaded: false,
  }),
  getters: {
    isLoggedIn: (s) => !!s.user,
    role: (s) => s.user?.role || '',
    isSuperAdmin: (s) => s.user?.role === 'super_admin',
    isDataAdmin: (s) => ['super_admin', 'data_admin'].includes(s.user?.role),
  },
  actions: {
    async fetchMe() {
      try {
        const data = await request.get('/auth/me')
        this.user = data
        this.loaded = true
        return data
      } catch (e) {
        this.user = null
        this.loaded = true
        throw e
      }
    },
    async logout() {
      try {
        await request.post('/auth/logout')
      } finally {
        this.user = null
      }
    },
  },
})
