import request from './request'

// ---- 概览统计 ----
export const getOverview = () => request.get('/admin/api/overview')

// ---- 用户管理 ----
export const listUsers = (page = 1, pageSize = 20) =>
  request.get('/auth/users', { params: { page, page_size: pageSize } })
export const getUser = (id) => request.get(`/auth/users/${id}`)
export const updateUser = (id, payload) => request.put(`/auth/users/${id}`, payload)
export const deleteUser = (id) => request.delete(`/auth/users/${id}`)
export const resetUsage = (id) => request.post(`/auth/users/${id}/reset-usage`, {})

// ---- 积分包管理（后端用 query params）----
export const listPackages = () => request.get('/auth/packages')
export const createPackage = (payload) =>
  request.post('/auth/packages', null, { params: payload })
export const updatePackage = (id, payload) =>
  request.put(`/auth/packages/${id}`, null, { params: payload })
