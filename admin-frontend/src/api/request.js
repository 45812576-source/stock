import axios from 'axios'
import { ElMessage } from 'element-plus'

// 同域反代下 baseURL 留空即可；withCredentials 保证 cookie 携带
const request = axios.create({
  baseURL: '',
  timeout: 60000,
  withCredentials: true,
})

request.interceptors.response.use(
  (resp) => resp.data,
  (error) => {
    const status = error?.response?.status
    if (status === 401) {
      // 未登录/过期，跳登录页
      const redirect = encodeURIComponent(window.location.hash.replace(/^#/, '') || '/')
      window.location.hash = `/login?redirect=${redirect}`
      ElMessage.warning('登录已过期，请重新登录')
    } else {
      const msg = error?.response?.data?.detail || error?.message || '请求失败'
      ElMessage.error(typeof msg === 'string' ? msg : '请求失败')
    }
    return Promise.reject(error)
  }
)

export default request
