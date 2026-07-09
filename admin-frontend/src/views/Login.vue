<template>
  <main class="flex h-screen w-full overflow-hidden">
    <!-- 左：品牌视觉区 -->
    <section class="hidden lg:flex w-7/12 relative overflow-hidden brand-mesh flex-col justify-between p-xl">
      <div class="absolute inset-0 opacity-30 pointer-events-none animate-drift brand-pattern"></div>

      <div class="relative z-10 flex items-center gap-md">
        <div class="w-12 h-12 rounded-xl bg-primary flex items-center justify-center shadow-lg">
          <span class="material-symbols-outlined filled text-white text-3xl">insights</span>
        </div>
        <div>
          <h1 class="font-headline-sm text-headline-sm text-white tracking-tight">运营后台</h1>
          <p class="text-body-sm text-primary-fixed-dim opacity-80 uppercase tracking-widest">Enterprise Console</p>
        </div>
      </div>

      <div class="relative z-10 max-w-md">
        <h2 class="font-display-lg text-display-lg text-white mb-md leading-tight">
          面向数据运营的<br />专业管理控制台
        </h2>
        <p class="text-body-base text-primary-fixed-dim leading-relaxed">
          统一管理源文档、清洗管线、知识图谱与股票分析业务的运营入口。信息分层清晰，任务反馈集中，为高频运营场景而生。
        </p>
      </div>

      <div class="relative z-10 flex items-center gap-lg text-primary-fixed-dim opacity-70">
        <div class="flex items-center gap-xs">
          <span class="material-symbols-outlined text-[18px]">verified_user</span>
          <span class="text-label-caps font-semibold uppercase tracking-wider">权限分级</span>
        </div>
        <div class="flex items-center gap-xs">
          <span class="material-symbols-outlined text-[18px]">history_toggle_off</span>
          <span class="text-label-caps font-semibold uppercase tracking-wider">全链路审计</span>
        </div>
      </div>
    </section>

    <!-- 右：登录表单区 -->
    <section class="w-full lg:w-5/12 bg-white flex flex-col justify-center items-center px-lg sm:px-xl">
      <div class="w-full max-w-[400px]">
        <!-- 移动端 Logo -->
        <div class="lg:hidden flex items-center gap-sm mb-xl">
          <div class="w-10 h-10 rounded bg-primary flex items-center justify-center">
            <span class="material-symbols-outlined filled text-white text-2xl">insights</span>
          </div>
          <span class="text-title-base font-semibold text-on-surface">运营后台</span>
        </div>

        <header class="mb-xl text-center lg:text-left">
          <h2 class="font-headline-md text-headline-md text-on-surface mb-xs">欢迎回来</h2>
          <p class="text-body-base text-text-secondary">登录以访问后台运营控制台</p>
        </header>

        <el-form :model="form" @submit.prevent class="login-form">
          <div class="form-item">
            <label class="form-label">用户名</label>
            <el-input
              v-model="form.username"
              placeholder="请输入用户名"
              size="large"
              :prefix-icon="User"
              @keyup.enter="handleLogin"
            />
          </div>

          <div class="form-item">
            <div class="form-label-row">
              <label class="form-label">密码</label>
            </div>
            <el-input
              v-model="form.password"
              type="password"
              placeholder="请输入密码"
              size="large"
              :prefix-icon="Lock"
              show-password
              @keyup.enter="handleLogin"
            />
          </div>

          <el-button
            type="primary"
            size="large"
            class="w-full login-btn"
            :loading="loading"
            @click="handleLogin"
          >
            <span>登录</span>
            <span class="material-symbols-outlined ml-2 text-lg">arrow_forward</span>
          </el-button>
        </el-form>

        <footer class="mt-xl pt-md border-t border-divider">
          <p class="text-body-sm text-text-tertiary text-center lg:text-left">
            © 2026 股票分析系统 · 数据运营控制台
          </p>
        </footer>
      </div>
    </section>
  </main>
</template>

<script setup>
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { User, Lock } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import request from '@/api/request'
import { useUserStore } from '@/stores/user'

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()
const loading = ref(false)
const form = ref({ username: '', password: '' })

async function handleLogin() {
  if (!form.value.username || !form.value.password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  loading.value = true
  try {
    await request.post('/auth/login', form.value)
    await userStore.fetchMe()
    const redirect = route.query.redirect || '/data'
    router.push(redirect)
  } catch (e) {
    // 拦截器已提示
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.brand-mesh {
  background-color: #001a43;
  background-image:
    radial-gradient(at 0% 0%, #1677ff 0px, transparent 50%),
    radial-gradient(at 100% 0%, #004398 0px, transparent 50%),
    radial-gradient(at 100% 100%, #001a43 0px, transparent 50%),
    radial-gradient(at 0% 100%, #0959c7 0px, transparent 50%);
}
.brand-pattern {
  background-image:
    radial-gradient(circle at 20% 20%, rgba(255, 255, 255, 0.08) 1px, transparent 1px),
    radial-gradient(circle at 80% 80%, rgba(255, 255, 255, 0.05) 1px, transparent 1px);
  background-size: 40px 40px, 32px 32px;
}
.animate-drift {
  animation: drift 20s ease-in-out infinite alternate;
}
@keyframes drift {
  from { transform: scale(1) translate(0, 0); }
  to   { transform: scale(1.1) translate(-2%, -2%); }
}

.login-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}
.form-item {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.form-label-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.form-label {
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--text-secondary);
}
.login-btn {
  height: 44px !important;
  font-size: 15px !important;
  font-weight: 500 !important;
  border-radius: 4px !important;
}
</style>
