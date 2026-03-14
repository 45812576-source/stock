# 账号体系测试用例

## 测试前准备

### 测试账号
| 角色 | 用户名 | 密码 | 用途 |
|------|--------|------|------|
| super_admin | admin | admin123 | 管理员测试 |
| data_admin | data_admin | admin123 | 数据管理员测试 |
| free_user | free_test | admin123 | 免费用户测试 |
| subscriber | sub_test | admin123 | 订阅用户测试 |

### 创建测试账号
```bash
# 登录后访问 API 创建
POST /auth/register
{"username": "data_admin", "password": "admin123", "role": "data_admin"}

POST /auth/register
{"username": "free_test", "password": "admin123", "role": "free_user"}

POST /auth/register
{"username": "sub_test", "password": "admin123", "role": "subscriber"}
```

---

## 一、认证测试

### 1.1 登录功能
| 用例ID | 描述 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| AUTH-01 | 正确账号密码登录 | POST /auth/login {"username":"admin","password":"admin123"} | 返回 token，用户信息，配额 |
| AUTH-02 | 错误密码 | POST /auth/login {"username":"admin","password":"wrong"} | 401 用户名或密码错误 |
| AUTH-03 | 不存在用户 | POST /auth/login {"username":"notexist","password":"admin123"} | 401 用户名或密码错误 |
| AUTH-04 | 登出 | POST /auth/logout | Cookie 清除 |
| AUTH-05 | 未登录访问需认证页面 | GET /overview | 正常访问（overview不需要认证） |
| AUTH-06 | 未登录访问hotspot | GET /hotspot | 401 重定向登录页 |

### 1.2 注册功能
| 用例ID | 描述 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| REG-01 | 正常注册免费用户 | POST /auth/register {"username":"newuser1","password":"123456","role":"free_user"} | 成功创建，默认free_user |
| REG-02 | 注册已存在用户名 | POST /auth/register {"username":"admin","password":"123456"} | 400 用户名已存在 |
| REG-03 | 注册订阅用户 | POST /auth/register {"username":"newsub","password":"123456","role":"subscriber"} | 成功创建，subscriber |

---

## 二、角色权限测试

### 2.1 功能模块访问
| 用例ID | 角色 | 模块 | 测试步骤 | 预期结果 |
|--------|------|------|----------|----------|
| ROLE-01 | free_user | /overview | GET /overview | 200 正常访问 |
| ROLE-02 | free_user | /hotspot | GET /hotspot | 403 无热点发现权限 |
| ROLE-03 | free_user | /portfolio | GET /portfolio | 200 正常访问 |
| ROLE-04 | free_user | /agent/ | GET /agent/ | 200 正常访问 |
| ROLE-05 | free_user | /kg/annotate | GET /kg/annotate | 403 需要标注权限 |
| ROLE-06 | subscriber | /hotspot | GET /hotspot | 200 正常访问 |
| ROLE-07 | subscriber | /kg/annotate | GET /kg/annotate | 403 需要标注权限（data_admin才可） |
| ROLE-08 | data_admin | /kg/annotate | GET /kg/annotate | 200 正常访问 |
| ROLE-09 | super_admin | /admin | GET /admin | 200 正常访问 |
| ROLE-10 | subscriber | /admin | GET /admin | 403 需要超级管理员权限 |

---

## 三、配额测试

### 3.1 Portfolio 配额
| 用例ID | 角色 | 当前数量 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|----------|
| PF-01 | free_user | 0 | 创建 portfolio | 成功 |
| PF-02 | free_user | 1 | 创建 portfolio | 成功 |
| PF-03 | free_user | 2 | 创建 portfolio | 成功（达到上限） |
| PF-04 | free_user | 2 | 再创建 portfolio | 403 Portfolio数量已达上限(2个) |
| PF-05 | subscriber | 0 | 创建 portfolio | 成功（无限制） |

### 3.2 AI Chat 配额
| 用例ID | 角色 | 已用 | 测试步骤 | 预期结果 |
|--------|------|------|----------|----------|
| AI-01 | free_user | 0 | POST /agent/chat | 200 成功 |
| AI-02 | free_user | 19 | POST /agent/chat (x2) | 第20次成功，第21次403 |
| AI-03 | subscriber | 0 | POST /agent/chat (x100) | 前100次成功，第101次需积分 |
| AI-04 | subscriber | 100 | 积分=0 POST /agent/chat | 403 次数已达上限，可用积分兑换 |
| AI-05 | subscriber | 100 | 积分>=10 POST /agent/chat | 200 消耗10积分 |

### 3.3 标签组配额
| 用例ID | 角色 | 已用 | 测试步骤 | 预期结果 |
|--------|------|------|----------|----------|
| TAG-01 | free_user | 0 | POST /hotspot/recommend | 403 免费用户无标签组权限 |
| TAG-02 | subscriber | 0 | POST /hotspot/recommend | 200 成功，消耗1次 |
| TAG-03 | subscriber | 4 | POST /hotspot/recommend | 200 成功 |
| TAG-04 | subscriber | 5 | POST /hotspot/recommend | 403 次数已达上限 |
| TAG-05 | subscriber | 5 | 积分>=10 POST /hotspot/recommend | 200 消耗10积分 |

### 3.4 深度研究配额
| 用例ID | 角色 | 已用 | 测试步骤 | 预期结果 |
|--------|------|------|----------|----------|
| RES-01 | free_user | 0 | POST /hotspot/research/1/run | 403 免费用户无深度研究权限 |
| RES-02 | subscriber | 0 | POST /hotspot/research/1/run | 200 成功 |
| RES-03 | subscriber | 9 | POST /hotspot/research/1/run | 200 成功 |
| RES-04 | subscriber | 10 | POST /hotspot/research/1/run | 403 次数已达上限 |
| RES-05 | subscriber | 10 | 积分>=30 POST /hotspot/research/1/run | 200 消耗30积分 |

### 3.5 K线分析配额
| 用例ID | 角色 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| CHART-01 | free_user | POST /stock/000001/api/chart-analysis | 403 免费用户无K线分析权限 |
| CHART-02 | subscriber | POST /stock/000001/api/chart-analysis | 200 成功 |

---

## 四、积分系统测试

### 4.1 积分查看
| 用例ID | 测试步骤 | 预期结果 |
|--------|----------|----------|
| PTS-01 | GET /auth/me | 返回 points_balance |
| PTS-02 | GET /auth/quota | 返回详细配额和积分 |

### 4.2 积分购买
| 用例ID | 测试步骤 | 预期结果 |
|--------|----------|----------|
| PTS-BUY-01 | GET /auth/packages/public | 返回可用积分包列表 |
| PTS-BUY-02 | POST /auth/points/purchase?package_id=1 | 积分增加，余额更新 |

### 4.3 积分消耗
| 用例ID | 场景 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| PTS-COST-01 | 标签组用积分 | 已达上限+积分足够 | 成功，积分-10 |
| PTS-COST-02 | 研究用积分 | 已达上限+积分足够 | 成功，积分-30 |
| PTS-COST-03 | 积分不足 | 已达上限+积分不足 | 403 次数已达上限 |

---

## 五、管理员功能测试

### 5.1 用户管理
| 用例ID | 测试角色 | 测试步骤 | 预期结果 |
|--------|----------|----------|----------|
| ADMIN-01 | super_admin | GET /auth/users | 返回用户列表 |
| ADMIN-02 | subscriber | GET /auth/users | 403 需要超级管理员 |
| ADMIN-03 | super_admin | GET /auth/users/2 | 返回指定用户详情 |
| ADMIN-04 | super_admin | PUT /auth/users/2 {"role":"subscriber"} | 成功，用户角色变更 |
| ADMIN-05 | super_admin | PUT /auth/users/2 {"points_balance":100} | 成功，积分增加 |
| ADMIN-06 | super_admin | POST /auth/users/2/reset-usage | 成功，使用量重置 |

### 5.2 积分包管理
| 用例ID | 测试角色 | 测试步骤 | 预期结果 |
|--------|----------|----------|----------|
| PKG-01 | super_admin | GET /auth/packages | 返回积分包列表 |
| PKG-02 | subscriber | GET /auth/packages | 403 需要超级管理员 |
| PKG-03 | super_admin | POST /auth/packages {"name":"测试包","points":100,"price":9.9} | 成功创建 |
| PKG-04 | super_admin | PUT /auth/packages/1 {"price":19.9} | 成功更新 |

### 5.3 管理后台页面
| 用例ID | 测试角色 | 测试步骤 | 预期结果 |
|--------|----------|----------|----------|
| ADMIN-PAGE-01 | super_admin | GET /admin | 200 管理后台首页 |
| ADMIN-PAGE-02 | super_admin | GET /admin/users | 200 用户管理页 |
| ADMIN-PAGE-03 | super_admin | GET /admin/packages | 200 积分包管理页 |
| ADMIN-PAGE-04 | subscriber | GET /admin | 403 无权限 |

---

## 六、边界情况测试

### 6.1 月度重置
| 用例ID | 描述 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| RESET-01 | 新月第一天 | ai_chat_used 自动重置为 0 | 重置成功 |

### 6.2 账号禁用
| 用例ID | 描述 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| DISABLE-01 | 账号被禁用 | admin设置 is_active=false | 登录返回 403 账号已被禁用 |

### 6.3 Session/Token
| 用例ID | 描述 | 测试步骤 | 预期结果 |
|--------|------|----------|----------|
| SESSION-01 | Token过期 | 使用过期token | 401 登录已过期 |

---

## 测试检查清单

- [ ] AUTH-01 ~ AUTH-06
- [ ] REG-01 ~ REG-03
- [ ] ROLE-01 ~ ROLE-10
- [ ] PF-01 ~ PF-05
- [ ] AI-01 ~ AI-05
- [ ] TAG-01 ~ TAG-05
- [ ] RES-01 ~ RES-05
- [ ] CHART-01 ~ CHART-02
- [ ] PTS-01 ~ PTS-02
- [ ] PTS-BUY-01 ~ PTS-BUY-02
- [ ] PTS-COST-01 ~ PTS-COST-03
- [ ] ADMIN-01 ~ ADMIN-06
- [ ] PKG-01 ~ PKG-04
- [ ] ADMIN-PAGE-01 ~ ADMIN-PAGE-04
- [ ] RESET-01
- [ ] DISABLE-01
- [ ] SESSION-01
