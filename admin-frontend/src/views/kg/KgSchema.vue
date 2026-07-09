<template>
  <div class="kg-schema">
    <el-row :gutter="16">
      <el-col :span="10">
        <el-card shadow="never">
          <template #header>实体类型（{{ entityTypeList.length }} 种）</template>
          <div
            v-for="et in entityTypeList"
            :key="et.key"
            class="type-item"
            :class="{ active: selected === et.key }"
            @click="selected = et.key"
          >
            <span class="type-dot" :style="{ background: et.cfg.bg }" />
            <span class="type-label">{{ et.cfg.label }}</span>
            <span class="type-key">{{ et.key }}</span>
            <span class="type-count">{{ (stats.entity_by_type || {})[et.key] || 0 }}</span>
          </div>
        </el-card>
      </el-col>

      <el-col :span="14">
        <el-card shadow="never" class="mb-16">
          <template #header>属性定义 — {{ selectedCfg.label || '请选择类型' }}</template>
          <div v-if="selected">
            <div class="prop-group-title">通用属性</div>
            <el-tag v-for="p in commonProps" :key="p" class="prop-tag" type="info">{{ p }}</el-tag>
            <div class="prop-group-title">专属属性</div>
            <template v-if="specificProps.length">
              <el-tag v-for="p in specificProps" :key="p" class="prop-tag">{{ p }}</el-tag>
            </template>
            <span v-else class="empty-hint">无专属属性</span>
          </div>
          <el-empty v-else description="选择左侧实体类型查看属性" :image-size="80" />
        </el-card>

        <el-card shadow="never">
          <template #header>关系类型（{{ relationList.length }} 种）</template>
          <div v-for="rt in relationList" :key="rt.key" class="rel-item">
            <span class="type-dot" :style="{ background: rt.cfg.color }" />
            <span class="type-label">{{ rt.cfg.label }}</span>
            <span class="type-key">{{ rt.key }}</span>
            <span class="type-count">{{ (stats.rel_by_type || {})[rt.key] || 0 }}</span>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({ meta: { type: Object, required: true } })

const stats = computed(() => props.meta.stats || {})
const entityTypeList = computed(() =>
  Object.entries(props.meta.entity_colors || {}).map(([key, cfg]) => ({ key, cfg })),
)
const relationList = computed(() =>
  Object.entries(props.meta.relation_labels || {}).map(([key, cfg]) => ({ key, cfg })),
)

const selected = ref('')
const selectedCfg = computed(() => (selected.value ? props.meta.entity_colors[selected.value] : {}))
const commonProps = ['entity_name', 'description', 'properties_json', 'investment_logic', 'created_at', 'updated_at']
const specificProps = computed(() => (selected.value ? props.meta.entity_schema[selected.value] || [] : []))
</script>

<style scoped>
.mb-16 { margin-bottom: 16px; }
.type-item, .rel-item {
  display: flex; align-items: center; gap: 10px; padding: 8px 12px;
  border-bottom: 1px solid var(--divider, #f0f0f0);
}
.type-item { cursor: pointer; border-radius: 6px; transition: background 0.15s; }
.type-item:hover { background: var(--secondary-bg, #fafafa); }
.type-item.active { background: rgba(22, 119, 255, 0.06); }
.type-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
.type-label { font-weight: 600; color: var(--text-main); }
.type-key { font-family: 'Fira Code', monospace; font-size: 12px; color: var(--text-tertiary); }
.type-count { margin-left: auto; font-weight: 700; font-variant-numeric: tabular-nums; color: var(--text-main); }
.prop-group-title { font-size: 12px; color: var(--text-tertiary); text-transform: uppercase; letter-spacing: 0.04em; margin: 12px 0 6px; font-weight: 600; }
.prop-tag { margin: 0 6px 6px 0; }
.empty-hint { color: var(--text-tertiary); font-size: 13px; }
</style>
