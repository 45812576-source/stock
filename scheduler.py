"""定时任务调度 — KG自动构建 + 推理

规则:
- 每天 06:00 和 20:00 自动执行 KG 增量构建（structured模式，不调Claude）
- 每次构建完成后自动运行一次推理引擎
- 手动触发构建完成后也自动跟一次推理
"""
import json
import logging
import traceback
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


# ── 运行日志 ──────────────────────────────────────────────────

def _ensure_run_log_table():
    execute_insert(
        """CREATE TABLE IF NOT EXISTS scheduler_run_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_id VARCHAR(128) NOT NULL,
            job_name VARCHAR(255) NOT NULL,
            started_at DATETIME NOT NULL,
            finished_at DATETIME DEFAULT NULL,
            status VARCHAR(32) DEFAULT 'running',
            result_summary TEXT DEFAULT NULL,
            error_msg TEXT DEFAULT NULL,
            INDEX idx_job_id (job_id),
            INDEX idx_started (started_at)
        )""", []
    )

_run_log_table_ready = False


def _log_run_start(job_id: str, job_name: str) -> int:
    """记录任务开始，返回 log id"""
    global _run_log_table_ready
    if not _run_log_table_ready:
        _ensure_run_log_table()
        _run_log_table_ready = True
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return execute_insert(
        "INSERT INTO scheduler_run_log (job_id, job_name, started_at, status) VALUES (%s, %s, %s, 'running')",
        [job_id, job_name, now],
    )


def _log_run_finish(log_id: int, status: str, result_summary: str = None, error_msg: str = None):
    """记录任务结束"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    summary = result_summary[:2000] if result_summary and len(result_summary) > 2000 else result_summary
    err = error_msg[:2000] if error_msg and len(error_msg) > 2000 else error_msg
    execute_insert(
        "UPDATE scheduler_run_log SET finished_at=%s, status=%s, result_summary=%s, error_msg=%s WHERE id=%s",
        [now, status, summary, err, log_id],
    )


def _wrap_job(job_id: str, job_name: str, func):
    """包装定时任务函数，自动记录运行日志（区分真正成功 vs 空跑）"""
    def wrapped(*args, **kwargs):
        log_id = _log_run_start(job_id, job_name)
        try:
            result = func(*args, **kwargs)
            summary = json.dumps(result, ensure_ascii=False, default=str) if result else None
            # 判断是否为"空跑"：result 为空或全零值
            status = 'success'
            if result and isinstance(result, dict):
                errors = result.get("errors", [])
                if errors:
                    status = 'partial'  # 有错误但没完全失败
            _log_run_finish(log_id, status, result_summary=summary)
            return result
        except Exception as e:
            _log_run_finish(log_id, 'failed', error_msg=f"{e}\n{traceback.format_exc()[-500:]}")
            logger.error(f"[Scheduler] {job_name} 执行失败: {e}")
    wrapped.__name__ = func.__name__
    wrapped.__doc__ = func.__doc__
    return wrapped


# ── 任务中心注册（让 scheduler 任务在前端 TaskCenter 可见）──────────────
def _task_register(task_id: str, label: str, total: int = 0):
    """注册一个 scheduler 任务到 UI 任务中心，供前端进度轮询"""
    try:
        from routers.datacollect import _bg_tasks
        _bg_tasks[task_id] = {
            "status": "running", "progress": 0, "total": total,
            "current": f"初始化 {label}", "results": [],
            "started_at": datetime.now().isoformat(),
        }
        return _bg_tasks[task_id]
    except Exception:
        return {"status": "running", "progress": 0, "total": total, "current": "", "results": []}


def _task_finish(task_id: str, summary: str = None):
    """标记任务完成"""
    try:
        from routers.datacollect import _bg_tasks
        task = _bg_tasks.get(task_id)
        if task:
            task["status"] = "done"
            task["progress"] = task.get("total", 0)
            if summary:
                task["current"] = summary
    except Exception:
        pass


# ── 任务元数据（批次 + 描述 + 预期结果指标）──────────────────────
_JOB_META = {
    # ─── 批次1: 数据采集 ───
    "zsxq_scanner_morning": {
        "batch": "1-1", "group": "collect",
        "desc": "从知识星球API采集当日新帖(文本/PDF/音频) → 自动提取清洗 → 推入管线 → 运行daily intel scanner提取事件",
        "expect": "zsxq_fetched>0 或 当日确无新帖; intel_events≥0",
    },
    "zsxq_scanner_afternoon": {
        "batch": "1-2", "group": "collect",
        "desc": "午后再次采集知识星球(捕获白天新帖) → 提取清洗 → daily intel scanner",
        "expect": "zsxq_fetched>0 或 当日确无新帖; intel_events≥0",
    },
    "akshare_daily": {
        "batch": "1-3", "group": "collect",
        "desc": "通过AKShare接口采集全市场日K线/涨跌停/北向资金等行情数据 → 写入云端 → 增量同步到本地MySQL",
        "expect": "fetch.inserted>0(交易日) 或 非交易日无数据; sync完成",
    },
    "macro_daily": {
        "batch": "1-4", "group": "collect",
        "desc": "采集Shibor/融资余额/全A估值PE/陆股通/海外ETF等日度宏观指标 → 同步到本地",
        "expect": "fetch中各指标有新数据(交易日); sync完成",
    },
    "macro_monthly": {
        "batch": "1-5", "group": "collect",
        "desc": "每月1日采集M2/社融/PMI等月度宏观数据 → 同步到本地",
        "expect": "新月度数据入库",
    },
    "market_data_monthly": {
        "batch": "1-6", "group": "collect",
        "desc": "每月1日同步市场增量数据(行业分类/概念板块/股票列表更新)",
        "expect": "同步条目数>0",
    },
    "wencai_indicators_daily": {
        "batch": "1-7", "group": "collect",
        "desc": "从同花顺问财采集热门行业指标(涨跌幅/成交量/主力资金) → LLM提取结构化指标 → 写入industry_indicators",
        "expect": "indicators_saved>0(交易日)",
    },
    # ─── 批次2: 知识图谱 ───
    "kg_auto_morning": {
        "batch": "2-1", "group": "kg",
        "desc": "增量构建知识图谱：读取上次构建后新增的cleaned_items → LLM提取实体+关系 → 写入Neo4j → 自动推理补全",
        "expect": "processed>0(有新内容时); entities≥0; relationships≥0",
    },
    "kg_auto_evening": {
        "batch": "2-2", "group": "kg",
        "desc": "晚间增量构建KG(与早间相同逻辑，捕获当天新增内容)",
        "expect": "processed>0(有新内容时); entities≥0; relationships≥0",
    },
    # ─── 批次3: 分析扫描 ───
    "robust_kline_morning": {
        "batch": "3-1", "group": "analysis",
        "desc": "扫描最新研报中提及的股票 → 拉取月K线 → 过滤符合条件标的 → 填充亮点摘要",
        "expect": "reports>0(有新研报时); stocks_extracted≥0; inserted≥0",
    },
    "robust_kline_afternoon": {
        "batch": "3-2", "group": "analysis",
        "desc": "午后再次扫描研报(捕获当日新发研报) → 月K线过滤 → 亮点填充",
        "expect": "reports>0(有新研报时); stocks_extracted≥0; inserted≥0",
    },
    "prediction_monitor_daily": {
        "batch": "3-3", "group": "analysis",
        "desc": "检查已记录的K线形态预测是否兑现(对比实际走势) → 更新命中率统计",
        "expect": "checked>0; hit/miss计数",
    },
    # ─── 批次4: 数据维护 ───
    "diagnose_failed_morning": {
        "batch": "4-1", "group": "maintain",
        "desc": "扫描source_documents中extract_status=failed的记录 → 诊断失败原因 → 对可恢复类型(网络超时等)自动重试",
        "expect": "total=待诊断数; retried>0(有可恢复时); recovered≥0",
    },
    "diagnose_failed_evening": {
        "batch": "4-2", "group": "maintain",
        "desc": "晚间再次诊断failed记录+自动重试(与早间相同)",
        "expect": "total=待诊断数; retried>0(有可恢复时); recovered≥0",
    },
    "auto_summarize_morning": {
        "batch": "4-3", "group": "maintain",
        "desc": "批量对extracted_texts中summary_status=pending的文档生成AI分族摘要(调用LLM)",
        "expect": "ok>0(有待摘要时); fail尽量=0; total=处理总数",
    },
    "auto_summarize_evening": {
        "batch": "4-4", "group": "maintain",
        "desc": "晚间批量摘要生成(与上午相同，处理白天新入管线的文档)",
        "expect": "ok>0(有待摘要时); fail尽量=0; total=处理总数",
    },
    "auto_chunk_index_morning": {
        "batch": "4-5", "group": "maintain",
        "desc": "对family=2的新摘要做向量切片 → 写入本地text_chunks + Milvus向量索引(依赖Milvus运行)",
        "expect": "ok>0(有新摘要时); fail=0(Milvus正常时)",
    },
    "auto_chunk_index_evening": {
        "batch": "4-6", "group": "maintain",
        "desc": "晚间chunk向量索引(与上午相同，处理新生成的摘要)",
        "expect": "ok>0(有新摘要时); fail=0(Milvus正常时)",
    },
    "daily_sync_nightly": {
        "batch": "4-7", "group": "maintain",
        "desc": "夜间兜底：chain_sync同步产业链配置到本地 + theme_merger合并当日主题情报",
        "expect": "chain_sync/theme_merger各自返回处理结果",
    },
    "pending_sweep_nightly": {
        "batch": "4-8", "group": "maintain",
        "desc": "扫描近7天source_documents中遗漏的pending/failed(txt/mixed/image)记录 → 统一提取+推入管线(兜底防遗漏)",
        "expect": "processed=遗漏数; extracted>0; pushed>0(有遗漏时)",
    },
}

# 任务分组映射
_JOB_GROUP_MAP = {
    # 数据采集
    "zsxq_scanner_morning": "collect",
    "zsxq_scanner_afternoon": "collect",
    "akshare_daily": "collect",
    "macro_daily": "collect",
    "macro_monthly": "collect",
    "market_data_monthly": "collect",
    "wencai_indicators_daily": "collect",
    # 知识图谱
    "kg_auto_morning": "kg",
    "kg_auto_evening": "kg",
    # 分析扫描
    "robust_kline_morning": "analysis",
    "robust_kline_afternoon": "analysis",
    "prediction_monitor_daily": "analysis",
    # 数据维护
    "diagnose_failed_morning": "maintain",
    "diagnose_failed_evening": "maintain",
    "daily_sync_nightly": "maintain",
    "pending_sweep_nightly": "maintain",
    "auto_summarize_morning": "maintain",
    "auto_summarize_evening": "maintain",
    "auto_chunk_index_morning": "maintain",
    "auto_chunk_index_evening": "maintain",
}

_JOB_GROUPS = [
    {"key": "collect",  "label": "数据采集", "order": 1},
    {"key": "kg",       "label": "知识图谱", "order": 2},
    {"key": "analysis", "label": "分析扫描", "order": 3},
    {"key": "maintain", "label": "数据维护", "order": 4},
    {"key": "custom",   "label": "自定义",   "order": 5},
]


def get_scheduler_jobs() -> list:
    """获取所有已注册的定时任务信息（分组 + 按下次执行时间排序）"""
    global _custom_jobs_table_ready
    if not _custom_jobs_table_ready:
        _ensure_custom_jobs_table()
        _custom_jobs_table_ready = True

    custom_ids = set()
    try:
        rows = execute_query("SELECT job_id FROM custom_scheduler_jobs") or []
        custom_ids = {r["job_id"] for r in rows}
    except Exception:
        pass

    jobs = []
    for job in scheduler.get_jobs():
        trigger = job.trigger
        schedule = str(trigger)
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else None
        paused = job.next_run_time is None
        is_custom = job.id in custom_ids
        group = "custom" if is_custom else _JOB_GROUP_MAP.get(job.id, "maintain")
        meta = _JOB_META.get(job.id, {})
        jobs.append({
            "job_id": job.id,
            "name": job.name or job.id,
            "batch": meta.get("batch", "—"),
            "group": group,
            "schedule": schedule,
            "next_run": next_run,
            "paused": paused,
            "is_custom": is_custom,
            "description": meta.get("desc") or (job.func.__doc__ or "").strip().split("\n")[0] if job.func else "",
            "expect": meta.get("expect", ""),
        })

    # 按分组 order + 下次执行时间排序
    group_order = {g["key"]: g["order"] for g in _JOB_GROUPS}
    jobs.sort(key=lambda j: (group_order.get(j["group"], 99), j["next_run"] or "9999"))
    return jobs


def get_job_groups() -> list:
    """获取任务分组定义"""
    return _JOB_GROUPS


def get_scheduler_run_history(limit: int = 50, job_id: str = None) -> list:
    """获取最近的任务运行历史（含批次+描述+预期）"""
    global _run_log_table_ready
    if not _run_log_table_ready:
        _ensure_run_log_table()
        _run_log_table_ready = True
    if job_id:
        rows = execute_query(
            "SELECT * FROM scheduler_run_log WHERE job_id=%s ORDER BY started_at DESC LIMIT %s",
            [job_id, limit],
        )
    else:
        rows = execute_query(
            "SELECT * FROM scheduler_run_log ORDER BY started_at DESC LIMIT %s",
            [limit],
        )
    result = []
    for r in (rows or []):
        row = dict(r)
        meta = _JOB_META.get(row.get("job_id", ""), {})
        row["batch"] = meta.get("batch", "—")
        row["desc"] = meta.get("desc", "")
        row["expect"] = meta.get("expect", "")
        result.append(row)
    return result


# ── 可调用的管线函数注册表 ───────────────────────────────

PIPELINE_REGISTRY = {
    "kg_update": {
        "label": "KG 增量构建 + 推理",
        "desc": "增量构建KG，处理上次构建后新增的 cleaned_items，完成后自动跑推理",
    },
    "macro_daily": {
        "label": "宏观日度采集",
        "desc": "Shibor/融资余额/全A PE/陆股通/海外ETF + 同步到本地",
    },
    "macro_monthly": {
        "label": "宏观月度采集",
        "desc": "M2/社融/PMI + 同步到本地",
    },
    "market_data_monthly": {
        "label": "市场增量数据同步",
        "desc": "insider_trading/shareholder_count/institutional_holding/margin_trading/valuation_history/etf_constituent",
    },
    "akshare_daily": {
        "label": "AKShare 行情采集+同步",
        "desc": "A股日线行情数据采集并同步到本地",
    },
    "robust_kline": {
        "label": "Robust Kline 扫描",
        "desc": "扫描报告提及 → 月K线过滤 → 亮点填充",
    },
    "wencai_indicators": {
        "label": "问财行业指标采集",
        "desc": "从问财采集行业指标 → LLM提取 → 写入 industry_indicators",
    },
    "prediction_monitor": {
        "label": "K线预测监控检测",
        "desc": "检测所有活跃预测监控的触发条件是否满足",
    },
    "zsxq_scanner": {
        "label": "zsxq采集+清洗+scanner",
        "desc": "zsxq采集 + 自动提取清洗入管线 + daily intel scanner",
    },
    "diagnose_failed": {
        "label": "自动诊断failed+重试",
        "desc": "诊断 failed 状态记录并智能重试（限制 50 条）",
    },
    "daily_sync_nightly": {
        "label": "chain_sync + theme_merger",
        "desc": "夜间兖底：产业链同步 + 主题合并",
    },
}

_PIPELINE_FUNC_MAP = {}  # 延迟初始化，避免循环导入


def _get_pipeline_func(pipeline_key: str):
    """获取管线函数（延迟导入）"""
    if not _PIPELINE_FUNC_MAP:
        _PIPELINE_FUNC_MAP.update({
            "kg_update": run_kg_update,
            "macro_daily": run_macro_daily,
            "macro_monthly": run_macro_monthly,
            "market_data_monthly": run_market_data_monthly,
            "akshare_daily": run_akshare_daily,
            "robust_kline": _run_robust_kline_daily,
            "wencai_indicators": run_wencai_indicators,
            "prediction_monitor": run_prediction_monitor_job,
        })
    return _PIPELINE_FUNC_MAP.get(pipeline_key)


# ── 自定义任务 CRUD ─────────────────────────────────────

def _ensure_custom_jobs_table():
    execute_insert(
        """CREATE TABLE IF NOT EXISTS custom_scheduler_jobs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_id VARCHAR(128) UNIQUE NOT NULL,
            job_name VARCHAR(255) NOT NULL,
            pipeline_key VARCHAR(128) NOT NULL,
            cron_expr VARCHAR(255) NOT NULL,
            enabled TINYINT DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )""", []
    )

_custom_jobs_table_ready = False


def _parse_cron(cron_expr: str) -> dict:
    """解析 cron 表达式 (minute hour day month day_of_week) 为 CronTrigger 参数"""
    parts = cron_expr.strip().split()
    if len(parts) < 2:
        raise ValueError(f"无效的 cron 表达式: {cron_expr}")
    kwargs = {}
    fields = ['minute', 'hour', 'day', 'month', 'day_of_week']
    for i, part in enumerate(parts[:5]):
        if part != '*':
            kwargs[fields[i]] = part
    return kwargs


def create_custom_job(job_name: str, pipeline_key: str, cron_expr: str) -> dict:
    """创建自定义定时任务"""
    global _custom_jobs_table_ready
    if not _custom_jobs_table_ready:
        _ensure_custom_jobs_table()
        _custom_jobs_table_ready = True

    if pipeline_key not in PIPELINE_REGISTRY:
        return {"ok": False, "error": f"未知管线: {pipeline_key}"}

    job_id = f"custom_{pipeline_key}_{int(datetime.now().timestamp())}"

    # 验证 cron
    try:
        cron_kwargs = _parse_cron(cron_expr)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # 存 DB
    execute_insert(
        "INSERT INTO custom_scheduler_jobs (job_id, job_name, pipeline_key, cron_expr, enabled) VALUES (%s,%s,%s,%s,1)",
        [job_id, job_name, pipeline_key, cron_expr],
    )

    # 注册到 scheduler
    func = _get_pipeline_func(pipeline_key)
    if func:
        scheduler.add_job(
            _wrap_job(job_id, job_name, func),
            CronTrigger(**cron_kwargs),
            id=job_id, replace_existing=True, name=job_name,
        )

    return {"ok": True, "job_id": job_id}


def update_custom_job(job_id: str, job_name: str = None, cron_expr: str = None, enabled: bool = None) -> dict:
    """编辑定时任务（自定义和系统任务均支持改调度规则）"""
    global _custom_jobs_table_ready
    if not _custom_jobs_table_ready:
        _ensure_custom_jobs_table()
        _custom_jobs_table_ready = True

    # 检查是否是自定义任务
    rows = execute_query("SELECT * FROM custom_scheduler_jobs WHERE job_id=%s", [job_id])
    is_custom = bool(rows)

    if cron_expr:
        try:
            cron_kwargs = _parse_cron(cron_expr)
        except Exception as e:
            return {"ok": False, "error": str(e)}

        # 重新调度
        try:
            scheduler.reschedule_job(job_id, trigger=CronTrigger(**cron_kwargs))
        except Exception as e:
            return {"ok": False, "error": f"重新调度失败: {e}"}

    if enabled is not None:
        if enabled:
            scheduler.resume_job(job_id)
        else:
            scheduler.pause_job(job_id)

    # 如果是自定义任务，更新 DB
    if is_custom:
        updates = []
        params = []
        if job_name:
            updates.append("job_name=%s")
            params.append(job_name)
        if cron_expr:
            updates.append("cron_expr=%s")
            params.append(cron_expr)
        if enabled is not None:
            updates.append("enabled=%s")
            params.append(1 if enabled else 0)
        if updates:
            params.append(job_id)
            execute_insert(f"UPDATE custom_scheduler_jobs SET {','.join(updates)} WHERE job_id=%s", params)

    return {"ok": True}


def delete_custom_job(job_id: str) -> dict:
    """删除自定义任务"""
    global _custom_jobs_table_ready
    if not _custom_jobs_table_ready:
        _ensure_custom_jobs_table()
        _custom_jobs_table_ready = True

    rows = execute_query("SELECT * FROM custom_scheduler_jobs WHERE job_id=%s", [job_id])
    if not rows:
        return {"ok": False, "error": "只能删除自定义任务"}

    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    execute_insert("DELETE FROM custom_scheduler_jobs WHERE job_id=%s", [job_id])
    return {"ok": True}


def load_custom_jobs():
    """启动时加载自定义任务并注册到 scheduler"""
    global _custom_jobs_table_ready
    if not _custom_jobs_table_ready:
        _ensure_custom_jobs_table()
        _custom_jobs_table_ready = True

    rows = execute_query("SELECT * FROM custom_scheduler_jobs WHERE enabled=1") or []
    for row in rows:
        func = _get_pipeline_func(row["pipeline_key"])
        if not func:
            continue
        try:
            cron_kwargs = _parse_cron(row["cron_expr"])
            scheduler.add_job(
                _wrap_job(row["job_id"], row["job_name"], func),
                CronTrigger(**cron_kwargs),
                id=row["job_id"], replace_existing=True, name=row["job_name"],
            )
        except Exception as e:
            logger.warning(f"[Scheduler] 加载自定义任务 {row['job_id']} 失败: {e}")


def get_pipeline_options() -> list:
    """获取可选的管线列表供前端创建任务用"""
    return [{"key": k, "label": v["label"], "desc": v["desc"]} for k, v in PIPELINE_REGISTRY.items()]

# ── 状态追踪 ──────────────────────────────────────────────────

def _ensure_state_table():
    execute_insert(
        """CREATE TABLE IF NOT EXISTS scheduler_state (
            `key` VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )""", []
    )

def _get_state(key, default=None):
    _ensure_state_table()
    rows = execute_query("SELECT value FROM scheduler_state WHERE `key`=%s", [key])
    return rows[0]["value"] if rows else default

def _set_state(key, value):
    _ensure_state_table()
    execute_insert(
        """INSERT INTO scheduler_state (`key`, value, updated_at)
           VALUES (%s, %s, CURRENT_TIMESTAMP)
           ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP""",
        [key, str(value)],
    )


# ── KG 构建任务 ──────────────────────────────────────────────

def run_kg_update():
    """增量构建KG：只处理上次构建后新增的 cleaned_items"""
    since = _get_state("kg_last_auto_update")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[Scheduler] KG自动构建开始, since={since}")

    try:
        from knowledge_graph.kg_updater import update_from_cleaned_items
        result = update_from_cleaned_items(since_date=since, use_claude=False)
        _set_state("kg_last_auto_update", now_str)
        logger.info(f"[Scheduler] KG构建完成: {result}")

        # 构建完成 → 自动跑推理
        run_inference_after_build()
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] KG自动构建失败: {e}")
        raise


def run_inference_after_build():
    """构建完成后自动运行推理引擎（4条规则全跑）"""
    logger.info("[Scheduler] 自动推理开始")
    try:
        from routers.knowledge_graph import _run_inference_sync
        discovered = _run_inference_sync(rule_type="all", auto_accept=True)
        logger.info(f"[Scheduler] 自动推理完成, 发现 {len(discovered)} 条关系, 已自动采纳高置信度结果")
        return discovered
    except Exception as e:
        logger.exception(f"[Scheduler] 自动推理失败: {e}")
        return []


# ── 宏观数据采集任务 ──────────────────────────────────────────

def run_macro_daily():
    """日度宏观数据采集：Shibor/融资余额/全A PE/陆股通/海外ETF + 同步到本地"""
    logger.info("[Scheduler] 宏观日度采集开始")
    try:
        from ingestion.macro_fetcher import fetch_all_macro
        from utils.db_utils import sync_macro_to_local
        fetch_result = fetch_all_macro()
        sync_result = sync_macro_to_local()
        logger.info(f"[Scheduler] 宏观日度采集完成: fetch={fetch_result}, sync={sync_result}")
        return {"fetch": fetch_result, "sync": sync_result}
    except Exception as e:
        logger.exception(f"[Scheduler] 宏观日度采集失败: {e}")
        return {"error": str(e)}


def run_macro_monthly():
    """月度宏观数据采集：M2/社融/PMI + 同步到本地"""
    logger.info("[Scheduler] 宏观月度采集开始")
    try:
        from ingestion.macro_fetcher import fetch_all_macro_monthly
        from utils.db_utils import sync_macro_to_local
        fetch_result = fetch_all_macro_monthly()
        sync_result = sync_macro_to_local()
        logger.info(f"[Scheduler] 宏观月度采集完成: fetch={fetch_result}, sync={sync_result}")
        return {"fetch": fetch_result, "sync": sync_result}
    except Exception as e:
        logger.exception(f"[Scheduler] 宏观月度采集失败: {e}")
        return {"error": str(e)}


def run_market_data_monthly():
    """月度市场增量数据同步：insider_trading / shareholder_count / institutional_holding /
    margin_trading / valuation_history (最近30天增量) + etf_constituent (全量刷新)。

    同步策略：查本地各表已有的最大 trade_date/end_date/report_date，
    只从云端拉取比本地更新的记录（增量同步）。
    ETF 成分股每次全量刷新最新一期。

    调度：每月第 5、15、25 日 20:30 执行。
    """
    logger.info("[Scheduler] 市场增量数据月度同步开始")
    result = {
        "insider_trading": 0,
        "shareholder_count": 0,
        "institutional_holding": 0,
        "margin_trading": 0,
        "valuation_history": 0,
        "etf_constituent": 0,
    }
    try:
        from utils.db_utils import _get_conn, _get_cloud_conn
        local = _get_conn()
        cloud = _get_cloud_conn()
        try:
            with local.cursor() as lc, cloud.cursor() as cc:

                # ── 1. insider_trading — 最近30天增量 ────────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM insider_trading")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,trade_date,person_name,person_role,"
                    "direction,trade_shares,trade_price,trade_amount,hold_shares_after,relation "
                    "FROM insider_trading WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO insider_trading
                           (id,stock_code,stock_name,trade_date,person_name,person_role,
                            direction,trade_shares,trade_price,trade_amount,hold_shares_after,relation)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['trade_date'],
                         r.get('person_name'), r.get('person_role'), r.get('direction'),
                         r.get('trade_shares'), r.get('trade_price'), r.get('trade_amount'),
                         r.get('hold_shares_after'), r.get('relation')],
                    )
                local.commit()
                result['insider_trading'] = len(rows)

                # ── 2. shareholder_count — 最新一期 ──────────────────────────
                lc.execute("SELECT COALESCE(MAX(end_date), '1970-01-01') as max_d FROM shareholder_count")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,end_date,holder_count,holder_count_change,"
                    "change_pct,avg_share_per_holder,avg_amount_per_holder "
                    "FROM shareholder_count WHERE end_date > %s ORDER BY end_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO shareholder_count
                           (id,stock_code,stock_name,end_date,holder_count,holder_count_change,
                            change_pct,avg_share_per_holder,avg_amount_per_holder)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['end_date'],
                         r.get('holder_count'), r.get('holder_count_change'), r.get('change_pct'),
                         r.get('avg_share_per_holder'), r.get('avg_amount_per_holder')],
                    )
                local.commit()
                result['shareholder_count'] = len(rows)

                # ── 3. institutional_holding — 最新季报 ──────────────────────
                lc.execute("SELECT COALESCE(MAX(report_date), '1970-01-01') as max_d FROM institutional_holding")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,report_date,institution_type,"
                    "hold_shares,hold_ratio,hold_change,hold_value "
                    "FROM institutional_holding WHERE report_date > %s ORDER BY report_date LIMIT 10000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO institutional_holding
                           (id,stock_code,stock_name,report_date,institution_type,
                            hold_shares,hold_ratio,hold_change,hold_value)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['report_date'],
                         r.get('institution_type'), r.get('hold_shares'),
                         r.get('hold_ratio'), r.get('hold_change'), r.get('hold_value')],
                    )
                local.commit()
                result['institutional_holding'] = len(rows)

                # ── 4. margin_trading — 最近30天增量 ─────────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM margin_trading")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,trade_date,margin_balance,margin_buy_amount,"
                    "margin_repay_amount,short_balance,short_sell_volume,short_repay_volume,"
                    "short_sell_amount,total_balance,exchange "
                    "FROM margin_trading WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO margin_trading
                           (id,stock_code,stock_name,trade_date,margin_balance,margin_buy_amount,
                            margin_repay_amount,short_balance,short_sell_volume,short_repay_volume,
                            short_sell_amount,total_balance,exchange)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['trade_date'],
                         r.get('margin_balance'), r.get('margin_buy_amount'),
                         r.get('margin_repay_amount'), r.get('short_balance'),
                         r.get('short_sell_volume'), r.get('short_repay_volume'),
                         r.get('short_sell_amount'), r.get('total_balance'), r.get('exchange')],
                    )
                local.commit()
                result['margin_trading'] = len(rows)

                # ── 5. valuation_history — 最近30天增量 ──────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM valuation_history")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,trade_date,pe_ttm,pb_mrq,ps_ttm,"
                    "dividend_yield,market_cap,circ_market_cap "
                    "FROM valuation_history WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 50000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO valuation_history
                           (id,stock_code,trade_date,pe_ttm,pb_mrq,ps_ttm,
                            dividend_yield,market_cap,circ_market_cap)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r['trade_date'],
                         r.get('pe_ttm'), r.get('pb_mrq'), r.get('ps_ttm'),
                         r.get('dividend_yield'), r.get('market_cap'), r.get('circ_market_cap')],
                    )
                local.commit()
                result['valuation_history'] = len(rows)

                # ── 6. etf_constituent — 最新一期全量刷新 ────────────────────
                cc.execute("SELECT MAX(report_date) as max_d FROM etf_constituent")
                r = cc.fetchone()
                latest_rd = r['max_d'] if r else None
                if latest_rd:
                    cc.execute(
                        "SELECT id,etf_code,etf_name,stock_code,stock_name,weight,shares,amount,report_date "
                        "FROM etf_constituent WHERE report_date=%s LIMIT 10000",
                        [latest_rd],
                    )
                    rows = cc.fetchall()
                    for r in rows:
                        lc.execute(
                            """INSERT IGNORE INTO etf_constituent
                               (id,etf_code,etf_name,stock_code,stock_name,weight,shares,amount,report_date)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [r['id'], r['etf_code'], r.get('etf_name'), r['stock_code'],
                             r.get('stock_name'), r.get('weight'), r.get('shares'),
                             r.get('amount'), r.get('report_date')],
                        )
                    local.commit()
                    result['etf_constituent'] = len(rows)

        finally:
            cloud.close()
            local.close()

        logger.info(f"[Scheduler] 市场增量数据月度同步完成: {result}")
    except Exception as e:
        logger.exception(f"[Scheduler] 市场增量数据月度同步失败: {e}")
        result['error'] = str(e)

    return result


# ── K线预测监控检测 ─────────────────────────────────────────

def run_prediction_monitor_job():
    """每交易日 19:30：检测所有活跃预测监控的触发条件是否满足"""
    logger.info("[Scheduler] K线预测监控检测开始")
    try:
        from analysis.kline_monitor import run_prediction_monitor
        result = run_prediction_monitor()
        logger.info(f"[Scheduler] K线预测监控检测完成: {result}")
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] K线预测监控检测失败: {e}")
        return {"error": str(e)}


# ── 问财行业指标采集 ─────────────────────────────────────────

def run_wencai_indicators():
    """每天 21:00：从问财采集行业指标 → LLM提取 → 写入 industry_indicators"""
    logger.info("[Scheduler] 问财行业指标采集开始")
    try:
        from ingestion.wencai_indicator_fetcher import run_wencai_indicator_fetch
        result = run_wencai_indicator_fetch()
        logger.info(f"[Scheduler] 问财行业指标采集完成: {result}")
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] 问财行业指标采集失败: {e}")
        raise


# ── Robust Kline 日扫描 ──────────────────────────────────────


def run_akshare_daily():
    """每天 19:00：AKShare 行情数据采集（写云端） + 增量同步到本地"""
    logger.info("[Scheduler] AKShare 日度采集开始")
    try:
        from ingestion.akshare_source import fetch_all_daily_data
        fetch_result = fetch_all_daily_data()
        logger.info(f"[Scheduler] AKShare 采集完成: {fetch_result}")
    except Exception as e:
        logger.exception(f"[Scheduler] AKShare 采集失败: {e}")
        fetch_result = {"error": str(e)}

    # 采集完成后同步到本地
    try:
        from utils.db_utils import sync_akshare_to_local
        sync_result = sync_akshare_to_local()
        logger.info(f"[Scheduler] AKShare 同步到本地完成: {sync_result}")
    except Exception as e:
        logger.exception(f"[Scheduler] AKShare 同步到本地失败: {e}")
        sync_result = {"error": str(e)}

    return {"fetch": fetch_result, "sync": sync_result}


def _run_robust_kline_daily():
    """每天16:00：扫描报告提及 → 月K线过滤 → 亮点填充"""
    logger.info("[Scheduler] Robust Kline 日扫描开始")
    try:
        from routers.robust_kline import run_robust_kline_scan
        result = run_robust_kline_scan()
        logger.info(f"[Scheduler] Robust Kline 完成: {result}")
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] Robust Kline 失败: {e}")
        raise


# ── 调度器启停 ──────────────────────────────────────────────

def start_scheduler():
    """启动定时任务（FastAPI启动时调用）"""
    if scheduler.running:
        return

    # 每天 06:00
    scheduler.add_job(
        _wrap_job("kg_auto_morning", "KG早间自动构建", run_kg_update),
        CronTrigger(hour=6, minute=0),
        id="kg_auto_morning", replace_existing=True,
        name="KG早间自动构建",
    )
    # 每天 20:00
    scheduler.add_job(
        _wrap_job("kg_auto_evening", "KG晚间自动构建", run_kg_update),
        CronTrigger(hour=20, minute=0),
        id="kg_auto_evening", replace_existing=True,
        name="KG晚间自动构建",
    )

    # 每天 18:30 — 宏观日度采集
    scheduler.add_job(
        _wrap_job("macro_daily", "宏观日度采集", run_macro_daily),
        CronTrigger(hour=18, minute=30),
        id="macro_daily", replace_existing=True,
        name="宏观日度采集",
    )
    # 每月 15 日 19:00 — 宏观月度采集
    scheduler.add_job(
        _wrap_job("macro_monthly", "宏观月度采集", run_macro_monthly),
        CronTrigger(day=15, hour=19, minute=0),
        id="macro_monthly", replace_existing=True,
        name="宏观月度采集",
    )
    # 每月 5/15/25 日 20:30 — 市场增量数据同步
    scheduler.add_job(
        _wrap_job("market_data_monthly", "市场增量数据月度同步", run_market_data_monthly),
        CronTrigger(day="5,15,25", hour=20, minute=30),
        id="market_data_monthly", replace_existing=True,
        name="市场增量数据月度同步",
    )
    # 每天 19:00 — AKShare 行情数据采集 + 同步
    scheduler.add_job(
        _wrap_job("akshare_daily", "AKShare行情日度采集+同步", run_akshare_daily),
        CronTrigger(hour=19, minute=0),
        id="akshare_daily", replace_existing=True,
        name="AKShare行情日度采集+同步",
    )
    # 每天 06:00 + 16:00 — Robust Kline 扫描
    scheduler.add_job(
        _wrap_job("robust_kline_morning", "Robust Kline 早间扫描", _run_robust_kline_daily),
        CronTrigger(hour=6, minute=0),
        id="robust_kline_morning", replace_existing=True,
        name="Robust Kline 早间扫描",
    )
    scheduler.add_job(
        _wrap_job("robust_kline_afternoon", "Robust Kline 午后扫描", _run_robust_kline_daily),
        CronTrigger(hour=16, minute=0),
        id="robust_kline_afternoon", replace_existing=True,
        name="Robust Kline 午后扫描",
    )
    # 每天 21:00 — 问财行业指标采集
    scheduler.add_job(
        _wrap_job("wencai_indicators_daily", "问财行业指标采集", run_wencai_indicators),
        CronTrigger(hour=21, minute=0),
        id="wencai_indicators_daily", replace_existing=True,
        name="问财行业指标采集",
    )
    # 每交易日 19:30 — 预测监控检测（日线数据入库后）
    scheduler.add_job(
        _wrap_job("prediction_monitor_daily", "K线预测监控检测", run_prediction_monitor_job),
        CronTrigger(hour=19, minute=30, day_of_week='mon-fri'),
        id="prediction_monitor_daily", replace_existing=True,
        name="K线预测监控检测",
    )

    # 每天 07:00 + 17:00 — zsxq 采集 + 自动提取清洗入管线 + daily intel scanner
    def _auto_extract_and_pipe(scan_day: str):
        """采集后自动提取+清洗+推入管线（全类型，无需人工审核）

        - pending/failed（txt/mixed/image）: 调 _do_extract_and_save 提取+清洗
        - extracted（PDF/xlsx/audio 采集时已即时提取）: 直接推管线
        """
        try:
            from utils.db_utils import execute_cloud_query
            from routers.datacollect import _do_extract_and_save
            from ingestion.source_extractor import push_to_extracted_texts_by_ids
        except Exception as e:
            logger.warning(f"[Scheduler] zsxq 自动清洗依赖导入失败 {scan_day}: {e}")
            return
        try:
            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, text_content, oss_url,
                          extracted_text, extract_status
                   FROM source_documents
                   WHERE source='zsxq' AND DATE(publish_date)=%s
                     AND extract_status IN ('pending','failed','extracted')""",
                [scan_day],
            ) or []
        except Exception as e:
            logger.warning(f"[Scheduler] zsxq 自动清洗查询失败 {scan_day}: {e}")
            return

        pipe_ids = []
        extracted = 0
        for r in rows:
            d = dict(r)
            if d.get("extract_status") in ("pending", "failed"):
                try:
                    _do_extract_and_save(d)
                    extracted += 1
                    pipe_ids.append(d["id"])
                except Exception as e:
                    logger.warning(f"[Scheduler] zsxq 自动提取失败 id={d['id']}: {e}")
            else:
                pipe_ids.append(d["id"])

        pushed = 0
        if pipe_ids:
            try:
                result = push_to_extracted_texts_by_ids(pipe_ids)
                pushed = result.get("pushed", 0)
            except Exception as e:
                logger.warning(f"[Scheduler] zsxq 自动推入管线失败 {scan_day}: {e}")
        logger.info(
            f"[Scheduler] zsxq 自动清洗入管线 {scan_day}: 提取{extracted} 推入{pushed}/{len(pipe_ids)}"
        )

    def _run_zsxq_and_scanner(scan_date: str = None):
        from datetime import date as _date
        day = scan_date or str(_date.today())
        summary = {"zsxq_fetched": 0, "extracted": 0, "pushed": 0, "intel_events": 0, "errors": []}

        # zsxq 采集
        try:
            from ingestion.zsxq_source import fetch_zsxq_data
            result = fetch_zsxq_data(start_date=day, end_date=day)
            summary["zsxq_fetched"] = result.get("saved", 0) if isinstance(result, dict) else (result or 0)
            logger.info(f"[Scheduler] zsxq 采集完成 {day}: {result}")
        except Exception as e:
            summary["errors"].append(f"zsxq采集: {e}")
            logger.warning(f"[Scheduler] zsxq 采集失败 {day}: {e}")

        # 采集后自动提取清洗入管线（全类型）
        _auto_extract_and_pipe(day)

        # daily intel scanner
        try:
            from datetime import date as _date2
            from daily_intel.scanner import run_daily_intel_pipeline
            result = run_daily_intel_pipeline(_date2.fromisoformat(day))
            summary["intel_events"] = result.get("events", 0) if isinstance(result, dict) else (result or 0)
            logger.info(f"[Scheduler] daily intel scanner 完成 {day}: {result}")
        except Exception as e:
            summary["errors"].append(f"scanner: {e}")
            logger.warning(f"[Scheduler] daily intel scanner 失败 {day}: {e}")

        # 如果全部失败，抛出异常让 _wrap_job 标记 failed
        if summary["errors"] and summary["zsxq_fetched"] == 0 and summary["intel_events"] == 0:
            raise RuntimeError(f"zsxq+scanner 全部失败: {'; '.join(summary['errors'])}")
        return summary

    scheduler.add_job(
        _wrap_job("zsxq_scanner_morning", "zsxq采集+daily intel scanner 早间", _run_zsxq_and_scanner),
        CronTrigger(hour=7, minute=0),
        id="zsxq_scanner_morning", replace_existing=True,
        name="zsxq采集+daily intel scanner 早间",
        misfire_grace_time=8 * 3600,  # 断线8小时内重连仍补跑
    )
    scheduler.add_job(
        _wrap_job("zsxq_scanner_afternoon", "zsxq采集+daily intel scanner 午后", _run_zsxq_and_scanner),
        CronTrigger(hour=17, minute=0),
        id="zsxq_scanner_afternoon", replace_existing=True,
        name="zsxq采集+daily intel scanner 午后",
        misfire_grace_time=8 * 3600,
    )

    # 每天 08:30 + 18:30 — 自动诊断 failed 记录 + 智能重试
    def _run_diagnose_failed():
        import time as _time
        tid = f"sched_diagnose_{int(_time.time())}"
        task = _task_register(tid, "诊断failed重试")
        try:
            from ingestion.source_extractor import diagnose_and_retry_failed
            task["total"] = 50
            task["current"] = "诊断中..."
            result = diagnose_and_retry_failed(limit=50, source="zsxq")
            summary = f"完成: {result}" if result else "完成"
            logger.info(f"[Scheduler] 自动诊断failed完成: {result}")
            _task_finish(tid, str(summary)[:100])
            return result or {"msg": "无failed记录"}
        except Exception as e:
            logger.warning(f"[Scheduler] 自动诊断failed失败: {e}")
            _task_finish(tid, f"失败: {e}")
            raise

    scheduler.add_job(
        _wrap_job("diagnose_failed_morning", "自动诊断failed+重试 早间", _run_diagnose_failed),
        CronTrigger(hour=8, minute=30),
        id="diagnose_failed_morning", replace_existing=True,
        name="自动诊断failed+重试 早间",
    )
    scheduler.add_job(
        _wrap_job("diagnose_failed_evening", "自动诊断failed+重试 晚间", _run_diagnose_failed),
        CronTrigger(hour=18, minute=30),
        id="diagnose_failed_evening", replace_existing=True,
        name="自动诊断failed+重试 晚间",
    )

    # 启动时 backfill：补跑过去7天内 zsxq 有数据但 scanner 未执行的日期
    def _backfill_missing_scanner_days():
        try:
            from datetime import date as _date, timedelta
            from utils.db_utils import execute_cloud_query
            today = _date.today()
            check_days = [(today - timedelta(days=i)).isoformat() for i in range(1, 8)]

            # 有 zsxq source_documents 的日期
            rows = execute_cloud_query(
                """SELECT DISTINCT DATE(publish_date) as day
                   FROM source_documents
                   WHERE source='zsxq' AND publish_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)""", []
            ) or []
            has_source = {str(r["day"]) for r in rows}

            # 已有 daily_intel_stocks 的日期
            rows2 = execute_cloud_query(
                """SELECT DISTINCT scan_date as day
                   FROM daily_intel_stocks
                   WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)""", []
            ) or []
            has_scanner = {str(r["day"]) for r in rows2}

            missing = sorted(has_source - has_scanner - {today.isoformat()})
            if missing:
                logger.info(f"[Scheduler] backfill 发现缺失日期: {missing}")
                for day in missing:
                    logger.info(f"[Scheduler] backfill 补跑 {day}")
                    _run_zsxq_and_scanner(scan_date=day)
            else:
                logger.info("[Scheduler] backfill 无缺失日期")
        except Exception as e:
            logger.warning(f"[Scheduler] backfill 失败: {e}")

    import threading
    threading.Thread(target=_backfill_missing_scanner_days, daemon=True, name="backfill").start()

    # 每天 23:00 — chain_sync + theme_merger 夜间兜底
    def _run_daily_sync_nightly():
        summary = {"chain_sync": None, "theme_merger": None, "errors": []}
        try:
            from config.chain_sync import run_chain_sync
            result = run_chain_sync()
            summary["chain_sync"] = result
            logger.info(f"[Scheduler] chain_sync 夜间完成: {result}")
        except Exception as e:
            summary["errors"].append(f"chain_sync: {e}")
            logger.warning(f"[Scheduler] chain_sync 夜间失败: {e}")
        try:
            from daily_intel.theme_merger import run_theme_merge
            result = run_theme_merge()
            summary["theme_merger"] = result
            logger.info(f"[Scheduler] theme_merger 夜间完成: {result}")
        except Exception as e:
            summary["errors"].append(f"theme_merger: {e}")
            logger.warning(f"[Scheduler] theme_merger 夜间失败: {e}")
        if len(summary["errors"]) == 2:
            raise RuntimeError(f"chain_sync+theme_merger 全部失败: {summary['errors']}")
        return summary

    scheduler.add_job(
        _wrap_job("daily_sync_nightly", "chain_sync + theme_merger 夜间兜底", _run_daily_sync_nightly),
        CronTrigger(hour=23, minute=0),
        id="daily_sync_nightly", replace_existing=True,
        name="chain_sync + theme_merger 夜间兜底",
    )

    # ── 每天 22:00 — pending sweep：补处理近7天内遗漏的 pending 文档 ────────
    def _run_pending_sweep():
        """扫描近7天内所有 pending 状态的 txt/mixed/image，统一提取+入管线"""
        import time as _time
        tid = f"sched_sweep_{int(_time.time())}"
        task = _task_register(tid, "pending sweep")
        try:
            from datetime import date as _date, timedelta
            from utils.db_utils import execute_cloud_query
            from routers.datacollect import _do_extract_and_save
            from ingestion.source_extractor import push_to_extracted_texts_by_ids

            today = _date.today()
            start_day = (today - timedelta(days=7)).isoformat()

            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, text_content, oss_url,
                          extracted_text, extract_status
                   FROM source_documents
                   WHERE source='zsxq' AND publish_date >= %s
                     AND extract_status IN ('pending','failed')
                     AND file_type IN ('txt','mixed','image')
                   ORDER BY publish_date DESC
                   LIMIT 200""",
                [start_day],
            ) or []

            if not rows:
                logger.info("[Scheduler] pending sweep: 无遗漏文档")
                _task_finish(tid, "无遗漏文档")
                return {"processed": 0, "extracted": 0, "pushed": 0, "msg": "无遗漏文档"}

            task["total"] = len(rows) + 1  # +1 for push step
            task["current"] = f"待处理 {len(rows)} 条"
            pipe_ids = []
            extracted = 0
            for i, r in enumerate(rows):
                d = dict(r)
                try:
                    _do_extract_and_save(d)
                    extracted += 1
                    pipe_ids.append(d["id"])
                except Exception:
                    pass
                task["progress"] = i + 1
                task["current"] = f"提取中 {i+1}/{len(rows)}"

            pushed = 0
            if pipe_ids:
                task["current"] = f"推入管线 {len(pipe_ids)} 条"
                try:
                    result = push_to_extracted_texts_by_ids(pipe_ids)
                    pushed = result.get("pushed", 0)
                except Exception as e:
                    logger.warning(f"[Scheduler] pending sweep push失败: {e}")

            summary = f"完成: 处理{len(rows)} 提取{extracted} 推入{pushed}"
            logger.info(f"[Scheduler] pending sweep {summary}")
            _task_finish(tid, summary)
            return {"processed": len(rows), "extracted": extracted, "pushed": pushed}
        except Exception as e:
            logger.warning(f"[Scheduler] pending sweep 失败: {e}")
            _task_finish(tid, f"失败: {e}")
            raise

    scheduler.add_job(
        _wrap_job("pending_sweep_nightly", "pending sweep 夜间兜底", _run_pending_sweep),
        CronTrigger(hour=22, minute=0),
        id="pending_sweep_nightly", replace_existing=True,
        name="pending sweep 夜间兜底(txt/mixed/image)",
    )

    # ── 每天 09:30 + 20:30 — 自动摘要生成 ──────────────────────────────────
    def _run_auto_summarize():
        """批量对 summary_status='pending' 的 extracted_texts 生成分族摘要"""
        import time as _time
        tid = f"sched_summarize_{int(_time.time())}"
        task = _task_register(tid, "自动摘要生成")
        try:
            from cleaning.content_summarizer import summarize_single
            from utils.db_utils import execute_cloud_query as _cq

            rows = _cq(
                """SELECT id FROM extracted_texts
                   WHERE summary_status='pending'
                     AND CHAR_LENGTH(TRIM(COALESCE(full_text,''))) >= 20
                   ORDER BY id DESC
                   LIMIT 1000""",
                None,
            ) or []

            if not rows:
                logger.info("[Scheduler] auto_summarize: 无待摘要文档")
                _task_finish(tid, "无待摘要文档")
                return {"ok": 0, "fail": 0, "total": 0, "msg": "无待摘要文档"}

            task["total"] = len(rows)
            task["current"] = f"待摘要 {len(rows)} 条"
            ok = fail = 0
            for i, r in enumerate(rows):
                try:
                    result = summarize_single(r["id"])
                    if result:
                        ok += 1
                    else:
                        fail += 1
                except Exception as e:
                    logger.debug(f"[Scheduler] summarize id={r['id']} 失败: {e}")
                    fail += 1
                task["progress"] = i + 1
                task["current"] = f"摘要 {i+1}/{len(rows)} (成功{ok} 失败{fail})"

            summary = f"完成: 成功{ok} 失败{fail} (共{len(rows)})"
            logger.info(f"[Scheduler] auto_summarize {summary}")
            _task_finish(tid, summary)
            return {"ok": ok, "fail": fail, "total": len(rows)}
        except Exception as e:
            logger.warning(f"[Scheduler] auto_summarize 整体失败: {e}")
            _task_finish(tid, f"失败: {e}")
            raise  # 让 _wrap_job 标记 failed

    scheduler.add_job(
        _wrap_job("auto_summarize_morning", "自动摘要生成(上午)", _run_auto_summarize),
        CronTrigger(hour=9, minute=30),
        id="auto_summarize_morning", replace_existing=True,
        name="自动摘要生成(上午)",
    )
    scheduler.add_job(
        _wrap_job("auto_summarize_evening", "自动摘要生成(晚间)", _run_auto_summarize),
        CronTrigger(hour=20, minute=30),
        id="auto_summarize_evening", replace_existing=True,
        name="自动摘要生成(晚间)",
    )

    # ── 每天 10:00 + 21:30 — 自动摘要 chunk 索引 ──────────────────────────────
    def _run_auto_chunk_index():
        """批量对 family=2 的新摘要建向量索引（Milvus + MySQL text_chunks）"""
        import time as _time
        tid = f"sched_chunk_{int(_time.time())}"
        task = _task_register(tid, "chunk索引")
        try:
            from retrieval.summary_chunker import index_summary_chunk, SUMMARY_CHUNK_INDEX_OFFSET
            from utils.db_utils import execute_cloud_query as _cq
            from utils.db_utils import execute_query as _lq

            cs_rows = _cq(
                """SELECT cs.id FROM content_summaries cs
                   WHERE cs.family = 2
                   ORDER BY cs.id DESC
                   LIMIT 2000""",
                None,
            ) or []

            if not cs_rows:
                logger.info("[Scheduler] auto_chunk_index: 无待索引摘要")
                _task_finish(tid, "无待索引摘要")
                return {"ok": 0, "skip": 0, "fail": 0, "msg": "无待索引摘要"}

            # 过滤已索引的
            existing = set()
            try:
                local_rows = _lq(
                    f"SELECT chunk_index FROM text_chunks WHERE chunk_type='summary' AND chunk_index >= {SUMMARY_CHUNK_INDEX_OFFSET}"
                ) or []
                existing = {r["chunk_index"] for r in local_rows}
            except Exception:
                pass

            pending = [r for r in cs_rows if (SUMMARY_CHUNK_INDEX_OFFSET + r["id"]) not in existing]

            if not pending:
                logger.info("[Scheduler] auto_chunk_index: 全部已索引")
                _task_finish(tid, "全部已索引")
                return {"ok": 0, "skip": 0, "fail": 0, "msg": "全部已索引"}

            task["total"] = min(len(pending), 1000)
            task["current"] = f"待索引 {len(pending)} 条"
            ok = skip = fail = 0
            for i, r in enumerate(pending[:1000]):
                try:
                    result = index_summary_chunk(r["id"])
                    if result:
                        ok += 1
                    else:
                        skip += 1
                except Exception as e:
                    logger.debug(f"[Scheduler] chunk_index cs_id={r['id']} 失败: {e}")
                    fail += 1
                task["progress"] = i + 1
                task["current"] = f"索引 {i+1}/{task['total']} (成功{ok} 失败{fail})"

            summary = f"完成: 索引{ok} 跳过{skip} 失败{fail}"
            logger.info(f"[Scheduler] auto_chunk_index {summary}")
            _task_finish(tid, summary)
            return {"ok": ok, "skip": skip, "fail": fail}
        except Exception as e:
            logger.warning(f"[Scheduler] auto_chunk_index 整体失败: {e}")
            raise  # 让 _wrap_job 标记 failed
            _task_finish(tid, f"失败: {e}")

    scheduler.add_job(
        _wrap_job("auto_chunk_index_morning", "自动摘要chunk索引(上午)", _run_auto_chunk_index),
        CronTrigger(hour=10, minute=0),
        id="auto_chunk_index_morning", replace_existing=True,
        name="自动摘要chunk索引(上午)",
    )
    scheduler.add_job(
        _wrap_job("auto_chunk_index_evening", "自动摘要chunk索引(晚间)", _run_auto_chunk_index),
        CronTrigger(hour=21, minute=30),
        id="auto_chunk_index_evening", replace_existing=True,
        name="自动摘要chunk索引(晚间)",
    )

    # 启动前清理上次遗留的 running 状态（服务重启导致的孤儿任务）
    try:
        from utils.db_utils import execute_insert
        cleaned = execute_insert(
            "UPDATE scheduler_run_log SET status='orphaned', error_msg='服务重启导致任务中断', finished_at=NOW() "
            "WHERE status='running'",
        )
        if cleaned:
            logger.info(f"[Scheduler] 清理了 {cleaned} 条遗留 running 状态记录")
    except Exception as e:
        logger.warning(f"[Scheduler] 清理 orphaned 记录失败: {e}")

    scheduler.start()
    # 加载用户自定义任务
    load_custom_jobs()
    logger.info("[Scheduler] 定时任务已启动: 07:00+17:00 zsxq采集, 08:30+18:30 诊断failed, 09:30+20:30 摘要生成, 10:00+21:30 chunk索引, 22:00 pending sweep, 06:00+20:00 KG, 06:00+16:00 Kline, 18:30 宏观日度, 19:30 预测监控, 21:00 问财, 23:00 chain_sync+theme_merger")


def stop_scheduler():
    """停止定时任务（FastAPI关闭时调用）"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[Scheduler] 定时任务已停止")
