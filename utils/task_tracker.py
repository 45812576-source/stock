"""轻量任务追踪 SDK — 让任意独立脚本的进度显示在后台任务中心

用法（未来脚本）:
    from utils.task_tracker import TaskTracker

    tracker = TaskTracker("全量补全", total=279)
    for i, item in enumerate(items):
        # ... 处理逻辑 ...
        tracker.update(i + 1, f"处理 {item['title'][:30]}")
    tracker.finish("成功171 失败40")

用法（监控已有日志文件）:
    from utils.task_tracker import LogWatcher

    watcher = LogWatcher(
        name="全量补全",
        log_path="scripts/zsxq_全量补全_20260101至今.log",
        progress_pattern=r'\[(\d+)/(\d+)\]',
        pid=77010,
    )
    watcher.start()  # 后台线程持续监控日志，直到进程结束或 total 达到
"""

import os
import re
import time
import logging
import threading
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import URLError
import json

logger = logging.getLogger(__name__)

_BACKEND_BASE = os.environ.get("TASK_TRACKER_BACKEND", "http://127.0.0.1:8501")


def _post(path: str, data: dict) -> dict:
    """发送 POST 请求到后端"""
    url = f"{_BACKEND_BASE}{path}"
    body = json.dumps(data).encode("utf-8")
    req = UrlRequest(url, data=body, headers={"Content-Type": "application/json"})
    try:
        resp = urlopen(req, timeout=5)
        return json.loads(resp.read())
    except (URLError, OSError, json.JSONDecodeError) as e:
        logger.debug(f"[TaskTracker] POST {path} failed: {e}")
        return {}


class TaskTracker:
    """供脚本 import 使用的进度追踪器，自动注册到后端任务中心"""

    def __init__(self, name: str, total: int = 0, pid: int = None):
        self.name = name
        self.total = total
        self.pid = pid or os.getpid()
        self.task_id = None
        self._register()

    def _register(self):
        result = _post("/data/api/external-task/register", {
            "name": self.name,
            "total": self.total,
            "pid": self.pid,
        })
        self.task_id = result.get("task_id")
        if self.task_id:
            logger.info(f"[TaskTracker] 已注册任务: {self.task_id}")
        else:
            logger.warning("[TaskTracker] 注册失败，进度将不会在任务中心显示")

    def update(self, progress: int, current: str = "", total: int = None):
        """更新进度"""
        if not self.task_id:
            return
        data = {"task_id": self.task_id, "progress": progress, "current": current}
        if total is not None:
            data["total"] = total
            self.total = total
        _post("/data/api/external-task/update", data)

    def finish(self, summary: str = "完成"):
        """标记任务完成"""
        if not self.task_id:
            return
        _post("/data/api/external-task/finish", {
            "task_id": self.task_id,
            "summary": summary,
        })
        logger.info(f"[TaskTracker] 任务已完成: {summary}")


class LogWatcher:
    """监控已有日志文件，自动解析进度并注入到任务中心

    适用于无法修改的正在运行的脚本。
    """

    def __init__(self, name: str, log_path: str, progress_pattern: str = r'\[(\d+)/(\d+)\]',
                 pid: int = None, poll_interval: int = 30):
        """
        Args:
            name: 任务名称
            log_path: 日志文件路径
            progress_pattern: 正则表达式，第1组=当前进度，第2组=总数
            pid: 脚本进程 PID（用于检测进程是否结束）
            poll_interval: 轮询间隔（秒）
        """
        self.name = name
        self.log_path = log_path
        self.pattern = re.compile(progress_pattern)
        self.pid = pid
        self.poll_interval = poll_interval
        self.task_id = None
        self._thread = None
        self._stop_flag = False

    def start(self):
        """启动后台监控线程"""
        result = _post("/data/api/external-task/register", {
            "name": self.name,
            "total": 0,
            "pid": self.pid,
        })
        self.task_id = result.get("task_id")
        if not self.task_id:
            logger.warning("[LogWatcher] 注册失败")
            return

        self._thread = threading.Thread(target=self._watch_loop, daemon=True, name=f"logwatch_{self.name}")
        self._thread.start()
        logger.info(f"[LogWatcher] 开始监控 {self.log_path} -> task_id={self.task_id}")

    def stop(self):
        """停止监控"""
        self._stop_flag = True

    def _is_process_alive(self) -> bool:
        """检查目标进程是否存活"""
        if not self.pid:
            return True
        try:
            os.kill(self.pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _parse_latest_progress(self) -> tuple:
        """从日志文件末尾解析最新进度，返回 (progress, total, last_line)"""
        if not os.path.exists(self.log_path):
            return 0, 0, ""
        try:
            with open(self.log_path, 'rb') as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 8192))
                tail = f.read().decode('utf-8', errors='ignore')

            lines = tail.strip().split('\n')
            progress = 0
            total = 0
            last_meaningful = ""

            for line in reversed(lines):
                m = self.pattern.search(line)
                if m:
                    progress = int(m.group(1))
                    total = int(m.group(2))
                    last_meaningful = line.strip()[-80:]
                    break

            return progress, total, last_meaningful
        except Exception as e:
            logger.debug(f"[LogWatcher] 解析日志失败: {e}")
            return 0, 0, ""

    def _watch_loop(self):
        """后台轮询循环"""
        while not self._stop_flag:
            progress, total, current = self._parse_latest_progress()

            if progress > 0:
                _post("/data/api/external-task/update", {
                    "task_id": self.task_id,
                    "progress": progress,
                    "total": total,
                    "current": current,
                })

            # 检查进程是否结束
            if not self._is_process_alive():
                progress, total, current = self._parse_latest_progress()
                _post("/data/api/external-task/finish", {
                    "task_id": self.task_id,
                    "summary": f"进程结束 [{progress}/{total}] {current[:50]}",
                })
                logger.info(f"[LogWatcher] 目标进程 PID {self.pid} 已结束，监控停止")
                break

            # 任务完成检测
            if total > 0 and progress >= total:
                _post("/data/api/external-task/finish", {
                    "task_id": self.task_id,
                    "summary": f"完成 [{progress}/{total}]",
                })
                logger.info(f"[LogWatcher] 任务完成 [{progress}/{total}]")
                break

            time.sleep(self.poll_interval)
