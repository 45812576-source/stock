"""股票分析系统配置"""
import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / "db" / "stock_analysis.db"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"

# Claude API
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY", "") or os.getenv("ANTHROPIC_AUTH_TOKEN", "")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# 数据源配置
JASPER_API_KEY = os.getenv("JASPER_API_KEY", "")
JASPER_BASE_URL = "https://api.jasper.ai"

# AKShare 无需API key
AKSHARE_RETRY = 3
AKSHARE_DELAY = 0.5  # 请求间隔秒数

# 研报平台
DJYANBAO_MONTHLY_LIMIT = 150
FXBAOGAO_MONTHLY_LIMIT = 150
HUIBO_DAILY_LIMIT = 29

# 知识星球
ZSXQ_GROUP_ID = "28885112854841"
ZSXQ_COOKIE = os.getenv("ZSXQ_COOKIE", "")  # 从环境变量读取，或在系统管理页面配置

# 数据采集时间窗口
NEWS_LOOKBACK_HOURS = 24
REPORT_LOOKBACK_DAYS = 1

# Skills目录（Claude Code Skills作为分析prompt）
SKILLS_DIR = Path.home() / ".claude" / "skills"

# 页面配置
PAGE_TITLE = "个人股票分析系统"
PAGE_ICON = "📈"
LAYOUT = "wide"
