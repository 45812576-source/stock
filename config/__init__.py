"""股票分析系统配置"""
import os
from pathlib import Path

# 项目根目录（config/ 的父目录）
PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema_mysql.sql"

# MySQL 连接配置（本地）
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "stock_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "stock_pass")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_analysis")

# 云端 MySQL（采集管道写入）
CLOUD_MYSQL_HOST = os.getenv("CLOUD_MYSQL_HOST", "8.134.184.254")
CLOUD_MYSQL_PORT = int(os.getenv("CLOUD_MYSQL_PORT", "3301"))
CLOUD_MYSQL_USER = os.getenv("CLOUD_MYSQL_USER", "root")
CLOUD_MYSQL_PASSWORD = os.getenv("CLOUD_MYSQL_PASSWORD", "ZRMwE#1!z!(WLPk4LtyRg2CK#*usUI")
CLOUD_MYSQL_DB = os.getenv("CLOUD_MYSQL_DB", "stock_analysis")

# Claude API
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY", "") or os.getenv("ANTHROPIC_AUTH_TOKEN", "")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# 数据源配置
JASPER_API_KEY = os.getenv("JASPER_API_KEY", "")
JASPER_BASE_URL = "https://api.jasper.ai"

# AKShare 无需API key
AKSHARE_RETRY = 3
AKSHARE_DELAY = 0.5  # 请求间隔秒数

# 富途 OpenAPI（需本地运行 FutuOpenD 网关）
FUTU_HOST = os.getenv("FUTU_HOST", "127.0.0.1")
FUTU_PORT = int(os.getenv("FUTU_PORT", "11111"))

# 研报平台
DJYANBAO_MONTHLY_LIMIT = 150
FXBAOGAO_MONTHLY_LIMIT = 150
HUIBO_DAILY_LIMIT = 29

# 知识星球
ZSXQ_GROUP_ID = "28885112854841"  # 保留兼容旧代码
ZSXQ_GROUP_IDS = os.getenv("ZSXQ_GROUP_IDS", "28885112854841").split(",")
ZSXQ_COOKIE = os.getenv("ZSXQ_COOKIE", "490AFECD-E3EE-436D-8258-878623783ED3_0D94B6F14EDB6784")

# 数据采集时间窗口
NEWS_LOOKBACK_HOURS = 24
REPORT_LOOKBACK_DAYS = 1

# Skills目录（Claude Code Skills作为分析prompt）
SKILLS_DIR = Path.home() / ".claude" / "skills"

# 问财 cookie（需登录 iwencai.com 后从浏览器复制，为空则跳过问财查询）
WENCAI_COOKIE = os.getenv("WENCAI_COOKIE", "")

# 问财行业指标采集
WENCAI_MIN_DELAY = int(os.getenv("WENCAI_MIN_DELAY", "15"))
WENCAI_MAX_DELAY = int(os.getenv("WENCAI_MAX_DELAY", "15"))
WENCAI_MAX_DAILY_QUERIES = int(os.getenv("WENCAI_MAX_DAILY_QUERIES", "9999"))
WENCAI_INDICATOR_DICT = PROJECT_ROOT / "config" / "industry_indicator_dict.yaml"

# 向量检索
MILVUS_HOST = os.getenv("MILVUS_HOST", "127.0.0.1")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
EMBEDDING_MODEL = "BAAI/bge-m3"
# 离线加载模型（避免访问被墙的 huggingface.co）
os.environ.setdefault("HF_HUB_OFFLINE", "1")
EMBEDDING_DIM = 1024
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100

# 页面配置
PAGE_TITLE = "个人股票分析系统"
PAGE_ICON = "📈"
LAYOUT = "wide"
