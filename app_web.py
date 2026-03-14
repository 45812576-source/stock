"""个人股票分析系统 — FastAPI Web 入口"""
import sys
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时：开启定时任务
    from scheduler import start_scheduler
    start_scheduler()
    # 启动 chat worker 后台线程
    import threading, time
    def _chat_worker_loop():
        from hotspot.chat_handler import process_pending_messages as hotspot_pending
        from portfolio.chat_handler import process_pending_messages as portfolio_pending
        while True:
            try:
                hotspot_pending()
                portfolio_pending()
            except Exception:
                pass
            time.sleep(3)
    t = threading.Thread(target=_chat_worker_loop, daemon=True, name="chat-worker")
    t.start()
    yield
    # 关闭时：停止定时任务
    from scheduler import stop_scheduler
    stop_scheduler()


app = FastAPI(title="个人股票分析系统", lifespan=lifespan)

# Jinja2 模板
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# 静态文件
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
(static_dir / "uploads").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# 路由
from routers.overview import router as overview_router
from routers.hotspot import router as hotspot_router
from routers.stock import router as stock_router
from routers.datacollect import router as data_router
from routers.knowledge_graph import router as kg_router
from routers.settings import router as settings_router
from routers.portfolio import router as portfolio_router
from routers.capital import router as capital_router
from routers.market import router as market_router
from routers.project_chat import router as project_chat_router
from routers.sector import router as sector_router
from routers.selector import router as selector_router
from routers.agent_chat import router as agent_chat_router
from routers.auth import router as auth_router
from routers.admin import router as admin_router
from routers.search import router as search_router
from routers.summary_review import router as summary_review_router
from routers.robust_kline import router as robust_kline_router
from routers.daily_intel import router as daily_intel_router
from routers.chain import router as chain_router
from routers.chain_intel import router as chain_intel_router

app.include_router(overview_router)
app.include_router(hotspot_router)
app.include_router(stock_router)
app.include_router(data_router)
app.include_router(kg_router)
app.include_router(settings_router)
app.include_router(portfolio_router)
app.include_router(capital_router)
app.include_router(market_router)
app.include_router(project_chat_router)
app.include_router(sector_router)
app.include_router(selector_router)
app.include_router(agent_chat_router)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(search_router)
app.include_router(summary_review_router)
app.include_router(robust_kline_router)
app.include_router(daily_intel_router)
app.include_router(chain_router)
app.include_router(chain_intel_router)


@app.get("/")
async def root():
    return RedirectResponse(url="/overview")
