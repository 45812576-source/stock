"""个人股票分析系统 — FastAPI Web 入口"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse

app = FastAPI(title="个人股票分析系统")

# Jinja2 模板
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# 静态文件
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# 路由
from routers.overview import router as overview_router
from routers.hotspot import router as hotspot_router
from routers.stock import router as stock_router
from routers.datacollect import router as data_router
from routers.knowledge_graph import router as kg_router
from routers.market import router as market_router
from routers.settings import router as settings_router

app.include_router(overview_router)
app.include_router(hotspot_router)
app.include_router(stock_router)
app.include_router(data_router)
app.include_router(kg_router)
app.include_router(market_router)
app.include_router(settings_router)


@app.get("/")
async def root():
    return RedirectResponse(url="/overview")
