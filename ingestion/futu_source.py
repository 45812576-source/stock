"""富途 OpenAPI 行情采集模块

依赖: futu-api (pip install futu-api)
前置: 本地运行 FutuOpenD 网关 (默认 127.0.0.1:11111)
"""
import logging
from contextlib import contextmanager
from datetime import datetime

from config import FUTU_HOST, FUTU_PORT
from utils.db_utils import get_db, execute_query

logger = logging.getLogger(__name__)

# ── 延迟导入 futu，未安装时优雅降级 ──────────────────────────
try:
    from futu import (
        OpenQuoteContext, KLType, SubType, Market,
        RET_OK, SysConfig,
    )
    FUTU_AVAILABLE = True
except ImportError:
    FUTU_AVAILABLE = False
    logger.warning("futu-api 未安装，富途行情功能不可用")


# ── 股票代码转换 ─────────────────────────────────────────────

def to_futu_code(code: str) -> str:
    """将简码转为富途格式: 000001 → SZ.000001, 00700 → HK.00700, 600519 → SH.600519"""
    code = code.strip()
    if "." in code:
        return code  # 已是 XX.XXXXX 格式
    if len(code) == 5:
        return f"HK.{code}"
    if code.startswith("6"):
        return f"SH.{code}"
    if code.startswith(("0", "3")):
        return f"SZ.{code}"
    return f"SZ.{code}"


def from_futu_code(futu_code: str) -> str:
    """富途格式转简码: SZ.000001 → 000001"""
    if "." in futu_code:
        return futu_code.split(".", 1)[1]
    return futu_code


# ── 连接管理 ─────────────────────────────────────────────────

@contextmanager
def _quote_ctx():
    """OpenQuoteContext 上下文管理器，自动 start/close"""
    if not FUTU_AVAILABLE:
        raise RuntimeError("futu-api 未安装")
    ctx = OpenQuoteContext(host=FUTU_HOST, port=FUTU_PORT)
    try:
        yield ctx
    finally:
        ctx.close()


def _is_futu_ready() -> bool:
    """检测 FutuOpenD 是否可连接（先用 socket 快速探测端口）"""
    if not FUTU_AVAILABLE:
        return False
    # 快速端口探测，避免 OpenQuoteContext 长时间阻塞
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    try:
        sock.connect((FUTU_HOST, FUTU_PORT))
        sock.close()
    except (socket.timeout, ConnectionRefusedError, OSError):
        sock.close()
        return False
    try:
        with _quote_ctx() as ctx:
            ret, _ = ctx.get_global_state()
            return ret == RET_OK
    except Exception:
        return False


# ── 实时报价 ─────────────────────────────────────────────────

def fetch_realtime_quote(codes: list[str]) -> list[dict]:
    """获取实时报价，更新 stock_info 表价格/市值，同时写入 stock_realtime 缓存表。
    codes: 简码列表，如 ["000001", "00700"]
    返回报价 dict 列表。
    """
    futu_codes = [to_futu_code(c) for c in codes]
    try:
        with _quote_ctx() as ctx:
            ret, df = ctx.get_market_snapshot(futu_codes)
            if ret != RET_OK or df is None or df.empty:
                logger.warning(f"获取实时报价失败: {df}")
                return []
    except Exception as e:
        logger.error(f"富途实时报价异常: {e}")
        return []

    results = []
    with get_db() as conn:
        for _, row in df.iterrows():
            code = from_futu_code(row["code"])
            prev_close = row.get("prev_close_price")
            last = row.get("last_price")
            change_pct = round((last - prev_close) / prev_close * 100, 2) if prev_close and last else None
            mkt_val = row.get("total_market_val")
            item = {
                "stock_code": code,
                "stock_name": row.get("name", ""),
                "last_price": last,
                "change_pct": change_pct,
                "volume": row.get("volume"),
                "amount": row.get("turnover"),
                "turnover_rate": row.get("turnover_rate"),
                "pe_ratio": row.get("pe_ttm_ratio"),
                "pb_ratio": row.get("pb_ratio"),
                "market_cap": round(mkt_val / 1e8, 2) if mkt_val else None,
            }
            results.append(item)

            # 更新 stock_info 市值
            if item["market_cap"]:
                conn.execute(
                    "UPDATE stock_info SET market_cap=?, updated_at=NOW() WHERE stock_code=?",
                    [item["market_cap"], code],
                )

            # 写入 stock_realtime 缓存
            market = "HK" if row["code"].startswith("HK.") else "A"
            conn.execute(
                """REPLACE INTO stock_realtime
                   (stock_code, stock_name, market, last_price, change_pct,
                    volume, amount, turnover_rate, pe_ratio, pb_ratio)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [code, item["stock_name"], market, item["last_price"],
                 item["change_pct"], item["volume"], item["amount"],
                 item["turnover_rate"], item["pe_ratio"], item["pb_ratio"]],
            )
    logger.info(f"实时报价更新: {len(results)} 只")
    return results


# ── 历史K线 ──────────────────────────────────────────────────

_KTYPE_MAP = {
    "daily": KLType.K_DAY if FUTU_AVAILABLE else None,
    "weekly": KLType.K_WEEK if FUTU_AVAILABLE else None,
    "monthly": KLType.K_MON if FUTU_AVAILABLE else None,
}


def fetch_history_kline(code: str, ktype: str = "daily",
                        start: str = "2024-01-01", end: str = None) -> int:
    """拉取历史K线写入 stock_daily 表。
    ktype: daily / weekly / monthly
    返回写入条数。
    """
    if not FUTU_AVAILABLE:
        logger.warning("futu-api 不可用，跳过K线拉取")
        return 0
    futu_code = to_futu_code(code)
    kl = _KTYPE_MAP.get(ktype, KLType.K_DAY)
    if end is None:
        end = datetime.now().strftime("%Y-%m-%d")

    try:
        with _quote_ctx() as ctx:
            ret, df, _ = ctx.request_history_kline(
                futu_code, start=start, end=end, ktype=kl, max_count=1000,
            )
            if ret != RET_OK or df is None or df.empty:
                logger.warning(f"K线拉取失败 {code}: {df}")
                return 0
    except Exception as e:
        logger.error(f"富途K线异常 {code}: {e}")
        return 0

    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            trade_date = str(row["time_key"])[:10]
            change_pct = row.get("change_rate")
            turnover_rate = row.get("turnover_rate")
            conn.execute(
                """REPLACE INTO stock_daily
                   (stock_code, trade_date, `open`, high, low, `close`,
                    volume, amount, turnover_rate, change_pct)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [code, trade_date, row["open"], row["high"], row["low"],
                 row["close"], row["volume"], row["turnover"],
                 turnover_rate, change_pct],
            )
            count += 1
    logger.info(f"{code} K线({ktype}): {count} 条")
    return count


# ── 资金分布 ─────────────────────────────────────────────────

def fetch_capital_distribution(code: str) -> int:
    """拉取个股资金分布写入 capital_flow 表。返回写入条数。"""
    if not FUTU_AVAILABLE:
        return 0
    futu_code = to_futu_code(code)
    try:
        with _quote_ctx() as ctx:
            ret, df = ctx.get_capital_distribution(futu_code)
            if ret != RET_OK or df is None or df.empty:
                logger.warning(f"资金分布拉取失败 {code}: {df}")
                return 0
    except Exception as e:
        logger.error(f"富途资金分布异常 {code}: {e}")
        return 0

    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as conn:
        row = df.iloc[0]
        conn.execute(
            """REPLACE INTO capital_flow
               (stock_code, trade_date, main_net_inflow, super_large_net,
                large_net, medium_net, small_net)
               VALUES (?,?,?,?,?,?,?)""",
            [code, today,
             row.get("capital_in_big", 0) - row.get("capital_out_big", 0),
             row.get("capital_in_super", 0) - row.get("capital_out_super", 0),
             row.get("capital_in_big", 0) - row.get("capital_out_big", 0),
             row.get("capital_in_mid", 0) - row.get("capital_out_mid", 0),
             row.get("capital_in_small", 0) - row.get("capital_out_small", 0)],
        )
    logger.info(f"{code} 资金分布已更新")
    return 1


# ── 资金流历史（日级） ───────────────────────────────────────

def fetch_capital_flow_history(code: str, days: int = 60) -> int:
    """拉取日级资金流历史写入 capital_flow 表。
    使用 Futu get_capital_flow(period_type='DAY')，可获取约 248 个交易日数据。
    返回写入条数。
    """
    if not FUTU_AVAILABLE:
        return 0
    futu_code = to_futu_code(code)
    try:
        with _quote_ctx() as ctx:
            ret, df = ctx.get_capital_flow(futu_code, period_type='DAY')
            if ret != RET_OK or df is None or df.empty:
                logger.warning(f"日级资金流拉取失败 {code}: {df}")
                return 0
    except Exception as e:
        logger.error(f"富途日级资金流异常 {code}: {e}")
        return 0

    # 只取最近 days 天
    df = df.tail(days)
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            trade_date = str(row.get("capital_flow_item_time", ""))[:10]
            if not trade_date or trade_date == "N/A":
                continue
            conn.execute(
                """REPLACE INTO capital_flow
                   (stock_code, trade_date, main_net_inflow, super_large_net,
                    large_net, medium_net, small_net)
                   VALUES (?,?,?,?,?,?,?)""",
                [code, trade_date,
                 row.get("main_in_flow", row.get("in_flow", 0)),
                 row.get("super_in_flow", 0),
                 row.get("big_in_flow", 0),
                 row.get("mid_in_flow", 0),
                 row.get("sml_in_flow", 0)],
            )
            count += 1
    logger.info(f"{code} 日级资金流: {count} 条")
    return count


# ── 筹码分布计算 ─────────────────────────────────────────────

def calc_chip_distribution(code: str, days: int = 120, decay: float = 0.97) -> dict:
    """从历史K线计算筹码分布。

    算法：将每日成交量按三角分布分配到 low~high 价格区间，
    旧筹码按 decay 因子衰减（模拟换手）。

    返回:
        {
            "current_price": float,
            "profit_ratio": float,      # 获利盘比例 (0~1)
            "trapped_ratio": float,      # 套牢盘比例 (0~1)
            "chip_peaks": [float, ...],  # 筹码峰价格（最多3个）
            "avg_cost": float,           # 平均成本
            "concentration_90": float,   # 90%筹码集中度
            "distribution": [(price, pct), ...],  # 价格-筹码分布（采样）
        }
    """
    # 从 DB 读取K线数据
    rows = execute_query(
        """SELECT trade_date, `open`, high, low, `close`, volume
           FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT ?""",
        [code, days],
    )
    if not rows or len(rows) < 20:
        logger.warning(f"{code} K线数据不足，无法计算筹码分布")
        return {}

    rows = list(reversed(rows))  # 按时间正序
    current_price = rows[-1]["close"]

    # 构建价格-筹码映射（精度 0.01 元）
    chips = {}
    for row in rows:
        high = row["high"] or row["close"]
        low = row["low"] or row["close"]
        vol = row["volume"] or 0
        if high <= 0 or low <= 0 or vol <= 0:
            continue

        # 衰减已有筹码
        for p in chips:
            chips[p] *= decay

        # 三角分布：中间价位分配更多成交量
        mid = (high + low) / 2
        price_range = max(high - low, 0.01)
        step = max(price_range / 50, 0.01)
        price = low
        total_weight = 0
        price_weights = []
        while price <= high:
            w = 1 - abs(price - mid) / (price_range / 2 + 0.001)
            w = max(w, 0.1)
            price_weights.append((round(price, 2), w))
            total_weight += w
            price += step

        if total_weight > 0:
            for p, w in price_weights:
                chips[p] = chips.get(p, 0) + vol * w / total_weight

    if not chips:
        return {}

    total_chips = sum(chips.values())
    if total_chips == 0:
        return {}

    # 获利盘 / 套牢盘
    profit_chips = sum(v for p, v in chips.items() if p <= current_price)
    profit_ratio = round(profit_chips / total_chips, 4)

    # 平均成本
    avg_cost = round(sum(p * v for p, v in chips.items()) / total_chips, 2)

    # 筹码峰（局部最大值）
    sorted_chips = sorted(chips.items(), key=lambda x: x[0])
    prices = [p for p, _ in sorted_chips]
    volumes = [v for _, v in sorted_chips]

    # 简单平滑后找峰
    window = max(len(volumes) // 20, 3)
    smoothed = []
    for i in range(len(volumes)):
        start = max(0, i - window)
        end = min(len(volumes), i + window + 1)
        smoothed.append(sum(volumes[start:end]) / (end - start))

    peaks = []
    for i in range(1, len(smoothed) - 1):
        if smoothed[i] > smoothed[i-1] and smoothed[i] > smoothed[i+1]:
            peaks.append((prices[i], smoothed[i]))
    peaks.sort(key=lambda x: x[1], reverse=True)
    chip_peaks = [p for p, _ in peaks[:3]]

    # 90% 筹码集中度
    cumulative = 0
    p5, p95 = prices[0], prices[-1]
    for p, v in sorted_chips:
        cumulative += v
        if cumulative >= total_chips * 0.05 and p5 == prices[0]:
            p5 = p
        if cumulative >= total_chips * 0.95:
            p95 = p
            break
    concentration_90 = round((p95 - p5) / ((p95 + p5) / 2) * 100, 2) if (p95 + p5) > 0 else 0

    # 采样分布（最多 50 个点）
    sample_step = max(len(sorted_chips) // 50, 1)
    distribution = [(p, round(v / total_chips * 100, 3))
                    for i, (p, v) in enumerate(sorted_chips) if i % sample_step == 0]

    return {
        "current_price": current_price,
        "profit_ratio": profit_ratio,
        "trapped_ratio": round(1 - profit_ratio, 4),
        "chip_peaks": chip_peaks,
        "avg_cost": avg_cost,
        "concentration_90": concentration_90,
        "distribution": distribution,
    }


# ── 股票基础信息 ─────────────────────────────────────────────

def fetch_stock_basicinfo(market: str = "HK") -> int:
    """拉取股票基础信息写入 stock_info 表。
    market: "HK" / "SH" / "SZ"
    """
    if not FUTU_AVAILABLE:
        return 0
    market_map = {
        "HK": Market.HK,
        "SH": Market.SH,
        "SZ": Market.SZ,
    }
    futu_market = market_map.get(market.upper())
    if futu_market is None:
        logger.error(f"不支持的市场: {market}")
        return 0

    try:
        with _quote_ctx() as ctx:
            ret, df = ctx.get_stock_basicinfo(futu_market, stock_type=None)
            if ret != RET_OK or df is None or df.empty:
                logger.warning(f"基础信息拉取失败: {df}")
                return 0
    except Exception as e:
        logger.error(f"富途基础信息异常: {e}")
        return 0

    count = 0
    mkt_label = "HK" if market.upper() == "HK" else "A"
    with get_db() as conn:
        for _, row in df.iterrows():
            code = from_futu_code(row["code"])
            conn.execute(
                """REPLACE INTO stock_info (stock_code, stock_name, market)
                   VALUES (?, ?, ?)""",
                [code, row.get("name", ""), mkt_label],
            )
            count += 1
    logger.info(f"{market} 基础信息: {count} 条")
    return count


# ── 实时快照（不入库，直接返回） ─────────────────────────────

def get_market_snapshot(codes: list[str]) -> dict:
    """获取实时快照，返回 {code: {price, volume, change_pct, ...}}"""
    if not FUTU_AVAILABLE:
        return {}
    futu_codes = [to_futu_code(c) for c in codes]
    try:
        with _quote_ctx() as ctx:
            ret, df = ctx.get_market_snapshot(futu_codes)
            if ret != RET_OK or df is None or df.empty:
                return {}
    except Exception:
        return {}

    result = {}
    for _, row in df.iterrows():
        code = from_futu_code(row["code"])
        prev_close = row.get("prev_close_price")
        last = row.get("last_price")
        change_pct = round((last - prev_close) / prev_close * 100, 2) if prev_close and last else None
        mkt_val = row.get("total_market_val")
        result[code] = {
            "price": last,
            "volume": row.get("volume"),
            "amount": row.get("turnover"),
            "change_pct": change_pct,
            "high": row.get("high_price"),
            "low": row.get("low_price"),
            "open": row.get("open_price"),
            "prev_close": prev_close,
            "pe": row.get("pe_ttm_ratio"),
            "pb": row.get("pb_ratio"),
            "market_cap": round(mkt_val / 1e8, 2) if mkt_val else None,
        }
    return result
