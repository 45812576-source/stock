"""宏观估值对接模块

从本地 MySQL 读取最新宏观数据，计算：
- liquidity_multiplier (0.8-1.2)：流动性乘数
- sentiment_multiplier (0.9-1.1)：情绪乘数

供 research/valuation_engine.py 的 _programmatic_synthesis() 调用。
"""
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# 乘数边界
LIQUIDITY_MIN, LIQUIDITY_MAX = 0.8, 1.2
SENTIMENT_MIN, SENTIMENT_MAX = 0.9, 1.1


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _get_latest(rows, key):
    """从查询结果列表取最新一条的指定字段"""
    if not rows:
        return None
    return _safe_float(rows[0].get(key))


def get_macro_valuation_context(industry: str = None) -> dict:
    """读取本地宏观数据，计算流动性乘数和情绪乘数。

    Returns:
        {
            "liquidity_multiplier": float,
            "liquidity_basis": str,
            "sentiment_multiplier": float,
            "sentiment_basis": str,
            "macro_data_available": bool,
            "multiplier_note": str,
            "raw": { ... }  # 原始数据快照
        }
    """
    from utils.db_utils import execute_query

    raw = {}
    components_liq = {}   # 流动性各分项得分（-1 ~ +1）
    components_sent = {}  # 情绪各分项得分（-1 ~ +1）

    # ── 1. Shibor 1W（权重30%，流动性）────────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT value FROM macro_indicators WHERE indicator_name='shibor_1w' "
            "ORDER BY indicator_date DESC LIMIT 10"
        )
        if rows:
            latest = _safe_float(rows[0]["value"])
            prev = _safe_float(rows[-1]["value"]) if len(rows) > 1 else latest
            raw["shibor_1w"] = latest
            # Shibor 上升 → 流动性收紧 → 负分；下降 → 正分
            # 基准：2.0%，每偏离0.5%对应0.2分
            if latest is not None:
                delta = (2.0 - latest) / 0.5 * 0.2
                components_liq["shibor"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取Shibor失败: {e}")

    # ── 2. M2超额流动性（权重25%，流动性）────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT value FROM macro_indicators WHERE indicator_name='M2_yoy' "
            "ORDER BY indicator_date DESC LIMIT 3"
        )
        if rows:
            m2_yoy = _safe_float(rows[0]["value"])
            raw["m2_yoy"] = m2_yoy
            # M2增速 > 9% 为宽松，< 7% 为偏紧；基准8%
            if m2_yoy is not None:
                delta = (m2_yoy - 8.0) / 1.0 * 0.3
                components_liq["m2"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取M2失败: {e}")

    # ── 3. 社融增速（权重20%，流动性）────────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT value FROM macro_indicators WHERE indicator_name='social_finance_yoy' "
            "ORDER BY indicator_date DESC LIMIT 3"
        )
        if rows:
            sf_yoy = _safe_float(rows[0]["value"])
            raw["social_finance_yoy"] = sf_yoy
            if sf_yoy is not None:
                delta = (sf_yoy - 8.0) / 2.0 * 0.3
                components_liq["social_finance"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取社融失败: {e}")

    # ── 4. 陆股通持股变动（权重15%，流动性）──────────────────────────────────
    try:
        rows = execute_query(
            "SELECT SUM(change_market_value) as total_chg FROM hsgt_holding "
            "WHERE trade_date >= %s",
            [(datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")]
        )
        if rows and rows[0]["total_chg"] is not None:
            chg = _safe_float(rows[0]["total_chg"])
            raw["hsgt_change_mv_5d"] = chg
            # 5日净增持 > 0 为正，每10亿对应0.1分
            if chg is not None:
                delta = chg / 1e9 * 0.1
                components_liq["hsgt"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取陆股通失败: {e}")

    # ── 5. 两市成交额（权重10%，流动性）──────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT sh_amount, sz_amount FROM market_valuation "
            "ORDER BY trade_date DESC LIMIT 20"
        )
        if rows:
            recent_5 = [
                (_safe_float(r["sh_amount"]) or 0) + (_safe_float(r["sz_amount"]) or 0)
                for r in rows[:5]
            ]
            hist_20 = [
                (_safe_float(r["sh_amount"]) or 0) + (_safe_float(r["sz_amount"]) or 0)
                for r in rows
            ]
            avg5 = sum(recent_5) / len(recent_5) if recent_5 else 0
            avg20 = sum(hist_20) / len(hist_20) if hist_20 else 0
            raw["turnover_avg5"] = avg5
            raw["turnover_avg20"] = avg20
            if avg20 > 0:
                ratio = avg5 / avg20
                delta = (ratio - 1.0) * 0.5
                components_liq["turnover"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取成交额失败: {e}")

    # ── 6. 全A PE分位数（权重35%，情绪）──────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT pe_quantile_10y FROM market_valuation "
            "WHERE pe_quantile_10y IS NOT NULL ORDER BY trade_date DESC LIMIT 1"
        )
        if rows:
            q10y = _safe_float(rows[0]["pe_quantile_10y"])
            raw["pe_quantile_10y"] = q10y
            # 分位数 < 30% 低估（正），> 70% 高估（负）；基准50%
            if q10y is not None:
                delta = (50.0 - q10y) / 20.0 * 0.3
                components_sent["pe_quantile"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取PE分位数失败: {e}")

    # ── 7. 融资余额变化（权重20%，情绪）──────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT margin_balance FROM margin_balance "
            "ORDER BY trade_date DESC LIMIT 20"
        )
        if rows and len(rows) >= 2:
            latest_mb = _safe_float(rows[0]["margin_balance"])
            prev_mb = _safe_float(rows[min(9, len(rows)-1)]["margin_balance"])
            raw["margin_balance_latest"] = latest_mb
            if latest_mb and prev_mb and prev_mb > 0:
                chg_pct = (latest_mb - prev_mb) / prev_mb * 100
                raw["margin_balance_chg_pct"] = chg_pct
                delta = chg_pct / 5.0 * 0.3
                components_sent["margin"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取融资余额失败: {e}")

    # ── 8. 海外ETF资金流向（权重15%，情绪）───────────────────────────────────
    try:
        rows = execute_query(
            "SELECT symbol, close FROM overseas_etf "
            "WHERE symbol IN ('KWEB','FXI','ASHR') "
            "ORDER BY trade_date DESC LIMIT 30"
        )
        if rows:
            by_sym = {}
            for r in rows:
                sym = r["symbol"]
                if sym not in by_sym:
                    by_sym[sym] = []
                by_sym[sym].append(_safe_float(r["close"]))
            scores = []
            for sym, prices in by_sym.items():
                if len(prices) >= 5:
                    chg = (prices[0] - prices[4]) / prices[4] if prices[4] else 0
                    scores.append(chg)
            if scores:
                avg_chg = sum(scores) / len(scores)
                raw["etf_5d_avg_chg"] = avg_chg
                delta = avg_chg / 0.05 * 0.3
                components_sent["etf"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取海外ETF失败: {e}")

    # ── 9. 市场波动率（权重25%，情绪）────────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT close FROM stock_daily WHERE stock_code='000001' "
            "ORDER BY trade_date DESC LIMIT 20"
        )
        if rows and len(rows) >= 10:
            prices = [_safe_float(r["close"]) for r in rows if r["close"]]
            if len(prices) >= 10:
                import statistics
                returns = [(prices[i] - prices[i+1]) / prices[i+1]
                           for i in range(len(prices)-1) if prices[i+1]]
                vol = statistics.stdev(returns) * 100 if len(returns) > 1 else 0
                raw["market_vol_20d"] = vol
                # 波动率 < 1% 低波（正），> 2% 高波（负）；基准1.5%
                delta = (1.5 - vol) / 0.5 * 0.3
                components_sent["volatility"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取波动率失败: {e}")

    # ── 10. PMI景气度（权重5%，情绪）─────────────────────────────────────────
    try:
        rows = execute_query(
            "SELECT value FROM macro_indicators WHERE indicator_name='pmi_manufacturing' "
            "ORDER BY indicator_date DESC LIMIT 1"
        )
        if rows:
            pmi = _safe_float(rows[0]["value"])
            raw["pmi_manufacturing"] = pmi
            if pmi is not None:
                # PMI > 50 扩张（正），< 50 收缩（负）
                delta = (pmi - 50.0) / 1.0 * 0.3
                components_sent["pmi"] = _clamp(delta, -1, 1)
    except Exception as e:
        logger.warning(f"读取PMI失败: {e}")

    # ── 计算加权乘数 ──────────────────────────────────────────────────────────
    # 流动性权重：shibor30% + m225% + social_finance20% + hsgt15% + turnover10%
    liq_weights = {"shibor": 0.30, "m2": 0.25, "social_finance": 0.20,
                   "hsgt": 0.15, "turnover": 0.10}
    # 情绪权重：pe_quantile35% + margin20% + etf15% + volatility25% + pmi5%
    sent_weights = {"pe_quantile": 0.35, "margin": 0.20, "etf": 0.15,
                    "volatility": 0.25, "pmi": 0.05}

    def _weighted_score(components, weights):
        total_w = 0.0
        total_s = 0.0
        for k, w in weights.items():
            if k in components:
                total_s += components[k] * w
                total_w += w
        if total_w < 0.3:  # 数据不足，返回中性
            return 0.0, total_w
        return total_s / total_w, total_w

    liq_score, liq_coverage = _weighted_score(components_liq, liq_weights)
    sent_score, sent_coverage = _weighted_score(components_sent, sent_weights)

    # 分数映射到乘数：score ∈ [-1,1] → multiplier ∈ [min, max]
    # liquidity: 0.8 ~ 1.2，中性1.0
    liq_range = (LIQUIDITY_MAX - LIQUIDITY_MIN) / 2
    liquidity_multiplier = _clamp(1.0 + liq_score * liq_range, LIQUIDITY_MIN, LIQUIDITY_MAX)

    # sentiment: 0.9 ~ 1.1，中性1.0
    sent_range = (SENTIMENT_MAX - SENTIMENT_MIN) / 2
    sentiment_multiplier = _clamp(1.0 + sent_score * sent_range, SENTIMENT_MIN, SENTIMENT_MAX)

    macro_data_available = liq_coverage >= 0.3 or sent_coverage >= 0.3

    # 生成说明文字
    liq_parts = []
    if "shibor" in components_liq:
        liq_parts.append(f"Shibor1W={raw.get('shibor_1w', 'N/A')}%")
    if "m2" in components_liq:
        liq_parts.append(f"M2同比={raw.get('m2_yoy', 'N/A')}%")
    if "social_finance" in components_liq:
        liq_parts.append(f"社融同比={raw.get('social_finance_yoy', 'N/A')}%")
    liquidity_basis = "、".join(liq_parts) if liq_parts else "数据不足，使用中性值"

    sent_parts = []
    if "pe_quantile" in components_sent:
        sent_parts.append(f"PE10年分位={raw.get('pe_quantile_10y', 'N/A')}%")
    if "margin" in components_sent:
        sent_parts.append(f"融资余额变化={raw.get('margin_balance_chg_pct', 'N/A'):.1f}%"
                          if raw.get('margin_balance_chg_pct') is not None else "融资余额")
    sentiment_basis = "、".join(sent_parts) if sent_parts else "数据不足，使用中性值"

    return {
        "liquidity_multiplier": round(liquidity_multiplier, 4),
        "liquidity_basis": liquidity_basis,
        "sentiment_multiplier": round(sentiment_multiplier, 4),
        "sentiment_basis": sentiment_basis,
        "macro_data_available": macro_data_available,
        "multiplier_note": (
            f"流动性覆盖率{liq_coverage:.0%}，情绪覆盖率{sent_coverage:.0%}"
            if macro_data_available else "宏观数据不足，使用中性默认值"
        ),
        "liquidity_breakdown": {k: round(v, 4) for k, v in components_liq.items()},
        "sentiment_breakdown": {k: round(v, 4) for k, v in components_sent.items()},
        "confidence": round((liq_coverage + sent_coverage) / 2, 2),
        "data_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw": raw,
    }
