"""17情形常量定义 — 阶段识别框架核心"""

# ── 情形名称 ──────────────────────────────────────────────────────────────────
SITUATION_NAMES = {
    1:  "早期吸筹",
    2:  "吸筹测试",
    3:  "吸筹确认",
    4:  "真突破",
    5:  "初涨段",
    6:  "加速上涨",
    7:  "顶部形成",
    8:  "初步派发",
    9:  "派发确认",
    10: "假突破/陷阱",
    11: "趋势反转",
    12: "初跌段",
    13: "反弹测试",
    14: "加速下跌",
    15: "恐慌抛售",
    16: "底部测试",
    17: "底部确认",
}

SITUATION_PHASES = {
    "accumulation": [1, 2, 3],
    "markup":       [4, 5, 6],
    "distribution": [7, 8, 9, 10],
    "markdown":     [11, 12, 13, 14, 15],
    "re_accumulation": [16, 17],
}

STAGE_COLORS = {
    "accumulation":    "#3b82f6",   # 蓝
    "markup":          "#22c55e",   # 绿
    "distribution":    "#ef4444",   # 红
    "markdown":        "#a855f7",   # 紫
    "re_accumulation": "#f59e0b",   # 橙
}

def get_phase(situation_id: int) -> str:
    for phase, ids in SITUATION_PHASES.items():
        if situation_id in ids:
            return phase
    return "unknown"

def get_color(situation_id: int) -> str:
    return STAGE_COLORS.get(get_phase(situation_id), "#64748b")

# ── 7维度期望范围 ─────────────────────────────────────────────────────────────
# 格式: (min, max) 或 None 表示不约束
# 维度: rsi, price_vs_ma20_pct, macd_hist_sign, capital_flow_sign,
#        profit_ratio_pct, ma_arrangement, volume_ratio
SITUATION_CRITERIA = {
    1:  {"rsi": (20, 45), "price_vs_ma20_pct": (-15, 5),  "macd_hist_sign": (-1, 0), "capital_flow_sign": (0, 1),  "profit_ratio_pct": (0, 40),  "volume_ratio": (0.3, 1.5)},
    2:  {"rsi": (25, 50), "price_vs_ma20_pct": (-10, 8),  "macd_hist_sign": (-1, 1), "capital_flow_sign": (-1, 1), "profit_ratio_pct": (10, 55), "volume_ratio": (0.3, 1.2)},
    3:  {"rsi": (35, 60), "price_vs_ma20_pct": (-5, 12),  "macd_hist_sign": (0, 1),  "capital_flow_sign": (0, 1),  "profit_ratio_pct": (30, 65), "volume_ratio": (0.8, 2.0)},
    4:  {"rsi": (50, 75), "price_vs_ma20_pct": (3, 20),   "macd_hist_sign": (1, 1),  "capital_flow_sign": (1, 1),  "profit_ratio_pct": (50, 80), "volume_ratio": (1.5, 5.0)},
    5:  {"rsi": (55, 75), "price_vs_ma20_pct": (5, 25),   "macd_hist_sign": (1, 1),  "capital_flow_sign": (1, 1),  "profit_ratio_pct": (55, 85), "volume_ratio": (1.0, 3.0)},
    6:  {"rsi": (65, 85), "price_vs_ma20_pct": (10, 40),  "macd_hist_sign": (1, 1),  "capital_flow_sign": (1, 1),  "profit_ratio_pct": (70, 95), "volume_ratio": (1.5, 6.0)},
    7:  {"rsi": (70, 90), "price_vs_ma20_pct": (8, 35),   "macd_hist_sign": (0, 1),  "capital_flow_sign": (-1, 1), "profit_ratio_pct": (75, 98), "volume_ratio": (1.0, 4.0)},
    8:  {"rsi": (55, 80), "price_vs_ma20_pct": (0, 20),   "macd_hist_sign": (-1, 0), "capital_flow_sign": (-1, 0), "profit_ratio_pct": (60, 90), "volume_ratio": (0.8, 2.5)},
    9:  {"rsi": (40, 65), "price_vs_ma20_pct": (-8, 10),  "macd_hist_sign": (-1, 0), "capital_flow_sign": (-1, 0), "profit_ratio_pct": (40, 75), "volume_ratio": (0.8, 2.0)},
    10: {"rsi": (45, 70), "price_vs_ma20_pct": (-5, 15),  "macd_hist_sign": (-1, 1), "capital_flow_sign": (-1, 0), "profit_ratio_pct": (35, 70), "volume_ratio": (0.5, 2.0)},
    11: {"rsi": (35, 60), "price_vs_ma20_pct": (-15, 5),  "macd_hist_sign": (-1, 0), "capital_flow_sign": (-1, 0), "profit_ratio_pct": (20, 60), "volume_ratio": (0.8, 2.5)},
    12: {"rsi": (30, 55), "price_vs_ma20_pct": (-20, 0),  "macd_hist_sign": (-1, 0), "capital_flow_sign": (-1, 0), "profit_ratio_pct": (10, 50), "volume_ratio": (0.5, 2.0)},
    13: {"rsi": (35, 60), "price_vs_ma20_pct": (-15, 5),  "macd_hist_sign": (-1, 1), "capital_flow_sign": (-1, 1), "profit_ratio_pct": (15, 55), "volume_ratio": (0.5, 1.8)},
    14: {"rsi": (20, 40), "price_vs_ma20_pct": (-30, -5), "macd_hist_sign": (-1, -1),"capital_flow_sign": (-1, -1),"profit_ratio_pct": (5, 30),  "volume_ratio": (1.0, 4.0)},
    15: {"rsi": (10, 30), "price_vs_ma20_pct": (-40, -10),"macd_hist_sign": (-1, -1),"capital_flow_sign": (-1, -1),"profit_ratio_pct": (0, 20),  "volume_ratio": (2.0, 8.0)},
    16: {"rsi": (20, 45), "price_vs_ma20_pct": (-20, 5),  "macd_hist_sign": (-1, 0), "capital_flow_sign": (-1, 1), "profit_ratio_pct": (0, 35),  "volume_ratio": (0.3, 1.5)},
    17: {"rsi": (30, 55), "price_vs_ma20_pct": (-10, 10), "macd_hist_sign": (0, 1),  "capital_flow_sign": (0, 1),  "profit_ratio_pct": (15, 50), "volume_ratio": (0.8, 2.0)},
}

# ── 转换矩阵 ─────────────────────────────────────────────────────────────────
# 值: 0=禁止, 1=低概率, 2=中概率, 3=高概率
# 行=from, 列=to (1-indexed, 用 [from-1][to-1])
_MATRIX_DATA = [
    # to: 1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17
    [2, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],  # from 1
    [2, 2, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # from 2
    [1, 1, 2, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # from 3
    [0, 0, 1, 2, 3, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],  # from 4
    [0, 0, 0, 1, 2, 3, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],  # from 5
    [0, 0, 0, 0, 1, 2, 3, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0],  # from 6
    [0, 0, 0, 0, 0, 1, 2, 3, 1, 1, 1, 0, 0, 0, 0, 0, 0],  # from 7
    [0, 0, 0, 0, 0, 0, 1, 2, 3, 1, 2, 1, 0, 0, 0, 0, 0],  # from 8
    [0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 2, 0, 0, 0, 0, 0],  # from 9
    [0, 0, 0, 1, 1, 0, 1, 1, 1, 2, 2, 1, 0, 0, 0, 0, 0],  # from 10
    [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 1, 1, 0, 0, 0],  # from 11
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 3, 1, 0, 0],  # from 12
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 2, 1, 1, 0],  # from 13
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 1, 0],  # from 14
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 1],  # from 15
    [1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3],  # from 16
    [2, 1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2],  # from 17
]

def get_transition_prob(from_id: int, to_id: int) -> int:
    """返回转换概率等级 0=禁止 1=低 2=中 3=高"""
    if not (1 <= from_id <= 17 and 1 <= to_id <= 17):
        return 0
    return _MATRIX_DATA[from_id - 1][to_id - 1]

def is_transition_allowed(from_id: int, to_id: int) -> bool:
    return get_transition_prob(from_id, to_id) > 0

# ── 退出信号 ──────────────────────────────────────────────────────────────────
EXIT_SIGNALS = {
    1:  ["RSI突破50", "MACD金叉", "主力连续3日净流入", "价格站上MA20"],
    2:  ["价格突破前高", "成交量放大1.5倍", "MACD柱由负转正"],
    3:  ["价格突破关键阻力位", "成交量放大2倍", "RSI>60"],
    4:  ["价格回踩MA20不破", "成交量缩量", "RSI回落至55-65区间"],
    5:  ["MACD顶背离", "成交量萎缩", "价格涨幅>30%"],
    6:  ["RSI>85", "成交量异常放大", "价格单日涨幅>5%"],
    7:  ["MACD死叉", "主力净流出连续3日", "价格跌破MA5"],
    8:  ["价格跌破MA20", "成交量放大下跌", "RSI<50"],
    9:  ["价格跌破关键支撑", "主力大幅净流出", "RSI<40"],
    10: ["价格重新站上突破位", "或价格跌破支撑确认假突破"],
    11: ["价格跌破MA60", "MACD加速下行", "主力持续净流出"],
    12: ["RSI<30", "成交量萎缩", "价格跌幅>20%"],
    13: ["反弹至MA20受阻", "成交量缩量", "MACD反弹后再度死叉"],
    14: ["RSI<20", "成交量异常放大", "价格单日跌幅>5%"],
    15: ["成交量极度放大后萎缩", "RSI<15", "出现长下影线"],
    16: ["价格站上MA20", "MACD金叉", "主力净流入"],
    17: ["价格突破前高", "成交量放大确认", "RSI>50"],
}
