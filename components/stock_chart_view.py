"""个股行情组件 — K线图、技术指标、资金流向、相关资讯"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.db_utils import execute_query
from datetime import datetime, timedelta


def load_daily_data(stock_code, start_date, end_date):
    rows = execute_query(
        """SELECT trade_date, open, high, low, close, volume, amount,
                  turnover_rate, change_pct
           FROM stock_daily WHERE stock_code=? AND trade_date BETWEEN ? AND ?
           ORDER BY trade_date""",
        [stock_code, start_date, end_date],
    )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def calc_ma(df, periods=[5, 10, 20, 60, 120, 250]):
    for p in periods:
        df[f"MA{p}"] = df["close"].rolling(window=p).mean()
    return df


def calc_boll(df, period=20, std_dev=2):
    df["BOLL_MID"] = df["close"].rolling(window=period).mean()
    df["BOLL_STD"] = df["close"].rolling(window=period).std()
    df["BOLL_UP"] = df["BOLL_MID"] + std_dev * df["BOLL_STD"]
    df["BOLL_DN"] = df["BOLL_MID"] - std_dev * df["BOLL_STD"]
    return df


def calc_macd(df, fast=12, slow=26, signal=9):
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    df["DIF"] = ema_fast - ema_slow
    df["DEA"] = df["DIF"].ewm(span=signal, adjust=False).mean()
    df["MACD_HIST"] = 2 * (df["DIF"] - df["DEA"])
    return df


def calc_rsi(df, periods=[6, 12, 24]):
    delta = df["close"].diff()
    for p in periods:
        gain = delta.clip(lower=0).rolling(window=p).mean()
        loss = (-delta.clip(upper=0)).rolling(window=p).mean()
        rs = gain / loss
        df[f"RSI{p}"] = 100 - (100 / (1 + rs))
    return df


def calc_kdj(df, n=9, m1=3, m2=3):
    low_n = df["low"].rolling(window=n).min()
    high_n = df["high"].rolling(window=n).max()
    rsv = (df["close"] - low_n) / (high_n - low_n) * 100
    df["K"] = rsv.ewm(com=m1 - 1, adjust=False).mean()
    df["D"] = df["K"].ewm(com=m2 - 1, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]
    return df


def calc_obv(df):
    obv = [0]
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            obv.append(obv[-1] + df["volume"].iloc[i])
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            obv.append(obv[-1] - df["volume"].iloc[i])
        else:
            obv.append(obv[-1])
    df["OBV"] = obv
    return df


def render_stock_chart(stock_code, key_prefix="chart"):
    """渲染个股行情图表（K线+技术指标+资金流+资讯），不含搜索框"""
    # 时间段选择
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        period_map = {"1月": 30, "3月": 90, "6月": 180, "1年": 365, "自定义": 0}
        period = st.selectbox("时间段", list(period_map.keys()), index=2,
                              key=f"{key_prefix}_period")
    with col2:
        kline_type = st.selectbox("K线周期", ["日K", "周K", "月K"],
                                  key=f"{key_prefix}_kline")

    if period == "自定义":
        with col3:
            date_range = st.date_input("日期范围",
                [datetime.now() - timedelta(days=180), datetime.now()],
                key=f"{key_prefix}_daterange")
            start_date = date_range[0].strftime("%Y-%m-%d")
            end_date = date_range[1].strftime("%Y-%m-%d") if len(date_range) > 1 else datetime.now().strftime("%Y-%m-%d")
    else:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=period_map[period])).strftime("%Y-%m-%d")

    # 加载数据
    df = load_daily_data(stock_code, start_date, end_date)
    if df is None or df.empty:
        st.warning(f"未找到 {stock_code} 的行情数据，请先在数据管理页面拉取数据")
        return

    # 周K/月K聚合
    if kline_type == "周K":
        df = df.set_index("trade_date").resample("W").agg({
            "open": "first", "high": "max", "low": "min", "close": "last",
            "volume": "sum", "amount": "sum", "turnover_rate": "mean", "change_pct": "sum",
        }).dropna().reset_index()
    elif kline_type == "月K":
        df = df.set_index("trade_date").resample("ME").agg({
            "open": "first", "high": "max", "low": "min", "close": "last",
            "volume": "sum", "amount": "sum", "turnover_rate": "mean", "change_pct": "sum",
        }).dropna().reset_index()

    # 计算指标
    df = calc_ma(df)
    df = calc_boll(df)
    df = calc_macd(df)
    df = calc_rsi(df)
    df = calc_kdj(df)
    df = calc_obv(df)

    # 指标开关
    st.subheader(f"{stock_code} {kline_type}")
    indicator_cols = st.columns(8)
    show_ma = indicator_cols[0].checkbox("均线", True, key=f"{key_prefix}_ma")
    show_boll = indicator_cols[1].checkbox("布林带", False, key=f"{key_prefix}_boll")
    show_vol = indicator_cols[2].checkbox("成交量", True, key=f"{key_prefix}_vol")
    show_macd = indicator_cols[3].checkbox("MACD", True, key=f"{key_prefix}_macd")
    show_rsi = indicator_cols[4].checkbox("RSI", False, key=f"{key_prefix}_rsi")
    show_kdj = indicator_cols[5].checkbox("KDJ", False, key=f"{key_prefix}_kdj")
    show_obv = indicator_cols[6].checkbox("OBV", False, key=f"{key_prefix}_obv")
    show_turnover = indicator_cols[7].checkbox("换手率", False, key=f"{key_prefix}_turnover")

    # 计算子图数量
    sub_count = 1
    if show_vol:
        sub_count += 1
    if show_macd:
        sub_count += 1
    if show_rsi:
        sub_count += 1
    if show_kdj:
        sub_count += 1
    if show_obv:
        sub_count += 1
    if show_turnover:
        sub_count += 1

    row_heights = [0.4] + [0.6 / max(sub_count - 1, 1)] * (sub_count - 1)
    fig = make_subplots(
        rows=sub_count, cols=1, shared_xaxes=True,
        vertical_spacing=0.02, row_heights=row_heights,
    )

    # K线主图
    fig.add_trace(go.Candlestick(
        x=df["trade_date"], open=df["open"], high=df["high"],
        low=df["low"], close=df["close"], name="K线",
        increasing_line_color="red", decreasing_line_color="green",
    ), row=1, col=1)

    # 均线
    if show_ma:
        colors = {"MA5": "#FF6B6B", "MA10": "#4ECDC4", "MA20": "#45B7D1",
                  "MA60": "#96CEB4", "MA120": "#FFEAA7", "MA250": "#DDA0DD"}
        for ma, color in colors.items():
            if ma in df.columns:
                fig.add_trace(go.Scatter(
                    x=df["trade_date"], y=df[ma], name=ma,
                    line=dict(width=1, color=color),
                ), row=1, col=1)

    # 布林带
    if show_boll:
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["BOLL_UP"], name="BOLL上",
                                 line=dict(width=1, dash="dash", color="gray")), row=1, col=1)
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["BOLL_DN"], name="BOLL下",
                                 line=dict(width=1, dash="dash", color="gray"),
                                 fill="tonexty", fillcolor="rgba(128,128,128,0.1)"), row=1, col=1)

    current_row = 2

    # 成交量
    if show_vol:
        colors_vol = ["red" if df["close"].iloc[i] >= df["open"].iloc[i] else "green"
                      for i in range(len(df))]
        fig.add_trace(go.Bar(
            x=df["trade_date"], y=df["volume"], name="成交量",
            marker_color=colors_vol, opacity=0.7,
        ), row=current_row, col=1)
        current_row += 1

    # MACD
    if show_macd:
        macd_colors = ["red" if v >= 0 else "green" for v in df["MACD_HIST"]]
        fig.add_trace(go.Bar(x=df["trade_date"], y=df["MACD_HIST"], name="MACD柱",
                             marker_color=macd_colors), row=current_row, col=1)
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["DIF"], name="DIF",
                                 line=dict(width=1, color="blue")), row=current_row, col=1)
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["DEA"], name="DEA",
                                 line=dict(width=1, color="orange")), row=current_row, col=1)
        current_row += 1

    # RSI
    if show_rsi:
        for p, color in [(6, "purple"), (12, "blue"), (24, "orange")]:
            fig.add_trace(go.Scatter(x=df["trade_date"], y=df[f"RSI{p}"], name=f"RSI{p}",
                                     line=dict(width=1, color=color)), row=current_row, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=current_row, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=current_row, col=1)
        current_row += 1

    # KDJ
    if show_kdj:
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["K"], name="K",
                                 line=dict(width=1, color="blue")), row=current_row, col=1)
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["D"], name="D",
                                 line=dict(width=1, color="orange")), row=current_row, col=1)
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["J"], name="J",
                                 line=dict(width=1, color="purple")), row=current_row, col=1)
        current_row += 1

    # OBV
    if show_obv:
        fig.add_trace(go.Scatter(x=df["trade_date"], y=df["OBV"], name="OBV",
                                 line=dict(width=1, color="teal")), row=current_row, col=1)
        current_row += 1

    # 换手率
    if show_turnover:
        fig.add_trace(go.Bar(x=df["trade_date"], y=df["turnover_rate"], name="换手率",
                             marker_color="rgba(100,100,200,0.5)"), row=current_row, col=1)

    fig.update_layout(
        height=200 + sub_count * 200,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_xaxes(type="category", nticks=20)
    st.plotly_chart(fig, use_container_width=True)

    # 资金流向
    st.markdown("---")
    st.subheader("资金流向")
    try:
        cf_rows = execute_query(
            """SELECT trade_date, main_net_inflow, super_large_net, large_net, medium_net, small_net
               FROM capital_flow WHERE stock_code=? AND trade_date BETWEEN ? AND ?
               ORDER BY trade_date""",
            [stock_code, start_date, end_date],
        )
        if cf_rows:
            cf_df = pd.DataFrame(cf_rows)
            fig_cf = go.Figure()
            for col_name, color in [("main_net_inflow", "red"), ("super_large_net", "orange"),
                                    ("large_net", "blue"), ("medium_net", "green"), ("small_net", "gray")]:
                if col_name in cf_df.columns:
                    label = {"main_net_inflow": "主力", "super_large_net": "超大单",
                             "large_net": "大单", "medium_net": "中单", "small_net": "小单"}
                    fig_cf.add_trace(go.Scatter(
                        x=cf_df["trade_date"], y=cf_df[col_name],
                        name=label.get(col_name, col_name), line=dict(color=color),
                    ))
            fig_cf.update_layout(height=300, template="plotly_dark", title="资金净流入")
            st.plotly_chart(fig_cf, use_container_width=True)
        else:
            st.caption("暂无资金流向数据")
    except Exception:
        st.caption("暂无资金流向数据")

    # 相关资讯
    st.markdown("---")
    st.subheader("相关资讯")
    try:
        news = execute_query(
            """SELECT ci.summary, ci.sentiment, ci.importance, ci.cleaned_at
               FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ic.stock_code=? AND ci.cleaned_at BETWEEN ? AND ?
               ORDER BY ci.importance DESC LIMIT 20""",
            [stock_code, start_date, end_date],
        )
        if news:
            for n in news:
                emoji = "🟢" if n["sentiment"] == "positive" else "🔴" if n["sentiment"] == "negative" else "⚪"
                st.markdown(f"{emoji} [{n['importance']}⭐] {n['summary']} _{n['cleaned_at']}_")
        else:
            st.caption("暂无相关资讯")
    except Exception:
        st.caption("暂无相关资讯")
