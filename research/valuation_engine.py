"""估值引擎 — 4步pipeline + tool_use混合架构

调用链:
  4a: 分部估值方法选择（结构化pipeline）
  4b: 知识库检索驱动因素预期值（tool_use循环）
  4c: 分部级估值计算（结构化pipeline）
  4d: 汇总 + 宏观溢价/折价(占位) + 内在价值（结构化pipeline）
"""
import json
import logging

logger = logging.getLogger(__name__)


def run_valuation(
    stock_code: str,
    stock_name: str,
    step1_result: dict,
    step2_result: dict,
    step3_result: dict,
    profile: dict,
    progress_callback=None,
) -> dict:
    """执行完整估值分析

    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        step1_result: 商业模式分析结果（含_for_valuation）
        step2_result: 产业链分析结果（含_for_valuation）
        step3_result: 财务分析结果（含_for_valuation）
        profile: universal_db.get_stock_profile() 返回
        progress_callback: 进度回调 fn(msg)

    Returns:
        dict，兼容旧前端字段 + 新 _valuation_detail
    """
    from utils.model_router import call_model_json, call_model_with_tools
    from research.valuation_prompts import (
        VALUATION_METHOD_SELECT_PROMPT,
        VALUATION_KG_RETRIEVAL_SYSTEM_PROMPT,
        VALUATION_CALCULATION_PROMPT,
        VALUATION_SYNTHESIS_PROMPT,
    )
    from research.valuation_tools import VALUATION_TOOLS, execute_tool

    def _progress(msg):
        if progress_callback:
            progress_callback(msg)

    # 提取上游 _for_valuation 数据
    bm_val = step1_result.get("_for_valuation", {}) or {}
    vc_val = step2_result.get("_for_valuation", {}) or {}
    fin_val = step3_result.get("_for_valuation", {}) or {}

    # ── Step 4a: 方法选择 ────────────────────────────────────────────────────
    _progress("[4a] 估值方法选择...")
    from research.valuation_method_matrix import format_method_matrix_for_prompt
    # 从 profile 读取行业信息
    info = (profile or {}).get("info") or {}
    industry_l1 = info.get("industry_l1") or info.get("industry") or ""
    industry_l2 = info.get("industry_l2") or ""

    # 优先复用 Step3 已程序化确定的 _method_selection，跳过 LLM 调用
    s3_method_sel = (step3_result or {}).get("_method_selection") if isinstance(step3_result, dict) else None
    if s3_method_sel and isinstance(s3_method_sel, dict):
        logger.info(f"[4a] 复用 Step3._method_selection（{len(s3_method_sel)} 个分部），跳过 LLM")
        method_result = _convert_step3_method_selection(s3_method_sel, step1_result)
    else:
        # fallback：调用 LLM 选方法
        industry_matrix_text = format_method_matrix_for_prompt(industry_l1, industry_l2)
        context_4a = _build_method_select_context(step1_result, step2_result, step3_result)
        prompt_4a = VALUATION_METHOD_SELECT_PROMPT.format(
            context=context_4a,
            industry_matrix_text=industry_matrix_text,
        )
        try:
            method_result = call_model_json(
                "valuation_method_select",
                prompt_4a,
                f"公司: {stock_code} {stock_name}\n请为每个收入分部选择估值方法。",
                max_tokens=4096,
            )
            if not isinstance(method_result, dict):
                method_result = {"segments": [], "cross_segment_notes": "方法选择解析失败"}
        except Exception as e:
            logger.warning(f"方法选择失败: {e}，使用默认方法")
            method_result = {
                "segments": _build_default_method_selection(step1_result),
                "cross_segment_notes": f"方法选择步骤失败({e})，使用默认PE方法",
            }

    # ── Step 4b: 知识库检索（tool_use）──────────────────────────────────────
    _progress("[4b] 知识库数据检索...")
    required_data = _extract_required_data_summary(
        method_result, stock_code, stock_name,
        upstream_bm=bm_val, upstream_fin=fin_val,
    )
    system_4b = VALUATION_KG_RETRIEVAL_SYSTEM_PROMPT.format(
        required_data_summary=required_data
    )

    # 收集 Step 1-3 中已注入的 RAG 上下文（优先复用，减少重复检索）
    upstream_rag_context = ""
    try:
        from research.rag_context import search_stock_context
        rag_ctx = search_stock_context(
            stock_code,
            f"{stock_name} 估值 可比公司 研报评级 目标价 驱动因素预期",
            top_k=8,
        )
        if rag_ctx:
            upstream_rag_context = f"\n=== Step 1-3 上游RAG已检索的数据（优先复用，无需重复检索）===\n{rag_ctx}\n"
    except Exception as _rag_e:
        logger.debug(f"4b RAG上游检索跳过: {_rag_e}")

    messages_4b = [
        {"role": "system", "content": system_4b},
        {"role": "user", "content": (
            f"公司: {stock_code} {stock_name}\n"
            f"{upstream_rag_context}"
            f"请开始检索数据，按工作流程逐步收集所有需要的估值数据。"
            f"注意：上游RAG数据已提供，只需补充其中未覆盖的驱动因素数据。"
        )},
    ]

    kg_result = {"content": "", "tool_calls_log": []}
    try:
        kg_result = call_model_with_tools(
            "valuation_kg_retrieval",
            messages_4b,
            VALUATION_TOOLS,
            tool_executor=execute_tool,
            max_rounds=8,
        )
    except Exception as e:
        logger.warning(f"知识库检索失败: {e}，继续使用空数据")
        kg_result = {
            "content": f"知识库检索失败: {e}",
            "tool_calls_log": [],
        }

    # ── Step 4c: 分部估值计算 ────────────────────────────────────────────────
    _progress("[4c] 分部估值计算...")
    upstream_val_data = {
        "business_model_for_valuation": bm_val,
        "value_chain_for_valuation": vc_val,
        "financial_for_valuation": fin_val,
    }

    prompt_4c = VALUATION_CALCULATION_PROMPT.format(
        method_selection=json.dumps(method_result, ensure_ascii=False, indent=2)[:3000],
        kg_retrieval_data=(kg_result.get("content") or "无数据")[:2000],
        upstream_valuation_data=_compress_upstream_val_data(upstream_val_data, limit=6000),
    )

    # 构建丰富的 user_message：包含原始 Step 1-3 分析 + 行情/财务原始数据 + 4b检索结果
    calc_user_parts = [f"公司: {stock_code} {stock_name}\n请计算每个分部的估值。"]

    # 注入 4b 知识库检索的具体数据（tool_calls_log 中的实际结果）
    kg_tool_data = kg_result.get("tool_calls_log") or []
    if kg_tool_data:
        kg_details = []
        for call in kg_tool_data:
            tool_result = call.get("result") or call.get("output") or {}
            if isinstance(tool_result, dict) and tool_result.get("data_available"):
                driver = tool_result.get("driver_name", "")
                for r in (tool_result.get("results") or [])[:3]:
                    quote = r.get("source_quote", "")
                    if quote:
                        kg_details.append(f"  [{driver}] {r.get('source', '')}: {quote[:200]}")
        if kg_details:
            calc_user_parts.append(
                "\n=== 知识库检索到的驱动因素预期数据 ===\n" + "\n".join(kg_details[:20])
            )

    # 注入 Step 1-3 原始分析结果（非 _for_valuation，而是完整的分析文本）
    for label, step_data in [("商业模式", step1_result), ("产业链", step2_result), ("财务分析", step3_result)]:
        if step_data:
            # 排除 _for_valuation（已在 prompt system 中传了），保留展示字段
            display_data = {k: v for k, v in step_data.items() if k != "_for_valuation"}
            if display_data:
                calc_user_parts.append(f"\n=== {label}分析结果 ===\n{json.dumps(display_data, ensure_ascii=False, indent=2)[:1500]}")
    # 注入行情/财务原始数据（统一转为亿元，与prompt要求一致）
    info = (profile or {}).get("info") or {}
    financials = (profile or {}).get("financials") or []
    daily = (profile or {}).get("daily") or []
    if info.get("market_cap"):
        calc_user_parts.append(f"\n总市值: {info['market_cap']}亿元")
    if daily:
        calc_user_parts.append(f"最新股价: {daily[0].get('close')}元")
    if financials:
        calc_user_parts.append("\n=== 财务报表数据（单位：亿元）===")
        for f in financials[:6]:
            rev = f.get('revenue')
            np_ = f.get('net_profit')
            # DB中存储单位为万元，转为亿元
            rev_bn = round(rev / 10000, 2) if rev else '—'
            np_bn = round(np_ / 10000, 2) if np_ else '—'
            calc_user_parts.append(
                f"  {f.get('report_period','')}: 营收{rev_bn}亿元 "
                f"净利{np_bn}亿元 ROE{f.get('roe','')}% "
                f"营收YoY{f.get('revenue_yoy','')}% 利润YoY{f.get('profit_yoy','')}% "
                f"EPS{f.get('eps','')}元"
            )
    calc_user_msg = "\n".join(calc_user_parts)

    try:
        calc_result = call_model_json(
            "valuation_calculation",
            prompt_4c,
            calc_user_msg,
            max_tokens=8192,
        )
        if not isinstance(calc_result, dict):
            calc_result = {"segment_valuations": []}
    except Exception as e:
        logger.warning(f"分部估值计算失败: {e}")
        calc_result = {"segment_valuations": [], "error": str(e)}

    # 校验 + 修正 4c 输出（传入行业信息用于方法合规校验）
    calc_result = _validate_and_fix_4c_output(calc_result, industry_l1=industry_l1, industry_l2=industry_l2)

    # ── Step 4d: 估值汇总（程序化计算 + LLM定性评估）────────────────────────
    _progress("[4d] 估值汇总...")
    company_basics = _build_company_basics(profile, fin_val, stock_code, stock_name)

    # 程序化汇总：从 4c segment_valuations 直接计算（不依赖 LLM 做算术）
    numeric_synthesis = _programmatic_synthesis(calc_result, company_basics)

    # LLM 只做定性评估：置信度、不确定因素、假设审计
    try:
        prompt_4d = VALUATION_SYNTHESIS_PROMPT.format(
            segment_valuations=json.dumps(
                calc_result.get("segment_valuations", []),
                ensure_ascii=False, indent=2
            )[:4000],
            company_basics=json.dumps(company_basics, ensure_ascii=False, indent=2)[:1500],
        )
        qualitative = call_model_json(
            "valuation_synthesis",
            prompt_4d,
            f"公司: {stock_code} {stock_name}\n请汇总各分部估值，计算内在价值。",
            max_tokens=8192,
        )
        if isinstance(qualitative, dict):
            # 只取 LLM 的定性字段，数字字段用程序化计算结果覆盖
            numeric_synthesis["confidence_assessment"] = qualitative.get(
                "confidence_assessment", numeric_synthesis.get("confidence_assessment", {})
            )
            numeric_synthesis["assumption_audit_trail"] = qualitative.get(
                "assumption_audit_trail", []
            )
            if qualitative.get("sum_of_parts", {}).get("synergy_adjustment", {}).get("detail"):
                numeric_synthesis["sum_of_parts"]["synergy_adjustment"]["detail"] = (
                    qualitative["sum_of_parts"]["synergy_adjustment"]["detail"]
                )
    except Exception as e:
        logger.warning(f"LLM定性评估失败: {e}，仅使用程序化汇总")

    return _format_valuation_output(numeric_synthesis, calc_result, kg_result, method_result)


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _convert_step3_method_selection(s3_sel: dict, step1_result: dict) -> dict:
    """将 Step3._method_selection 的格式转换为 valuation_engine 期望的 method_result 格式

    Step3 格式: {segment_name: {method, notes, industry_l1, industry_l2, forbidden, ...}}
    engine 期望: {segments: [{segment_name, primary_method, industry_classification, ...}], ...}
    """
    from research.valuation_method_matrix import select_method_for_industry

    segments_out = []
    for seg_name, seg_info in s3_sel.items():
        l1 = seg_info.get("industry_l1", "")
        l2 = seg_info.get("industry_l2", "")
        method = seg_info.get("method", "PE")
        rule = select_method_for_industry(l1, l2)

        # 从 step1 找该分部的驱动因素，填 required_driver_expectations
        req_drivers = []
        seg_drivers = ((step1_result or {}).get("_for_valuation") or {}).get("segment_drivers") or []
        for sd in seg_drivers:
            if sd.get("segment_name") == seg_name:
                for d in (sd.get("drivers") or []):
                    driver_name = d.get("driver_name") or d.get("name") or ""
                    if driver_name:
                        req_drivers.append({"driver_name": driver_name, "periods": ["2025", "2026", "2027"]})

        segments_out.append({
            "segment_name": seg_name,
            "industry_classification": l1,
            "industry_l2": l2,
            "primary_method": method,
            "forbidden_methods": rule.get("forbidden", []),
            "method_reason": seg_info.get("notes", "") + f"（Step3程序确定，匹配依据: {seg_info.get('_matched_by', '')}）",
            "required_driver_expectations": req_drivers,
            "required_financial_elements": ["revenue_forward", "eps_forward"],
            "alternative_method": rule.get("alternative", ["PE"])[0] if rule.get("alternative") else "PE",
            "alternative_reason": "备选方法（来自行业矩阵）",
            "_source": "step3_program",
        })

    return {
        "segments": segments_out,
        "cross_segment_notes": f"方法由 Step3 程序化确定（共{len(segments_out)}个分部）",
    }


def _build_method_select_context(step1: dict, step2: dict, step3: dict) -> str:
    """构建方法选择的上下文"""
    parts = []

    # Step1 核心信息
    if step1:
        parts.append("## 商业模式")
        segs = step1.get("revenue_segments", [])
        if segs:
            parts.append("收入分部: " + ", ".join(
                f"{s.get('name', '')}({s.get('pct', '')}%)" for s in segs
            ))
        val1 = step1.get("_for_valuation", {}) or {}
        if val1.get("segment_drivers"):
            parts.append("驱动因素分部: " + json.dumps(
                val1["segment_drivers"], ensure_ascii=False
            )[:800])

    # Step2 产业链
    if step2:
        parts.append("\n## 产业链")
        val2 = step2.get("_for_valuation", {}) or {}
        if val2.get("segment_industry_context"):
            parts.append(json.dumps(
                val2["segment_industry_context"], ensure_ascii=False
            )[:800])

    # Step3 财务
    if step3:
        parts.append("\n## 财务概况")
        rt = step3.get("revenue_trend", [])
        if rt:
            parts.append("营收趋势: " + json.dumps(rt[:4], ensure_ascii=False))
        val3 = step3.get("_for_valuation", {}) or {}
        if val3.get("valuation_ready_data"):
            parts.append("估值数据: " + json.dumps(
                val3["valuation_ready_data"], ensure_ascii=False
            ))

    return "\n".join(parts) if parts else "无上游分析数据"


def _extract_required_data_summary(
    method_result: dict,
    stock_code: str,
    stock_name: str,
    upstream_bm: dict = None,
    upstream_fin: dict = None,
) -> str:
    """从方法选择结果提取需要检索的数据摘要

    已在上游 _for_valuation 中有数据的 driver 跳过搜索，避免重复检索。
    """
    # 收集上游已有数据的 driver 名称
    covered_drivers = set()
    for seg_drivers in ((upstream_bm or {}).get("segment_drivers") or []):
        for d in (seg_drivers.get("drivers") or [seg_drivers]):
            name = d.get("driver_name") or d.get("name") or ""
            if name and (d.get("price_latest_value") or d.get("quantity_latest_value")):
                covered_drivers.add(name)
    for mapping in ((upstream_fin or {}).get("driver_financial_mapping") or []):
        name = mapping.get("driver_name") or ""
        if name and mapping.get("implied_asp"):
            covered_drivers.add(name)

    lines = [f"公司: {stock_code} {stock_name}", "\n需要检索的数据（已跳过上游已覆盖的驱动因素）:"]
    if covered_drivers:
        lines.append(f"上游已覆盖，跳过搜索: {', '.join(sorted(covered_drivers))}")

    for seg in (method_result.get("segments") or []):
        seg_name = seg.get("segment_name", "未知分部")
        lines.append(f"\n### {seg_name} ({seg.get('primary_method', 'PE')}估值)")
        for req in (seg.get("required_driver_expectations") or []):
            driver = req.get("driver_name", "")
            if driver in covered_drivers:
                lines.append(f"  - [跳过，上游已有] {driver}")
                continue
            periods = req.get("periods", ["2026", "2027"])
            lines.append(f"  - 驱动因素: {driver}，预期年份: {', '.join(periods)}")
        for elem in (seg.get("required_financial_elements") or []):
            lines.append(f"  - 财务要素: {elem}")
        # 可比公司倍数始终需要搜索
        lines.append(f"  - 可比公司估值倍数（{seg.get('primary_method', 'PE')}中位数）")

    return "\n".join(lines)


def _build_default_method_selection(step1: dict) -> list:
    """方法选择失败时构建默认分部列表"""
    segs = []
    for s in (step1.get("revenue_segments") or []):
        segs.append({
            "segment_name": s.get("name", "主营业务"),
            "primary_method": "PE",
            "method_reason": "默认PE估值（方法选择步骤失败）",
            "required_driver_expectations": [],
            "required_financial_elements": ["eps_forward"],
        })
    if not segs:
        segs = [{"segment_name": "整体业务", "primary_method": "PE",
                 "method_reason": "默认PE估值", "required_driver_expectations": []}]
    return segs


def _build_company_basics(profile: dict, fin_val: dict,
                           stock_code: str, stock_name: str) -> dict:
    """构建公司基础数据"""
    info = (profile or {}).get("info") or {}
    daily = (profile or {}).get("daily") or []

    current_price = None
    if daily:
        current_price = daily[0].get("close")

    # 从市值和股价推算总股本
    market_cap_bn = info.get("market_cap")  # 亿元
    total_shares = None
    if market_cap_bn and current_price and current_price > 0:
        total_shares = round(market_cap_bn * 1e8 / current_price)  # 股

    basics = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "market_cap_bn": market_cap_bn,
        "market_cap": (market_cap_bn * 1e8) if market_cap_bn else None,
        "industry": info.get("industry_l2") or info.get("industry_l1"),
        "current_price": current_price,
        "total_shares": total_shares,
        "net_cash_or_debt": None,
    }

    # 如果 fin_val 有更精确的数据，覆盖
    vrd = (fin_val or {}).get("valuation_ready_data") or {}
    if vrd:
        if vrd.get("latest_price"):
            basics["current_price"] = vrd["latest_price"]
        if vrd.get("total_shares"):
            basics["total_shares"] = vrd["total_shares"]
        if vrd.get("net_cash_or_debt") is not None:
            basics["net_cash_or_debt"] = vrd["net_cash_or_debt"]
        basics["latest_pe_ttm"] = vrd.get("latest_pe_ttm")
        basics["latest_pb"] = vrd.get("latest_pb")
        basics["latest_ps_ttm"] = vrd.get("latest_ps_ttm")

    return basics


def _programmatic_synthesis(calc_result: dict, company_basics: dict) -> dict:
    """程序化汇总 — 从4c的segment_valuations直接计算，不依赖LLM做算术

    从每个分部的 segment_value 或 present_value 提取 base/bull/bear，
    加总得到EV，减去净债务得到股权价值，除以总股本得到每股价值。
    """
    from research.macro_valuation import get_macro_valuation_context
    seg_vals = calc_result.get("segment_valuations") or []

    segment_values_list = []
    total_base = 0
    total_bull = 0
    total_bear = 0

    for sv in seg_vals:
        seg_name = sv.get("segment_name", "未知")

        # 优先取 present_value（折现后），否则取 segment_value
        pv = sv.get("present_value") or sv.get("segment_value") or {}
        base = _to_num(pv.get("base_case", 0))
        bull = _to_num(pv.get("bull_case", 0))
        bear = _to_num(pv.get("bear_case", 0))

        # 单位归一化到"元"
        # 使用 LLM 输出的 value_unit 字段做确定性转换
        # prompt 明确要求输出亿元，缺失时也按亿元处理（不做启发式猜测）
        value_unit = sv.get("value_unit", "").strip()
        if not value_unit:
            value_unit = "亿元"  # prompt要求的默认单位
            logger.warning(f"分部'{seg_name}' 缺少value_unit字段，按亿元处理")

        if value_unit == "亿元":
            base *= 1e8
            bull *= 1e8
            bear *= 1e8
        elif value_unit == "万元":
            base *= 1e4
            bull *= 1e4
            bear *= 1e4
        elif value_unit == "元":
            pass  # 已经是元
        else:
            # 未知单位，按亿元处理
            base *= 1e8
            bull *= 1e8
            bear *= 1e8
            logger.warning(f"分部'{seg_name}' value_unit='{value_unit}' 未识别，按亿元处理")

        # 合理性校验（仅日志告警，不修改数值）
        market_cap = _to_num(company_basics.get("market_cap"))  # 元
        if market_cap <= 0:
            market_cap_bn = _to_num(company_basics.get("market_cap_bn", 0))
            market_cap = market_cap_bn * 1e8
        if base > 0 and market_cap > 0:
            ratio = base / market_cap
            if ratio > 5.0:
                logger.warning(
                    f"⚠️ 分部'{seg_name}' 估值{base/1e8:.0f}亿 是市值{market_cap/1e8:.0f}亿的"
                    f" {ratio:.1f}倍，可能存在单位或计算异常"
                )
            elif ratio < 0.001:
                logger.warning(
                    f"⚠️ 分部'{seg_name}' 估值{base/1e8:.4f}亿 远小于市值{market_cap/1e8:.0f}亿"
                    f" (ratio={ratio:.6f})，可能存在单位异常"
                )

        segment_values_list.append({
            "name": seg_name,
            "base": base,
            "bull": bull,
            "bear": bear,
        })
        total_base += base
        total_bull += bull
        total_bear += bear

    # 净现金/净债务
    net_cash = _to_num(company_basics.get("net_cash_or_debt", 0))

    # 股权价值 = EV + 净现金（正为现金，负为负债）
    equity_base = total_base + net_cash
    equity_bull = total_bull + net_cash
    equity_bear = total_bear + net_cash

    # 总股本 & 每股价值
    shares = _to_num(company_basics.get("total_shares", 0))
    current_price = _to_num(company_basics.get("current_price", 0))

    ps_base = round(equity_base / shares, 2) if shares > 0 else 0
    ps_bull = round(equity_bull / shares, 2) if shares > 0 else 0
    ps_bear = round(equity_bear / shares, 2) if shares > 0 else 0

    upside = round((ps_base / current_price - 1) * 100, 1) if current_price > 0 and ps_base > 0 else 0

    # ── 宏观乘数 ──────────────────────────────────────────────────────────────
    try:
        macro_ctx = get_macro_valuation_context()
    except Exception as e:
        logger.warning(f"宏观乘数获取失败，使用中性值: {e}")
        macro_ctx = {
            "liquidity_multiplier": 1.0,
            "liquidity_basis": f"获取失败({e})，使用中性值",
            "sentiment_multiplier": 1.0,
            "sentiment_basis": "获取失败，使用中性值",
            "macro_data_available": False,
            "multiplier_note": "宏观数据获取异常",
        }

    liq_m = macro_ctx.get("liquidity_multiplier", 1.0)
    sent_m = macro_ctx.get("sentiment_multiplier", 1.0)

    # 应用宏观乘数到 EV
    adjusted_base = total_base * liq_m * sent_m
    adjusted_bull = total_bull * liq_m * sent_m
    adjusted_bear = total_bear * liq_m * sent_m

    adj_equity_base = adjusted_base + net_cash
    adj_equity_bull = adjusted_bull + net_cash
    adj_equity_bear = adjusted_bear + net_cash

    adj_ps_base = round(adj_equity_base / shares, 2) if shares > 0 else 0
    adj_ps_bull = round(adj_equity_bull / shares, 2) if shares > 0 else 0
    adj_ps_bear = round(adj_equity_bear / shares, 2) if shares > 0 else 0

    adj_upside = round((adj_ps_base / current_price - 1) * 100, 1) if current_price > 0 and adj_ps_base > 0 else upside

    return {
        "sum_of_parts": {
            "segment_values": segment_values_list,
            "segments_total": {"base": total_base, "bull": total_bull, "bear": total_bear},
            "synergy_adjustment": {"value": 0, "detail": ""},
            "base_enterprise_value": {"base": total_base, "bull": total_bull, "bear": total_bear},
        },
        "macro_adjustment": {
            "liquidity_multiplier": liq_m,
            "liquidity_basis": macro_ctx.get("liquidity_basis", ""),
            "sentiment_multiplier": sent_m,
            "sentiment_basis": macro_ctx.get("sentiment_basis", ""),
            "macro_data_available": macro_ctx.get("macro_data_available", False),
            "multiplier_note": macro_ctx.get("multiplier_note", ""),
            "adjusted_ev": {"base": adjusted_base, "bull": adjusted_bull, "bear": adjusted_bear},
        },
        "equity_bridge": {
            "enterprise_value": {"base": adjusted_base, "bull": adjusted_bull, "bear": adjusted_bear},
            "net_cash_or_debt": net_cash,
            "equity_value": {"base": adj_equity_base, "bull": adj_equity_bull, "bear": adj_equity_bear},
            "shares_outstanding": shares,
            "per_share_value": {"base": adj_ps_base, "bull": adj_ps_bull, "bear": adj_ps_bear},
        },
        "vs_market": {
            "current_price": current_price,
            "base_upside_pct": adj_upside,
            "margin_of_safety_pct": round(-adj_upside, 1) if adj_upside < 0 else 0,
        },
        "confidence_assessment": {
            "overall_confidence": "medium" if seg_vals else "low",
            "high_confidence_segments": [],
            "low_confidence_segments": [],
            "key_uncertainties": [],
            "data_gaps": ["_for_valuation数据不完整"] if not seg_vals else [],
        },
        "assumption_audit_trail": [],
    }


def _compress_upstream_val_data(upstream_val_data: dict, limit: int = 6000) -> str:
    """智能压缩上游_for_valuation数据：保留所有driver的关键字段，去掉冗余描述

    压缩策略：
    1. 保留 segment_drivers 中每个 driver 的 name/price_latest_value/quantity_latest_value/source
    2. 保留 driver_financial_mapping 的 implied_asp/driver_name
    3. 按 segment_pct 降序保留最重要分部（如仍超限）
    """
    def _slim_drivers(drivers_list):
        slim = []
        for d in (drivers_list or []):
            entry = {}
            for key in ("driver_name", "name", "price_latest_value", "quantity_latest_value",
                        "source", "unit", "pct_of_revenue"):
                if d.get(key) is not None:
                    entry[key] = d[key]
            if entry:
                slim.append(entry)
        return slim

    compressed = {}
    bm = upstream_val_data.get("business_model_for_valuation") or {}
    fin = upstream_val_data.get("financial_for_valuation") or {}
    vc = upstream_val_data.get("value_chain_for_valuation") or {}

    # 压缩 segment_drivers
    seg_drivers = bm.get("segment_drivers") or []
    compressed_segs = []
    for seg in seg_drivers:
        c_seg = {
            "segment_name": seg.get("segment_name") or seg.get("name"),
            "segment_pct": seg.get("segment_pct"),
            "drivers": _slim_drivers(seg.get("drivers") or [seg]),
        }
        compressed_segs.append(c_seg)
    # 按 segment_pct 降序
    compressed_segs.sort(key=lambda x: float(x.get("segment_pct") or 0), reverse=True)
    if compressed_segs:
        compressed["segment_drivers"] = compressed_segs

    # 压缩 driver_financial_mapping
    dfm = fin.get("driver_financial_mapping") or []
    if dfm:
        compressed["driver_financial_mapping"] = [
            {k: v for k, v in m.items() if k in ("driver_name", "implied_asp", "unit", "source")}
            for m in dfm
        ]

    # 保留 valuation_ready_data（关键财务倍数）
    vrd = fin.get("valuation_ready_data") or {}
    if vrd:
        compressed["valuation_ready_data"] = vrd

    # 保留产业链关键数据
    if vc.get("segment_industry_context"):
        compressed["segment_industry_context"] = vc["segment_industry_context"]

    result = json.dumps(compressed, ensure_ascii=False, indent=2)
    if len(result) <= limit:
        return result

    # 仍超限：只保留 top-3 分部
    if compressed.get("segment_drivers"):
        compressed["segment_drivers"] = compressed["segment_drivers"][:3]
    result = json.dumps(compressed, ensure_ascii=False, indent=2)
    if len(result) <= limit:
        return result

    # 最终截断
    return result[:limit] + "\n... (已截断，请优先参考上方数据)"


def _to_num(v) -> float:
    """安全地将值转为 float"""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _validate_and_fix_4c_output(calc_result: dict, industry_l1: str = "",
                                 industry_l2: str = "") -> dict:
    """校验并修正 4c LLM 输出，确保关键字段存在且合规

    新增校验：
    - 风险因素 impact_pct 按来源质量程序化 cap
    - 估值方法合规校验（对照 INDUSTRY_METHOD_MATRIX）
    - 增速假设空泛检测增强（检查是否引用了 upstream_reference）
    """
    from research.valuation_method_matrix import check_method_compliance

    VAGUE_PATTERNS = ["保守估计", "假设增长", "预计增长", "默认", "温和增长", "保守假设",
                      "假设X%", "假设增速", "适度增长", "稳健增长"]
    SPECULATIVE_WORDS = ["可能", "或许", "预计", "猜测", "推测", "估计可能", "大概"]

    seg_vals = calc_result.get("segment_valuations")
    if not isinstance(seg_vals, list):
        logger.warning("4c输出缺少segment_valuations列表")
        calc_result["segment_valuations"] = []
        return calc_result

    for sv in seg_vals:
        seg_name = sv.get("segment_name", "未知")

        # 1. 强制 value_unit = "亿元"
        if sv.get("value_unit", "").strip() != "亿元":
            logger.warning(f"分部'{seg_name}' value_unit='{sv.get('value_unit')}' → 强制修正为亿元")
            sv["value_unit"] = "亿元"

        # 2. segment_value 不允许全为0
        seg_val = sv.get("segment_value") or {}
        if _to_num(seg_val.get("base_case")) == 0:
            logger.warning(f"分部'{seg_name}' segment_value.base_case=0，标记data_sufficient=false")
            sv["data_sufficient"] = False

        # 3. 估值方法合规校验
        method_used = sv.get("method") or ""
        if method_used and industry_l1:
            is_ok, reason = check_method_compliance(method_used, industry_l1, industry_l2)
            if not is_ok:
                logger.warning(f"分部'{seg_name}' 方法不合规: {reason}")
                sv["_method_compliance_warning"] = reason

        # 4. 风险因素 impact_pct 程序化 cap
        for cf in (sv.get("constraint_factors") or []):
            source = (cf.get("source") or "").strip()
            impact = _to_num(cf.get("impact_pct", 0))

            if not source:
                # 无来源：cap 到 ±5%
                capped_impact = max(impact, -5.0) if impact < 0 else min(impact, 5.0)
                if capped_impact != impact:
                    cf["impact_pct"] = capped_impact
                    cf["_capped"] = True
                    cf["_cap_reason"] = f"无来源引用，impact_pct 原值{impact}%已 cap 至±5%上限"
                    logger.info(f"分部'{seg_name}' 风险因素'{cf.get('factor','')}' cap: {impact}% → {capped_impact}%")
            elif any(w in source for w in SPECULATIVE_WORDS):
                # 推测性来源：cap 到 ±10%
                capped_impact = max(impact, -10.0) if impact < 0 else min(impact, 10.0)
                if capped_impact != impact:
                    cf["impact_pct"] = capped_impact
                    cf["_capped"] = True
                    cf["_cap_reason"] = f"来源含推测性表述，impact_pct 原值{impact}%已 cap 至±10%上限"
                    logger.info(f"分部'{seg_name}' 风险因素'{cf.get('factor','')}' 推测性 cap: {impact}% → {capped_impact}%")

        # 5. revenue_forecast 必须有 upstream_drivers_used
        rf = sv.get("revenue_forecast") or {}
        for yr_key in ["year_1", "year_2", "year_3"]:
            yr = rf.get(yr_key)
            if not isinstance(yr, dict):
                continue
            if not yr.get("upstream_drivers_used"):
                logger.warning(f"分部'{seg_name}' {yr_key} 缺少upstream_drivers_used")
                yr["_validation_warning"] = "未标注使用的上游驱动因素"

        # 6. assumptions derivation 空泛检测（增强版）
        for a in (sv.get("assumptions") or []):
            deriv = (a.get("derivation") or "").strip()
            basis = (a.get("upstream_reference") or "").strip()
            issues = []

            if any(vague in deriv for vague in VAGUE_PATTERNS):
                issues.append(f"derivation含空泛表述: '{deriv[:60]}'")

            # 检查 derivation 是否包含具体数字
            import re
            if deriv and not re.search(r'\d+', deriv):
                issues.append("derivation无具体数字，缺乏量化推导")

            # 检查是否引用了 upstream_reference
            if not basis and len(deriv) < 30:
                issues.append("未提供 upstream_reference，且 derivation 过短")

            if issues:
                for issue in issues:
                    logger.warning(f"分部'{seg_name}' assumption '{a.get('item','')}': {issue}")
                a["confidence"] = "low"
                existing_note = a.get("data_gap_note") or ""
                a["data_gap_note"] = (existing_note + " | " if existing_note else "") + "; ".join(issues)

    return calc_result


def _build_fallback_synthesis(calc_result: dict, company_basics: dict) -> dict:
    """估值汇总失败时的降级合成 — 委托给 _programmatic_synthesis"""
    return _programmatic_synthesis(calc_result, company_basics)


def _format_valuation_output(synthesis: dict, calc_result: dict,
                              kg_result: dict, method_result: dict) -> dict:
    """格式化输出 — 兼容旧前端字段 + 新详细数据"""
    equity = synthesis.get("equity_bridge") or {}
    per_share = equity.get("per_share_value") or {}
    base_val = per_share.get("base") or 0
    bull_val = per_share.get("bull") or 0
    bear_val = per_share.get("bear") or 0
    current = (synthesis.get("vs_market") or {}).get("current_price") or 0

    # 从calc_result提取可比公司列表
    comparables = []
    for sv in (calc_result.get("segment_valuations") or []):
        bench = sv.get("peer_benchmark") or {}
        if bench.get("peer_median"):
            comparables.append({
                "company": f"同行中位({sv.get('segment_name', '')})",
                "pe": bench.get("peer_median") if bench.get("metric_name") == "PE" else None,
                "pb": None,
                "ps": bench.get("peer_median") if bench.get("metric_name") == "PS" else None,
            })

    # 提取关键步骤作为 calculation_steps（旧前端展示用）
    calc_steps = []
    seg_vals_raw = calc_result.get("segment_valuations") or []

    for sv in seg_vals_raw:
        seg_name = sv.get("segment_name", "")
        method = sv.get("method", "")
        seg_val = sv.get("segment_value") or sv.get("present_value") or {}
        base_v = _to_num(seg_val.get("base_case", 0))

        # 从 assumptions 提取关键推导
        key_assumptions = []
        for a in (sv.get("assumptions") or [])[:3]:
            if a.get("value") and a["value"] != 0:
                key_assumptions.append(f"{a.get('item','')}: {a.get('value')}{a.get('unit','')}")

        # 从 revenue_forecast 提取
        rf = sv.get("revenue_forecast") or {}
        yr1 = rf.get("year_1", {})
        if yr1.get("value"):
            key_assumptions.append(f"Year1收入预测: {yr1['value']}")

        formula_parts = f"{method}法"
        if key_assumptions:
            formula_parts += f"（{'; '.join(key_assumptions[:2])}）"

        # 单位友好展示 — LLM输出的segment_value单位为亿元
        value_unit = sv.get("value_unit", "亿元")
        if base_v > 0:
            result_str = f"基准: {base_v:.1f}{value_unit}"
        else:
            result_str = "数据不足"

        calc_steps.append({
            "step": f"{seg_name} 分部估值",
            "formula": formula_parts,
            "result": result_str,
        })

    # 汇总步骤
    sop = synthesis.get("sum_of_parts") or {}
    total = sop.get("segments_total", {})
    total_base = _to_num(total.get("base", 0))
    if total_base > 0:
        calc_steps.append({
            "step": "分部加总 → 企业价值(EV)",
            "formula": " + ".join(
                f"{sv.get('name', '')}({sv.get('base', 0)/1e8:.0f}亿)" if sv.get('base', 0) > 1e8
                else f"{sv.get('name', '')}"
                for sv in (sop.get("segment_values") or [])
            ),
            "result": f"EV = {total_base/1e8:.1f}亿元",
        })

    # 宏观乘数步骤
    macro_adj = synthesis.get("macro_adjustment") or {}
    liq_m = macro_adj.get("liquidity_multiplier", 1.0)
    sent_m = macro_adj.get("sentiment_multiplier", 1.0)
    if macro_adj.get("macro_data_available") and (liq_m != 1.0 or sent_m != 1.0):
        adj_ev = _to_num((macro_adj.get("adjusted_ev") or {}).get("base", 0))
        calc_steps.append({
            "step": "宏观乘数调整",
            "formula": f"EV × 流动性{liq_m:.3f} × 情绪{sent_m:.3f}",
            "result": f"调整后EV = {adj_ev/1e8:.1f}亿元",
        })

    net_cash = _to_num(equity.get("net_cash_or_debt", 0))
    if net_cash != 0:
        calc_steps.append({
            "step": "加/减 净现金/净负债",
            "formula": f"EV {'+ 净现金' if net_cash > 0 else '- 净负债'} {abs(net_cash)/1e8:.1f}亿",
            "result": f"股权价值 = {_to_num(equity.get('equity_value', {}).get('base', 0))/1e8:.1f}亿元",
        })

    shares = _to_num(equity.get("shares_outstanding", 0))
    if shares > 0 and base_val > 0:
        calc_steps.append({
            "step": "每股价值 = 股权价值 / 总股本",
            "formula": f"总股本 {shares/1e8:.2f}亿股",
            "result": f"每股 {base_val:.2f}元 (乐观{bull_val:.2f} / 悲观{bear_val:.2f})",
        })

    upside_str = f"{((base_val/current - 1)*100):.1f}%" if current and base_val else "N/A"

    confidence = synthesis.get("confidence_assessment") or {}
    conclusion_parts = []
    if confidence.get("overall_confidence"):
        conclusion_parts.append(f"置信度{confidence['overall_confidence']}")
    if confidence.get("key_uncertainties"):
        conclusion_parts.append("关键不确定因素: " + "、".join(
            confidence["key_uncertainties"][:2]
        ))

    # 旧前端兼容字段
    legacy_output = {
        "method": "分部加总估值(SOTP)",
        "method_reason": (method_result.get("cross_segment_notes")
                          or "各收入分部特征不同，采用分部独立估值再加总"),
        "calculation_steps": calc_steps[:5],
        "intrinsic_value": base_val,
        "value_range": {"low": bear_val, "high": bull_val},
        "current_price": current,
        "upside": upside_str,
        "comparables": comparables[:4],
        "conclusion": "；".join(conclusion_parts) if conclusion_parts else "请查看详细估值数据",
    }

    return {
        **legacy_output,
        "_valuation_detail": synthesis,
        "_segment_valuations": calc_result.get("segment_valuations", []),
        "_method_selection": method_result,
        "_kg_retrieval_log": kg_result.get("tool_calls_log", []),
    }
