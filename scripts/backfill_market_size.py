"""批量补充产业链 baseline 中 market_size_billion 字段

数据来源: 问财（pywencai）— 概念板块总市值作为实时市场价值代理
查询模式: "XX概念股 总市值" → query_type='stock' → 汇总 getDataList 返回的总市值列

方案: 使用 pywencai 底层 API（_direct_request + convert）手动控制请求间隔，
      避免内部 retry 太激进导致空响应。

反爬策略:
- 每次请求间隔 10~18s
- 每10条暂停 30~60s
- 失败3次进入冷却 2min
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import random
import logging
from datetime import datetime

import pandas as pd
import pydash as _

from pywencai.wencai import _direct_request
from pywencai.headers import headers as wencai_headers
from pywencai.convert import convert, parse_url_params
from utils.db_utils import execute_cloud_query, execute_cloud_insert

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# ── 反爬配置 ─────────────────────────────────────────────────────
MIN_DELAY = 10
MAX_DELAY = 18
BATCH_PAUSE_EVERY = 10
BATCH_PAUSE_SECONDS = (30, 60)
MAX_CONSECUTIVE_FAIL = 3
COOLDOWN_SECONDS = 120

# ── 环节 → 问财查询关键词映射 ─────────────────────────────────────
# 部分环节名称不能直接作为概念板块搜索，需要映射
SEGMENT_QUERY_MAP = {
    # AI算力基础设施
    "GPU": "英伟达概念股 总市值",
    "TPU": "AI芯片概念股 总市值",
    "NPU": "AI芯片概念股 总市值",
    "FPGA": "芯片概念股 总市值",
    "AI服务器": "AI服务器概念股 总市值",
    "信创服务器": "信创概念股 总市值",
    "存储设备": "数据存储概念股 总市值",
    "网络设备": "交换机概念股 总市值",
    "供电系统": "液冷服务器概念股 总市值",
    "散热系统": "液冷服务器概念股 总市值",
    "光通信": "光模块概念股 总市值",
    "机房环境": "数据中心概念股 总市值",
    "AI算法平台": "人工智能概念股 总市值",
    "云服务": "云计算概念股 总市值",
    "垂直应用": "AIGC概念股 总市值",
    "开发工具": "国产软件概念股 总市值",
    # 半导体 v3
    "硅片": "硅片概念股 总市值",
    "靶材": "靶材概念股 总市值",
    "光刻胶": "光刻胶概念股 总市值",
    "湿电子化学品": "电子化学品概念股 总市值",
    "特气": "电子特气概念股 总市值",
    "刻蚀设备": "刻蚀概念股 总市值",
    "CVD设备": "半导体设备概念股 总市值",
    "测试设备": "半导体设备概念股 总市值",
    "单晶炉": "半导体设备概念股 总市值",
    "光刻机": "光刻机概念股 总市值",
    "存储器": "存储芯片概念股 总市值",
    "模拟芯片": "模拟芯片概念股 总市值",
    "射频芯片": "射频概念股 总市值",
    "MCU": "MCU概念股 总市值",
    "功率器件": "功率半导体概念股 总市值",
    "晶圆代工": "晶圆代工概念股 总市值",
    "IDM": "IDM概念股 总市值",
    "封装测试": "封装测试概念股 总市值",
    "先进封装": "先进封装概念股 总市值",
    "高端PCB": "PCB概念股 总市值",
    "电阻": "被动元件概念股 总市值",
    "电容": "MLCC概念股 总市值",
    "电感": "被动元件概念股 总市值",
    # 风电 v1
    "铸件": "风电铸件概念股 总市值",
    "主轴": "风电概念股 总市值",
    "轴承": "轴承概念股 总市值",
    "叶片": "风电叶片概念股 总市值",
    "碳纤维": "碳纤维概念股 总市值",
    "玻纤": "玻璃纤维概念股 总市值",
    "陆上风机": "风电整机概念股 总市值",
    "海上风机": "海上风电概念股 总市值",
    "直驱永磁": "永磁电机概念股 总市值",
    "双馈异步": "风电概念股 总市值",
    "塔筒": "风电塔筒概念股 总市值",
    "海缆": "海底电缆概念股 总市值",
    "升压站": "海上风电概念股 总市值",
    "安装服务": "海上风电概念股 总市值",
}


def _get_robot_data(question: str) -> dict:
    """调用 robot-data API 获取 condition 和 url_params"""
    data = {
        'question': question,
        'perpage': '10',
        'page': 1,
        'source': 'Ths_iwencai_Xuangu',
        'version': '2.0',
        'secondary_intent': 'stock',
        'add_info': json.dumps({'urp': {'scene': 1, 'company': 1, 'business': 1}, 'contentType': 'json', 'searchInfo': True}),
        'log_info': json.dumps({'input_type': 'click'}),
    }
    resp = _direct_request(
        method='POST',
        url='http://www.iwencai.com/customized/chart/get-robot-data',
        json=data,
        headers=wencai_headers(None, None),
    )
    if resp.status_code != 200:
        raise RuntimeError(f"robot-data HTTP {resp.status_code}")

    result = json.loads(resp.text)
    content_raw = _.get(result, 'data.answer.0.txt.0.content')
    if not content_raw:
        raise RuntimeError(f"robot-data 无 content")
    content = json.loads(content_raw) if isinstance(content_raw, str) else content_raw
    comps = content.get('components', [])
    if not comps:
        raise RuntimeError("robot-data 无 components")

    c0 = comps[0]
    url = _.get(c0, 'config.other_info.footer_info.url')
    url_params = parse_url_params(url)
    row_count = _.get(c0, 'data.meta.extra.row_count') or 0
    return {'url_params': url_params, 'row_count': row_count}


def _get_data_list(url_params: dict) -> list:
    """调用 getDataList API 获取股票数据列表"""
    post_data = {
        **url_params,
        'perpage': 100,
        'page': 1,
    }
    resp = _direct_request(
        method='POST',
        url='http://www.iwencai.com/gateway/urp/v7/landing/getDataList',
        data=post_data,
        headers=wencai_headers(None, None),
        timeout=(5, 15),
    )
    if resp.status_code != 200:
        raise RuntimeError(f"getDataList HTTP {resp.status_code}")

    raw = json.loads(resp.text)
    datas = _.get(raw, 'answer.components.0.data.datas') or []
    return datas


def query_segment_market_cap(segment_name: str) -> dict:
    """查询单个环节的概念板块总市值

    使用底层 API 手动控制请求流程，确保稳定性。

    Returns:
        {"market_size_billion": float, "stock_count": int, "query": str, "date": str}
        或 None（查询失败）
    """
    query = SEGMENT_QUERY_MAP.get(segment_name, f"{segment_name}概念股 总市值")

    try:
        # Step 1: 获取 condition + url_params
        params = _get_robot_data(query)
        if not params.get('url_params'):
            logger.warning(f"  robot-data 无 url_params: {query}")
            return None

        # Step 2: 短暂等待后拉取数据（避免太快被拒）
        time.sleep(random.uniform(1.5, 3.0))

        # Step 3: 获取数据列表
        datas = _get_data_list(params['url_params'])
        if not datas:
            logger.warning(f"  getDataList 返回空: {query}")
            return None

        # Step 4: 解析总市值
        mcap_key = None
        for k in datas[0].keys():
            if '总市值' in str(k):
                mcap_key = k
                break

        if mcap_key is None:
            logger.warning(f"  未找到总市值列，可用列: {list(datas[0].keys())[:5]}")
            return None

        # 汇总（问财市值单位是"元"）
        total = sum(d.get(mcap_key) or 0 for d in datas if isinstance(d.get(mcap_key), (int, float)))
        total_billion = round(total / 1e8, 1)

        # 从列名中提取日期，格式如 "总市值[20260709]"
        date_str = ""
        if '[' in mcap_key and ']' in mcap_key:
            date_str = mcap_key.split('[')[1].split(']')[0]
        if not date_str:
            date_str = datetime.now().strftime("%Y%m%d")

        return {
            "market_size_billion": total_billion,
            "stock_count": len(datas),
            "query": query,
            "date": date_str,
        }

    except Exception as e:
        logger.error(f"  查询异常 [{segment_name}]: {e}")
        return None


def load_baselines() -> list[dict]:
    """加载需要补充的 baselines（每个链取最新版本）"""
    rows = execute_cloud_query("""
        SELECT chain_name, version, baseline_json
        FROM chain_baseline
        WHERE (chain_name, version) IN (
            SELECT chain_name, MAX(version) FROM chain_baseline GROUP BY chain_name
        )
    """)
    return rows or []


def update_baseline_market_sizes(chain_name: str, version: int,
                                  baseline_json: dict, results: dict) -> bool:
    """将查询结果回写到 baseline_json 并更新数据库"""
    structure = baseline_json.get("structure", [])
    updated = False

    for tier in structure:
        segs = tier.get("key_segments", [])
        # 处理旧格式（字符串数组）
        if segs and isinstance(segs[0], str):
            segs = [{"name": s, "market_size_billion": None, "growth_rate_pct": None} for s in segs]
            tier["key_segments"] = segs

        for seg in segs:
            name = seg.get("name", "")
            if name in results and results[name] is not None:
                seg["market_size_billion"] = results[name]["market_size_billion"]
                seg["_source"] = "wencai_concept_mcap"
                seg["_query_date"] = results[name]["date"]
                seg["_stock_count"] = results[name]["stock_count"]
                updated = True

        # 重算 tier 总量
        total = sum(s.get("market_size_billion") or 0 for s in segs)
        tier["tier_market_size_billion"] = round(total, 1) if total > 0 else None

    if updated:
        baseline_json["structure"] = structure
        baseline_json["_market_size_updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        execute_cloud_insert(
            "UPDATE chain_baseline SET baseline_json=%s WHERE chain_name=%s AND version=%s",
            [json.dumps(baseline_json, ensure_ascii=False), chain_name, version],
        )
        logger.info(f"✓ 已更新 {chain_name} v{version} 的 baseline_json")
        return True
    return False


def run(dry_run: bool = False):
    """主流程"""
    logger.info("=" * 60)
    logger.info("开始批量补充 market_size_billion（问财概念板块总市值）")
    logger.info("=" * 60)

    baselines = load_baselines()
    if not baselines:
        logger.error("未找到任何 baseline 数据")
        return

    # 收集所有需要查询的环节
    all_segments = []  # [(chain_name, version, baseline_json_parsed, segment_name)]
    for row in baselines:
        bl = json.loads(row["baseline_json"]) if isinstance(row["baseline_json"], str) else row["baseline_json"]
        structure = bl.get("structure", [])
        for tier in structure:
            segs = tier.get("key_segments", [])
            for s in segs:
                name = s if isinstance(s, str) else s.get("name", "")
                if name:
                    all_segments.append((row["chain_name"], row["version"], bl, name))

    # 去重（同名环节只查一次）
    unique_segments = list(dict.fromkeys(s[3] for s in all_segments))
    logger.info(f"共 {len(unique_segments)} 个唯一环节需要查询")

    # 批量查询
    results = {}
    consecutive_fail = 0
    for i, seg_name in enumerate(unique_segments):
        logger.info(f"[{i+1}/{len(unique_segments)}] 查询: {seg_name}")

        res = query_segment_market_cap(seg_name)
        results[seg_name] = res

        if res:
            logger.info(f"  ✓ {seg_name}: {res['market_size_billion']}亿 ({res['stock_count']}只股) [{res['date']}]")
            consecutive_fail = 0
        else:
            consecutive_fail += 1
            logger.warning(f"  ✗ {seg_name}: 查询失败 (连续失败 {consecutive_fail})")

        if consecutive_fail >= MAX_CONSECUTIVE_FAIL:
            logger.error(f"连续失败 {MAX_CONSECUTIVE_FAIL} 次，冷却 {COOLDOWN_SECONDS}s...")
            time.sleep(COOLDOWN_SECONDS)
            consecutive_fail = 0

        # 反爬间隔
        if (i + 1) % BATCH_PAUSE_EVERY == 0:
            pause = random.randint(*BATCH_PAUSE_SECONDS)
            logger.info(f"  [批量暂停] 第{i+1}条，暂停 {pause}s")
            time.sleep(pause)
        else:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)

    # 统计
    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"\n查询完成: {success}/{len(unique_segments)} 成功")

    # 保存中间结果
    output_path = "/tmp/segment_market_caps.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"中间结果已保存到: {output_path}")

    if dry_run:
        logger.info("[dry-run] 不写入数据库")
        return results

    # 回写数据库
    for row in baselines:
        bl = json.loads(row["baseline_json"]) if isinstance(row["baseline_json"], str) else row["baseline_json"]
        updated = update_baseline_market_sizes(
            row["chain_name"], row["version"], bl, results
        )
        if updated:
            logger.info(f"  ✓ {row['chain_name']} v{row['version']} 已更新")

    logger.info("全部完成！")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="批量补充产业链 market_size_billion")
    parser.add_argument("--dry-run", action="store_true", help="只查询不写入")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
