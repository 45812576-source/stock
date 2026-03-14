import json
from typing import List
import math
import socket
import struct

import requests as rq
from requests.adapters import HTTPAdapter
from urllib3.util.connection import allowed_gai_family
import pandas as pd
import time
import logging
import pydash as _
from .convert import convert
from .headers import headers


# ── 绕过 TUN 全局代理直连 iwencai.com ──────────────────────
# Clash TUN 模式会劫持所有流量 + DNS(Fake IP)。
# 策略：手动 DNS 解析拿真实 IP → 绑定物理网卡(en0) IP 直连。

def _get_local_ip():
    """获取本机物理网卡 IP（跳过 loopback 和 198.18.x TUN 地址）"""
    try:
        import netifaces
        for iface in ('en0', 'en1', 'eth0'):
            addrs = netifaces.ifaddresses(iface).get(netifaces.AF_INET, [])
            for a in addrs:
                ip = a.get('addr', '')
                if ip and not ip.startswith('127.') and not ip.startswith('198.18.'):
                    return ip
    except ImportError:
        pass
    # fallback: 连一个内网地址取本地 IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('192.168.1.1', 80))
        ip = s.getsockname()[0]
        s.close()
        if not ip.startswith('198.18.'):
            return ip
    except Exception:
        pass
    return None


def _resolve_real_ip(domain, dns_server='114.114.114.114'):
    """绕过系统 DNS resolver，手动 UDP 查询拿真实 A 记录"""
    local_ip = _get_local_ip()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    if local_ip:
        sock.bind((local_ip, 0))
    tid = b'\xab\xcd'
    flags = b'\x01\x00'
    counts = b'\x00\x01\x00\x00\x00\x00\x00\x00'
    qname = b''
    for p in domain.split('.'):
        qname += bytes([len(p)]) + p.encode()
    qname += b'\x00'
    packet = tid + flags + counts + qname + b'\x00\x01\x00\x01'
    sock.sendto(packet, (dns_server, 53))
    data, _ = sock.recvfrom(512)
    sock.close()
    idx = 12
    while data[idx] != 0:
        idx += data[idx] + 1
    idx += 5
    answers = struct.unpack('>H', data[6:8])[0]
    ips = []
    for _ in range(answers):
        if data[idx] & 0xc0 == 0xc0:
            idx += 2
        else:
            while data[idx] != 0:
                idx += data[idx] + 1
            idx += 1
        rtype = struct.unpack('>H', data[idx:idx+2])[0]
        rdlen = struct.unpack('>H', data[idx+8:idx+10])[0]
        idx += 10
        if rtype == 1 and rdlen == 4:
            ips.append('.'.join(str(b) for b in data[idx:idx+4]))
        idx += rdlen
    return ips


class _DirectAdapter(HTTPAdapter):
    """HTTPAdapter 绑定物理网卡 IP，绕过 TUN"""
    def __init__(self, source_ip, **kw):
        self._source_ip = source_ip
        super().__init__(**kw)

    def init_poolmanager(self, *args, **kw):
        kw['source_address'] = (self._source_ip, 0)
        super().init_poolmanager(*args, **kw)


# 全局直连 Session（懒初始化）
_direct_session = None
_iwencai_real_ip = None

def _get_direct_session():
    """返回绑定物理网卡 + 真实 IP 的 requests.Session，可直连 iwencai"""
    global _direct_session, _iwencai_real_ip
    if _direct_session is not None:
        return _direct_session, _iwencai_real_ip

    local_ip = _get_local_ip()
    real_ips = _resolve_real_ip('www.iwencai.com')
    if not real_ips:
        return None, None
    _iwencai_real_ip = real_ips[0]

    sess = rq.Session()
    sess.trust_env = False  # 不读系统代理
    if local_ip:
        adapter = _DirectAdapter(local_ip)
        sess.mount('http://', adapter)
        sess.mount('https://', adapter)
    _direct_session = sess
    return _direct_session, _iwencai_real_ip


def _check_captcha(resp):
    """检测 captcha 风控（401 + captcha_url），不应重试"""
    if resp.status_code == 401:
        try:
            body = resp.json()
            if 'captcha' in str(body.get('data', {}).get('captcha_url', '')).lower():
                raise CaptchaError(f"问财验证码风控，请在浏览器打开 iwencai.com 完成验证后重试")
        except (ValueError, AttributeError):
            pass
        raise CaptchaError(f"问财 401: {resp.text[:200]}")


def _direct_request(method, url, **kwargs):
    """优先走系统网络（与浏览器出口 IP 一致），失败降级到直连绕 TUN"""
    # 1. 先尝试系统网络（与浏览器共享验证码解锁状态）
    try:
        resp = rq.request(method, url, timeout=kwargs.get('timeout', 10), **{
            k: v for k, v in kwargs.items() if k != 'timeout'
        })
        if resp.status_code != 502:
            _check_captcha(resp)
            return resp
        # 502 说明 TUN 代理不通，降级到直连
    except (rq.exceptions.ConnectionError, rq.exceptions.Timeout):
        pass  # 系统网络不通，降级

    # 2. 降级：直连绕 TUN
    sess, real_ip = _get_direct_session()
    if sess and real_ip:
        direct_url = url.replace('www.iwencai.com', real_ip).replace('http://', 'http://')
        h = kwargs.get('headers') or {}
        h['Host'] = 'www.iwencai.com'
        kwargs['headers'] = h
        resp = sess.request(method, direct_url, **kwargs)
        _check_captcha(resp)
        return resp

    # 3. 兜底
    resp = rq.request(method, url, **kwargs)
    _check_captcha(resp)
    return resp

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[pywencai] %(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class CaptchaError(Exception):
    """问财触发了验证码风控，不应重试"""
    pass


def while_do(do, retry=10, sleep=0, log=False):
    count = 0
    while count < retry:
        time.sleep(sleep)
        try:
            return do()
        except CaptchaError:
            log and logger.warning('问财触发验证码风控，停止重试')
            return None
        except:
            log and logger.warning(f'{count+1}次尝试失败')
            count += 1
    return None


def get_robot_data(**kwargs):
    '''获取condition'''
    retry = kwargs.get('retry', 10)
    sleep = kwargs.get('sleep', 0)
    question = kwargs.get('query')
    log = kwargs.get('log', False)
    query_type = kwargs.get('query_type', 'stock')
    cookie = kwargs.get('cookie', None)
    user_agent = kwargs.get('user_agent', None)
    request_params = kwargs.get('request_params', {})
    data = {
        'add_info': "{\"urp\":{\"scene\":1,\"company\":1,\"business\":1},\"contentType\":\"json\",\"searchInfo\":true}",
        'perpage': '10',
        'page': 1,
        'source': 'Ths_iwencai_Xuangu',
        'log_info': "{\"input_type\":\"click\"}",
        'version': '2.0',
        'secondary_intent': query_type,
        'question': question
    }

    pro = kwargs.get('pro', False)

    if pro:
        data['iwcpro'] = 1

    log and logger.info(f'获取condition开始')

    def do():
        res = _direct_request(
            method='POST',
            url='http://www.iwencai.com/customized/chart/get-robot-data',
            json=data,
            headers=headers(cookie, user_agent),
        )
        params = convert(res)
        log and logger.info(f'获取get_robot_data成功')
        return params

    result = while_do(do, retry, sleep, log)

    if result is None:
        log and logger.info(f'获取get_robot_data失败')

    return result


def replace_key(key):
    '''替换key'''
    key_map = {
        'question': 'query',
        'sort_key': 'urp_sort_index',
        'sort_order': 'urp_sort_way'
    }
    return key_map.get(key, key)


def get_page(url_params, **kwargs):
    '''获取每页数据'''
    retry = kwargs.pop('retry', 10)
    sleep = kwargs.pop('sleep', 0)
    log = kwargs.pop('log', False)
    cookie = kwargs.pop('cookie', None)
    user_agent = kwargs.get('user_agent', None)
    find = kwargs.pop('find', None)
    query_type = kwargs.get('query_type', 'stock')
    request_params = kwargs.get('request_params', {})
    pro = kwargs.get('pro', False)
    if find is None:
        data = {
            **url_params,
            'perpage': 100,
            'page': 1,
            **kwargs
        }
        target_url = 'http://www.iwencai.com/gateway/urp/v7/landing/getDataList'
        if pro:
            target_url = f'{target_url}?iwcpro=1'
        path = 'answer.components.0.data.datas'
    else:
        if isinstance(find, List):
            # 传入股票代码列表时，拼接
            find = ','.join(find)
        data = {
             **url_params,
            'perpage': 100,
            'page': 1,
            'query_type': query_type,
            'question': find,
            **kwargs
        }
        target_url = 'http://www.iwencai.com/unifiedwap/unified-wap/v2/stock-pick/find'
        path = 'data.data.datas'
    
    log and logger.info(f'第{data.get("page")}页开始')

    def do():
        res = _direct_request(
            method='POST',
            url=target_url,
            data=data,
            headers=headers(cookie, user_agent),
            timeout=(5, 10),
        )
        result_do = json.loads(res.text)
        data_list = _.get(result_do, path)

        if len(data_list) == 0:
            log and logger.error(f'第{data.get("page")}页返回空！')
            raise Exception("data_list is empty!")
        log and logger.info(f'第{data.get("page")}页成功')
        return pd.DataFrame.from_dict(data_list)
    
    result = while_do(do, retry, sleep, log)

    if result is None:
        log and logger.error(f'第{data.get("page")}页失败')

    return result


def can_loop(loop, count):
    return count < loop


def loop_page(loop, row_count, url_params, **kwargs):
    '''循环分页'''
    count = 0
    perpage = kwargs.pop('perpage', 100)
    max_page = math.ceil(row_count / perpage)
    result = None
    if 'page' not in kwargs:
        kwargs['page'] = 1
    initPage = kwargs['page']
    loop_count = max_page if loop is True else loop
    while can_loop(loop_count, count):
        kwargs['page'] = initPage + count
        resultPage = get_page(url_params, **kwargs)
        count = count + 1
        if result is None:
            result = resultPage
        else:
            result = pd.concat([result, resultPage], ignore_index=True)

    return result


def get(loop=False, **kwargs):
    '''获取结果'''
    kwargs = {replace_key(key): value for key, value in kwargs.items()}
    params = get_robot_data(**kwargs)
    if params is None:
        return None
    data = params.get('data')
    url_params = params.get('url_params')
    condition = _.get(data, 'condition')
    
    if condition is not None:
        kwargs = {**kwargs, **data}
        find = kwargs.get('find', None)
        if loop and find is None:
            row_count = params.get('row_count')
            return loop_page(loop, row_count, url_params, **kwargs)
        else:
            return get_page(url_params, **kwargs)
    else:
        no_detail = kwargs.get('no_detail')
        if no_detail != True:
            return data
        else:
            return None