#!/bin/bash
# 为 iwencai.com 的真实 IP 添加直连路由，绕过 Clash TUN 代理
# 用法: sudo bash scripts/bypass_tun_iwencai.sh

GATEWAY="192.168.5.1"

# 通过绑定物理网卡 IP 做 DNS 查询获取真实 IP
REAL_IPS=$(python3 -c "
import sys; sys.path.insert(0, '.')
from pywencai.wencai import _resolve_real_ip
for ip in _resolve_real_ip('www.iwencai.com'):
    print(ip)
" 2>/dev/null)

if [ -z "$REAL_IPS" ]; then
    echo "无法解析 iwencai.com 真实 IP，使用已知地址"
    REAL_IPS="58.220.49.148 58.220.49.152 58.220.49.154 58.220.49.157 58.220.49.130"
fi

echo "网关: $GATEWAY"
echo "iwencai 真实 IP: $REAL_IPS"

for ip in $REAL_IPS; do
    route delete -host "$ip" 2>/dev/null
    route add -host "$ip" "$GATEWAY"
    echo "  路由添加: $ip -> $GATEWAY"
done

echo "完成。测试: curl -s http://$( echo $REAL_IPS | awk '{print $1}' ) -H 'Host: www.iwencai.com' -o /dev/null -w '%{http_code}'"
