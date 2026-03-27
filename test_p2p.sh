#!/bin/bash
# P2P网络通信模块 + RBC协议 Docker测试脚本
# 用法: bash test_p2p.sh

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # 无颜色

info()  { echo -e "${BLUE}[INFO]${NC} $1"; }
ok()    { echo -e "${GREEN}[PASS]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; }

ALL_NODES="seed node1 node2 node3 node4 node5 node6"
NODE_COUNT=7

echo "=========================================="
echo "  高容错分布式数据分发系统 Docker测试"
echo "  节点数: $NODE_COUNT (n=7, t=2)"
echo "=========================================="
echo ""

# ==========================================
# 测试1: 构建镜像
# ==========================================
info "测试1: 构建Docker镜像..."
if docker compose build --no-cache 2>&1; then
    ok "Docker镜像构建成功"
else
    fail "Docker镜像构建失败"
    exit 1
fi
echo ""

# ==========================================
# 测试2: 启动种子节点
# ==========================================
info "测试2: 启动种子节点..."
docker compose up -d seed
sleep 3

if docker compose ps seed | grep -q "Up\|running"; then
    ok "种子节点启动成功"
else
    fail "种子节点启动失败"
    docker compose logs seed
    exit 1
fi
echo ""

# ==========================================
# 测试3: 启动所有普通节点
# ==========================================
info "测试3: 启动所有普通节点..."
docker compose up -d node1 node2 node3 node4 node5 node6
sleep 5

RUNNING_COUNT=$(docker compose ps --status running | grep -c "p2p-" || true)
if [ "$RUNNING_COUNT" -ge "$NODE_COUNT" ]; then
    ok "所有${NODE_COUNT}个节点启动成功（运行中: $RUNNING_COUNT）"
else
    warn "部分节点启动失败（运行中: $RUNNING_COUNT/$NODE_COUNT）"
    docker compose ps
fi
echo ""

# ==========================================
# 测试4: 验证节点间握手连接
# ==========================================
info "测试4: 检查节点间握手连接..."
sleep 5  # 等待握手完成

HANDSHAKE_COUNT=0
for node in $ALL_NODES; do
    if docker compose logs "$node" 2>&1 | grep -q "握手成功"; then
        HANDSHAKE_COUNT=$((HANDSHAKE_COUNT + 1))
    fi
done

if [ "$HANDSHAKE_COUNT" -ge 6 ]; then
    ok "节点握手验证通过（$HANDSHAKE_COUNT 个节点完成握手）"
else
    warn "握手不完整（$HANDSHAKE_COUNT 个节点完成握手）"
    info "查看种子节点日志:"
    docker compose logs seed 2>&1 | tail -20
fi
echo ""

# ==========================================
# 测试5: 验证心跳机制
# ==========================================
info "测试5: 等待心跳检测（15秒）..."
sleep 15

PING_COUNT=0
for node in $ALL_NODES; do
    if docker compose logs "$node" 2>&1 | grep -q "心跳请求\|心跳响应\|心跳超时"; then
        PING_COUNT=$((PING_COUNT + 1))
    fi
done

if [ "$PING_COUNT" -ge 6 ]; then
    ok "心跳机制正常工作（$PING_COUNT 个节点有心跳记录）"
else
    warn "心跳检测不完整（$PING_COUNT/$NODE_COUNT）"
fi
echo ""

# ==========================================
# 测试6: 容错测试 - 单节点宕机
# ==========================================
info "测试6: 容错测试 - 模拟node3宕机（系统应能容忍t=2个故障）..."
docker compose stop node3
sleep 10

DISCONNECT_DETECTED=false
for node in seed node1 node2 node4 node5 node6; do
    if docker compose logs "$node" 2>&1 | grep -q "已断开\|心跳超时"; then
        DISCONNECT_DETECTED=true
        break
    fi
done

if [ "$DISCONNECT_DETECTED" = true ]; then
    ok "容错检测通过：node3宕机被其他节点发现"
else
    warn "未检测到断开通知（可能需要更长的超时时间）"
fi

REMAINING=$(docker compose ps --status running | grep -c "p2p-" || true)
if [ "$REMAINING" -ge 6 ]; then
    ok "其余节点正常运行（运行中: $REMAINING）"
else
    warn "部分节点异常（运行中: $REMAINING）"
fi
echo ""

# ==========================================
# 测试7: 容错测试 - 双节点宕机（t=2极限测试）
# ==========================================
info "测试7: 容错测试 - 再停止node4（模拟2个节点同时故障）..."
docker compose stop node4
sleep 10

REMAINING=$(docker compose ps --status running | grep -c "p2p-" || true)
if [ "$REMAINING" -ge 5 ]; then
    ok "双节点故障后，剩余${REMAINING}个节点仍正常运行（满足n-t=5的最低要求）"
else
    warn "存活节点不足（运行中: $REMAINING，需要至少5个）"
fi
echo ""

# ==========================================
# 测试8: 容错测试 - 节点恢复
# ==========================================
info "测试8: 容错测试 - 恢复node3和node4..."
docker compose start node3 node4
sleep 8

RECOVERED=0
for node in node3 node4; do
    if docker compose ps "$node" | grep -q "Up\|running"; then
        RECOVERED=$((RECOVERED + 1))
        if docker compose logs "$node" 2>&1 | tail -20 | grep -q "握手成功"; then
            ok "$node 恢复并重新连接到网络"
        else
            warn "$node 已启动但可能未重新连接"
        fi
    else
        fail "$node 恢复失败"
    fi
done

if [ "$RECOVERED" -eq 2 ]; then
    ok "所有故障节点恢复成功"
fi
echo ""

# ==========================================
# 测试9: 节点发现测试
# ==========================================
info "测试9: 检查节点发现机制..."
sleep 35  # 等待一轮发现周期

DISCOVER_COUNT=0
for node in $ALL_NODES; do
    if docker compose logs "$node" 2>&1 | grep -q "发现请求\|发现响应\|DiscoverRequest\|DiscoverResponse"; then
        DISCOVER_COUNT=$((DISCOVER_COUNT + 1))
    fi
done

if [ "$DISCOVER_COUNT" -ge 5 ]; then
    ok "节点发现机制正常（$DISCOVER_COUNT 个节点参与发现）"
else
    warn "节点发现可能未正常工作（$DISCOVER_COUNT/$NODE_COUNT）"
fi
echo ""

# ==========================================
# 输出汇总日志
# ==========================================
info "各节点最新日志:"
echo "------------------------------------------"
for node in $ALL_NODES; do
    echo -e "${YELLOW}[$node]${NC}"
    docker compose logs --tail 5 "$node" 2>&1 | sed 's/^/  /'
    echo ""
done

# ==========================================
# 清理
# ==========================================
echo ""
read -p "是否清理所有容器？(y/N): " CLEANUP
if [ "$CLEANUP" = "y" ] || [ "$CLEANUP" = "Y" ]; then
    info "清理容器和网络..."
    docker compose down -v --remove-orphans
    ok "清理完成"
else
    info "容器保留运行中，手动清理请执行: docker compose down"
fi

echo ""
echo "=========================================="
echo "  测试完成"
echo "=========================================="