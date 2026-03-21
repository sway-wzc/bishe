#!/bin/bash
# P2P网络通信模块 Docker测试脚本
# 用法: bash scripts/test_p2p.sh

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

echo "=========================================="
echo "  P2P网络通信模块 Docker测试"
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
docker compose up -d node1 node2 node3 node4
sleep 5

RUNNING_COUNT=$(docker compose ps --status running | grep -c "p2p-" || true)
if [ "$RUNNING_COUNT" -ge 5 ]; then
    ok "所有5个节点启动成功（运行中: $RUNNING_COUNT）"
else
    warn "部分节点启动失败（运行中: $RUNNING_COUNT/5）"
    docker compose ps
fi
echo ""

# ==========================================
# 测试4: 验证节点间握手连接
# ==========================================
info "测试4: 检查节点间握手连接..."
sleep 5  # 等待握手完成

HANDSHAKE_COUNT=0
for node in seed node1 node2 node3 node4; do
    if docker compose logs "$node" 2>&1 | grep -q "握手成功"; then
        HANDSHAKE_COUNT=$((HANDSHAKE_COUNT + 1))
    fi
done

if [ "$HANDSHAKE_COUNT" -ge 4 ]; then
    ok "节点握手验证通过（$HANDSHAKE_COUNT 个节点完成握手）"
else
    warn "握手不完整（$HANDSHAKE_COUNT/4 个节点完成握手）"
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
for node in seed node1 node2 node3 node4; do
    if docker compose logs "$node" 2>&1 | grep -q "心跳请求\|心跳响应\|心跳超时"; then
        PING_COUNT=$((PING_COUNT + 1))
    fi
done

if [ "$PING_COUNT" -ge 4 ]; then
    ok "心跳机制正常工作（$PING_COUNT 个节点有心跳记录）"
else
    warn "心跳检测不完整（$PING_COUNT/5）"
fi
echo ""

# ==========================================
# 测试6: 容错测试 - 节点宕机
# ==========================================
info "测试6: 容错测试 - 模拟node3宕机..."
docker compose stop node3
sleep 10

# 检查其他节点是否检测到node3断开
DISCONNECT_DETECTED=false
for node in seed node1 node2 node4; do
    if docker compose logs "$node" 2>&1 | grep -q "已断开\|心跳超时"; then
        DISCONNECT_DETECTED=true
        break
    fi
done

if [ "$DISCONNECT_DETECTED" = true ]; then
    ok "容错检测通过：节点宕机被其他节点发现"
else
    warn "未检测到断开通知（可能需要更长的超时时间）"
fi

# 验证其余节点仍然正常运行
REMAINING=$(docker compose ps --status running | grep -c "p2p-" || true)
if [ "$REMAINING" -ge 4 ]; then
    ok "其余节点正常运行（运行中: $REMAINING）"
else
    warn "部分节点异常（运行中: $REMAINING）"
fi
echo ""

# ==========================================
# 测试7: 容错测试 - 节点恢复
# ==========================================
info "测试7: 容错测试 - 恢复node3..."
docker compose start node3
sleep 8

if docker compose ps node3 | grep -q "Up\|running"; then
    ok "node3恢复启动成功"
    # 检查是否重新建立连接
    if docker compose logs node3 2>&1 | tail -20 | grep -q "握手成功"; then
        ok "node3重新连接到网络"
    else
        warn "node3可能未重新连接（检查日志）"
    fi
else
    fail "node3恢复失败"
fi
echo ""

# ==========================================
# 测试8: 节点发现测试
# ==========================================
info "测试8: 检查节点发现机制..."
sleep 35  # 等待一轮发现周期

DISCOVER_COUNT=0
for node in seed node1 node2 node3 node4; do
    if docker compose logs "$node" 2>&1 | grep -q "发现请求\|发现响应\|DiscoverRequest\|DiscoverResponse"; then
        DISCOVER_COUNT=$((DISCOVER_COUNT + 1))
    fi
done

if [ "$DISCOVER_COUNT" -ge 3 ]; then
    ok "节点发现机制正常（$DISCOVER_COUNT 个节点参与发现）"
else
    warn "节点发现可能未正常工作（$DISCOVER_COUNT/5）"
fi
echo ""

# ==========================================
# 输出汇总日志
# ==========================================
info "各节点最新日志:"
echo "------------------------------------------"
for node in seed node1 node2 node3 node4; do
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