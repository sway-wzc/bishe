#!/bin/bash
# P2P网络通信模块 + RBC协议 Docker测试脚本
# 用法: bash test_p2p.sh [-n 节点总数]
# 示例: bash test_p2p.sh          # 默认7节点
#       bash test_p2p.sh -n 4     # 4节点测试
#       bash test_p2p.sh -n 10    # 10节点测试

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

# ==========================================
# 解析命令行参数
# ==========================================
NODE_COUNT=7  # 默认节点数

while getopts "n:h" opt; do
    case $opt in
        n) NODE_COUNT="$OPTARG" ;;
        h)
            echo "用法: bash test_p2p.sh [-n 节点总数]"
            echo "  -n  总节点数（最小4，默认7）"
            echo "  -h  显示帮助"
            exit 0
            ;;
        *) echo "未知参数，使用 -h 查看帮助"; exit 1 ;;
    esac
done

# 验证节点数量
if [ "$NODE_COUNT" -lt 4 ]; then
    fail "节点数量最少为4（BFT要求 n >= 3t+1，最小 t=1 时 n=4）"
    exit 1
fi

# 计算容错参数
T=$(( (NODE_COUNT - 1) / 3 ))  # 最大容错数 t = floor((n-1)/3)

# 生成节点名称列表
ALL_NODES="seed"
NORMAL_NODES=""
for i in $(seq 1 $((NODE_COUNT - 1))); do
    ALL_NODES="$ALL_NODES node${i}"
    if [ -z "$NORMAL_NODES" ]; then
        NORMAL_NODES="node${i}"
    else
        NORMAL_NODES="$NORMAL_NODES node${i}"
    fi
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.dynamic.yml"
SEED_IP="172.28.0.10"
SUBNET="172.28.0.0/16"

# ==========================================
# 动态生成 docker-compose 文件
# ==========================================
generate_compose_file() {
    info "动态生成 docker-compose 配置（${NODE_COUNT}个节点）..."

    cat > "$COMPOSE_FILE" << YAML
# 自动生成的docker-compose文件 - ${NODE_COUNT}个节点 (n=${NODE_COUNT}, t=${T})
services:
  seed:
    build: .
    container_name: p2p-seed
    hostname: seed
    environment:
      - NODE_ID=seed
      - LISTEN_PORT=8000
      - IS_SEED=true
      - RUST_LOG=info
      - RBC_OUTPUT_DIR=/app/rbc_output
      - RBC_TEST_FILE=\${RBC_TEST_FILE:-}
      - RBC_BROADCAST_DELAY=\${RBC_BROADCAST_DELAY:-35}
    networks:
      p2p-net:
        ipv4_address: ${SEED_IP}
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'echo > /dev/tcp/127.0.0.1/8000' 2>/dev/null || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 15s
YAML

    for i in $(seq 1 $((NODE_COUNT - 1))); do
        local NODE_IP="172.28.1.${i}"
        local HOST_PORT=$((8000 + i))
        cat >> "$COMPOSE_FILE" << YAML

  node${i}:
    build: .
    container_name: p2p-node${i}
    hostname: node${i}
    environment:
      - NODE_ID=node${i}
      - LISTEN_PORT=8000
      - SEED_ADDR=${SEED_IP}:8000
      - RUST_LOG=info
      - RBC_OUTPUT_DIR=/app/rbc_output
    networks:
      p2p-net:
        ipv4_address: ${NODE_IP}
    ports:
      - "${HOST_PORT}:8000"
    depends_on:
      seed:
        condition: service_healthy
YAML
    done

    cat >> "$COMPOSE_FILE" << YAML

networks:
  p2p-net:
    driver: bridge
    ipam:
      config:
        - subnet: ${SUBNET}
YAML

    ok "docker-compose 配置已生成: $COMPOSE_FILE"
}

# docker compose 命令封装，使用动态生成的文件
dc() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

echo "=========================================="
echo "  高容错分布式数据分发系统 Docker测试"
echo "  节点数: $NODE_COUNT (n=${NODE_COUNT}, t=${T})"
echo "=========================================="
echo ""

# 生成compose文件
generate_compose_file
echo ""

# ==========================================
# 测试1: 构建镜像
# ==========================================
info "测试1: 构建Docker镜像..."
if dc build --no-cache 2>&1; then
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
dc up -d seed
sleep 3

if dc ps seed | grep -q "Up\|running"; then
    ok "种子节点启动成功"
else
    fail "种子节点启动失败"
    dc logs seed
    exit 1
fi
echo ""

# ==========================================
# 测试3: 启动所有普通节点
# ==========================================
info "测试3: 启动所有普通节点（共$((NODE_COUNT - 1))个）..."
dc up -d $NORMAL_NODES
sleep 5

RUNNING_COUNT=$(dc ps --status running | grep -c "p2p-" || true)
if [ "$RUNNING_COUNT" -ge "$NODE_COUNT" ]; then
    ok "所有${NODE_COUNT}个节点启动成功（运行中: $RUNNING_COUNT）"
else
    warn "部分节点启动失败（运行中: $RUNNING_COUNT/$NODE_COUNT）"
    dc ps
fi
echo ""

# ==========================================
# 测试4: 验证节点间握手连接
# ==========================================
info "测试4: 检查节点间握手连接..."
sleep 5  # 等待握手完成

HANDSHAKE_COUNT=0
for node in $ALL_NODES; do
    if dc logs "$node" 2>&1 | grep -q "握手成功"; then
        HANDSHAKE_COUNT=$((HANDSHAKE_COUNT + 1))
    fi
done

HANDSHAKE_THRESHOLD=$((NODE_COUNT - 1))
if [ "$HANDSHAKE_COUNT" -ge "$HANDSHAKE_THRESHOLD" ]; then
    ok "节点握手验证通过（$HANDSHAKE_COUNT 个节点完成握手）"
else
    warn "握手不完整（$HANDSHAKE_COUNT 个节点完成握手，期望 >= $HANDSHAKE_THRESHOLD）"
    info "查看种子节点日志:"
    dc logs seed 2>&1 | tail -20
fi
echo ""

# ==========================================
# 测试5: 验证心跳机制
# ==========================================
info "测试5: 等待心跳检测（15秒）..."
sleep 15

PING_COUNT=0
for node in $ALL_NODES; do
    if dc logs "$node" 2>&1 | grep -q "心跳请求\|心跳响应\|心跳超时"; then
        PING_COUNT=$((PING_COUNT + 1))
    fi
done

PING_THRESHOLD=$((NODE_COUNT - 1))
if [ "$PING_COUNT" -ge "$PING_THRESHOLD" ]; then
    ok "心跳机制正常工作（$PING_COUNT 个节点有心跳记录）"
else
    warn "心跳检测不完整（$PING_COUNT/$NODE_COUNT）"
fi
echo ""

# ==========================================
# 测试6: 容错测试 - 单节点宕机
# ==========================================
# 选择最后一个普通节点作为故障节点
FAULT_NODE_1="node$((NODE_COUNT - 1))"
info "测试6: 容错测试 - 模拟${FAULT_NODE_1}宕机（系统应能容忍t=${T}个故障）..."
dc stop "$FAULT_NODE_1"
sleep 10

# 构建存活节点列表（排除故障节点）
ALIVE_NODES=""
for node in $ALL_NODES; do
    if [ "$node" != "$FAULT_NODE_1" ]; then
        ALIVE_NODES="$ALIVE_NODES $node"
    fi
done

DISCONNECT_DETECTED=false
for node in $ALIVE_NODES; do
    if dc logs "$node" 2>&1 | grep -q "已断开\|心跳超时"; then
        DISCONNECT_DETECTED=true
        break
    fi
done

if [ "$DISCONNECT_DETECTED" = true ]; then
    ok "容错检测通过：${FAULT_NODE_1}宕机被其他节点发现"
else
    warn "未检测到断开通知（可能需要更长的超时时间）"
fi

REMAINING=$(dc ps --status running | grep -c "p2p-" || true)
EXPECTED_REMAINING=$((NODE_COUNT - 1))
if [ "$REMAINING" -ge "$EXPECTED_REMAINING" ]; then
    ok "其余节点正常运行（运行中: $REMAINING）"
else
    warn "部分节点异常（运行中: $REMAINING）"
fi
echo ""

# ==========================================
# 测试7: 容错测试 - t个节点宕机（极限测试）
# ==========================================
if [ "$T" -ge 2 ]; then
    # 需要再停 t-1 个节点（已经停了1个）
    FAULT_NODES="$FAULT_NODE_1"
    EXTRA_STOP=""
    for i in $(seq $((NODE_COUNT - 2)) -1 $((NODE_COUNT - T))); do
        EXTRA_STOP="$EXTRA_STOP node${i}"
        FAULT_NODES="$FAULT_NODES node${i}"
    done

    info "测试7: 容错测试 - 再停止${EXTRA_STOP}（模拟${T}个节点同时故障，t=${T}极限）..."
    for stop_node in $EXTRA_STOP; do
        dc stop "$stop_node"
    done
    sleep 10

    REMAINING=$(dc ps --status running | grep -c "p2p-" || true)
    MIN_ALIVE=$((NODE_COUNT - T))
    if [ "$REMAINING" -ge "$MIN_ALIVE" ]; then
        ok "${T}节点故障后，剩余${REMAINING}个节点仍正常运行（满足n-t=${MIN_ALIVE}的最低要求）"
    else
        warn "存活节点不足（运行中: $REMAINING，需要至少${MIN_ALIVE}个）"
    fi
else
    # t=1时，已经停了1个节点，就是极限了
    FAULT_NODES="$FAULT_NODE_1"
    info "测试7: 跳过（t=${T}，单节点故障已是极限）"
    REMAINING=$(dc ps --status running | grep -c "p2p-" || true)
    MIN_ALIVE=$((NODE_COUNT - T))
    if [ "$REMAINING" -ge "$MIN_ALIVE" ]; then
        ok "${T}节点故障后，剩余${REMAINING}个节点仍正常运行（满足n-t=${MIN_ALIVE}的最低要求）"
    fi
fi
echo ""

# ==========================================
# 测试8: 容错测试 - 节点恢复
# ==========================================
info "测试8: 容错测试 - 恢复所有故障节点..."
RECOVER_NODES=""
for node in $FAULT_NODES; do
    RECOVER_NODES="$RECOVER_NODES $node"
done
dc start $RECOVER_NODES
sleep 8

RECOVERED=0
for node in $RECOVER_NODES; do
    if dc ps "$node" | grep -q "Up\|running"; then
        RECOVERED=$((RECOVERED + 1))
        if dc logs "$node" 2>&1 | tail -20 | grep -q "握手成功"; then
            ok "$node 恢复并重新连接到网络"
        else
            warn "$node 已启动但可能未重新连接"
        fi
    else
        fail "$node 恢复失败"
    fi
done

EXPECTED_RECOVER=$(echo "$RECOVER_NODES" | wc -w)
if [ "$RECOVERED" -ge "$EXPECTED_RECOVER" ]; then
    ok "所有故障节点恢复成功（$RECOVERED/$EXPECTED_RECOVER）"
fi
echo ""

# ==========================================
# 测试9: 节点发现测试
# ==========================================
info "测试9: 检查节点发现机制..."
sleep 35  # 等待一轮发现周期

DISCOVER_COUNT=0
for node in $ALL_NODES; do
    if dc logs "$node" 2>&1 | grep -q "发现请求\|发现响应\|DiscoverRequest\|DiscoverResponse"; then
        DISCOVER_COUNT=$((DISCOVER_COUNT + 1))
    fi
done

DISCOVER_THRESHOLD=$((NODE_COUNT - 2))
if [ "$DISCOVER_COUNT" -ge "$DISCOVER_THRESHOLD" ]; then
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
    dc logs --tail 5 "$node" 2>&1 | sed 's/^/  /'
    echo ""
done

# ==========================================
# 清理
# ==========================================
echo ""
read -p "是否清理所有容器？(y/N): " CLEANUP
if [ "$CLEANUP" = "y" ] || [ "$CLEANUP" = "Y" ]; then
    info "清理容器和网络..."
    dc down -v --remove-orphans
    rm -f "$COMPOSE_FILE"
    ok "清理完成"
else
    info "容器保留运行中，手动清理请执行: docker compose -f $COMPOSE_FILE down"
fi

echo ""
echo "=========================================="
echo "  测试完成 (n=${NODE_COUNT}, t=${T})"
echo "=========================================="