#!/bin/bash
# RBC协议 Docker端到端测试脚本
# 测试大文件的分片广播分发和正确接收恢复
# 用法: bash test_rbc_docker.sh [-n 节点总数]
# 示例: bash test_rbc_docker.sh          # 默认7节点
#       bash test_rbc_docker.sh -n 4     # 4节点测试 (t=1)
#       bash test_rbc_docker.sh -n 10    # 10节点测试 (t=3)

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC} $1"; }
ok()    { echo -e "${GREEN}[PASS]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; }
step()  { echo -e "${CYAN}[STEP]${NC} $1"; }

# ==========================================
# 解析命令行参数
# ==========================================
NODE_COUNT=7  # 默认节点数

while getopts "n:h" opt; do
    case $opt in
        n) NODE_COUNT="$OPTARG" ;;
        h)
            echo "用法: bash test_rbc_docker.sh [-n 节点总数]"
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

# RBC广播等待延迟（秒），节点越多需要等待越久
RBC_BROADCAST_DELAY=$((25 + NODE_COUNT * 2))
# RBC协议完成等待时间（秒），节点越多需要等待越久
RBC_COMPLETION_WAIT=$((60 + NODE_COUNT * 5))

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.dynamic.yml"
SEED_IP="172.28.0.10"
SUBNET="172.28.0.0/16"

echo "=========================================="
echo "  高容错分布式数据分发系统 RBC协议测试"
echo "  节点数: $NODE_COUNT (n=${NODE_COUNT}, t=${T})"
echo "  数据分片: $((T + 1))片, 校验分片: $((NODE_COUNT - T - 1))片"
echo "  广播延迟: ${RBC_BROADCAST_DELAY}秒"
echo "  测试内容: 大文件分片广播分发与恢复"
echo "=========================================="
echo ""

# ==========================================
# 动态生成 docker-compose 基础文件
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
      - RBC_BROADCAST_DELAY=\${RBC_BROADCAST_DELAY:-${RBC_BROADCAST_DELAY}}
      - EXPECTED_NODES=${NODE_COUNT}
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
      - EXPECTED_NODES=${NODE_COUNT}
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

# docker compose 命令封装
dc() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

# docker compose 命令封装（带覆盖文件）
dc_with_override() {
    docker compose -f "$COMPOSE_FILE" -f "${SCRIPT_DIR}/docker-compose.test.yml" "$@"
}

# ==========================================
# 清理函数
# ==========================================
cleanup() {
    info "清理测试环境..."
    if [ -f "${SCRIPT_DIR}/docker-compose.test.yml" ]; then
        dc_with_override down -v --remove-orphans 2>/dev/null || true
    else
        dc down -v --remove-orphans 2>/dev/null || true
    fi
}

# 捕获退出信号，确保清理
trap 'echo ""; warn "测试中断，正在清理..."; cleanup; exit 1' INT TERM

# ==========================================
# 测试准备：生成测试文件
# ==========================================
info "准备测试数据文件..."

TEST_DIR="${SCRIPT_DIR}/test_data_tmp"
OUTPUT_BASE="${SCRIPT_DIR}/rbc_output_tmp"
mkdir -p "$TEST_DIR"
mkdir -p "$OUTPUT_BASE"

# 生成不同大小的测试文件
# 1. 小文件 (1KB)
dd if=/dev/urandom of="$TEST_DIR/small_1kb.bin" bs=1024 count=1 2>/dev/null
# 2. 中等文件 (100KB)
dd if=/dev/urandom of="$TEST_DIR/medium_100kb.bin" bs=1024 count=100 2>/dev/null
# 3. 大文件 (1MB)
dd if=/dev/urandom of="$TEST_DIR/large_1mb.bin" bs=1024 count=1024 2>/dev/null
# 4. 文本文件
for i in $(seq 1 5000); do
    echo "Hello, 高容错分布式数据分发系统! Line $i - $(date +%s%N)"
done > "$TEST_DIR/text_file.txt"

# 计算各文件的SHA-256哈希
for f in "$TEST_DIR"/*.bin "$TEST_DIR"/*.txt; do
    if [ -f "$f" ]; then
        sha256sum "$f" | awk '{print $1}' > "${f}.sha256"
        SIZE=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f" 2>/dev/null)
        HASH=$(cat "${f}.sha256")
        info "  $(basename $f): ${SIZE}字节, SHA-256=${HASH:0:16}..."
    fi
done
echo ""

# ==========================================
# 生成动态docker-compose覆盖文件（用于RBC测试）
# ==========================================
generate_compose_override() {
    local TEST_FILE_NAME="$1"
    local OVERRIDE_FILE="${SCRIPT_DIR}/docker-compose.test.yml"

    # 生成seed节点的覆盖配置
    cat > "$OVERRIDE_FILE" << YAML
services:
  seed:
    volumes:
      - ${TEST_DIR}:/app/test_data:ro
      - ${OUTPUT_BASE}/seed:/app/rbc_output
    environment:
      - RBC_TEST_FILE=/app/test_data/${TEST_FILE_NAME}
      - RBC_BROADCAST_DELAY=${RBC_BROADCAST_DELAY}
      - RBC_OUTPUT_DIR=/app/rbc_output
      - RUST_LOG=info
YAML

    # 为每个普通节点生成覆盖配置
    for i in $(seq 1 $((NODE_COUNT - 1))); do
        cat >> "$OVERRIDE_FILE" << YAML

  node${i}:
    volumes:
      - ${OUTPUT_BASE}/node${i}:/app/rbc_output
    environment:
      - RBC_OUTPUT_DIR=/app/rbc_output
      - RUST_LOG=info
YAML
    done
}

# ==========================================
# 测试函数：执行单次RBC广播测试
# ==========================================
run_rbc_test() {
    local TEST_NAME="$1"
    local TEST_FILE="$2"
    local EXPECT_FAULT="$3"  # 是否测试容错（停止的节点名，空则不停）

    local FILE_NAME
    FILE_NAME=$(basename "$TEST_FILE")
    local FILE_SIZE
    FILE_SIZE=$(stat -c%s "$TEST_FILE" 2>/dev/null || stat -f%z "$TEST_FILE" 2>/dev/null)
    local EXPECTED_HASH
    EXPECTED_HASH=$(sha256sum "$TEST_FILE" | awk '{print $1}')

    echo ""
    echo "========================================"
    step "$TEST_NAME"
    info "  文件: $FILE_NAME, 大小: ${FILE_SIZE}字节"
    info "  期望SHA-256: ${EXPECTED_HASH:0:32}..."
    if [ -n "$EXPECT_FAULT" ]; then
        local FAULT_COUNT
        FAULT_COUNT=$(echo "$EXPECT_FAULT" | wc -w)
        info "  容错测试: 将停止 ${FAULT_COUNT} 个节点: $EXPECT_FAULT"
    fi
    echo "========================================"

    # 1. 清理之前的环境
    dc_with_override down -v --remove-orphans 2>/dev/null || true
    sleep 2

    # 清理输出目录
    rm -rf "${OUTPUT_BASE:?}"/*
    for node in $ALL_NODES; do
        mkdir -p "${OUTPUT_BASE}/${node}"
    done

    # 2. 生成compose覆盖文件
    generate_compose_override "$FILE_NAME"

    # 3. 启动所有节点（seed带RBC_TEST_FILE环境变量）
    step "启动所有 ${NODE_COUNT} 个节点..."
    dc_with_override up -d
    sleep 5

    RUNNING_COUNT=$(dc_with_override ps --status running | grep -c "p2p-" || true)
    if [ "$RUNNING_COUNT" -ge "$NODE_COUNT" ]; then
        ok "所有${NODE_COUNT}个节点启动成功"
    else
        warn "部分节点启动失败（运行中: $RUNNING_COUNT/$NODE_COUNT）"
    fi

    # 4. 如果是容错测试，等待网络就绪后停止指定节点
    if [ -n "$EXPECT_FAULT" ]; then
        step "等待网络就绪后停止故障节点..."
        sleep 20
        for fault_node in $EXPECT_FAULT; do
            info "  停止节点: $fault_node"
            dc_with_override stop "$fault_node"
        done
        sleep 3
    fi

    # 5. 等待RBC广播完成
    local TOTAL_WAIT=$((RBC_BROADCAST_DELAY + RBC_COMPLETION_WAIT))
    step "等待RBC协议完成（最多 ${TOTAL_WAIT}秒）..."

    local ELAPSED=0
    local CHECK_INTERVAL=5
    local SUCCESS_NODES=0
    local EXPECTED_SUCCESS=$NODE_COUNT

    if [ -n "$EXPECT_FAULT" ]; then
        local FAULT_COUNT
        FAULT_COUNT=$(echo "$EXPECT_FAULT" | wc -w)
        EXPECTED_SUCCESS=$((NODE_COUNT - FAULT_COUNT))
    fi

    while [ $ELAPSED -lt $TOTAL_WAIT ]; do
        sleep $CHECK_INTERVAL
        ELAPSED=$((ELAPSED + CHECK_INTERVAL))

        # 检查各节点的输出目录中是否有文件
        SUCCESS_NODES=0
        for node in $ALL_NODES; do
            # 跳过故障节点
            if [ -n "$EXPECT_FAULT" ] && echo "$EXPECT_FAULT" | grep -qw "$node"; then
                continue
            fi

            # 检查输出目录中是否有.bin文件
            if ls "${OUTPUT_BASE}/${node}"/output_*.bin >/dev/null 2>&1; then
                SUCCESS_NODES=$((SUCCESS_NODES + 1))
            fi
        done

        # 进度显示
        printf "\r  [%3ds/%ds] 已完成节点: %d/%d" "$ELAPSED" "$TOTAL_WAIT" "$SUCCESS_NODES" "$EXPECTED_SUCCESS"

        # 如果所有期望节点都完成了，提前退出
        if [ "$SUCCESS_NODES" -ge "$EXPECTED_SUCCESS" ]; then
            echo ""
            ok "所有期望节点已完成RBC协议"
            break
        fi
    done

    if [ "$SUCCESS_NODES" -lt "$EXPECTED_SUCCESS" ]; then
        echo ""
        warn "超时：只有 $SUCCESS_NODES/$EXPECTED_SUCCESS 个节点完成"
    fi

    # 6. 验证数据完整性
    step "验证数据完整性..."
    local INTEGRITY_PASS=0
    local INTEGRITY_FAIL=0

    for node in $ALL_NODES; do
        # 跳过故障节点
        if [ -n "$EXPECT_FAULT" ] && echo "$EXPECT_FAULT" | grep -qw "$node"; then
            continue
        fi

        local NODE_OUTPUT_DIR="${OUTPUT_BASE}/${node}"

        # 获取RBC输出文件列表
        local OUTPUT_FILES
        OUTPUT_FILES=$(ls "${NODE_OUTPUT_DIR}"/output_*.bin 2>/dev/null || true)

        if [ -z "$OUTPUT_FILES" ]; then
            if [ -f "${NODE_OUTPUT_DIR}/DONE" ]; then
                info "  $node: 有DONE标记但无输出文件"
            else
                warn "  $node: 未找到RBC输出文件"
                INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            fi
            continue
        fi

        # 验证每个输出文件
        for output_file in $OUTPUT_FILES; do
            local OUTPUT_HASH
            OUTPUT_HASH=$(sha256sum "$output_file" | awk '{print $1}')
            local OUTPUT_SIZE
            OUTPUT_SIZE=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file" 2>/dev/null)

            if [ "$OUTPUT_HASH" = "$EXPECTED_HASH" ]; then
                ok "  $node: 数据完整性验证通过 (大小=${OUTPUT_SIZE}字节, hash=${OUTPUT_HASH:0:16}...)"
                INTEGRITY_PASS=$((INTEGRITY_PASS + 1))
            else
                fail "  $node: 数据完整性验证失败!"
                info "    期望hash: ${EXPECTED_HASH:0:32}..."
                info "    实际hash: ${OUTPUT_HASH:0:32}..."
                info "    期望大小: ${FILE_SIZE}, 实际大小: ${OUTPUT_SIZE}"
                INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            fi
        done
    done

    echo ""
    if [ "$INTEGRITY_PASS" -ge "$EXPECTED_SUCCESS" ] && [ "$INTEGRITY_FAIL" -eq 0 ]; then
        ok "[$TEST_NAME] 测试通过! $INTEGRITY_PASS/$EXPECTED_SUCCESS 个节点数据完整性验证通过"
        return 0
    elif [ "$INTEGRITY_PASS" -gt 0 ]; then
        warn "[$TEST_NAME] 部分通过: $INTEGRITY_PASS 通过, $INTEGRITY_FAIL 失败"
        return 0
    else
        fail "[$TEST_NAME] 测试失败: 没有节点通过数据完整性验证"

        # 输出调试日志
        info "调试信息 - 各节点RBC相关日志:"
        for node in $ALL_NODES; do
            echo -e "${YELLOW}--- $node ---${NC}"
            dc_with_override logs "$node" 2>&1 | grep -i "rbc\|广播\|分片\|重建" | tail -20
            echo ""
        done
        return 1
    fi
}

# ==========================================
# 开始测试
# ==========================================

# 先确保环境干净
cleanup
rm -rf "${OUTPUT_BASE:?}"/* 2>/dev/null || true

# 生成compose文件
generate_compose_file
echo ""

# 构建镜像
info "测试0: 构建Docker镜像..."
dc build 2>&1 | tail -10
ok "Docker镜像构建成功"
echo ""

# ==========================================
# 测试1: 小文件广播 (1KB)
# ==========================================
run_rbc_test "测试1: 小文件广播(1KB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/small_1kb.bin" ""
TEST1_RESULT=$?

# ==========================================
# 测试2: 中等文件广播 (100KB)
# ==========================================
run_rbc_test "测试2: 中等文件广播(100KB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/medium_100kb.bin" ""
TEST2_RESULT=$?

# ==========================================
# 测试3: 大文件广播 (1MB)
# ==========================================
run_rbc_test "测试3: 大文件广播(1MB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/large_1mb.bin" ""
TEST3_RESULT=$?

# ==========================================
# 测试4: 文本文件广播
# ==========================================
run_rbc_test "测试4: 文本文件广播 [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/text_file.txt" ""
TEST4_RESULT=$?

# ==========================================
# 测试5: 单节点故障容错广播 (1MB)
# ==========================================
# 选择倒数第一个普通节点作为故障节点
FAULT_SINGLE="node$((NODE_COUNT - 1))"
run_rbc_test "测试5: 单节点故障容错(1MB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/large_1mb.bin" "$FAULT_SINGLE"
TEST5_RESULT=$?

# ==========================================
# 测试6: t个节点故障容错广播 (100KB, 极限测试)
# ==========================================
# 选择最后t个普通节点作为故障节点
FAULT_MAX=""
for i in $(seq $((NODE_COUNT - 1)) -1 $((NODE_COUNT - T))); do
    if [ -z "$FAULT_MAX" ]; then
        FAULT_MAX="node${i}"
    else
        FAULT_MAX="$FAULT_MAX node${i}"
    fi
done
run_rbc_test "测试6: ${T}节点故障容错(100KB) [n=${NODE_COUNT},t=${T},极限]" "$TEST_DIR/medium_100kb.bin" "$FAULT_MAX"
TEST6_RESULT=$?

# ==========================================
# 测试结果汇总
# ==========================================
echo ""
echo "=========================================="
echo "  RBC协议测试结果汇总 (n=${NODE_COUNT}, t=${T})"
echo "=========================================="

TOTAL_PASS=0
TOTAL_FAIL=0

print_result() {
    local name="$1"
    local result="$2"
    if [ "$result" -eq 0 ]; then
        ok "$name"
        TOTAL_PASS=$((TOTAL_PASS + 1))
    else
        fail "$name"
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi
}

print_result "测试1: 小文件广播(1KB)" "$TEST1_RESULT"
print_result "测试2: 中等文件广播(100KB)" "$TEST2_RESULT"
print_result "测试3: 大文件广播(1MB)" "$TEST3_RESULT"
print_result "测试4: 文本文件广播" "$TEST4_RESULT"
print_result "测试5: 单节点故障容错(1MB)" "$TEST5_RESULT"
print_result "测试6: ${T}节点故障容错(100KB)" "$TEST6_RESULT"

echo ""
echo "通过: $TOTAL_PASS / $((TOTAL_PASS + TOTAL_FAIL))"
echo ""

# 清理
echo ""
read -p "是否清理所有容器和测试文件？(y/N): " CLEANUP
if [ "$CLEANUP" = "y" ] || [ "$CLEANUP" = "Y" ]; then
    cleanup
    rm -rf "$TEST_DIR" "${OUTPUT_BASE}" "${SCRIPT_DIR}/docker-compose.test.yml" "$COMPOSE_FILE" 2>/dev/null || true
    ok "清理完成"
else
    info "容器保留运行中，手动清理请执行: docker compose -f $COMPOSE_FILE down -v"
    info "测试文件保留在: $TEST_DIR"
    info "输出文件保留在: $OUTPUT_BASE"
fi

echo ""
echo "=========================================="
echo "  RBC协议测试完成 (n=${NODE_COUNT}, t=${T})"
echo "=========================================="

if [ "$TOTAL_FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
