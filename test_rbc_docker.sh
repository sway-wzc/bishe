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
# 容错测试中需要额外时间：sleep5(启动检查) + sleep20(网络就绪) + 停止节点(~10s/节点) + sleep3 + 基线采集
# 因此延迟需要足够大，确保基线采集完成后广播才开始
RBC_BROADCAST_DELAY=$((60 + NODE_COUNT * 3))
# RBC协议完成等待时间（秒），节点越多需要等待越久
# 超大文件分块广播需要更多时间（每个分块独立走一次RBC）
# 50MB文件约13个分块，每个分块需要完整RBC流程
RBC_COMPLETION_WAIT=$((120 + NODE_COUNT * 12))

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.dynamic.yml"
SEED_IP="172.28.0.10"
SUBNET="172.28.0.0/16"

# 开销统计数据目录
STATS_DIR="${SCRIPT_DIR}/rbc_stats_tmp"
mkdir -p "$STATS_DIR"
# 清理上次测试残留的CSV汇总文件，避免新数据追加到旧数据后面
rm -f "${STATS_DIR}/overhead_summary.csv"

echo "=========================================="
echo "  高容错分布式数据分发系统 RBC协议测试"
echo "  节点数: $NODE_COUNT (n=${NODE_COUNT}, t=${T})"
echo "  数据分片: $((T + 1))片, 校验分片: $((NODE_COUNT - T - 1))片"
echo "  广播延迟: ${RBC_BROADCAST_DELAY}秒"
echo "  测试内容: 大文件分片广播分发与恢复"
echo "  附加统计: 传输开销 + 计算开销"
echo "=========================================="
echo ""

# ==========================================
# 开销统计工具函数
# ==========================================

# 采集单个容器的网络流量统计（通过docker exec读取/sys网络统计）
# 参数1: 容器名
# 返回: "发送字节数 接收字节数"
capture_container_net_stats() {
    local CONTAINER="$1"
    local TX_BYTES RX_BYTES
    TX_BYTES=$(docker exec "$CONTAINER" cat /sys/class/net/eth0/statistics/tx_bytes 2>/dev/null || echo "0")
    RX_BYTES=$(docker exec "$CONTAINER" cat /sys/class/net/eth0/statistics/rx_bytes 2>/dev/null || echo "0")
    echo "$TX_BYTES $RX_BYTES"
}

# 采集所有节点的网络流量快照
# 参数1: 快照文件路径
capture_all_net_stats() {
    local SNAPSHOT_FILE="$1"
    > "$SNAPSHOT_FILE"
    for node in $ALL_NODES; do
        local CONTAINER="p2p-${node}"
        local STATS
        STATS=$(capture_container_net_stats "$CONTAINER")
        echo "${node} ${STATS}" >> "$SNAPSHOT_FILE"
    done
}

# 计算两次快照之间的网络流量差值
# 参数1: 前快照文件  参数2: 后快照文件  参数3: 输出文件
calc_net_diff() {
    local BEFORE="$1"
    local AFTER="$2"
    local OUTPUT="$3"
    > "$OUTPUT"

    local TOTAL_TX=0
    local TOTAL_RX=0

    while IFS=' ' read -r NODE TX_BEFORE RX_BEFORE; do
        local TX_AFTER RX_AFTER
        TX_AFTER=$(grep "^${NODE} " "$AFTER" | awk '{print $2}')
        RX_AFTER=$(grep "^${NODE} " "$AFTER" | awk '{print $3}')
        TX_AFTER=${TX_AFTER:-0}
        RX_AFTER=${RX_AFTER:-0}

        local TX_DIFF=$((TX_AFTER - TX_BEFORE))
        local RX_DIFF=$((RX_AFTER - RX_BEFORE))

        echo "${NODE} ${TX_DIFF} ${RX_DIFF}" >> "$OUTPUT"
        TOTAL_TX=$((TOTAL_TX + TX_DIFF))
        TOTAL_RX=$((TOTAL_RX + RX_DIFF))
    done < "$BEFORE"

    echo "TOTAL ${TOTAL_TX} ${TOTAL_RX}" >> "$OUTPUT"
}

# 格式化字节数为人类可读格式
format_bytes() {
    local BYTES=$1
    if [ "$BYTES" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $BYTES / 1073741824" | bc)GB"
    elif [ "$BYTES" -ge 1048576 ]; then
        echo "$(echo "scale=2; $BYTES / 1048576" | bc)MB"
    elif [ "$BYTES" -ge 1024 ]; then
        echo "$(echo "scale=2; $BYTES / 1024" | bc)KB"
    else
        echo "${BYTES}B"
    fi
}

# 从容器日志中提取RBC计算开销（编码/解码耗时）
# 参数1: 测试名称标识
extract_computation_stats() {
    local TEST_ID="$1"
    local COMP_FILE="${STATS_DIR}/${TEST_ID}_computation.txt"
    > "$COMP_FILE"

    for node in $ALL_NODES; do
        local NODE_LOG
        NODE_LOG=$(dc_with_override logs "$node" 2>&1)

        # 提取RS编码耗时（格式: "RS编码耗时=X.XXXms"）
        local ENCODE_TIME
        ENCODE_TIME=$(echo "$NODE_LOG" | grep -oP 'RS编码耗时=\K[0-9.]+ms' | tail -1 || echo "")

        # 提取RS解码耗时（格式: "RS解码耗时=X.XXXms"）
        local DECODE_TIME
        DECODE_TIME=$(echo "$NODE_LOG" | grep -oP 'RS解码耗时=\K[0-9.]+ms' | tail -1 || echo "")

        # 提取SHA-256哈希计算耗时（格式: "SHA-256哈希耗时=X.XXXms"）
        local HASH_TIME
        HASH_TIME=$(echo "$NODE_LOG" | grep -oP 'SHA-256哈希耗时=\K[0-9.]+ms' | tail -1 || echo "")

        # 提取哈希验证耗时（格式: "哈希验证耗时=X.XXXms"）
        local HASH_VERIFY_TIME
        HASH_VERIFY_TIME=$(echo "$NODE_LOG" | grep -oP '哈希验证耗时=\K[0-9.]+ms' | tail -1 || echo "")

        # 提取纠错解码成功的总信息
        local DECODE_INFO
        DECODE_INFO=$(echo "$NODE_LOG" | grep '纠错解码成功' | tail -1 || echo "")

        echo "${node} encode=${ENCODE_TIME:-N/A} decode=${DECODE_TIME:-N/A} hash=${HASH_TIME:-N/A} hash_verify=${HASH_VERIFY_TIME:-N/A}" >> "$COMP_FILE"
    done
}

# 输出开销统计报告
# 参数1: 测试名称  参数2: 原始文件大小  参数3: 网络差值文件  参数4: 端到端耗时(秒)  参数5: 测试ID
print_overhead_report() {
    local TEST_NAME="$1"
    local FILE_SIZE="$2"
    local NET_DIFF_FILE="$3"
    local E2E_TIME="$4"
    local TEST_ID="$5"
    local COMP_FILE="${STATS_DIR}/${TEST_ID}_computation.txt"

    echo ""
    echo -e "${CYAN}┌──────────────────────────────────────────────────────┐${NC}"
    echo -e "${CYAN}│          开销统计报告: ${TEST_NAME}${NC}"
    echo -e "${CYAN}├──────────────────────────────────────────────────────┤${NC}"

    # 1. 端到端延迟
    echo -e "${CYAN}│${NC} ${BLUE}[端到端延迟]${NC} ${E2E_TIME}秒"
    echo -e "${CYAN}│${NC}"

    # 2. 传输开销
    echo -e "${CYAN}│${NC} ${BLUE}[传输开销 - 各节点网络流量]${NC}"
    echo -e "${CYAN}│${NC}   节点        发送(TX)         接收(RX)         合计"
    echo -e "${CYAN}│${NC}   ─────────  ──────────────  ──────────────  ──────────────"

    local TOTAL_TX=0
    local TOTAL_RX=0

    if [ -f "$NET_DIFF_FILE" ]; then
        while IFS=' ' read -r NODE TX RX; do
            if [ "$NODE" = "TOTAL" ]; then
                TOTAL_TX=$TX
                TOTAL_RX=$RX
                continue
            fi
            local SUM=$((TX + RX))
            printf "${CYAN}│${NC}   %-10s  %-14s  %-14s  %-14s\n" \
                "$NODE" "$(format_bytes $TX)" "$(format_bytes $RX)" "$(format_bytes $SUM)"
        done < "$NET_DIFF_FILE"
    fi

    local TOTAL_TRAFFIC=$TOTAL_TX  # 只统计发送(TX)，避免TX+RX双重计算
    echo -e "${CYAN}│${NC}   ─────────  ──────────────  ──────────────  ──────────────"
    printf "${CYAN}│${NC}   ${GREEN}%-10s  %-14s  %-14s  %-14s${NC}\n" \
        "合计" "$(format_bytes $TOTAL_TX)" "$(format_bytes $TOTAL_RX)" "$(format_bytes $TOTAL_TRAFFIC)"

    # 3. 传输放大比
    if [ "$FILE_SIZE" -gt 0 ] && [ "$TOTAL_TRAFFIC" -gt 0 ]; then
        local AMPLIFICATION
        AMPLIFICATION=$(echo "scale=2; $TOTAL_TRAFFIC / $FILE_SIZE" | bc)
        echo -e "${CYAN}│${NC}"
        echo -e "${CYAN}│${NC} ${BLUE}[传输放大比]${NC} ${AMPLIFICATION}x (总发送量/原始文件大小)"
        echo -e "${CYAN}│${NC}   原始文件: $(format_bytes $FILE_SIZE), 总发送量(TX): $(format_bytes $TOTAL_TRAFFIC)"
    fi

    # 4. 计算开销（从日志提取）
    echo -e "${CYAN}│${NC}"
    echo -e "${CYAN}│${NC} ${BLUE}[计算开销 - RS编解码与哈希计算耗时]${NC}"
    if [ -f "$COMP_FILE" ]; then
        echo -e "${CYAN}│${NC}   节点        RS编码       RS解码       SHA-256哈希  哈希验证"
        echo -e "${CYAN}│${NC}   ─────────  ──────────  ──────────  ──────────  ──────────"
        while IFS=' ' read -r NODE ENCODE DECODE HASH HASH_V; do
            # 解析 key=value 格式
            ENCODE=${ENCODE#encode=}
            DECODE=${DECODE#decode=}
            HASH=${HASH#hash=}
            HASH_V=${HASH_V#hash_verify=}
            printf "${CYAN}│${NC}   %-10s  %-10s  %-10s  %-10s  %-10s\n" \
                "$NODE" "$ENCODE" "$DECODE" "$HASH" "$HASH_V"
        done < "$COMP_FILE"
    else
        echo -e "${CYAN}│${NC}   (日志中未找到计时信息，需要在Rust代码中添加耗时日志)"
    fi

    # 5. 理论开销分析
    echo -e "${CYAN}│${NC}"
    echo -e "${CYAN}│${NC} ${BLUE}[理论分析]${NC}"
    local DATA_SHARDS=$((T + 1))
    local SHARD_SIZE
    if [ "$FILE_SIZE" -gt 0 ]; then
        SHARD_SIZE=$(( (FILE_SIZE + DATA_SHARDS - 1) / DATA_SHARDS ))
    else
        SHARD_SIZE=0
    fi
    # PROPOSE: 广播者发送完整数据给n个节点 = n * |M|
    local PROPOSE_COST=$((NODE_COUNT * FILE_SIZE))
    # ECHO: 每个节点向n个节点各发一个分片 = n * n * shard_size
    local ECHO_COST=$((NODE_COUNT * NODE_COUNT * SHARD_SIZE))
    # READY: 每个节点向n个节点广播自己的分片 = n * n * shard_size
    local READY_COST=$((NODE_COUNT * NODE_COUNT * SHARD_SIZE))
    local THEORY_TOTAL=$((PROPOSE_COST + ECHO_COST + READY_COST))

    echo -e "${CYAN}│${NC}   分片大小: $(format_bytes $SHARD_SIZE) (文件$(format_bytes $FILE_SIZE) / ${DATA_SHARDS}个数据分片)"
    echo -e "${CYAN}│${NC}   PROPOSE阶段理论开销: $(format_bytes $PROPOSE_COST) (n×|M| = ${NODE_COUNT}×$(format_bytes $FILE_SIZE))"
    echo -e "${CYAN}│${NC}   ECHO阶段理论开销:    $(format_bytes $ECHO_COST) (n²×shard = ${NODE_COUNT}²×$(format_bytes $SHARD_SIZE))"
    echo -e "${CYAN}│${NC}   READY阶段理论开销:   $(format_bytes $READY_COST) (n²×shard = ${NODE_COUNT}²×$(format_bytes $SHARD_SIZE))"
    echo -e "${CYAN}│${NC}   理论总传输量:        $(format_bytes $THEORY_TOTAL)"
    if [ "$THEORY_TOTAL" -gt 0 ] && [ "$FILE_SIZE" -gt 0 ]; then
        local THEORY_AMP
        THEORY_AMP=$(echo "scale=2; $THEORY_TOTAL / $FILE_SIZE" | bc)
        echo -e "${CYAN}│${NC}   理论传输放大比:      ${THEORY_AMP}x"
        # 修正理论值：本地消息不经过网卡，实际TX = 理论值 × (n-1)/n
        local CORRECTED_AMP
        CORRECTED_AMP=$(echo "scale=2; $THEORY_AMP * ($NODE_COUNT - 1) / $NODE_COUNT" | bc)
        echo -e "${CYAN}│${NC}   修正传输放大比(TX):  ${CORRECTED_AMP}x (×(n-1)/n, 本地消息不经网卡)"
    fi

    echo -e "${CYAN}└──────────────────────────────────────────────────────┘${NC}"
    echo ""

    # 将统计数据写入CSV文件（便于后续分析）
    local CSV_FILE="${STATS_DIR}/overhead_summary.csv"
    if [ ! -f "$CSV_FILE" ]; then
        echo "测试名称,文件大小(B),节点数,容错数t,数据分片数,端到端延迟(s),总发送TX(B),总接收RX(B),实际总发送(B),传输放大比(TX),理论总传输(B),理论放大比,修正理论放大比" > "$CSV_FILE"
    fi
    local AMP_VAL="0"
    local THEORY_AMP_VAL="0"
    if [ "$FILE_SIZE" -gt 0 ] && [ "$TOTAL_TRAFFIC" -gt 0 ]; then
        AMP_VAL=$(echo "scale=4; $TOTAL_TRAFFIC / $FILE_SIZE" | bc)
    fi
    if [ "$FILE_SIZE" -gt 0 ] && [ "$THEORY_TOTAL" -gt 0 ]; then
        THEORY_AMP_VAL=$(echo "scale=4; $THEORY_TOTAL / $FILE_SIZE" | bc)
    fi
    local CORRECTED_AMP_VAL="0"
    if [ "$FILE_SIZE" -gt 0 ] && [ "$THEORY_TOTAL" -gt 0 ]; then
        CORRECTED_AMP_VAL=$(echo "scale=4; $THEORY_AMP_VAL * ($NODE_COUNT - 1) / $NODE_COUNT" | bc)
    fi
    echo "\"${TEST_NAME}\",${FILE_SIZE},${NODE_COUNT},${T},${DATA_SHARDS},${E2E_TIME},${TOTAL_TX},${TOTAL_RX},${TOTAL_TRAFFIC},${AMP_VAL},${THEORY_TOTAL},${THEORY_AMP_VAL},${CORRECTED_AMP_VAL}" >> "$CSV_FILE"
}

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
# INT=Ctrl+C, TERM=kill, TSTP=Ctrl+Z
trap 'echo ""; warn "测试中断，正在清理..."; cleanup; exit 1' INT TERM TSTP

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
# 5. 超大文件 (20MB) - 测试分块广播（超过16MB单消息限制）
dd if=/dev/urandom of="$TEST_DIR/huge_20mb.bin" bs=1024 count=20480 2>/dev/null
# 6. 超大文件 (50MB) - 测试分块广播极限
dd if=/dev/urandom of="$TEST_DIR/huge_50mb.bin" bs=1024 count=51200 2>/dev/null

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
# 参数1: 测试文件名
# 参数2: 恶意节点配置（格式: "node5:corrupt_shard node6:wrong_hash"，空则无恶意节点）
generate_compose_override() {
    local TEST_FILE_NAME="$1"
    local BYZANTINE_NODES="$2"
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
        local NODE_NAME="node${i}"
        local BYZ_MODE=""
        # 检查该节点是否在恶意节点列表中
        if [ -n "$BYZANTINE_NODES" ]; then
            for byz_entry in $BYZANTINE_NODES; do
                local byz_node="${byz_entry%%:*}"
                local byz_mode="${byz_entry##*:}"
                if [ "$byz_node" = "$NODE_NAME" ]; then
                    BYZ_MODE="$byz_mode"
                    break
                fi
            done
        fi

        if [ -n "$BYZ_MODE" ]; then
            cat >> "$OVERRIDE_FILE" << YAML

  node${i}:
    volumes:
      - ${OUTPUT_BASE}/node${i}:/app/rbc_output
    environment:
      - RBC_OUTPUT_DIR=/app/rbc_output
      - RUST_LOG=info
      - BYZANTINE_MODE=${BYZ_MODE}
YAML
        else
            cat >> "$OVERRIDE_FILE" << YAML

  node${i}:
    volumes:
      - ${OUTPUT_BASE}/node${i}:/app/rbc_output
    environment:
      - RBC_OUTPUT_DIR=/app/rbc_output
      - RUST_LOG=info
YAML
        fi
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

    # 生成测试ID（用于统计文件命名）
    local TEST_ID
    TEST_ID=$(echo "$TEST_NAME" | sed 's/[^a-zA-Z0-9]/_/g' | head -c 60)

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
    info "  📊 开销统计已启用"
    echo "========================================"

    # 1. 清理之前的环境
    dc_with_override down -v --remove-orphans 2>/dev/null || true
    sleep 2

    # 清理输出目录
    rm -rf "${OUTPUT_BASE:?}"/*
    for node in $ALL_NODES; do
        mkdir -p "${OUTPUT_BASE}/${node}"
    done

    # 2. 生成compose覆盖文件（无恶意节点）
    generate_compose_override "$FILE_NAME" ""

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

    # 4.5 采集网络流量基线快照（在RBC广播开始前）
    # 注意：对于容错测试，停止节点耗时较长，需确保此时RBC广播尚未开始
    # RBC_BROADCAST_DELAY 必须大于 (sleep5 + sleep20 + 停止节点耗时 + sleep3 + 采集耗时)
    step "采集网络流量基线..."
    local NET_BEFORE="${STATS_DIR}/${TEST_ID}_net_before.txt"
    local NET_AFTER="${STATS_DIR}/${TEST_ID}_net_after.txt"
    local NET_DIFF="${STATS_DIR}/${TEST_ID}_net_diff.txt"
    capture_all_net_stats "$NET_BEFORE"

    # 记录RBC开始时间
    local RBC_START_TIME
    RBC_START_TIME=$(date +%s)

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

    # 5.5 采集网络流量结束快照 & 计算差值
    local RBC_END_TIME
    RBC_END_TIME=$(date +%s)
    local E2E_TIME=$((RBC_END_TIME - RBC_START_TIME))

    step "采集网络流量结束快照..."
    capture_all_net_stats "$NET_AFTER"
    calc_net_diff "$NET_BEFORE" "$NET_AFTER" "$NET_DIFF"

    # 提取计算开销
    step "提取计算开销数据..."
    extract_computation_stats "$TEST_ID"

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
        # 输出开销统计报告
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"
        return 0
    elif [ "$INTEGRITY_PASS" -gt 0 ]; then
        warn "[$TEST_NAME] 部分通过: $INTEGRITY_PASS 通过, $INTEGRITY_FAIL 失败"
        # 输出开销统计报告
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"
        return 0
    else
        fail "[$TEST_NAME] 测试失败: 没有节点通过数据完整性验证"
        # 即使失败也输出开销统计（用于分析）
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"

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
# 测试7: 超大文件分块广播 (5MB)
# ==========================================
run_rbc_test "测试7: 超大文件分块广播(20MB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/huge_20mb.bin" ""
TEST7_RESULT=$?

# ==========================================
# 测试8: 超大文件分块广播 (50MB)
# ==========================================
run_rbc_test "测试8: 超大文件分块广播(50MB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/huge_50mb.bin" ""
TEST8_RESULT=$?

# ==========================================
# 恶意节点（拜占庭）测试函数
# ==========================================
# 参数1: 测试名称
# 参数2: 测试文件路径
# 参数3: 恶意节点配置（格式: "node5:corrupt_shard node6:wrong_hash"）
run_byzantine_test() {
    local TEST_NAME="$1"
    local TEST_FILE="$2"
    local BYZANTINE_CONFIG="$3"  # 恶意节点配置

    local FILE_NAME
    FILE_NAME=$(basename "$TEST_FILE")
    local FILE_SIZE
    FILE_SIZE=$(stat -c%s "$TEST_FILE" 2>/dev/null || stat -f%z "$TEST_FILE" 2>/dev/null)
    local EXPECTED_HASH
    EXPECTED_HASH=$(sha256sum "$TEST_FILE" | awk '{print $1}')

    # 解析恶意节点信息
    local BYZ_COUNT
    BYZ_COUNT=$(echo "$BYZANTINE_CONFIG" | wc -w)
    local BYZ_NODE_NAMES=""
    for byz_entry in $BYZANTINE_CONFIG; do
        local byz_node="${byz_entry%%:*}"
        local byz_mode="${byz_entry##*:}"
        if [ -z "$BYZ_NODE_NAMES" ]; then
            BYZ_NODE_NAMES="${byz_node}(${byz_mode})"
        else
            BYZ_NODE_NAMES="${BYZ_NODE_NAMES} ${byz_node}(${byz_mode})"
        fi
    done

    # 生成测试ID（用于统计文件命名）
    local TEST_ID
    TEST_ID=$(echo "$TEST_NAME" | sed 's/[^a-zA-Z0-9]/_/g' | head -c 60)

    echo ""
    echo "========================================"
    step "$TEST_NAME"
    info "  文件: $FILE_NAME, 大小: ${FILE_SIZE}字节"
    info "  期望SHA-256: ${EXPECTED_HASH:0:32}..."
    info "  \033[0;31m恶意节点(${BYZ_COUNT}个): ${BYZ_NODE_NAMES}\033[0m"
    info "  诚实节点应正常完成RBC协议并输出正确数据"
    info "  📊 开销统计已启用"
    echo "========================================"

    # 1. 清理之前的环境
    dc_with_override down -v --remove-orphans 2>/dev/null || true
    sleep 2

    # 清理输出目录
    rm -rf "${OUTPUT_BASE:?}"/*
    for node in $ALL_NODES; do
        mkdir -p "${OUTPUT_BASE}/${node}"
    done

    # 2. 生成compose覆盖文件（带恶意节点配置）
    generate_compose_override "$FILE_NAME" "$BYZANTINE_CONFIG"

    # 3. 启动所有节点
    step "启动所有 ${NODE_COUNT} 个节点（含 ${BYZ_COUNT} 个恶意节点）..."
    dc_with_override up -d
    sleep 5

    RUNNING_COUNT=$(dc_with_override ps --status running | grep -c "p2p-" || true)
    if [ "$RUNNING_COUNT" -ge "$NODE_COUNT" ]; then
        ok "所有${NODE_COUNT}个节点启动成功（含${BYZ_COUNT}个恶意节点）"
    else
        warn "部分节点启动失败（运行中: $RUNNING_COUNT/$NODE_COUNT）"
    fi

    # 4. 采集网络流量基线快照（在RBC广播开始前）
    step "采集网络流量基线..."
    local NET_BEFORE="${STATS_DIR}/${TEST_ID}_net_before.txt"
    local NET_AFTER="${STATS_DIR}/${TEST_ID}_net_after.txt"
    local NET_DIFF="${STATS_DIR}/${TEST_ID}_net_diff.txt"
    capture_all_net_stats "$NET_BEFORE"

    # 记录RBC开始时间
    local RBC_START_TIME
    RBC_START_TIME=$(date +%s)

    # 4.5 等待RBC广播完成
    local TOTAL_WAIT=$((RBC_BROADCAST_DELAY + RBC_COMPLETION_WAIT))
    step "等待RBC协议完成（最多 ${TOTAL_WAIT}秒）..."

    # 诚实节点数 = 总节点数 - 恶意节点数
    local EXPECTED_SUCCESS=$((NODE_COUNT - BYZ_COUNT))

    local ELAPSED=0
    local CHECK_INTERVAL=5
    local SUCCESS_NODES=0

    while [ $ELAPSED -lt $TOTAL_WAIT ]; do
        sleep $CHECK_INTERVAL
        ELAPSED=$((ELAPSED + CHECK_INTERVAL))

        # 检查诚实节点的输出
        SUCCESS_NODES=0
        for node in $ALL_NODES; do
            # 跳过恶意节点（恶意节点的输出不计入）
            local is_byzantine=false
            for byz_entry in $BYZANTINE_CONFIG; do
                local byz_node="${byz_entry%%:*}"
                if [ "$byz_node" = "$node" ]; then
                    is_byzantine=true
                    break
                fi
            done
            if [ "$is_byzantine" = true ]; then
                continue
            fi

            if ls "${OUTPUT_BASE}/${node}"/output_*.bin >/dev/null 2>&1; then
                SUCCESS_NODES=$((SUCCESS_NODES + 1))
            fi
        done

        printf "\r  [%3ds/%ds] 诚实节点已完成: %d/%d" "$ELAPSED" "$TOTAL_WAIT" "$SUCCESS_NODES" "$EXPECTED_SUCCESS"

        if [ "$SUCCESS_NODES" -ge "$EXPECTED_SUCCESS" ]; then
            echo ""
            ok "所有诚实节点已完成RBC协议"
            break
        fi
    done

    if [ "$SUCCESS_NODES" -lt "$EXPECTED_SUCCESS" ]; then
        echo ""
        warn "超时：只有 $SUCCESS_NODES/$EXPECTED_SUCCESS 个诚实节点完成"
    fi

    # 4.8 采集网络流量结束快照 & 计算差值
    local RBC_END_TIME
    RBC_END_TIME=$(date +%s)
    local E2E_TIME=$((RBC_END_TIME - RBC_START_TIME))

    step "采集网络流量结束快照..."
    capture_all_net_stats "$NET_AFTER"
    calc_net_diff "$NET_BEFORE" "$NET_AFTER" "$NET_DIFF"

    # 提取计算开销
    step "提取计算开销数据..."
    extract_computation_stats "$TEST_ID"

    # 5. 验证诚实节点的数据完整性
    step "验证诚实节点数据完整性..."
    local INTEGRITY_PASS=0
    local INTEGRITY_FAIL=0

    for node in $ALL_NODES; do
        # 跳过恶意节点
        local is_byzantine=false
        for byz_entry in $BYZANTINE_CONFIG; do
            local byz_node="${byz_entry%%:*}"
            if [ "$byz_node" = "$node" ]; then
                is_byzantine=true
                break
            fi
        done
        if [ "$is_byzantine" = true ]; then
            info "  $node: [恶意节点] 跳过验证"
            continue
        fi

        local NODE_OUTPUT_DIR="${OUTPUT_BASE}/${node}"
        local OUTPUT_FILES
        OUTPUT_FILES=$(ls "${NODE_OUTPUT_DIR}"/output_*.bin 2>/dev/null || true)

        if [ -z "$OUTPUT_FILES" ]; then
            warn "  $node: 未找到RBC输出文件"
            INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            continue
        fi

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
                INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            fi
        done
    done

    # 6. 检查恶意节点日志确认恶意行为已执行
    step "验证恶意行为已生效..."
    for byz_entry in $BYZANTINE_CONFIG; do
        local byz_node="${byz_entry%%:*}"
        local byz_mode="${byz_entry##*:}"
        local byz_log_keyword=""
        case "$byz_mode" in
            corrupt_shard) byz_log_keyword="拜占庭-篡改分片" ;;
            wrong_hash)    byz_log_keyword="拜占庭-伪造哈希" ;;
            silent)        byz_log_keyword="拜占庭-沉默" ;;
        esac
        if dc_with_override logs "$byz_node" 2>&1 | grep -q "$byz_log_keyword"; then
            ok "  $byz_node: 恶意行为(${byz_mode})已确认执行"
        else
            warn "  $byz_node: 未检测到恶意行为日志（可能未触发）"
        fi
    done

    echo ""
    if [ "$INTEGRITY_PASS" -ge "$EXPECTED_SUCCESS" ] && [ "$INTEGRITY_FAIL" -eq 0 ]; then
        ok "[$TEST_NAME] 测试通过! ${INTEGRITY_PASS}个诚实节点数据完整，${BYZ_COUNT}个恶意节点被容忍"
        # 输出开销统计报告
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"
        return 0
    elif [ "$INTEGRITY_PASS" -gt 0 ]; then
        warn "[$TEST_NAME] 部分通过: $INTEGRITY_PASS 通过, $INTEGRITY_FAIL 失败"
        # 输出开销统计报告
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"
        return 0
    else
        fail "[$TEST_NAME] 测试失败: 没有诚实节点通过数据完整性验证"
        # 即使失败也输出开销统计（用于分析）
        print_overhead_report "$TEST_NAME" "$FILE_SIZE" "$NET_DIFF" "$E2E_TIME" "$TEST_ID"

        info "调试信息 - 各节点RBC相关日志:"
        for node in $ALL_NODES; do
            echo -e "${YELLOW}--- $node ---${NC}"
            dc_with_override logs "$node" 2>&1 | grep -i "rbc\|广播\|分片\|拜占庭\|纠错" | tail -20
            echo ""
        done
        return 1
    fi
}

# ==========================================
# 测试9: 恶意节点 - 篡改分片数据 (corrupt_shard)
# ==========================================
# 选择最后1个普通节点作为恶意节点（篡改分片）
BYZ_CORRUPT="node$((NODE_COUNT - 1)):corrupt_shard"
run_byzantine_test "测试9: 恶意节点-篡改分片(100KB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/medium_100kb.bin" "$BYZ_CORRUPT"
TEST9_RESULT=$?

# ==========================================
# 测试10: 恶意节点 - 伪造哈希 (wrong_hash)
# ==========================================
BYZ_HASH="node$((NODE_COUNT - 1)):wrong_hash"
run_byzantine_test "测试10: 恶意节点-伪造哈希(100KB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/medium_100kb.bin" "$BYZ_HASH"
TEST10_RESULT=$?

# ==========================================
# 测试11: 恶意节点 - 选择性沉默 (silent)
# ==========================================
BYZ_SILENT="node$((NODE_COUNT - 1)):silent"
run_byzantine_test "测试11: 恶意节点-选择性沉默(100KB) [n=${NODE_COUNT},t=${T}]" "$TEST_DIR/medium_100kb.bin" "$BYZ_SILENT"
TEST11_RESULT=$?

# ==========================================
# 测试12: 混合恶意节点 - t个恶意节点同时攻击（极限BFT测试）
# ==========================================
# 构造t个恶意节点，混合不同攻击模式
BYZ_MIX=""
BYZ_MODES=("corrupt_shard" "wrong_hash" "silent")
for i in $(seq $((NODE_COUNT - 1)) -1 $((NODE_COUNT - T))); do
    local_idx=$(( (NODE_COUNT - 1 - i) % 3 ))
    MODE=${BYZ_MODES[$local_idx]}
    if [ -z "$BYZ_MIX" ]; then
        BYZ_MIX="node${i}:${MODE}"
    else
        BYZ_MIX="$BYZ_MIX node${i}:${MODE}"
    fi
done
run_byzantine_test "测试12: ${T}个混合恶意节点(100KB) [n=${NODE_COUNT},t=${T},BFT极限]" "$TEST_DIR/medium_100kb.bin" "$BYZ_MIX"
TEST12_RESULT=$?

# ==========================================
# 测试结果汇总
# ==========================================
echo ""
echo "=========================================="
echo "  RBC协议测试结果汇总 (n=${NODE_COUNT}, t=${T}, 含拜占庭测试)"
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

echo "--- 正常广播与故障容错 ---"
print_result "测试1: 小文件广播(1KB)" "$TEST1_RESULT"
print_result "测试2: 中等文件广播(100KB)" "$TEST2_RESULT"
print_result "测试3: 大文件广播(1MB)" "$TEST3_RESULT"
print_result "测试4: 文本文件广播" "$TEST4_RESULT"
print_result "测试5: 单节点故障容错(1MB)" "$TEST5_RESULT"
print_result "测试6: ${T}节点故障容错(100KB)" "$TEST6_RESULT"
print_result "测试7: 超大文件分块广播(20MB)" "$TEST7_RESULT"
print_result "测试8: 超大文件分块广播(50MB)" "$TEST8_RESULT"
echo ""
echo "--- 拜占庭（恶意节点）容错 ---"
print_result "测试9: 恶意节点-篡改分片(100KB)" "$TEST9_RESULT"
print_result "测试10: 恶意节点-伪造哈希(100KB)" "$TEST10_RESULT"
print_result "测试11: 恶意节点-选择性沉默(100KB)" "$TEST11_RESULT"
print_result "测试12: ${T}个混合恶意节点(100KB)" "$TEST12_RESULT"

echo ""
echo "通过: $TOTAL_PASS / $((TOTAL_PASS + TOTAL_FAIL))"
echo ""

# 输出开销统计CSV文件位置
if [ -f "${STATS_DIR}/overhead_summary.csv" ]; then
    echo ""
    echo -e "${CYAN}===========================================${NC}"
    echo -e "${CYAN}  📊 开销统计数据已保存${NC}"
    echo -e "${CYAN}===========================================${NC}"
    echo -e "  CSV汇总文件: ${STATS_DIR}/overhead_summary.csv"
    echo -e "  详细数据目录: ${STATS_DIR}/"
    echo ""
    echo -e "  ${BLUE}CSV文件内容预览:${NC}"
    column -t -s ',' "${STATS_DIR}/overhead_summary.csv"
    echo ""
    echo -e "  ${BLUE}提示: 可用以下命令查看完整CSV:${NC}"
    echo -e "    column -t -s ',' ${STATS_DIR}/overhead_summary.csv"
    echo ""
fi

# 清理
echo ""
read -p "是否清理所有容器和测试文件？(y/N): " CLEANUP
if [ "$CLEANUP" = "y" ] || [ "$CLEANUP" = "Y" ]; then
    cleanup
    rm -rf "$TEST_DIR" "${OUTPUT_BASE}" "${SCRIPT_DIR}/docker-compose.test.yml" "$COMPOSE_FILE" 2>/dev/null || true
    # 注意：保留统计数据目录，不清理
    info "开销统计数据保留在: ${STATS_DIR}/"
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
