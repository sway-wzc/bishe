#!/bin/bash
# 50MB BFT极限场景 多次重复测试脚本
# 目的：只测试50MB大文件 + t个混合恶意节点（BFT极限），多次重复以减少Docker环境误差
# 用法: bash test_50mb_bft_repeat.sh [-n 节点总数] [-r 重复次数]
# 示例: bash test_50mb_bft_repeat.sh              # 默认n=13, 重复3次
#       bash test_50mb_bft_repeat.sh -n 7 -r 5    # n=7, 重复5次
#       bash test_50mb_bft_repeat.sh -n 10 -r 3   # n=10, 重复3次

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
NODE_COUNT=13
REPEAT_COUNT=3

while getopts "n:r:h" opt; do
    case $opt in
        n) NODE_COUNT="$OPTARG" ;;
        r) REPEAT_COUNT="$OPTARG" ;;
        h)
            echo "用法: bash test_50mb_bft_repeat.sh [-n 节点总数] [-r 重复次数]"
            echo "  -n  总节点数（最小4，默认13）"
            echo "  -r  重复测试次数（默认3）"
            echo "  -h  显示帮助"
            exit 0
            ;;
        *) echo "未知参数，使用 -h 查看帮助"; exit 1 ;;
    esac
done

if [ "$NODE_COUNT" -lt 4 ]; then
    fail "节点数量最少为4"
    exit 1
fi
if [ "$REPEAT_COUNT" -lt 1 ]; then
    fail "重复次数最少为1"
    exit 1
fi

T=$(( (NODE_COUNT - 1) / 3 ))

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

RBC_BROADCAST_DELAY=$((60 + NODE_COUNT * 3))
RBC_COMPLETION_WAIT=$((120 + NODE_COUNT * 12))

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.dynamic.yml"
SEED_IP="172.28.0.10"
SUBNET="172.28.0.0/16"

# 结果目录
STATS_DIR="${SCRIPT_DIR}/rbc_stats_50mb_bft"
mkdir -p "$STATS_DIR"

# CSV结果文件（带时间戳避免覆盖）
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_FILE="${STATS_DIR}/repeat_n${NODE_COUNT}_${TIMESTAMP}.csv"

echo "=========================================="
echo "  50MB BFT极限场景 多次重复测试"
echo "  节点数: n=${NODE_COUNT}, t=${T}"
echo "  重复次数: ${REPEAT_COUNT}"
echo "  结果文件: ${CSV_FILE}"
echo "=========================================="
echo ""

# ==========================================
# 工具函数（从原脚本复用）
# ==========================================

capture_container_net_stats() {
    local CONTAINER="$1"
    local TX_BYTES RX_BYTES
    TX_BYTES=$(docker exec "$CONTAINER" cat /sys/class/net/eth0/statistics/tx_bytes 2>/dev/null || echo "0")
    RX_BYTES=$(docker exec "$CONTAINER" cat /sys/class/net/eth0/statistics/rx_bytes 2>/dev/null || echo "0")
    echo "$TX_BYTES $RX_BYTES"
}

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

extract_computation_stats() {
    local TEST_ID="$1"
    local COMP_FILE="${STATS_DIR}/${TEST_ID}_computation.txt"
    local COMP_SUMMARY="${STATS_DIR}/${TEST_ID}_computation_summary.txt"
    > "$COMP_FILE"

    local SEED_ENCODE_TOTAL=0
    local SEED_HASH_TOTAL=0
    local DECODE_SUM=0
    local HASH_VERIFY_SUM=0
    local DECODE_COUNT=0

    for node in $ALL_NODES; do
        local NODE_LOG
        NODE_LOG=$(dc_with_override logs "$node" 2>&1)

        local ENCODE_TIME
        ENCODE_TIME=$(echo "$NODE_LOG" | grep -oP 'RS编码耗时=\K[0-9.]+' || echo "")
        if [ -n "$ENCODE_TIME" ]; then
            local ENCODE_TOTAL
            ENCODE_TOTAL=$(echo "$ENCODE_TIME" | awk '{s+=$1} END {printf "%.3f", s}')
            if [ "$node" = "seed" ]; then
                SEED_ENCODE_TOTAL=$ENCODE_TOTAL
            fi
        fi

        local DECODE_TIME
        DECODE_TIME=$(echo "$NODE_LOG" | grep -oP 'RS解码耗时=\K[0-9.]+' || echo "")
        if [ -n "$DECODE_TIME" ]; then
            local DECODE_TOTAL
            DECODE_TOTAL=$(echo "$DECODE_TIME" | awk '{s+=$1} END {printf "%.3f", s}')
            DECODE_SUM=$(echo "$DECODE_SUM + $DECODE_TOTAL" | bc)
            DECODE_COUNT=$((DECODE_COUNT + 1))
        fi

        local HASH_TIME
        HASH_TIME=$(echo "$NODE_LOG" | grep -oP 'SHA-256哈希耗时=\K[0-9.]+' || echo "")
        if [ -n "$HASH_TIME" ]; then
            local HASH_TOTAL
            HASH_TOTAL=$(echo "$HASH_TIME" | awk '{s+=$1} END {printf "%.3f", s}')
            if [ "$node" = "seed" ]; then
                SEED_HASH_TOTAL=$HASH_TOTAL
            fi
        fi

        local HASH_VERIFY_TIME
        HASH_VERIFY_TIME=$(echo "$NODE_LOG" | grep -oP '哈希验证耗时=\K[0-9.]+' || echo "")
        if [ -n "$HASH_VERIFY_TIME" ]; then
            local HASH_VERIFY_TOTAL
            HASH_VERIFY_TOTAL=$(echo "$HASH_VERIFY_TIME" | awk '{s+=$1} END {printf "%.3f", s}')
            HASH_VERIFY_SUM=$(echo "$HASH_VERIFY_SUM + $HASH_VERIFY_TOTAL" | bc)
        fi

        echo "${node} encode=${ENCODE_TOTAL:-N/A}ms decode=${DECODE_TOTAL:-N/A}ms hash=${HASH_TOTAL:-N/A}ms hash_verify=${HASH_VERIFY_TOTAL:-N/A}ms" >> "$COMP_FILE"
    done

    local AVG_DECODE=0
    local AVG_HASH_VERIFY=0
    if [ "$DECODE_COUNT" -gt 0 ]; then
        AVG_DECODE=$(echo "scale=3; $DECODE_SUM / $DECODE_COUNT" | bc)
        AVG_HASH_VERIFY=$(echo "scale=3; $HASH_VERIFY_SUM / $DECODE_COUNT" | bc)
    fi
    local TOTAL_CODEC
    TOTAL_CODEC=$(echo "scale=3; $SEED_ENCODE_TOTAL + $AVG_DECODE" | bc)

    echo "encode=${SEED_ENCODE_TOTAL} decode_avg=${AVG_DECODE} hash=${SEED_HASH_TOTAL} hash_verify_avg=${AVG_HASH_VERIFY} total_codec=${TOTAL_CODEC}" > "$COMP_SUMMARY"
}

calc_real_e2e_latency_ms() {
    local OUT_BASE="$1"
    local SKIP_NODES="$2"

    local START_MS=""
    if [ -f "${OUT_BASE}/seed/broadcast_start_ms.txt" ]; then
        START_MS=$(cat "${OUT_BASE}/seed/broadcast_start_ms.txt" 2>/dev/null)
    fi
    if [ -z "$START_MS" ] || [ "$START_MS" = "0" ]; then
        echo "-1"
        return
    fi

    local MAX_END_MS=0
    local FOUND_ANY=false
    for node in $ALL_NODES; do
        local should_skip=false
        if [ -n "$SKIP_NODES" ]; then
            for byz_entry in $SKIP_NODES; do
                local byz_node="${byz_entry%%:*}"
                if [ "$byz_node" = "$node" ]; then
                    should_skip=true
                    break
                fi
            done
        fi
        if [ "$should_skip" = true ]; then
            continue
        fi

        for ts_file in "${OUT_BASE}/${node}"/output_*_end_ms.txt; do
            if [ -f "$ts_file" ]; then
                local END_MS
                END_MS=$(cat "$ts_file" 2>/dev/null)
                if [ -n "$END_MS" ] && [ "$END_MS" != "0" ]; then
                    FOUND_ANY=true
                    if [ "$END_MS" -gt "$MAX_END_MS" ] 2>/dev/null; then
                        MAX_END_MS=$END_MS
                    fi
                fi
            fi
        done
    done

    if [ "$FOUND_ANY" = false ]; then
        echo "-1"
        return
    fi

    local LATENCY_MS
    LATENCY_MS=$(echo "$MAX_END_MS - $START_MS" | bc 2>/dev/null || echo "-1")
    echo "$LATENCY_MS"
}

# ==========================================
# docker-compose 生成与管理
# ==========================================

generate_compose_file() {
    cat > "$COMPOSE_FILE" << YAML
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
}

generate_compose_override() {
    local TEST_FILE_NAME="$1"
    local BYZANTINE_NODES="$2"
    local OVERRIDE_FILE="${SCRIPT_DIR}/docker-compose.test.yml"

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

    for i in $(seq 1 $((NODE_COUNT - 1))); do
        local NODE_NAME="node${i}"
        local BYZ_MODE=""
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

dc() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

dc_with_override() {
    docker compose -f "$COMPOSE_FILE" -f "${SCRIPT_DIR}/docker-compose.test.yml" "$@"
}

cleanup() {
    info "清理测试环境..."
    if [ -f "${SCRIPT_DIR}/docker-compose.test.yml" ]; then
        dc_with_override down -v --remove-orphans 2>/dev/null || true
    else
        dc down -v --remove-orphans 2>/dev/null || true
    fi
}

trap 'echo ""; warn "测试中断，正在清理..."; cleanup; exit 1' INT TERM TSTP

# ==========================================
# 准备测试数据
# ==========================================
TEST_DIR="${SCRIPT_DIR}/test_data_tmp"
OUTPUT_BASE="${SCRIPT_DIR}/rbc_output_tmp"
mkdir -p "$TEST_DIR"
mkdir -p "$OUTPUT_BASE"

# 只生成50MB测试文件
if [ ! -f "$TEST_DIR/huge_50mb.bin" ]; then
    info "生成50MB测试文件..."
    dd if=/dev/urandom of="$TEST_DIR/huge_50mb.bin" bs=1024 count=51200 2>/dev/null
fi

TEST_FILE="$TEST_DIR/huge_50mb.bin"
FILE_SIZE=$(stat -c%s "$TEST_FILE" 2>/dev/null || stat -f%z "$TEST_FILE" 2>/dev/null)
EXPECTED_HASH=$(sha256sum "$TEST_FILE" | awk '{print $1}')
info "测试文件: 50MB (${FILE_SIZE}字节), SHA-256=${EXPECTED_HASH:0:16}..."

# 构造BFT极限恶意节点配置（t个混合恶意节点）
BYZ_MODES=("corrupt_shard" "wrong_hash" "silent")
BYZ_CONFIG=""
for i in $(seq $((NODE_COUNT - 1)) -1 $((NODE_COUNT - T))); do
    local_idx=$(( (NODE_COUNT - 1 - i) % 3 ))
    MODE=${BYZ_MODES[$local_idx]}
    if [ -z "$BYZ_CONFIG" ]; then
        BYZ_CONFIG="node${i}:${MODE}"
    else
        BYZ_CONFIG="$BYZ_CONFIG node${i}:${MODE}"
    fi
done

BYZ_COUNT=$(echo "$BYZ_CONFIG" | wc -w)
EXPECTED_SUCCESS=$((NODE_COUNT - BYZ_COUNT))

info "恶意节点配置(${BYZ_COUNT}个): ${BYZ_CONFIG}"
info "期望完成的诚实节点数: ${EXPECTED_SUCCESS}"
echo ""

# ==========================================
# 生成compose文件 & 构建镜像
# ==========================================
cleanup
generate_compose_file

info "构建Docker镜像..."
dc build 2>&1 | tail -5
ok "Docker镜像构建成功"
echo ""

# ==========================================
# 写入CSV表头
# ==========================================
echo "轮次,端到端延迟(ms),RS编码耗时(ms),RS解码耗时(ms),SHA256哈希耗时(ms),哈希验证耗时(ms),编解码总耗时(ms),吞吐量(MB/s),总发送TX(B),传输放大比(TX),数据完整性,备注" > "$CSV_FILE"

# ==========================================
# 多次重复测试
# ==========================================
PASS_COUNT=0
FAIL_COUNT=0

for ROUND in $(seq 1 $REPEAT_COUNT); do
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║  第 ${ROUND}/${REPEAT_COUNT} 轮测试: 50MB BFT极限 [n=${NODE_COUNT},t=${T}]  ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════╝${NC}"

    TEST_ID="round_${ROUND}"

    # 1. 清理环境
    dc_with_override down -v --remove-orphans 2>/dev/null || true
    sleep 3

    rm -rf "${OUTPUT_BASE:?}"/*
    for node in $ALL_NODES; do
        mkdir -p "${OUTPUT_BASE}/${node}"
    done

    # 2. 生成覆盖文件并启动
    generate_compose_override "huge_50mb.bin" "$BYZ_CONFIG"

    step "[轮次${ROUND}] 启动所有 ${NODE_COUNT} 个节点..."
    dc_with_override up -d
    sleep 5

    RUNNING_COUNT=$(dc_with_override ps --status running | grep -c "p2p-" || true)
    if [ "$RUNNING_COUNT" -ge "$NODE_COUNT" ]; then
        ok "[轮次${ROUND}] 所有${NODE_COUNT}个节点启动成功"
    else
        warn "[轮次${ROUND}] 部分节点启动失败（运行中: $RUNNING_COUNT/$NODE_COUNT）"
    fi

    # 3. 采集网络基线
    step "[轮次${ROUND}] 采集网络流量基线..."
    NET_BEFORE="${STATS_DIR}/${TEST_ID}_net_before.txt"
    NET_AFTER="${STATS_DIR}/${TEST_ID}_net_after.txt"
    NET_DIFF="${STATS_DIR}/${TEST_ID}_net_diff.txt"
    capture_all_net_stats "$NET_BEFORE"

    # 4. 等待RBC完成
    TOTAL_WAIT=$((RBC_BROADCAST_DELAY + RBC_COMPLETION_WAIT))
    step "[轮次${ROUND}] 等待RBC协议完成（最多 ${TOTAL_WAIT}秒）..."

    ELAPSED=0
    CHECK_INTERVAL=5
    SUCCESS_NODES=0

    while [ $ELAPSED -lt $TOTAL_WAIT ]; do
        sleep $CHECK_INTERVAL
        ELAPSED=$((ELAPSED + CHECK_INTERVAL))

        SUCCESS_NODES=0
        for node in $ALL_NODES; do
            local is_byzantine=false
            for byz_entry in $BYZ_CONFIG; do
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
            ok "[轮次${ROUND}] 所有诚实节点已完成RBC协议"
            break
        fi
    done

    if [ "$SUCCESS_NODES" -lt "$EXPECTED_SUCCESS" ]; then
        echo ""
        warn "[轮次${ROUND}] 超时：只有 $SUCCESS_NODES/$EXPECTED_SUCCESS 个诚实节点完成"
    fi

    # 5. 采集网络结束快照
    step "[轮次${ROUND}] 采集网络流量结束快照..."
    capture_all_net_stats "$NET_AFTER"
    calc_net_diff "$NET_BEFORE" "$NET_AFTER" "$NET_DIFF"

    # 6. 提取计算开销
    step "[轮次${ROUND}] 提取计算开销数据..."
    extract_computation_stats "$TEST_ID"

    # 7. 计算端到端延迟
    E2E_TIME_MS=$(calc_real_e2e_latency_ms "$OUTPUT_BASE" "$BYZ_CONFIG")
    if [ "$E2E_TIME_MS" -gt 0 ] 2>/dev/null; then
        E2E_SEC=$(echo "scale=3; $E2E_TIME_MS / 1000" | bc)
        ok "[轮次${ROUND}] 端到端延迟: ${E2E_TIME_MS}ms (${E2E_SEC}秒)"
    else
        warn "[轮次${ROUND}] 无法获取精确时间戳"
        E2E_TIME_MS="N/A"
    fi

    # 8. 验证数据完整性
    step "[轮次${ROUND}] 验证数据完整性..."
    INTEGRITY_PASS=0
    INTEGRITY_FAIL=0

    for node in $ALL_NODES; do
        local is_byzantine=false
        for byz_entry in $BYZ_CONFIG; do
            local byz_node="${byz_entry%%:*}"
            if [ "$byz_node" = "$node" ]; then
                is_byzantine=true
                break
            fi
        done
        if [ "$is_byzantine" = true ]; then
            continue
        fi

        OUTPUT_FILES=$(ls "${OUTPUT_BASE}/${node}"/output_*.bin 2>/dev/null || true)
        if [ -z "$OUTPUT_FILES" ]; then
            INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            continue
        fi

        for output_file in $OUTPUT_FILES; do
            OUTPUT_HASH=$(sha256sum "$output_file" | awk '{print $1}')
            if [ "$OUTPUT_HASH" = "$EXPECTED_HASH" ]; then
                INTEGRITY_PASS=$((INTEGRITY_PASS + 1))
            else
                INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
            fi
        done
    done

    # 判断本轮结果
    ROUND_STATUS="PASS"
    ROUND_NOTE=""
    if [ "$INTEGRITY_PASS" -ge "$EXPECTED_SUCCESS" ] && [ "$INTEGRITY_FAIL" -eq 0 ]; then
        ok "[轮次${ROUND}] 数据完整性验证通过 (${INTEGRITY_PASS}/${EXPECTED_SUCCESS})"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        fail "[轮次${ROUND}] 数据完整性验证失败 (通过:${INTEGRITY_PASS}, 失败:${INTEGRITY_FAIL})"
        ROUND_STATUS="FAIL"
        ROUND_NOTE="integrity_fail"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # 9. 读取计算开销汇总
    COMP_SUMMARY="${STATS_DIR}/${TEST_ID}_computation_summary.txt"
    CSV_ENCODE="N/A"
    CSV_DECODE="N/A"
    CSV_HASH="N/A"
    CSV_HASH_VERIFY="N/A"
    CSV_TOTAL_CODEC="N/A"
    if [ -f "$COMP_SUMMARY" ]; then
        SUMMARY_LINE=$(cat "$COMP_SUMMARY")
        CSV_ENCODE=$(echo "$SUMMARY_LINE" | grep -oP 'encode=\K[0-9.]+' || echo "N/A")
        CSV_DECODE=$(echo "$SUMMARY_LINE" | grep -oP 'decode_avg=\K[0-9.]+' || echo "N/A")
        CSV_HASH=$(echo "$SUMMARY_LINE" | grep -oP 'hash=\K[0-9.]+' || echo "N/A")
        CSV_HASH_VERIFY=$(echo "$SUMMARY_LINE" | grep -oP 'hash_verify_avg=\K[0-9.]+' || echo "N/A")
        CSV_TOTAL_CODEC=$(echo "$SUMMARY_LINE" | grep -oP 'total_codec=\K[0-9.]+' || echo "N/A")
    fi

    # 网络流量
    TOTAL_TX=0
    if [ -f "$NET_DIFF" ]; then
        TOTAL_TX=$(grep "^TOTAL " "$NET_DIFF" | awk '{print $2}')
    fi
    AMP_VAL="0"
    if [ "$FILE_SIZE" -gt 0 ] && [ "$TOTAL_TX" -gt 0 ]; then
        AMP_VAL=$(echo "scale=4; $TOTAL_TX / $FILE_SIZE" | bc)
    fi

    # 吞吐量
    THROUGHPUT="0.0000"
    if [ "$E2E_TIME_MS" != "N/A" ] && [ "$E2E_TIME_MS" -gt 0 ] 2>/dev/null && [ "$FILE_SIZE" -gt 0 ]; then
        THROUGHPUT=$(echo "scale=4; ($FILE_SIZE / 1048576) / ($E2E_TIME_MS / 1000)" | bc)
    fi

    # 10. 写入CSV
    echo "${ROUND},${E2E_TIME_MS},${CSV_ENCODE},${CSV_DECODE},${CSV_HASH},${CSV_HASH_VERIFY},${CSV_TOTAL_CODEC},${THROUGHPUT},${TOTAL_TX},${AMP_VAL},${ROUND_STATUS},${ROUND_NOTE}" >> "$CSV_FILE"

    # 打印本轮摘要
    echo ""
    echo -e "${BLUE}  ┌─ 第${ROUND}轮摘要 ─────────────────────────────────┐${NC}"
    echo -e "${BLUE}  │${NC} 端到端延迟:   ${E2E_TIME_MS}ms"
    echo -e "${BLUE}  │${NC} RS编码耗时:   ${CSV_ENCODE}ms"
    echo -e "${BLUE}  │${NC} RS解码耗时:   ${CSV_DECODE}ms"
    echo -e "${BLUE}  │${NC} 编解码总耗时: ${CSV_TOTAL_CODEC}ms"
    echo -e "${BLUE}  │${NC} 吞吐量:       ${THROUGHPUT} MB/s"
    echo -e "${BLUE}  │${NC} 传输放大比:   ${AMP_VAL}x"
    echo -e "${BLUE}  │${NC} 数据完整性:   ${ROUND_STATUS}"
    echo -e "${BLUE}  └──────────────────────────────────────────────┘${NC}"

    # 轮次间休息（让Docker环境冷却，减少热效应）
    if [ "$ROUND" -lt "$REPEAT_COUNT" ]; then
        info "等待10秒后开始下一轮测试（冷却期）..."
        sleep 10
    fi
done

# ==========================================
# 汇总统计
# ==========================================
echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║              ${REPEAT_COUNT}轮测试完成 - 汇总统计                         ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "  通过: ${PASS_COUNT}/${REPEAT_COUNT}, 失败: ${FAIL_COUNT}/${REPEAT_COUNT}"
echo ""

# 用awk计算各指标的平均值、最小值、最大值、标准差
if [ "$PASS_COUNT" -gt 0 ]; then
    echo -e "${BLUE}  各指标统计（仅统计PASS轮次）:${NC}"
    echo ""

    # 从CSV中提取数据并计算统计值
    # 跳过表头行，只统计PASS的轮次
    awk -F',' '
    NR > 1 && $11 == "PASS" {
        n++
        # 端到端延迟
        if ($2 != "N/A" && $2+0 > 0) { e2e[n] = $2+0; e2e_sum += $2; e2e_count++ }
        # RS编码
        if ($3 != "N/A" && $3+0 > 0) { enc[n] = $3+0; enc_sum += $3; enc_count++ }
        # RS解码
        if ($4 != "N/A" && $4+0 > 0) { dec[n] = $4+0; dec_sum += $4; dec_count++ }
        # 编解码总耗时
        if ($7 != "N/A" && $7+0 > 0) { codec[n] = $7+0; codec_sum += $7; codec_count++ }
        # 吞吐量
        if ($8 != "N/A" && $8+0 > 0) { tp[n] = $8+0; tp_sum += $8; tp_count++ }
    }
    END {
        if (e2e_count > 0) {
            e2e_avg = e2e_sum / e2e_count
            e2e_min = 999999999; e2e_max = 0
            for (i in e2e) {
                if (e2e[i] < e2e_min) e2e_min = e2e[i]
                if (e2e[i] > e2e_max) e2e_max = e2e[i]
                e2e_sq += (e2e[i] - e2e_avg)^2
            }
            e2e_std = (e2e_count > 1) ? sqrt(e2e_sq / (e2e_count - 1)) : 0
            printf "  %-18s  平均: %10.1fms  最小: %10.1fms  最大: %10.1fms  标准差: %10.1fms\n", "端到端延迟", e2e_avg, e2e_min, e2e_max, e2e_std
        }
        if (enc_count > 0) {
            enc_avg = enc_sum / enc_count
            enc_min = 999999999; enc_max = 0
            for (i in enc) {
                if (enc[i] < enc_min) enc_min = enc[i]
                if (enc[i] > enc_max) enc_max = enc[i]
                enc_sq += (enc[i] - enc_avg)^2
            }
            enc_std = (enc_count > 1) ? sqrt(enc_sq / (enc_count - 1)) : 0
            printf "  %-18s  平均: %10.3fms  最小: %10.3fms  最大: %10.3fms  标准差: %10.3fms\n", "RS编码耗时", enc_avg, enc_min, enc_max, enc_std
        }
        if (dec_count > 0) {
            dec_avg = dec_sum / dec_count
            dec_min = 999999999; dec_max = 0
            for (i in dec) {
                if (dec[i] < dec_min) dec_min = dec[i]
                if (dec[i] > dec_max) dec_max = dec[i]
                dec_sq += (dec[i] - dec_avg)^2
            }
            dec_std = (dec_count > 1) ? sqrt(dec_sq / (dec_count - 1)) : 0
            printf "  %-18s  平均: %10.3fms  最小: %10.3fms  最大: %10.3fms  标准差: %10.3fms\n", "RS解码耗时", dec_avg, dec_min, dec_max, dec_std
        }
        if (codec_count > 0) {
            codec_avg = codec_sum / codec_count
            codec_min = 999999999; codec_max = 0
            for (i in codec) {
                if (codec[i] < codec_min) codec_min = codec[i]
                if (codec[i] > codec_max) codec_max = codec[i]
                codec_sq += (codec[i] - codec_avg)^2
            }
            codec_std = (codec_count > 1) ? sqrt(codec_sq / (codec_count - 1)) : 0
            printf "  %-18s  平均: %10.3fms  最小: %10.3fms  最大: %10.3fms  标准差: %10.3fms\n", "编解码总耗时", codec_avg, codec_min, codec_max, codec_std
        }
        if (tp_count > 0) {
            tp_avg = tp_sum / tp_count
            tp_min = 999999999; tp_max = 0
            for (i in tp) {
                if (tp[i] < tp_min) tp_min = tp[i]
                if (tp[i] > tp_max) tp_max = tp[i]
                tp_sq += (tp[i] - tp_avg)^2
            }
            tp_std = (tp_count > 1) ? sqrt(tp_sq / (tp_count - 1)) : 0
            printf "  %-18s  平均: %10.4fMB/s 最小: %10.4fMB/s 最大: %10.4fMB/s 标准差: %10.4fMB/s\n", "吞吐量", tp_avg, tp_min, tp_max, tp_std
        }
    }
    ' "$CSV_FILE"

    echo ""
fi

echo -e "${CYAN}  CSV结果文件: ${CSV_FILE}${NC}"
echo ""

# 清理容器
cleanup
info "测试完成，容器已清理"
info "测试数据保留在: ${TEST_DIR}"
info "统计结果保留在: ${STATS_DIR}/"

echo ""
echo "=========================================="
echo "  50MB BFT极限重复测试完成"
echo "  n=${NODE_COUNT}, t=${T}, 重复${REPEAT_COUNT}次"
echo "  通过: ${PASS_COUNT}, 失败: ${FAIL_COUNT}"
echo "=========================================="

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
fi
exit 0
