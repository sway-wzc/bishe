# P2P网络通信模块

## 模块结构

```
network/
├── mod.rs          # 模块声明
├── config.rs       # 节点配置
├── message.rs      # 消息协议（帧编解码、消息类型）
├── peer.rs         # 对等节点管理（状态、节点表）
├── connection.rs   # 连接管理（TCP读写协程）
├── discovery.rs    # 节点发现服务
└── node.rs         # P2P核心节点（事件循环、消息路由）
```

## 本地测试

```bash
# 启动种子节点
RUST_LOG=info cargo run -- 8000

# 启动普通节点（连接到种子节点）
RUST_LOG=info cargo run -- 8001 127.0.0.1:8000
RUST_LOG=info cargo run -- 8002 127.0.0.1:8000
```

## Docker 测试

### 快速启动

```bash
# 构建并启动5节点P2P网络
docker compose up -d --build

# 查看所有节点状态
docker compose ps

# 查看某个节点日志
docker compose logs -f seed
docker compose logs -f node1

# 关闭所有节点
docker compose down
```

### 自动化测试

```bash
# 运行完整测试脚本（包含握手、心跳、容错、节点发现）
chmod +x scripts/test_p2p.sh
bash scripts/test_p2p.sh
```

### 手动容错测试

```bash
# 1. 启动全部节点
docker compose up -d --build

# 2. 模拟节点宕机
docker compose stop node3

# 3. 观察其他节点检测到断开
docker compose logs -f seed

# 4. 恢复节点
docker compose start node3

# 5. 动态扩容：添加新节点
docker run -d --name p2p-node5 \
  --network bishe1_p2p-net \
  -e RUST_LOG=info \
  bishe1-seed \
  8000 172.20.0.10:8000
```

### 网络分区测试

```bash
# 模拟网络分区：断开node1与seed之间的网络
docker network disconnect bishe1_p2p-net p2p-node1

# 等待心跳超时后查看日志
sleep 20
docker compose logs seed | tail -10

# 恢复网络
docker network connect bishe1_p2p-net p2p-node1
```

## 网络拓扑

```
              ┌──────────┐
              │   seed   │  172.20.0.10:8000
              │ (引导节点) │
              └────┬─────┘
          ┌────────┼────────┐
     ┌────┴───┐┌───┴────┐┌──┴─────┐
     │ node1  ││ node2  ││ node3  │
     │ .0.11  ││ .0.12  ││ .0.13  │
     └────────┘└────────┘└───┬────┘
                          ┌──┴─────┐
                          │ node4  │
                          │ .0.14  │
                          └────────┘
```