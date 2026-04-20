# ==================== 构建阶段 ====================
FROM rust:1.85-bookworm AS builder

WORKDIR /app

# 先复制依赖文件，利用Docker缓存
COPY Cargo.toml Cargo.lock ./

# 创建空的源文件用于预编译依赖（需要同时创建main.rs和lib.rs）
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    echo '' > src/lib.rs
RUN cargo build --release 2>/dev/null || true

# 复制实际源码
COPY src/ src/

# 清除之前的空编译产物，重新编译
RUN touch src/main.rs src/lib.rs && cargo build --release

# ==================== 运行阶段 ====================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 从构建阶段复制二进制
COPY --from=builder /app/target/release/bishe1 /app/bishe1

# 暴露 P2P 端口
EXPOSE 8000

# 设置环境变量
ENV RUST_LOG=info

CMD ["/app/bishe1"]