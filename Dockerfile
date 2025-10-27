# =========================
# 1. Build stage
# =========================
FROM rust:1.90-alpine AS builder
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf target/release/deps/*
COPY . .
RUN cargo build --release

# =========================
# 2. Runtime stage
# =========================
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/target/release/redis-proxy /app/redis-proxy

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/app/redis-proxy"]
