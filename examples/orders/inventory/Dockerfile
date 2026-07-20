FROM rust:1.90-slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p inventory

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/inventory /usr/local/bin/inventory
HEALTHCHECK CMD curl -fsS http://127.0.0.1:8081/healthz || exit 1
CMD ["/usr/local/bin/inventory"]
