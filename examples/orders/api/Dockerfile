FROM rust:1.90-slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p api

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/api /usr/local/bin/api
HEALTHCHECK CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
CMD ["/usr/local/bin/api"]
