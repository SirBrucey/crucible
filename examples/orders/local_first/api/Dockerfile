# syntax=docker/dockerfile:1
FROM rust:1.97-slim-bookworm AS builder
WORKDIR /build
COPY . .
# Its own workspace, since it reaches out of the examples directory for the
# span adapter.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/examples/orders/3_local_first/target,sharing=locked \
    cargo build --release --manifest-path examples/orders/3_local_first/Cargo.toml -p orders-local-api \
    && cp examples/orders/3_local_first/target/release/orders-local-api /api

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /api /usr/local/bin/api
# Where its two stores live, which is the container's own disk.
RUN mkdir -p /data
HEALTHCHECK CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
CMD ["/usr/local/bin/api"]
