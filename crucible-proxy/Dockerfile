# syntax=docker/dockerfile:1
FROM rust:1.97-slim-bookworm AS builder
WORKDIR /build
COPY . .
# The registry and the build directory outlive the layer, so a source change
# recompiles what changed rather than the whole dependency graph. Nothing in a
# cache mount reaches the image, so the binary is copied out where the next
# stage can take it.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release -p crucible-proxy \
    && cp target/release/crucible-proxy /crucible-proxy

FROM debian:bookworm-slim
COPY --from=builder /crucible-proxy /usr/local/bin/crucible-proxy
ENTRYPOINT ["/usr/local/bin/crucible-proxy"]
