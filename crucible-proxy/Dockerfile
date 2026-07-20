FROM rust:1.97-slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p crucible-proxy

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/crucible-proxy /usr/local/bin/crucible-proxy
ENTRYPOINT ["/usr/local/bin/crucible-proxy"]
