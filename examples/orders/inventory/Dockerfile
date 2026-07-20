FROM rust:1.90-slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p inventory

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/inventory /usr/local/bin/inventory
CMD ["/usr/local/bin/inventory"]
