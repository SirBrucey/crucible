FROM rust:1.90-slim-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p api

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/api /usr/local/bin/api
CMD ["/usr/local/bin/api"]
