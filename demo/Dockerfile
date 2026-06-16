# ──────────────────────────────────────────────────────────────────────
# Multi-stage build for pg-activation-engine
# ──────────────────────────────────────────────────────────────────────

FROM rust:1.91-alpine AS builder
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

# ── Runtime image ──
FROM alpine:3.19
RUN apk add --no-cache ca-certificates libgcc

COPY --from=builder /app/target/release/setu /usr/local/bin/
COPY demo/activation.yaml /app/activation.yaml

WORKDIR /app
CMD ["setu"]
