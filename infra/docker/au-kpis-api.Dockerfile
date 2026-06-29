# syntax=docker/dockerfile:1.7

FROM rust:1.85-bookworm AS builder
WORKDIR /app
ENV RUSTC_WRAPPER=""
COPY . .
RUN --mount=type=cache,id=s/f8e332a7-8bf5-465e-9da7-ab4bb4513916-/usr/local/cargo/registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s/f8e332a7-8bf5-465e-9da7-ab4bb4513916-/usr/local/cargo/git,target=/usr/local/cargo/git \
    --mount=type=cache,id=s/f8e332a7-8bf5-465e-9da7-ab4bb4513916-/app/target,target=/app/target \
    cargo build --release --locked --bin au-kpis-api \
    && cp target/release/au-kpis-api /tmp/au-kpis-api

FROM debian:bookworm-slim AS local
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --create-home --home-dir /app --shell /usr/sbin/nologin au-kpis
WORKDIR /app
COPY --from=builder /tmp/au-kpis-api /usr/local/bin/au-kpis-api
EXPOSE 3000
USER au-kpis:au-kpis
ENTRYPOINT ["/usr/local/bin/au-kpis-api"]

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=builder /tmp/au-kpis-api /usr/local/bin/au-kpis-api
EXPOSE 3000
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/au-kpis-api"]
