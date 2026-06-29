# syntax=docker/dockerfile:1.7

FROM rust:1.85-bookworm AS builder
WORKDIR /app
ENV RUSTC_WRAPPER=""
COPY . .
RUN cargo build --release --locked --bin au-kpis-ingestion \
    && cp target/release/au-kpis-ingestion /tmp/au-kpis-ingestion

FROM debian:bookworm-slim AS local
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --create-home --home-dir /app --shell /usr/sbin/nologin au-kpis
WORKDIR /app
COPY --from=builder /tmp/au-kpis-ingestion /usr/local/bin/au-kpis-ingestion
EXPOSE 3000
USER au-kpis:au-kpis
ENTRYPOINT ["/usr/local/bin/au-kpis-ingestion"]
CMD ["run"]

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=builder /tmp/au-kpis-ingestion /usr/local/bin/au-kpis-ingestion
EXPOSE 3000
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/au-kpis-ingestion"]
CMD ["run"]
