# syntax=docker/dockerfile:1.7

FROM rust:1.85-bookworm AS builder
WORKDIR /app
ENV RUSTC_WRAPPER=""
COPY . .
RUN --mount=type=cache,id=s/83deebd7-84dc-4342-a512-0140a2e74aa2-/usr/local/cargo/registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=s/83deebd7-84dc-4342-a512-0140a2e74aa2-/usr/local/cargo/git,target=/usr/local/cargo/git \
    --mount=type=cache,id=s/83deebd7-84dc-4342-a512-0140a2e74aa2-/app/target,target=/app/target \
    cargo build --release --locked --bin au-kpis-scheduler \
    && cp target/release/au-kpis-scheduler /tmp/au-kpis-scheduler

FROM debian:bookworm-slim AS local
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --create-home --home-dir /app --shell /usr/sbin/nologin au-kpis
WORKDIR /app
COPY --from=builder /tmp/au-kpis-scheduler /usr/local/bin/au-kpis-scheduler
EXPOSE 3000
USER au-kpis:au-kpis
ENTRYPOINT ["/usr/local/bin/au-kpis-scheduler"]
CMD ["run"]

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
WORKDIR /app
COPY --from=builder /tmp/au-kpis-scheduler /usr/local/bin/au-kpis-scheduler
EXPOSE 3000
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/au-kpis-scheduler"]
CMD ["run"]
