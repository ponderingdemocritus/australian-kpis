# syntax=docker/dockerfile:1.7

FROM rust:1.85-bookworm AS chef
WORKDIR /app
RUN cargo install cargo-chef --locked --version 0.1.72

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV RUSTC_WRAPPER=""
COPY rust-toolchain.toml rust-toolchain.toml
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --bin au-kpis-scheduler --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin au-kpis-scheduler \
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
