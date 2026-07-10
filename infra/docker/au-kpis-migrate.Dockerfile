# syntax=docker/dockerfile:1.7

FROM lukemathwalker/cargo-chef:0.1.71-rust-1.85.0-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV RUSTC_WRAPPER=""
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --bin au-kpis-migrate --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin au-kpis-migrate \
    && cp target/release/au-kpis-migrate /tmp/au-kpis-migrate

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /tmp/au-kpis-migrate /usr/local/bin/au-kpis-migrate
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/au-kpis-migrate"]
