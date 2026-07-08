# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim AS builder
WORKDIR /app
ENV CI=true \
    NEXT_TELEMETRY_DISABLED=1

RUN corepack enable \
    && corepack prepare pnpm@9.12.0 --activate

COPY .npmrc pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm fetch --ignore-scripts

COPY . .
RUN pnpm install --offline --frozen-lockfile --filter @au-kpis/web... \
    && pnpm --filter @au-kpis/web... build

FROM node:22-bookworm-slim AS runtime
WORKDIR /app
ENV HOSTNAME=0.0.0.0 \
    NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=3000

RUN useradd --uid 10001 --user-group --create-home --home-dir /app --shell /usr/sbin/nologin au-kpis \
    && rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

COPY --from=builder --chown=au-kpis:au-kpis /app/apps/web/.next/standalone ./
COPY --from=builder --chown=au-kpis:au-kpis /app/apps/web/.next/static ./apps/web/.next/static
COPY --from=builder --chown=au-kpis:au-kpis /app/apps/web/public ./apps/web/public

EXPOSE 3000
USER au-kpis:au-kpis
CMD ["node", "apps/web/server.js"]
