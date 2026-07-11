FROM otel/opentelemetry-collector-contrib:0.114.0
COPY infra/observability/otel-collector-production.yml /etc/otelcol-contrib/config.yml
ENTRYPOINT ["/otelcol-contrib"]
CMD ["--config=/etc/otelcol-contrib/config.yml"]
