# syntax=docker/dockerfile:1.4
# Hardened Delta Lake Storage Initializer & Maintenance Container for FerrousCX
FROM python:3.12-alpine

LABEL org.opencontainers.image.title="ferrous-lakehouse" \
      org.opencontainers.image.description="Hardened Delta Lake storage initializer, schema catalog, and maintenance service for FerrousCX" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="FerrousCX" \
      org.opencontainers.image.licenses="Apache-2.0"

# Create unprivileged non-root user and group (UID/GID 10001) adhering to AD-23
RUN addgroup -g 10001 appgroup && \
    adduser -u 10001 -G appgroup -s /sbin/nologin -D appuser && \
    mkdir -p /app /data/delta && \
    chown -R 10001:10001 /app /data/delta

WORKDIR /app

# Copy application artifacts
COPY --chown=10001:10001 config/ ./config/
COPY --chown=10001:10001 schemas/ ./schemas/
COPY --chown=10001:10001 scripts/ ./scripts/
COPY --chown=10001:10001 entrypoint.py ./entrypoint.py

# Switch to rootless runtime user
USER 10001:10001

# Volume mount for persistent Delta Lake tables
VOLUME ["/data/delta"]

# Healthcheck probe verifying filesystem access and schema readiness
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["python3", "/app/entrypoint.py", "--healthcheck"]

ENTRYPOINT ["python3", "/app/entrypoint.py"]
CMD ["--daemon"]
