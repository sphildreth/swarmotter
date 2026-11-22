# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.96.1

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /src

ARG SWARMOTTER_BUILD_COMMIT=unknown
ENV CARGO_TERM_COLOR=always
ENV SWARMOTTER_BUILD_COMMIT=${SWARMOTTER_BUILD_COMMIT}

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --locked --release --bin swarmotterd --bin swarmotterctl \
    && cp /src/target/release/swarmotterd /tmp/swarmotterd \
    && cp /src/target/release/swarmotterctl /tmp/swarmotterctl

FROM scratch AS binary
COPY --from=builder /tmp/swarmotterd /swarmotterd
COPY --from=builder /tmp/swarmotterctl /swarmotterctl

FROM debian:bookworm-slim AS runtime-base

ARG SWARMOTTER_BUILD_COMMIT=unknown
ARG SWARMOTTER_IMAGE_CREATED=unknown
ARG SWARMOTTER_VERSION=3.1.1

LABEL org.opencontainers.image.title="SwarmOtter" \
      org.opencontainers.image.description="SwarmOtter BitTorrent daemon and web UI" \
      org.opencontainers.image.version="${SWARMOTTER_VERSION}" \
      org.opencontainers.image.created="${SWARMOTTER_IMAGE_CREATED}" \
      org.opencontainers.image.revision="${SWARMOTTER_BUILD_COMMIT}" \
      org.opencontainers.image.source="https://github.com/sphildreth/swarmotter" \
      org.opencontainers.image.url="https://github.com/sphildreth/swarmotter" \
      org.opencontainers.image.licenses="Apache-2.0" \
      io.swarmotter.config-check="true"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 swarmotter \
    && useradd --system --uid 10001 --gid swarmotter --home-dir /var/lib/swarmotter --shell /usr/sbin/nologin swarmotter \
    && install -d -m 0700 -o swarmotter -g swarmotter /etc/swarmotter \
    && install -d -o swarmotter -g swarmotter /var/lib/swarmotter /data/downloads /data/incomplete

COPY --chown=swarmotter:swarmotter --chmod=0600 config/swarmotter.container.toml.example /etc/swarmotter/swarmotter.toml

COPY --chmod=0755 deploy/container-health.sh /usr/local/bin/swarmotter-health

VOLUME ["/etc/swarmotter", "/data/downloads", "/data/incomplete", "/var/lib/swarmotter"]
EXPOSE 9091
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/usr/local/bin/swarmotter-health"]
USER swarmotter:swarmotter
ENTRYPOINT ["/bin/sh", "-c", "umask 077; exec /usr/local/bin/swarmotterd \"$@\"", "swarmotter-entrypoint"]
CMD ["--config", "/etc/swarmotter/swarmotter.toml"]

FROM runtime-base AS runtime-prebuilt
ARG TARGETARCH
COPY --chown=root:root image-binaries/linux-${TARGETARCH}/swarmotterd /usr/local/bin/swarmotterd
COPY --chown=root:root image-binaries/linux-${TARGETARCH}/swarmotterctl /usr/local/bin/swarmotterctl

FROM runtime-base AS runtime
COPY --from=builder --chown=root:root /tmp/swarmotterd /usr/local/bin/swarmotterd
COPY --from=builder --chown=root:root /tmp/swarmotterctl /usr/local/bin/swarmotterctl
