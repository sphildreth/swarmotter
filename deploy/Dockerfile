# Example Dockerfile for SwarmOtter.
#
# Build: docker build -t swarmotter .
# Run:   see docker-compose example in design/deployment.md
#
# For strict network containment, run the daemon inside a VPN network
# namespace or a container attached to a VPN network so torrent traffic
# cannot use the default route. The control plane (API/Web UI) should be
# exposed separately on the LAN.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin swarmotterd

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/swarmotterd /usr/local/bin/swarmotterd
COPY config/swarmotter.toml.example /etc/swarmotter/swarmotter.toml
VOLUME ["/data/downloads", "/data/incomplete", "/var/lib/swarmotter"]
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/swarmotterd"]
CMD ["--config", "/etc/swarmotter/swarmotter.toml"]