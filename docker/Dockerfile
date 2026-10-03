# syntax=docker/dockerfile:1.4

FROM romeprotocol/agave-validator:v4.0.0-rc.1 AS solana

FROM rust:1.93-slim-trixie AS base

RUN apt-get update -qq && \
    apt-get upgrade -y -qq && \
    apt-get install -y -qq --no-install-recommends \
    perl pkg-config libssl-dev \
    libpq-dev build-essential \
    curl protobuf-compiler cmake libudev-dev git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

FROM base AS builder
ARG FEATURE

COPY ./rome-evm /opt/rome-evm
COPY ./rome-sdk /opt/rome-sdk
# rome-sdk path-depends on ../mollusk (rome-sdk #475); it must sit beside it.
COPY ./mollusk /opt/mollusk
COPY ./rome-apps /opt/rome-apps

WORKDIR /opt/rome-apps/

# BuildKit cache mounts:
# - /usr/local/cargo/registry + git: avoid re-downloading + re-decoding every crate
# - /opt/rome-apps/target: avoid re-linking unchanged crates across builds
# Cache-mount contents are NOT layered into the final image, so binaries must
# be copied to /out/ (a real directory) before the mount unmounts, so the
# runtime stage can pick them up via COPY --from=builder /out/...
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/opt/rome-apps/target,sharing=locked \
    RUSTFLAGS="-D warnings" cargo build --release --features "${FEATURE}" --no-default-features && \
    ls -l /opt/rome-apps/target/release/ && \
    mkdir -p /out && \
    cp /opt/rome-apps/target/release/proxy \
       /opt/rome-apps/target/release/cli \
       /opt/rome-apps/target/release/hercules \
       /opt/rome-apps/target/release/rome-via-api \
       /opt/rome-apps/target/release/rome-via-sync \
       /opt/rome-apps/target/release/rome-via-enrich \
       /opt/rome-apps/target/release/rome-audit \
       /out/

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo install diesel_cli --no-default-features --features postgres

FROM base AS runtime

COPY --from=solana /opt/agave/bin/solana /usr/bin/solana
COPY --from=builder /usr/local/cargo/bin/diesel /opt
COPY --from=builder /out/proxy /opt/
COPY --from=builder /out/cli /opt/
COPY --from=builder /out/hercules /opt/
COPY --from=builder /out/rome-via-api /opt/
COPY --from=builder /out/rome-via-sync /opt/
COPY --from=builder /out/rome-via-enrich /opt/
COPY --from=builder /out/rome-audit /opt/
COPY ./rome-apps/docker/entrypoint.sh /opt/
COPY ./rome-apps/docker/cli.sh /opt/
COPY ./rome-apps/docker/cli-deploy.sh /opt/
COPY ./rome-apps/docker/apply_migrations /opt/
COPY ./rome-sdk/rome-evm-client/src/indexer/pg_storage/migrations /opt/migrations

RUN groupadd --system rome && \
    useradd --system --gid rome --create-home --shell /bin/bash rome && \
    chown -R rome:rome /opt

USER rome:rome

WORKDIR /opt

ENTRYPOINT ["./entrypoint.sh"]
