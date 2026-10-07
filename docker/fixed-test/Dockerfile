# Fixed-resource validation images (both Alpine; see
# docs/reports/containerized-validation.md).
#
#   --target toolchain  Rust + Node on Alpine/musl. Runs the correctness matrix
#                       for the previous and current trees and builds each
#                       tree's native addon + SDK (verify phase).
#   --target bench      Minimal Alpine + Node runtime (no compiler). Runs the
#                       end-to-end A/B benchmark against the addons the verify
#                       phase built (bench phase).
#
# Resource limits are NOT baked in: scripts/docker-fixed-test.sh applies the
# same 2 CPU / 2 GiB / no-swap cgroup to every phase at `docker run` time.
ARG RUST_IMAGE=rust:1.97.1-alpine3.24
ARG RUNTIME_IMAGE=alpine:3.24
ARG BUILD_REV=unknown

FROM ${RUST_IMAGE} AS toolchain

RUN apk add --no-cache \
      bash \
      build-base \
      git \
      nodejs \
      npm \
  && rustup component add rustfmt clippy

# rust-toolchain.toml says `channel = "stable"`, which would make rustup
# download whatever stable is current at run time. Pin the image's own
# toolchain instead (RUST_VERSION is set by the official rust image) so the
# previous and current trees always compile with the same compiler.
# musl targets link the C runtime statically by default, which cannot produce
# the cdylib Node loads; link musl dynamically (napi-rs' Alpine setting).
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    RUSTFLAGS="-C target-feature=-crt-static" \
    CARGO_BUILD_JOBS=2 \
    CARGO_TERM_COLOR=never \
    CI=true \
    npm_config_fund=false \
    npm_config_audit=false \
    npm_config_update_notifier=false \
    RUST_BACKTRACE=1

WORKDIR /workspace
# Include the checkout revision in the Docker cache key and record it for the
# entrypoint's source-revision check. The runner rejects stale/mistagged images.
ARG BUILD_REV
RUN printf '%s\n' "$BUILD_REV" > /image-build-revision
# Keep .git in the image: verify.sh extracts the exact previous tree with
# `git archive`, so both trees are evaluated by the same image and toolchain.
COPY . /workspace
COPY docker/fixed-test/verify.sh /usr/local/bin/beamsocket-verify
COPY docker/fixed-test/evaluate.mjs /usr/local/lib/beamsocket/evaluate.mjs
RUN chmod +x /usr/local/bin/beamsocket-verify

ENTRYPOINT ["/usr/local/bin/beamsocket-verify"]

FROM ${RUNTIME_IMAGE} AS bench

# Only what the benchmark needs at run time: no compiler, no npm, no sources.
# The musl addons, SDK builds and benchmark node_modules come from the verify
# phase's output directory, mounted at /results.
RUN apk add --no-cache bash nodejs

COPY docker/fixed-test/bench.sh /usr/local/bin/beamsocket-bench
COPY docker/fixed-test/evaluate.mjs /usr/local/lib/beamsocket/evaluate.mjs
RUN chmod +x /usr/local/bin/beamsocket-bench

ENTRYPOINT ["/usr/local/bin/beamsocket-bench"]
