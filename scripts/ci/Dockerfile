ARG PLAYWRIGHT_VERSION=1.61.1
FROM ghcr.io/astral-sh/uv:latest AS uv
FROM mcr.microsoft.com/playwright:v${PLAYWRIGHT_VERSION}-noble

ARG RUST_VERSION=1.96.0
ARG TOXIPROXY_VERSION=2.12.0
ARG TARGETARCH

COPY --from=uv /uv /uvx /usr/local/bin/

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential ca-certificates cmake curl docker.io git gnupg jq lcov libssl-dev lsb-release pkg-config ripgrep shellcheck software-properties-common \
    && curl --fail --location --output /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && chmod +x /tmp/llvm.sh \
    && /tmp/llvm.sh 22 \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh

ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV UV_TOOL_BIN_DIR=/usr/local/bin
ENV PATH="/usr/local/cargo/bin:${PATH}"
RUN curl --proto '=https' --tlsv1.2 --silent --show-error --fail https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" \
    && rustup component add clippy llvm-tools-preview rustfmt \
    && rustup toolchain install nightly --profile minimal --component miri --component rust-src \
    && cargo install \
        cargo-auditable cargo-cyclonedx cargo-deny cargo-dist cargo-fuzz cargo-hack cargo-insta \
        cargo-leptos cargo-llvm-cov cargo-mutants cargo-nextest cargo-semver-checks just zola \
    && uv tool install prek \
    && curl --fail --location --output /usr/local/bin/toxiproxy-server \
        "https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}/toxiproxy-server-linux-${TARGETARCH}" \
    && chmod +x /usr/local/bin/toxiproxy-server

RUN mkdir -p /cargo /target /tmp/peryx-home \
    && chmod 1777 /cargo /target /tmp/peryx-home

ENV HOME=/tmp/peryx-home

WORKDIR /workspace
