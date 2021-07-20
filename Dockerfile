
# ------------------------------------------------------------------------------
# Cargo Dependency Build Stage
# ------------------------------------------------------------------------------

FROM rustlang/rust:nightly-buster AS builder

WORKDIR /app

COPY Cargo.lock Cargo.lock
COPY Cargo.toml Cargo.toml

RUN mkdir -p src/bin
RUN touch src/lib.rs
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/bin/main.rs

RUN cargo build --release

RUN cargo clean -p polkadot-account-monitoring
RUN rm -rf target/release/.fingerprint/polkadot-account-monitoring-*

COPY . .

RUN cargo build --release

# # ------------------------------------------------------------------------------
# # Final Stage
# # ------------------------------------------------------------------------------

FROM debian:buster-slim

RUN apt-get update && apt-get install -y libssl-dev ca-certificates
RUN update-ca-certificates --fresh

WORKDIR /app

RUN mkdir config

COPY --from=builder /app/target/release/monitor .
#COPY config/config.yml config/
#COPY config/accounts.yml config/

CMD ["./monitor"]
