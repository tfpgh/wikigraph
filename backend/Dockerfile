# syntax=docker/dockerfile:1
FROM rust:1-trixie AS build

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bins && \
    cp target/release/build_index target/release/server /usr/local/bin/

COPY build_data/search_docs.jsonl /tmp/search_docs.jsonl
RUN /usr/local/bin/build_index /tmp/search_docs.jsonl /opt/data/index \
    && rm /tmp/search_docs.jsonl

FROM debian:trixie-slim

WORKDIR /app

COPY build_data/graph.csr /opt/data/graph.csr

COPY --from=build /opt/data/index /opt/data/index
COPY --from=build /app/target/release/server /usr/local/bin/server

ENV DATA_DIR=/opt/data
ENV PORT=8080
EXPOSE 8080
CMD ["server"]
