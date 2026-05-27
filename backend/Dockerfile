# syntax=docker/dockerfile:1
#
# Builds the server binary and bakes both data artifacts into the image:
#   - build_data/graph.csr        (built by offline/build_graph_csr.py)
#   - build_data/search_docs.jsonl (built by offline/build_search_docs.py)
# Copy those two files into backend/build_data/ before `wrangler deploy`.

FROM rust:1-trixie AS build
WORKDIR /app

# Compile both binaries. (Cargo.lock is generated on first build if absent.)
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release --bins

# Bake the Tantivy index, built by the same tantivy version as the server.
COPY build_data/search_docs.jsonl /tmp/search_docs.jsonl
RUN /app/target/release/build_index /tmp/search_docs.jsonl /opt/data/index \
    && rm /tmp/search_docs.jsonl

FROM debian:trixie-slim
WORKDIR /app
COPY --from=build /app/target/release/server /usr/local/bin/server
COPY --from=build /opt/data/index /opt/data/index
COPY build_data/graph.csr /opt/data/graph.csr

ENV DATA_DIR=/opt/data
ENV PORT=8080
EXPOSE 8080
CMD ["server"]
