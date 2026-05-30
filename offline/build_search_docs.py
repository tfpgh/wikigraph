"""Export the documents the Tantivy title index is built from.

Emits `output/search_docs.jsonl`, one JSON object per article, consumed by the
Rust `build_index` binary at container-build time. Kept as line-delimited JSON
(not parquet) so the Rust side needs only serde_json — no arrow/parquet deps.

Per-line shape (keys match what the backend returns to the frontend, plus
`imp` for ranking):
    {"id": 123, "t": "Germany", "x": 12.3, "y": -4.5, "r": 40.0, "cl": 7, "imp": 1.2e-4}

`imp` is the raw PageRank score (higher = more important); the backend orders
matches by it so the most important matching article surfaces first. x/y/radius
are Float32 in the parquet (cuGraph), so cast to Float64 before rounding or the
JSON floats serialize long.

`a` is the list of redirect titles pointing at the node (e.g. ["USA", "America"]
for United States); the backend indexes them as extra searchable values of the
title field so a query for an alias resolves to the node, then drops aliases
that tokenize identically to the canonical title. An empty list when the node
has no redirects.
"""

from pathlib import Path

import polars as pl
from loguru import logger

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
REDIRECTS_INPUT_PATH = Path("intermediates/extracted_redirects.parquet")
SEARCH_DOCS_OUTPUT_PATH = Path("output/search_docs.jsonl")

COORD_DECIMALS = 2

if __name__ == "__main__":
    logger.info("Exporting search documents")

    nodes = pl.read_parquet(
        NODES_INPUT_PATH,
        columns=["id", "title", "x", "y", "radius", "partition", "pagerank"],
    )
    logger.info(f"Loaded {len(nodes):,} nodes")

    docs = nodes.select(
        pl.col("id").cast(pl.UInt32),
        pl.col("title").alias("t"),
        pl.col("x").cast(pl.Float64).round(COORD_DECIMALS),
        pl.col("y").cast(pl.Float64).round(COORD_DECIMALS),
        pl.col("radius").cast(pl.Float64).round(COORD_DECIMALS).alias("r"),
        pl.col("partition").cast(pl.UInt32).alias("cl"),
        pl.col("pagerank").cast(pl.Float64).alias("imp"),
    )

    # Attach redirect aliases as a per-node list under `a`. Group to one row per
    # id so the left join can't fan out, and fill nodes with no redirects with an
    # empty list (a JSON null would fail to deserialize into the backend's Vec).
    aliases = (
        pl.read_parquet(REDIRECTS_INPUT_PATH, columns=["alias", "id"])
        .group_by("id")
        .agg(pl.col("alias").alias("a"))
    )
    logger.info(f"Loaded redirect aliases for {len(aliases):,} nodes")

    docs = docs.join(aliases, on="id", how="left").with_columns(
        pl.col("a").fill_null(pl.lit([], dtype=pl.List(pl.String)))
    )

    SEARCH_DOCS_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    docs.write_ndjson(SEARCH_DOCS_OUTPUT_PATH)

    size_mb = SEARCH_DOCS_OUTPUT_PATH.stat().st_size / 1e6
    logger.success(
        f"Wrote {SEARCH_DOCS_OUTPUT_PATH} ({size_mb:.1f} MB, {len(docs):,} docs)"
    )
