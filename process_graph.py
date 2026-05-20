from pathlib import Path

import cudf
import cugraph
import polars as pl
from loguru import logger

NODES_INPUT_PATH = Path("intermediates/extracted_nodes.parquet")
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

PAGERANK_PATH = Path("intermediates/pagerank.parquet")
CLUSTERS_PATH = Path("intermediates/clusters.parquet")
LAYOUT_PATH = Path("intermediates/layout.parquet")

NODES_ENRICHED_PATH = Path("enriched_nodes.parquet")

WORLD_EXTENT = 2**16

# Top N largest clusters get distinct palette colors
TOP_N_CLUSTERS = 40


def compute_pagerank() -> None:
    """Compute PageRank on the directed graph.

    Only stage using the directed graph.
    """
    if PAGERANK_PATH.exists():
        logger.info("PageRank already computed, skipping")
        return

    logger.info("Computing PageRank")
    edges_df = cudf.read_parquet(EDGES_INPUT_PATH)

    G = cugraph.Graph(directed=True)
    G.from_cudf_edgelist(edges_df, source="src", destination="dst")

    pagerank = cugraph.pagerank(
        G,
        alpha=0.85,
        tol=1e-6,
        max_iter=100,
    ).rename(columns={"vertex": "id"})

    pagerank_column = pagerank["pagerank"]
    assert pagerank_column is not None

    logger.info(
        f"PageRank stats: min={pagerank_column.min():.2e}, "
        f"max={pagerank_column.max():.2e}, "
        f"mean={pagerank_column.mean():.2e}"
    )

    pagerank.to_parquet(PAGERANK_PATH, compression="zstd")
    logger.success(f"Wrote PageRank to {PAGERANK_PATH}")


def compute_clusters() -> None:
    """Run Leiden clustering on the undirected graph."""
    if CLUSTERS_PATH.exists():
        logger.info("Clusters already computed, skipping")
        return

    logger.info("Computing Leiden clusters")
    edges_df = cudf.read_parquet(EDGES_INPUT_PATH)

    G = cugraph.Graph(directed=False)
    G.from_cudf_edgelist(edges_df, source="src", destination="dst")

    partitions, modularity = cugraph.leiden(G, resolution=1.0)
    partitions = partitions.rename(columns={"vertex": "id"})

    n_clusters = int(partitions["partition"].nunique())  # pyright: ignore[reportOptionalMemberAccess, reportArgumentType]
    logger.info(f"Found {n_clusters:,} clusters, modularity = {modularity:.4f}")

    partitions.to_parquet(CLUSTERS_PATH, compression="zstd")
    logger.success(f"Wrote clusters to {CLUSTERS_PATH}")


def compute_layout() -> None:
    """Run ForceAtlas2 on the undirected graph."""
    if LAYOUT_PATH.exists():
        logger.info("Layout already computed, skipping")
        return

    logger.info("Running ForceAtlas2")
    edges_df = cudf.read_parquet(EDGES_INPUT_PATH)

    pagerank_df = cudf.read_parquet(PAGERANK_PATH)
    vertex_radius = pagerank_df.rename(columns={"id": "vertex"}).assign(
        radius=lambda d: d["pagerank"] ** 0.3 * 100
    )[["vertex", "radius"]]

    G = cugraph.Graph(directed=False)
    G.from_cudf_edgelist(edges_df, source="src", destination="dst")

    pos = cugraph.force_atlas2(
        G,
        max_iter=750,
        scaling_ratio=2.0,
        gravity=1.0,
        strong_gravity_mode=False,
        lin_log_mode=False,
        edge_weight_influence=1.0,
        jitter_tolerance=1.0,
        barnes_hut_optimize=True,
        barnes_hut_theta=0.5,
        outbound_attraction_distribution=True,
        prevent_overlapping=False,
        verbose=True,
    )

    pos_x = pos["x"]
    pos_y = pos["y"]

    assert pos_x is not None and pos_y is not None

    cx = float(pos_x.median())
    cy = float(pos_y.median())
    max_abs = max(
        float((pos_x - cx).abs().max()),
        float((pos_y - cy).abs().max()),
    )
    scale = (WORLD_EXTENT / 2) / max_abs
    vertex_radius = vertex_radius.assign(radius=lambda d: d["radius"] / scale)

    logger.info("Running ForceAtlas2 overlap cleanup")
    pos = cugraph.force_atlas2(
        G,
        max_iter=50,
        pos_list=pos,
        scaling_ratio=2.0,
        gravity=1.0,
        strong_gravity_mode=False,
        lin_log_mode=False,
        edge_weight_influence=1.0,
        jitter_tolerance=0.05,
        barnes_hut_optimize=True,
        barnes_hut_theta=0.5,
        outbound_attraction_distribution=True,
        prevent_overlapping=True,
        vertex_radius=vertex_radius,
        overlap_scaling_ratio=1.0,
        verbose=True,
    ).rename(columns={"vertex": "id"})

    pos_x = pos["x"]
    pos_y = pos["y"]

    assert pos_x is not None and pos_y is not None

    logger.info(
        f"Raw layout extents: "
        f"x: [{float(pos_x.min()):.1f}, {float(pos_x.max()):.1f}], "
        f"y:  [{float(pos_y.min()):.1f}, {float(pos_y.max()):.1f}]"
    )

    pos.to_parquet(LAYOUT_PATH, compression="zstd")
    logger.success(f"Wrote layout to {LAYOUT_PATH}")


def normalize_coordinates(layout: pl.DataFrame) -> pl.DataFrame:
    """Center the layout on the median and scale uniformly to fit WORLD_EXTENT."""
    cx = float(layout["x"].median())  # pyright: ignore[reportArgumentType]
    cy = float(layout["y"].median())  # pyright: ignore[reportArgumentType]
    centered = layout.with_columns(
        (pl.col("x") - cx).alias("x"),
        (pl.col("y") - cy).alias("y"),
    )

    max_abs = max(
        float(centered["x"].abs().max()),  # pyright: ignore[reportArgumentType]
        float(centered["y"].abs().max()),  # pyright: ignore[reportArgumentType]
    )
    scale = (WORLD_EXTENT / 2) / max_abs
    logger.info(
        f"Centering at ({cx:.2f}, {cy:.2f}), scaling by {scale:.4f} "
        f"to fit WORLD_EXTENT={WORLD_EXTENT}"
    )

    return centered.with_columns(
        (pl.col("x") * scale).alias("x"),
        (pl.col("y") * scale).alias("y"),
    )


def assign_color_indices(clusters: pl.DataFrame) -> pl.DataFrame:
    """Assign palette indices 0..TOP_N_CLUSTERS-1 to the largest clusters."""
    logger.info("Assigning color indices")
    top = (
        clusters.group_by("partition")
        .len()
        .sort("len", descending=True)
        .head(TOP_N_CLUSTERS)
        .with_row_index("color_index")
        .with_columns(pl.col("color_index").cast(pl.UInt32))
        .select(["partition", "color_index"])
    )

    return clusters.join(top, on="partition", how="left").with_columns(
        pl.col("color_index").fill_null(TOP_N_CLUSTERS).cast(pl.UInt32)
    )


def merge_and_write() -> None:
    """Join nodes, pagerank, clusters, and layout into the final enriched table."""
    logger.info("Merging into final node table")

    nodes = pl.read_parquet(NODES_INPUT_PATH)
    pagerank = pl.read_parquet(PAGERANK_PATH)
    clusters = pl.read_parquet(CLUSTERS_PATH)
    layout = pl.read_parquet(LAYOUT_PATH)

    layout = normalize_coordinates(layout)
    clusters = assign_color_indices(clusters)

    enriched = (
        nodes.join(pagerank, on="id", how="inner")
        .join(clusters, on="id", how="inner")
        .join(layout, on="id", how="inner")
        .sort("id")
    )

    logger.info(f"Final enriched node table: {len(enriched):,} rows")
    logger.info(f"Schema: {enriched.schema}")
    enriched.write_parquet(NODES_ENRICHED_PATH, compression="zstd")
    logger.success(f"Wrote {NODES_ENRICHED_PATH}")


if __name__ == "__main__":
    logger.info("Processing graph")

    compute_pagerank()
    compute_clusters()
    compute_layout()
    merge_and_write()

    logger.success("Graph successfully processed")
