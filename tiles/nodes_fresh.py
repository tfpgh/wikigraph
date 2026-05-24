"""Render the node tile pyramid by drawing every zoom level fresh from source.

Same input (enriched_nodes.parquet) and output (node_tiles.pmtiles) as
tiles/nodes.py. The difference is the *how*: instead of rendering only
z=max_z with skia and then mean/Lanczos/etc-downsampling everything else,
this script lets skia draw each zoom level natively at its own scale.

Avoids the entire chain of quality trade-offs (p-norm vs density flattening,
exposure vs AA, filter ringing, 8-bit compounding) by never downsampling in
the first place. Quality at every zoom is whatever skia's native AA can do
for circles at that pixel size — typically very good.

Cost: roughly 2x the pyramid's render work (geometric series over levels)
instead of 4/3x, plus z=0 is one tile containing all nodes which is a fat
serial render. Still well within a 192-core budget.
"""

import time

import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from tqdm import tqdm

from tiles.common import compute_max_zoom, write_pmtiles
from tiles.nodes import (
    NODE_TILES_OUTPUT_PATH,
    NODES_INPUT_PATH,
    PALETTE_OUTPUT_PATH,
    bucket_nodes_by_tile,
)
from tiles.nodes import render_max_tile as render_node_tile  # generic in z
from tiles.palette import compute_palette


def render_layer(
    nodes_with_palette: pl.DataFrame, z: int
) -> dict[tuple[int, int], bytes]:
    """Bucket nodes into this zoom level's tile grid and render each tile."""
    t_bucket = time.perf_counter()
    bucketed = bucket_nodes_by_tile(nodes_with_palette, z)
    n_tiles = len(bucketed)
    bucket_s = time.perf_counter() - t_bucket

    if n_tiles == 0:
        logger.warning(f"z={z}: no tiles contain nodes, skipping")
        return {}

    logger.info(f"z={z}: bucketed in {bucket_s:.1f}s → rendering {n_tiles:,} tile(s)")

    layer: dict[tuple[int, int], bytes] = {}
    t_render = time.perf_counter()
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(render_node_tile)(tx, ty, z, xs, ys, rs, reds, greens, blues)
        for tx, ty, xs, ys, rs, reds, greens, blues in bucketed.iter_rows()
    )
    for tx, ty, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=n_tiles, desc=f"Rendering z={z}", unit=" tiles"
    ):
        layer[(tx, ty)] = data
    render_s = time.perf_counter() - t_render

    total_bytes = sum(len(b) for b in layer.values())
    logger.info(
        f"z={z}: rendered in {render_s:.1f}s — {n_tiles:,} tiles, "
        f"{total_bytes / 1e9:.3f} GB (avg {total_bytes / n_tiles / 1024:.1f} KB/tile)"
    )
    return layer


if __name__ == "__main__":
    logger.info(
        f"Rendering node tiles fresh at each zoom level → {NODE_TILES_OUTPUT_PATH}"
    )

    nodes = pl.read_parquet(NODES_INPUT_PATH).select(
        ["id", "x", "y", "radius", "partition"]
    )
    logger.info(f"Loaded {len(nodes):,} nodes")

    max_z = compute_max_zoom(nodes["radius"])

    palette = compute_palette(nodes["partition"])
    palette.write_parquet(PALETTE_OUTPUT_PATH, compression="zstd")
    logger.success(
        f"Wrote palette ({len(palette):,} clusters) to {PALETTE_OUTPUT_PATH}"
    )

    nodes_with_palette = nodes.join(palette, on="partition", how="inner")

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}
    t_total = time.perf_counter()
    for z in range(max_z + 1):
        pyramid[z] = render_layer(nodes_with_palette, z)
    total_s = time.perf_counter() - t_total

    total_tiles = sum(len(layer) for layer in pyramid.values())
    total_bytes = sum(sum(len(b) for b in layer.values()) for layer in pyramid.values())
    logger.info(
        f"All {max_z + 1} levels rendered in {total_s:.1f}s: "
        f"{total_tiles:,} tiles, {total_bytes / 1e9:.2f} GB in memory"
    )

    write_pmtiles(pyramid, max_z, NODE_TILES_OUTPUT_PATH)
    logger.success(f"Wrote tile pyramid to {NODE_TILES_OUTPUT_PATH}")
