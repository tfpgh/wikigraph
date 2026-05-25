"""Render the edge tile pyramid by drawing every zoom level fresh from source.

Mirrors tiles/nodes.py's "draw each zoom natively, never downsample" strategy,
but edges have a memory problem nodes don't: a single edge can cross dozens of
tiles at high zoom, so the (edge, tile) bucketing table explodes to tens of
billions of rows at z=max — that's what OOMed the old downsample pipeline even
on 1 TB.

The fix is a spatial pre-partition. We cut the world into a coarse CHUNK_Z grid
and assign each edge to every chunk it crosses, *clipping the edge to the chunk
box* as we go (Liang-Barsky). Two payoffs:

  * Each fine tile at z >= CHUNK_Z lives in exactly one chunk, so chunks are
    independent render units — we batch whole chunks and a fine tile is never
    rendered twice or left partial.
  * Because segments are pre-clipped to their chunk, walking a clipped segment
    at any zoom only ever emits tiles inside that chunk, and the walk length is
    bounded by the chunk's tile span — no quadratic blowup from long edges.

For z < CHUNK_Z a fine tile spans many chunks, so we skip the chunk frame and
walk the original (un-duplicated) edges in one pass — cheap at coarse zoom
where each edge crosses only a handful of tiles. z=0 is one fat serial tile;
we don't special-case it.
"""

import math
import time
from pathlib import Path

import numpy as np
import polars as pl
import skia
from joblib import Parallel, delayed
from loguru import logger
from tqdm import tqdm

from tiles.common import (
    TILE_SIZE,
    WORLD_EXTENT,
    compute_max_zoom,
    encode_webp_lossless,
    write_pmtiles,
)
from tiles.palette import compute_palette

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

EDGE_TILES_OUTPUT_PATH = Path("intermediates/edge_tiles.pmtiles")

# Coarse grid the world is partitioned into for memory-bounded rendering. Each
# chunk is a self-contained render unit at z >= CHUNK_Z. 7 -> 128x128 = 16,384
# chunks; at ~200M edges that's a tiny per-chunk tile-table.
CHUNK_Z = 7

# Edge stroke width in world units, converted to px per zoom level. Kept in
# world units (no min-pixel clamp) so apparent thickness is perfectly
# consistent across levels; edges go sub-pixel at high zoom, which is fine —
# there are enough of them that skia's coverage AA still renders density.
EDGE_WIDTH_WORLD = 0.5

# Per-edge alpha (0-255). Low so overlapping edges stack via alpha-over rather
# than saturating immediately — that accumulation is the density signal.
EDGE_ALPHA = 25

# sRGB-style gamma on the alpha channel before WebP encoding (stored = α^(1/γ)),
# same as the node layer so the frontend decodes both the same way. Gives 8-bit
# precision to the dim end instead of quantizing faint edge density to zero.
# Frontend must decode with α^γ. Identity at γ=1.0.
ALPHA_GAMMA = 2.0

# Cap on the (edge, tile) walk table per batch at z >= CHUNK_Z. Sets how many
# whole-chunk batches a level is split into so the wide intermediate stays
# bounded regardless of zoom.
TARGET_TILE_ROWS = 300_000_000

COORD_COLS = ["src_x", "src_y", "dst_x", "dst_y", "r", "g", "b"]


def walk_tiles(frame: pl.DataFrame, z: int) -> pl.DataFrame:
    """Return (idx, tx, ty) for every z-tile each segment crosses (deduped).

    Polars-vectorized line-tile walk: supersample at 2x along the segment in
    tile space and dedupe, so each tile the line actually crosses gets one row.
    Bounding-box explosion over-tags badly for long diagonals; this walks only
    the cells the line touches. `frame` must carry an `idx` column plus the
    COORD_COLS endpoints. Runs lazy and drops to a narrow (idx, tx, ty) frame at
    the widest point to keep the explode cheap.
    """
    tile_w = WORLD_EXTENT / (2**z)
    n_axis = 2**z
    half_w = WORLD_EXTENT / 2

    return (
        frame.lazy()
        .select(
            "idx",
            ((pl.col("src_x") + half_w) / tile_w).cast(pl.Float32).alias("a0"),
            ((pl.col("src_y") + half_w) / tile_w).cast(pl.Float32).alias("b0"),
            ((pl.col("dst_x") + half_w) / tile_w).cast(pl.Float32).alias("a1"),
            ((pl.col("dst_y") + half_w) / tile_w).cast(pl.Float32).alias("b1"),
        )
        .with_columns(
            (pl.col("a1") - pl.col("a0")).alias("da"),
            (pl.col("b1") - pl.col("b0")).alias("db"),
        )
        .with_columns(
            # 2x oversample so diagonals never skip a tile-corner crossing;
            # min 2 so degenerate within-tile segments still emit both ends.
            pl.max_horizontal(
                2
                * pl.max_horizontal(
                    pl.col("da").abs().ceil(), pl.col("db").abs().ceil()
                )
                + 1,
                pl.lit(2.0),
            )
            .cast(pl.UInt32)
            .alias("n")
        )
        .with_columns(pl.int_ranges(0, pl.col("n"), dtype=pl.UInt32).alias("s"))
        .explode("s")
        .select(
            "idx",
            (
                pl.col("a0")
                + (pl.col("s").cast(pl.Float32) / (pl.col("n") - 1).cast(pl.Float32))
                * pl.col("da")
            )
            .floor()
            .cast(pl.Int32)
            .alias("tx"),
            (
                pl.col("b0")
                + (pl.col("s").cast(pl.Float32) / (pl.col("n") - 1).cast(pl.Float32))
                * pl.col("db")
            )
            .floor()
            .cast(pl.Int32)
            .alias("ty"),
        )
        .filter(
            (pl.col("tx") >= 0)
            & (pl.col("tx") < n_axis)
            & (pl.col("ty") >= 0)
            & (pl.col("ty") < n_axis)
        )
        .unique()
        .collect()
    )


def assign_and_clip_chunks(edges: pl.DataFrame, chunk_z: int) -> pl.DataFrame:
    """Explode edges into per-chunk clipped segments at the CHUNK_Z grid.

    Each edge is assigned to every chunk it crosses, and within each chunk its
    endpoints are replaced by the Liang-Barsky clip of the segment against that
    chunk's world box. The result has one row per (edge, chunk) carrying the
    clipped segment, its target-cluster color, and the chunk coords (ctx, cty).
    """
    cw = WORLD_EXTENT / (2**chunk_z)
    half = WORLD_EXTENT / 2
    inf = float("inf")

    indexed = edges.with_row_index("idx")
    crossed = walk_tiles(indexed, chunk_z).rename({"tx": "ctx", "ty": "cty"})

    joined = crossed.join(indexed, on="idx", how="inner")
    logger.info(
        f"Assigned {len(edges):,} edges to {len(joined):,} (edge, chunk) pairs "
        f"at CHUNK_Z={chunk_z} (avg {len(joined) / len(edges):.1f} chunks/edge)"
    )

    dx = pl.col("dst_x") - pl.col("src_x")
    dy = pl.col("dst_y") - pl.col("src_y")

    return (
        joined.with_columns(
            dx.alias("dx"),
            dy.alias("dy"),
            (pl.col("ctx") * cw - half).alias("xmin"),
            (pl.col("cty") * cw - half).alias("ymin"),
        )
        .with_columns(
            (pl.col("xmin") + cw).alias("xmax"),
            (pl.col("ymin") + cw).alias("ymax"),
        )
        .with_columns(
            ((pl.col("xmin") - pl.col("src_x")) / pl.col("dx")).alias("tx1"),
            ((pl.col("xmax") - pl.col("src_x")) / pl.col("dx")).alias("tx2"),
            ((pl.col("ymin") - pl.col("src_y")) / pl.col("dy")).alias("ty1"),
            ((pl.col("ymax") - pl.col("src_y")) / pl.col("dy")).alias("ty2"),
        )
        .with_columns(
            # Axis-aligned segments have no constraint from that slab (the chunk
            # already contains them on that axis), so widen the bound to ±inf.
            pl.when(pl.col("dx") == 0)
            .then(pl.lit(-inf))
            .otherwise(pl.min_horizontal("tx1", "tx2"))
            .alias("tx_near"),
            pl.when(pl.col("dx") == 0)
            .then(pl.lit(inf))
            .otherwise(pl.max_horizontal("tx1", "tx2"))
            .alias("tx_far"),
            pl.when(pl.col("dy") == 0)
            .then(pl.lit(-inf))
            .otherwise(pl.min_horizontal("ty1", "ty2"))
            .alias("ty_near"),
            pl.when(pl.col("dy") == 0)
            .then(pl.lit(inf))
            .otherwise(pl.max_horizontal("ty1", "ty2"))
            .alias("ty_far"),
        )
        .with_columns(
            pl.max_horizontal(pl.lit(0.0), "tx_near", "ty_near").alias("t_enter"),
            pl.min_horizontal(pl.lit(1.0), "tx_far", "ty_far").alias("t_exit"),
        )
        # Drop chunks the walk over-tagged on a corner (segment misses the box).
        .filter(pl.col("t_enter") <= pl.col("t_exit"))
        .select(
            "ctx",
            "cty",
            (pl.col("src_x") + pl.col("t_enter") * pl.col("dx")).alias("src_x"),
            (pl.col("src_y") + pl.col("t_enter") * pl.col("dy")).alias("src_y"),
            (pl.col("src_x") + pl.col("t_exit") * pl.col("dx")).alias("dst_x"),
            (pl.col("src_y") + pl.col("t_exit") * pl.col("dy")).alias("dst_y"),
            "r",
            "g",
            "b",
        )
        .with_columns((pl.col("cty") * (2**chunk_z) + pl.col("ctx")).alias("chunk_id"))
    )


def bucket_edges(frame: pl.DataFrame, z: int) -> pl.DataFrame:
    """Walk segments at zoom z and group into per-tile list-columns."""
    indexed = frame.with_row_index("idx")
    walk = walk_tiles(indexed, z)
    return (
        walk.join(indexed, on="idx", how="inner")
        .group_by(["tx", "ty"], maintain_order=False)
        .agg(*COORD_COLS)
    )


def render_edge_tile(
    tx: int,
    ty: int,
    z: int,
    src_xs: list[float],
    src_ys: list[float],
    dst_xs: list[float],
    dst_ys: list[float],
    reds: list[int],
    greens: list[int],
    blues: list[int],
    alpha_gamma: float = 1.0,
) -> tuple[int, int, bytes]:
    """Render one tile of edges at zoom z to lossless WebP bytes.

    Edges are grouped by target-cluster color and stroked as one batched SkPath
    per color. Skia clips strokes to the tile viewport in hardware, so segments
    whose endpoints fall outside the tile still render where they cross. When
    alpha_gamma > 1 the alpha channel is sRGB-style encoded before WebP encoding
    (see ALPHA_GAMMA); the frontend decodes with α^γ.
    """
    surface = skia.Surface(TILE_SIZE, TILE_SIZE)
    canvas = surface.getCanvas()
    canvas.clear(skia.Color4f(0, 0, 0, 0))

    ppwu = TILE_SIZE * (2**z) / WORLD_EXTENT
    origin_x = tx * WORLD_EXTENT / (2**z) - WORLD_EXTENT / 2
    origin_y = ty * WORLD_EXTENT / (2**z) - WORLD_EXTENT / 2
    stroke_px = EDGE_WIDTH_WORLD * ppwu

    paths: dict[tuple[int, int, int], skia.Path] = {}
    for sx, sy, dx, dy, red, green, blue in zip(
        src_xs, src_ys, dst_xs, dst_ys, reds, greens, blues
    ):
        color = (red, green, blue)
        path = paths.get(color)
        if path is None:
            path = skia.Path()
            paths[color] = path
        path.moveTo((sx - origin_x) * ppwu, (sy - origin_y) * ppwu)
        path.lineTo((dx - origin_x) * ppwu, (dy - origin_y) * ppwu)

    for (red, green, blue), path in paths.items():
        canvas.drawPath(
            path,
            skia.Paint(
                AntiAlias=True,
                Color=skia.Color(red, green, blue, EDGE_ALPHA),
                Style=skia.Paint.kStroke_Style,
                StrokeWidth=stroke_px,
            ),
        )

    image = surface.makeImageSnapshot()
    arr = image.toarray(
        colorType=skia.ColorType.kRGBA_8888_ColorType,
        alphaType=skia.AlphaType.kUnpremul_AlphaType,
    )
    if alpha_gamma != 1.0:
        arr = arr.copy()  # toarray can return a view of skia's buffer
        a = arr[..., 3].astype(np.float32) / 255.0
        a = np.power(a, 1.0 / alpha_gamma)
        arr[..., 3] = np.rint(a * 255.0).astype(np.uint8)
    return tx, ty, encode_webp_lossless(arr)


def render_bucketed(bucketed: pl.DataFrame, z: int) -> dict[tuple[int, int], bytes]:
    """Render every bucketed tile in parallel into a {(tx, ty): webp} dict."""
    tiles: dict[tuple[int, int], bytes] = {}
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(render_edge_tile)(
            tx, ty, z, sxs, sys, dxs, dys, reds, greens, blues, ALPHA_GAMMA
        )
        for tx, ty, sxs, sys, dxs, dys, reds, greens, blues in bucketed.iter_rows()
    )
    for tx, ty, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=len(bucketed), desc=f"Rendering z={z}", unit=" tiles"
    ):
        tiles[(tx, ty)] = data
    return tiles


def render_level_global(edges: pl.DataFrame, z: int) -> dict[tuple[int, int], bytes]:
    """Render a coarse level (z < CHUNK_Z) in a single global walk."""
    bucketed = bucket_edges(edges, z)
    if len(bucketed) == 0:
        logger.warning(f"z={z}: no tiles contain edges, skipping")
        return {}
    logger.info(f"z={z}: {len(bucketed):,} tiles (global walk)")
    return render_bucketed(bucketed, z)


def render_level_chunked(
    clipped: pl.DataFrame, z: int, n_edge_pairs: int
) -> dict[tuple[int, int], bytes]:
    """Render a fine level (z >= CHUNK_Z) batch-by-batch over whole chunks.

    A level is split into n_batches whole-chunk batches so the per-batch walk
    table stays under TARGET_TILE_ROWS. Whole-chunk batching keeps each fine
    tile inside exactly one batch, so tiles are never split or double-rendered.
    """
    fan = 2 ** (z - CHUNK_Z)  # tiles spanned per clipped segment, this level
    n_batches = max(1, math.ceil(n_edge_pairs * fan / TARGET_TILE_ROWS))

    logger.info(f"z={z}: {n_batches} chunk batch(es)")
    layer: dict[tuple[int, int], bytes] = {}
    for b in range(n_batches):
        batch = clipped.filter(pl.col("chunk_id") % n_batches == b)
        bucketed = bucket_edges(batch, z)
        if len(bucketed) == 0:
            continue
        layer.update(render_bucketed(bucketed, z))
    return layer


if __name__ == "__main__":
    logger.info(
        f"Rendering edge tiles fresh at each zoom level → {EDGE_TILES_OUTPUT_PATH}"
    )

    nodes = pl.read_parquet(NODES_INPUT_PATH)
    logger.info(f"Loaded {len(nodes):,} nodes")

    max_z = compute_max_zoom(nodes["radius"])

    palette = compute_palette(nodes["partition"])

    edges = pl.read_parquet(EDGES_INPUT_PATH)
    logger.info(f"Loaded {len(edges):,} edges")

    # Attach src coords, dst coords, and target-cluster color to every edge.
    # Color comes from dst's partition so the raster is "colored by target,"
    # matching the hover-vector convention.
    edges_with_coords = (
        edges.join(
            nodes.select("id", "x", "y").rename(
                {"id": "src", "x": "src_x", "y": "src_y"}
            ),
            on="src",
            how="inner",
        )
        .join(
            nodes.select("id", "x", "y", "partition").rename(
                {"id": "dst", "x": "dst_x", "y": "dst_y"}
            ),
            on="dst",
            how="inner",
        )
        .join(palette, on="partition", how="inner")
        .select(COORD_COLS)
    )
    logger.info(f"Joined {len(edges_with_coords):,} edges with coords + palette")

    clipped = assign_and_clip_chunks(edges_with_coords, CHUNK_Z)
    n_edge_pairs = len(clipped)

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}
    t_total = time.perf_counter()
    for z in range(max_z + 1):
        t_z = time.perf_counter()
        if z < CHUNK_Z:
            pyramid[z] = render_level_global(edges_with_coords, z)
        else:
            pyramid[z] = render_level_chunked(clipped, z, n_edge_pairs)
        layer_bytes = sum(len(b) for b in pyramid[z].values())
        logger.info(
            f"z={z}: {len(pyramid[z]):,} tiles, {layer_bytes / 1e9:.3f} GB "
            f"in {time.perf_counter() - t_z:.1f}s"
        )
    total_s = time.perf_counter() - t_total

    total_tiles = sum(len(layer) for layer in pyramid.values())
    total_bytes = sum(sum(len(b) for b in layer.values()) for layer in pyramid.values())
    logger.info(
        f"All {max_z + 1} levels rendered in {total_s:.1f}s: "
        f"{total_tiles:,} tiles, {total_bytes / 1e9:.2f} GB in memory"
    )

    write_pmtiles(pyramid, max_z, EDGE_TILES_OUTPUT_PATH)
    logger.success(f"Wrote tile pyramid to {EDGE_TILES_OUTPUT_PATH}")
