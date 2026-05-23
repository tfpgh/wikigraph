from pathlib import Path

import polars as pl
import skia
from joblib import Parallel, delayed
from loguru import logger
from tqdm import tqdm

from tiles.common import (
    EDGE_WIDTH_WORLD,
    TILE_SIZE,
    WORLD_EXTENT,
    build_parent_level,
    compute_max_zoom,
    encode_webp_lossless,
    log_layer_summary,
    write_pmtiles,
)
from tiles.palette import compute_palette

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

EDGE_TILES_OUTPUT_PATH = Path("intermediates/edge_tiles.pmtiles")

# Per-edge alpha (0-255). Low so overlapping edges stack via alpha-over
# rather than saturating immediately — survives pyramid downsampling.
EDGE_ALPHA = 25


def bucket_edges_by_tile(edges: pl.DataFrame, max_z: int) -> pl.DataFrame:
    """Bucket edges into z=MAX tiles via a polars-vectorized line-tile walk.

    For each edge, supersample at 2x along the segment in tile space and dedupe
    so each tile the line actually crosses gets one row. Bounding-box explosion
    (the trick used for nodes) over-tags badly for long diagonal edges; this
    walks only the cells the line touches.

    The explode runs lazy on a narrow frame and fuses the lerp into the select
    that drops every carry column right at the widest point — at z=11 on 200M+
    edges the wide intermediate is what eats memory, not the final rows.
    """
    tile_w = WORLD_EXTENT / (2**max_z)
    n_axis = 2**max_z
    half_w = WORLD_EXTENT / 2

    edges_indexed = edges.with_row_index("edge_idx")

    walks = (
        edges_indexed.lazy()
        .select(
            "edge_idx",
            ((pl.col("src_x") + half_w) / tile_w).cast(pl.Float32).alias("tx0"),
            ((pl.col("src_y") + half_w) / tile_w).cast(pl.Float32).alias("ty0"),
            ((pl.col("dst_x") + half_w) / tile_w).cast(pl.Float32).alias("tx1"),
            ((pl.col("dst_y") + half_w) / tile_w).cast(pl.Float32).alias("ty1"),
        )
        .with_columns(
            (pl.col("tx1") - pl.col("tx0")).alias("dtx"),
            (pl.col("ty1") - pl.col("ty0")).alias("dty"),
        )
        .with_columns(
            # 2x oversample so diagonal lines never skip a tile-corner crossing;
            # min 2 so degenerate within-tile edges still emit both endpoints.
            pl.max_horizontal(
                2
                * pl.max_horizontal(
                    pl.col("dtx").abs().ceil(),
                    pl.col("dty").abs().ceil(),
                )
                + 1,
                pl.lit(2.0),
            )
            .cast(pl.UInt32)
            .alias("n_samples")
        )
        .with_columns(
            pl.int_ranges(0, pl.col("n_samples"), dtype=pl.UInt32).alias("step")
        )
        .explode("step")
        .select(
            "edge_idx",
            (
                pl.col("tx0")
                + (
                    pl.col("step").cast(pl.Float32)
                    / (pl.col("n_samples") - 1).cast(pl.Float32)
                )
                * pl.col("dtx")
            )
            .floor()
            .cast(pl.Int32)
            .alias("tx"),
            (
                pl.col("ty0")
                + (
                    pl.col("step").cast(pl.Float32)
                    / (pl.col("n_samples") - 1).cast(pl.Float32)
                )
                * pl.col("dty")
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
    logger.info(f"Bucketed {len(edges):,} edges into {len(walks):,} tile-edge rows")

    return (
        walks.join(edges_indexed, on="edge_idx", how="inner")
        .group_by(["tx", "ty"], maintain_order=False)
        .agg("src_x", "src_y", "dst_x", "dst_y", "r", "g", "b")
    )


def render_max_edge_tile(
    tx: int,
    ty: int,
    max_z: int,
    src_xs: list[float],
    src_ys: list[float],
    dst_xs: list[float],
    dst_ys: list[float],
    reds: list[int],
    greens: list[int],
    blues: list[int],
) -> tuple[int, int, bytes]:
    """Render one z=MAX edge tile to lossless WebP bytes.

    Edges are grouped by target-cluster color and stroked as one batched SkPath
    per color. Stroke width is EDGE_WIDTH_WORLD in world units, converted to
    pixels here — defined this way so changing MAX_Z later doesn't visually
    halve the lines at lower zoom levels after the pyramid downsample.

    Skia clips strokes to the tile viewport in hardware, so edges whose
    endpoints fall outside the tile still render correctly where they cross.
    """
    surface = skia.Surface(TILE_SIZE, TILE_SIZE)
    canvas = surface.getCanvas()
    canvas.clear(skia.Color4f(0, 0, 0, 0))

    ppwu = TILE_SIZE * (2**max_z) / WORLD_EXTENT
    origin_x = tx * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2
    origin_y = ty * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2
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
    return tx, ty, encode_webp_lossless(arr)


if __name__ == "__main__":
    logger.info("Rendering edge tiles")

    nodes = pl.read_parquet(NODES_INPUT_PATH)
    logger.info(f"Loaded {len(nodes):,} nodes")

    max_z = compute_max_zoom(nodes["radius"])

    palette = compute_palette(nodes["partition"])

    edges = pl.read_parquet(EDGES_INPUT_PATH)
    logger.info(f"Loaded {len(edges):,} edges")

    # Attach src coords, dst coords + target-cluster color to every edge.
    # Color comes from dst's partition so the static raster is "colored by
    # target," matching the hover-vector convention.
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
        .select(["src_x", "src_y", "dst_x", "dst_y", "r", "g", "b"])
    )
    logger.info(f"Joined {len(edges_with_coords):,} edges with coords + palette")

    bucketed = bucket_edges_by_tile(edges_with_coords, max_z)
    logger.info(f"{len(bucketed):,} z={max_z} tiles contain at least one edge")

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}

    logger.info(f"Rendering z={max_z} (max zoom)")
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(render_max_edge_tile)(
            tx, ty, max_z, sxs, sys, dxs, dys, reds, greens, blues
        )
        for tx, ty, sxs, sys, dxs, dys, reds, greens, blues in bucketed.iter_rows()
    )
    pyramid[max_z] = {}
    for tx, ty, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=len(bucketed), desc=f"Rendering z={max_z}", unit=" tiles"
    ):
        pyramid[max_z][(tx, ty)] = data
    log_layer_summary(max_z, pyramid[max_z])

    logger.info(f"Building pyramid from z={max_z - 1} down to z=0")
    for z in range(max_z - 1, -1, -1):
        pyramid[z] = build_parent_level(pyramid[z + 1], z)
        log_layer_summary(z, pyramid[z])

    pyramid_bytes = sum(
        sum(len(b) for b in layer.values()) for layer in pyramid.values()
    )
    pyramid_tiles = sum(len(layer) for layer in pyramid.values())
    logger.info(
        f"Pyramid complete: {pyramid_tiles:,} tiles, "
        f"{pyramid_bytes / 1e9:.2f} GB in memory"
    )

    write_pmtiles(pyramid, max_z, EDGE_TILES_OUTPUT_PATH)
    logger.success(f"Wrote tile pyramid to {EDGE_TILES_OUTPUT_PATH}")
