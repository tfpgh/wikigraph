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
    bake_exposure,
    build_parent_level,
    compute_max_zoom,
    encode_webp_lossless,
    log_layer_summary,
    write_pmtiles,
)
from tiles.palette import compute_palette

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")

PALETTE_OUTPUT_PATH = Path("intermediates/cluster_palette.parquet")
NODE_TILES_OUTPUT_PATH = Path("intermediates/node_tiles.pmtiles")


def bucket_nodes_by_tile(nodes: pl.DataFrame, max_z: int) -> pl.DataFrame:
    """Group nodes by their z=MAX tile.

    Each node is exploded into every tile its bounding circle touches so big
    nodes appear in every tile they overlap. Returns one row per non-empty
    tile with list-columns of node attributes.
    """
    tile_w = WORLD_EXTENT / (2**max_z)
    n_axis = 2**max_z

    exploded = (
        nodes.with_columns(
            ((pl.col("x") - pl.col("radius") + WORLD_EXTENT / 2) / tile_w)
            .floor()
            .cast(pl.Int32)
            .alias("tx_min"),
            ((pl.col("x") + pl.col("radius") + WORLD_EXTENT / 2) / tile_w)
            .floor()
            .cast(pl.Int32)
            .alias("tx_max"),
            ((pl.col("y") - pl.col("radius") + WORLD_EXTENT / 2) / tile_w)
            .floor()
            .cast(pl.Int32)
            .alias("ty_min"),
            ((pl.col("y") + pl.col("radius") + WORLD_EXTENT / 2) / tile_w)
            .floor()
            .cast(pl.Int32)
            .alias("ty_max"),
        )
        .with_columns(pl.int_ranges(pl.col("tx_min"), pl.col("tx_max") + 1).alias("tx"))
        .explode("tx")
        .with_columns(pl.int_ranges(pl.col("ty_min"), pl.col("ty_max") + 1).alias("ty"))
        .explode("ty")
        .filter(
            (pl.col("tx") >= 0)
            & (pl.col("tx") < n_axis)
            & (pl.col("ty") >= 0)
            & (pl.col("ty") < n_axis)
        )
        .select(["tx", "ty", "x", "y", "radius", "r", "g", "b"])
    )
    logger.info(f"Bucketed {len(nodes):,} nodes into {len(exploded):,} tile-node rows")

    return exploded.group_by(["tx", "ty"], maintain_order=False).agg(
        "x", "y", "radius", "r", "g", "b"
    )


def render_max_tile(
    tx: int,
    ty: int,
    max_z: int,
    xs: list[float],
    ys: list[float],
    radii: list[float],
    reds: list[int],
    greens: list[int],
    blues: list[int],
    alpha_gamma: float = 1.0,
) -> tuple[int, int, bytes]:
    """Render one tile of nodes to lossless WebP bytes.

    Nodes are grouped by color in Python (tiles usually contain only a handful
    of partitions) and drawn as one batched SkPath per color, not per-node.
    Background is transparent; the frontend composites it onto whatever
    background it wants.

    Works at any zoom level (max_z is named for the pyramid use case but is
    really just "the z of this tile"). When alpha_gamma > 1, alpha is sRGB-
    style encoded (stored = α^(1/γ)) before WebP encoding to give 8-bit
    precision to the dim end of the range instead of quantizing it to zero;
    the frontend must decode with α^γ before applying any further curve.
    Defaults to 1.0 (identity) so the existing pyramid pipeline is unchanged.
    """
    surface = skia.Surface(TILE_SIZE, TILE_SIZE)
    canvas = surface.getCanvas()
    canvas.clear(skia.Color4f(0, 0, 0, 0))

    ppwu = TILE_SIZE * (2**max_z) / WORLD_EXTENT
    origin_x = tx * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2
    origin_y = ty * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2

    paths: dict[tuple[int, int, int], skia.Path] = {}
    for x, y, r, red, green, blue in zip(xs, ys, radii, reds, greens, blues):
        color = (red, green, blue)
        path = paths.get(color)
        if path is None:
            path = skia.Path()
            paths[color] = path
        path.addCircle((x - origin_x) * ppwu, (y - origin_y) * ppwu, r * ppwu)

    for (red, green, blue), path in paths.items():
        canvas.drawPath(
            path,
            skia.Paint(AntiAlias=True, Color=skia.Color(red, green, blue)),
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


if __name__ == "__main__":
    logger.info("Rendering node tiles")

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

    bucketed = bucket_nodes_by_tile(
        nodes.join(palette, on="partition", how="inner"), max_z
    )
    logger.info(f"{len(bucketed):,} z={max_z} tiles contain at least one node")

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}

    logger.info(f"Rendering z={max_z} (max zoom)")
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(render_max_tile)(tx, ty, max_z, xs, ys, rs, reds, greens, blues)
        for tx, ty, xs, ys, rs, reds, greens, blues in bucketed.iter_rows()
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

    bake_exposure(pyramid, max_z)

    write_pmtiles(pyramid, max_z, NODE_TILES_OUTPUT_PATH)
    logger.success(f"Wrote tile pyramid to {NODE_TILES_OUTPUT_PATH}")
