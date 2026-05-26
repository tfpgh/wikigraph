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

PALETTE_OUTPUT_PATH = Path("intermediates/cluster_palette.parquet")
NODE_TILES_OUTPUT_PATH = Path("intermediates/node_tiles.pmtiles")

# Tiles are rasterized at SSAA × TILE_SIZE and box-averaged down to TILE_SIZE
# for anti-aliasing. Everything stays F32 through the downsample and the tone
# curve; the only quantization is the final collapse to 8-bit WebP.
SSAA = 4

# Brightening tone curve baked onto the alpha (straight coverage) channel,
# applied AFTER the downsample so it maps true per-pixel coverage:
# stored = α^(1/γ). Lifts low-density regions into visible alpha. This is the
# final stored alpha — there is NO frontend decode. γ=1.0 is identity.
ALPHA_GAMMA = 1.0


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


def render_node_tile(
    tx: int,
    ty: int,
    z: int,
    xs: list[float],
    ys: list[float],
    radii: list[float],
    reds: list[int],
    greens: list[int],
    blues: list[int],
    alpha_gamma: float = 1.0,
    ssaa: int = 1,
) -> tuple[int, int, bytes]:
    """Render one tile of nodes at zoom z to lossless WebP bytes.

    Nodes are grouped by color in Python (tiles usually contain only a handful
    of partitions) and drawn as one batched SkPath per color, not per-node.
    Background is transparent; the frontend composites it onto whatever
    background it wants.

    The tile is rasterized at ssaa × TILE_SIZE and box-averaged down to
    TILE_SIZE (premultiplied F32, so edges don't bleed). The α^(1/γ)
    brightening curve is applied AFTER the downsample, so it maps the true
    per-pixel coverage. Everything stays F32 until the final 8-bit collapse for
    WebP; the frontend does not decode the alpha.
    """
    hi = TILE_SIZE * ssaa
    info = skia.ImageInfo.Make(
        hi,
        hi,
        skia.ColorType.kRGBA_F32_ColorType,
        skia.AlphaType.kPremul_AlphaType,
        skia.ColorSpace.MakeSRGB(),
    )
    surface = skia.Surface.MakeRaster(info)
    canvas = surface.getCanvas()
    canvas.clear(skia.Color4f(0, 0, 0, 0))

    ppwu = hi * (2**z) / WORLD_EXTENT
    origin_x = tx * WORLD_EXTENT / (2**z) - WORLD_EXTENT / 2
    origin_y = ty * WORLD_EXTENT / (2**z) - WORLD_EXTENT / 2

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

    # Read premultiplied so the box-average filters correctly — averaging
    # straight alpha would bleed the transparent texels' black into the edges.
    image = surface.makeImageSnapshot()
    premul = image.toarray(
        colorType=skia.ColorType.kRGBA_F32_ColorType,
        alphaType=skia.AlphaType.kPremul_AlphaType,
    )

    # Box-average ssaa×ssaa blocks down to TILE_SIZE, in F32 premultiplied space.
    down = premul.reshape(TILE_SIZE, ssaa, TILE_SIZE, ssaa, 4).mean(axis=(1, 3))

    # Un-premultiply back to straight color; alpha is now the true coverage.
    a = down[..., 3]
    with np.errstate(divide="ignore", invalid="ignore"):
        rgb = np.where(a[..., None] > 0.0, down[..., :3] / a[..., None], 0.0)

    # Brightening curve, applied AFTER the downsample so it maps true coverage.
    a = np.clip(a, 0.0, 1.0)
    if alpha_gamma != 1.0:
        a = np.power(a, 1.0 / alpha_gamma)

    out = np.empty((TILE_SIZE, TILE_SIZE, 4), dtype=np.float32)
    out[..., :3] = rgb
    out[..., 3] = a
    out = np.rint(np.clip(out * 255.0, 0, 255)).astype(np.uint8)

    return tx, ty, encode_webp_lossless(out)


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
        delayed(render_node_tile)(
            tx, ty, z, xs, ys, rs, reds, greens, blues, ALPHA_GAMMA, SSAA
        )
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
