import io
import math
from colorsys import hsv_to_rgb
from pathlib import Path

import numpy as np
import polars as pl
import skia
from joblib import Parallel, delayed
from loguru import logger
from PIL import Image as PILImage
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from tqdm import tqdm

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")

PALETTE_OUTPUT_PATH = Path("intermediates/cluster_palette.parquet")
TILES_OUTPUT_PATH = Path("intermediates/graph_tiles.pmtiles")

WORLD_EXTENT = 2**16
TILE_SIZE = 256

MIN_NODE_TARGET_PX = 10.0
RADIUS_PERCENTILE_FOR_MAX_Z = 0.005

GOLDEN_RATIO_CONJUGATE = 0.618033988749895
COLOR_SATURATION = 0.9
COLOR_VALUE = 1.0


def compute_max_zoom(radii: pl.Series) -> int:
    """Choose MAX_Z so the small-radius percentile is at least MIN_NODE_TARGET_PX at max zoom."""
    r_small = float(radii.quantile(RADIUS_PERCENTILE_FOR_MAX_Z))  # pyright: ignore[reportArgumentType]
    ppwu_needed = MIN_NODE_TARGET_PX / r_small
    max_z = max(1, math.ceil(math.log2(ppwu_needed * WORLD_EXTENT / TILE_SIZE)))
    total = (4 ** (max_z + 1) - 1) // 3
    logger.info(
        f"p{RADIUS_PERCENTILE_FOR_MAX_Z:.0%} radius = {r_small:.4f}, max_z = {max_z} "
        f"({2**max_z:,} tiles per side, {total:,} total in pyramid)"
    )
    return max_z


def compute_palette(partitions: pl.Series) -> pl.DataFrame:
    """Map each cluster id to a stable RGB color via golden-ratio HSV spacing."""
    rows: list[tuple[int, int, int, int]] = []
    for p in partitions.unique().sort():
        hue = (int(p) * GOLDEN_RATIO_CONJUGATE) % 1.0
        r, g, b = hsv_to_rgb(hue, COLOR_SATURATION, COLOR_VALUE)
        rows.append((int(p), int(r * 255), int(g * 255), int(b * 255)))

    return pl.DataFrame(
        rows, schema=["partition", "r", "g", "b"], orient="row"
    ).with_columns(
        pl.col("partition").cast(partitions.dtype),
        pl.col("r").cast(pl.UInt8),
        pl.col("g").cast(pl.UInt8),
        pl.col("b").cast(pl.UInt8),
    )


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


def encode_webp_lossless(arr: np.ndarray) -> bytes:
    """Encode an RGBA (straight alpha) array to lossless WebP bytes."""
    buf = io.BytesIO()
    PILImage.fromarray(arr, mode="RGBA").save(
        buf, format="WEBP", lossless=True, quality=100
    )
    return buf.getvalue()


def decode_webp(data: bytes) -> np.ndarray:
    """Decode lossless WebP bytes into an RGBA (straight alpha) array."""
    return np.array(PILImage.open(io.BytesIO(data)).convert("RGBA"))


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
) -> tuple[int, int, bytes]:
    """Render one z=MAX tile to lossless WebP bytes.

    Nodes are grouped by color in Python (tiles usually contain only a handful
    of partitions) and drawn as one batched SkPath per color, not per-node.
    Background is transparent; the frontend composites it onto whatever
    background it wants, so the alpha curve can be tuned without re-rendering.
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
    return tx, ty, encode_webp_lossless(arr)


def downsample_4_to_1(
    tl: np.ndarray | None,
    tr: np.ndarray | None,
    bl: np.ndarray | None,
    br: np.ndarray | None,
) -> np.ndarray:
    """Combine 4 RGBA children into a 256x256 parent via alpha-aware 2x2 box filter.

    Missing children become fully transparent. Premultiply-mean-unpremultiply
    keeps translucent edges from bleeding background through during the average.
    """
    blank = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    combined = np.empty((TILE_SIZE * 2, TILE_SIZE * 2, 4), dtype=np.uint8)
    combined[:TILE_SIZE, :TILE_SIZE] = tl if tl is not None else blank
    combined[:TILE_SIZE, TILE_SIZE:] = tr if tr is not None else blank
    combined[TILE_SIZE:, :TILE_SIZE] = bl if bl is not None else blank
    combined[TILE_SIZE:, TILE_SIZE:] = br if br is not None else blank

    floats = combined.astype(np.float32)
    alpha = floats[..., 3:4] / 255.0
    floats[..., :3] *= alpha

    blocks = floats.reshape(TILE_SIZE, 2, TILE_SIZE, 2, 4).mean(axis=(1, 3))

    out_alpha = blocks[..., 3:4]
    out_alpha_frac = out_alpha / 255.0
    out_rgb = np.where(
        out_alpha_frac > 0,
        blocks[..., :3] / np.maximum(out_alpha_frac, 1e-8),
        0,
    )

    out = np.empty((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    out[..., :3] = np.clip(out_rgb, 0, 255).astype(np.uint8)
    out[..., 3] = np.clip(out_alpha[..., 0], 0, 255).astype(np.uint8)
    return out


def log_layer_summary(z: int, layer: dict[tuple[int, int], bytes]) -> None:
    """One-line summary of a finished pyramid level (count + compressed size)."""
    n = len(layer)
    total = sum(len(b) for b in layer.values())
    avg = total / n if n else 0
    logger.info(
        f"z={z}: {n:,} tiles, {total / 1e9:.2f} GB compressed "
        f"(avg {avg / 1024:.1f} KB/tile)"
    )


CHILD_OFFSETS = [(0, 0), (1, 0), (0, 1), (1, 1)]


def build_parent_tile(
    px: int,
    py: int,
    tl: bytes | None,
    tr: bytes | None,
    bl: bytes | None,
    br: bytes | None,
) -> tuple[int, int, bytes]:
    """Build one parent tile from up to 4 child WebP blobs at the level below."""
    decoded = [decode_webp(b) if b is not None else None for b in (tl, tr, bl, br)]
    merged = downsample_4_to_1(*decoded)
    return px, py, encode_webp_lossless(merged)


def build_parent_level(
    children: dict[tuple[int, int], bytes], z: int
) -> dict[tuple[int, int], bytes]:
    """Build every parent tile at zoom z by 2x2-combining children at z+1.

    Each task carries only its own 4 child blobs, not the whole level dict.
    This keeps joblib from spilling the entire layer to $TMPDIR per round.
    """
    parent_coords = {(cx // 2, cy // 2) for (cx, cy) in children}

    def task(px: int, py: int):
        blobs = [children.get((2 * px + dx, 2 * py + dy)) for dx, dy in CHILD_OFFSETS]
        return delayed(build_parent_tile)(px, py, *blobs)

    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        task(px, py) for px, py in parent_coords
    )
    parents: dict[tuple[int, int], bytes] = {}
    for px, py, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=len(parent_coords), desc=f"Downsampling z={z}", unit=" tiles"
    ):
        parents[(px, py)] = data
    return parents


def write_pmtiles(
    pyramid: dict[int, dict[tuple[int, int], bytes]],
    max_z: int,
    path: Path,
) -> None:
    """Pack all zoom levels into a single PMTiles archive on disk."""
    total = sum(len(layer) for layer in pyramid.values())
    logger.info(f"Writing {total:,} tiles to {path}")

    with open(path, "wb") as f:
        writer = Writer(f)
        with tqdm(total=total, desc="Packing PMTiles", unit=" tiles") as pbar:
            for z in range(max_z + 1):
                for (x, y), data in pyramid[z].items():
                    writer.write_tile(zxy_to_tileid(z, x, y), data)
                    pbar.update(1)

        writer.finalize(
            {
                "tile_type": TileType.WEBP,
                "tile_compression": Compression.NONE,
                "min_zoom": 0,
                "max_zoom": max_z,
                "min_lon_e7": int(-180 * 10**7),  # Just advisory metadata, not used
                "max_lon_e7": int(180 * 10**7),  # Just advisory metadata, not used
                "min_lat_e7": int(-85 * 10**7),  # Just advisory metadata, not used
                "max_lat_e7": int(85 * 10**7),  # Just advisory metadata, not used
                "center_zoom": 0,
                "center_lon_e7": 0,
                "center_lat_e7": 0,
            },
            {},
        )


if __name__ == "__main__":
    logger.info("Rendering graph tiles")

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

    write_pmtiles(pyramid, max_z, TILES_OUTPUT_PATH)
    logger.success(f"Wrote tile pyramid to {TILES_OUTPUT_PATH}")
