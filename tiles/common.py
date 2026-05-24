import io
import math
from pathlib import Path

import numpy as np
import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from PIL import Image as PILImage
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from tqdm import tqdm

WORLD_EXTENT = 2**16
TILE_SIZE = 256

# Edge stroke width in world units. Defined zoom-invariant so changing max_z
# later doesn't change the apparent edge thickness at lower zoom levels.
EDGE_WIDTH_WORLD = 0.5

# p-norm exponent for the alpha channel during 2x2 downsample (p=1 is plain
# mean). p>1 boosts sparse features so a single bright child pixel survives
# many levels of downsampling in 8-bit alpha instead of quantizing to zero.
# Affects alpha only; RGB stays alpha-weighted mean so colors don't shift.
P_NORM_ALPHA = 4.0

# Max zoom is picked so the small-radius percentile of nodes is at least
# MIN_NODE_TARGET_PX pixels at that zoom.
MIN_NODE_TARGET_PX = 0.5
RADIUS_PERCENTILE_FOR_MAX_Z = 0.001


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
