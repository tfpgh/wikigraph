import io
import math
from pathlib import Path

import numpy as np
import polars as pl
from loguru import logger
from PIL import Image as PILImage
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer
from tqdm import tqdm

WORLD_EXTENT = 2**16
TILE_SIZE = 1024

# Max zoom is picked so the small-radius percentile of nodes is at least
# MIN_NODE_TARGET_PX pixels at that zoom.
MIN_NODE_TARGET_PX = 10.0
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


def write_pmtiles(
    pyramid: dict[int, dict[tuple[int, int], bytes]],
    max_z: int,
    path: Path,
    tile_type: TileType = TileType.WEBP,
    tile_compression: Compression = Compression.NONE,
) -> None:
    """Pack all zoom levels into a single PMTiles archive on disk.

    Defaults pack lossless-WebP raster tiles uncompressed (the node/edge
    pyramids). Pass tile_type=TileType.UNKNOWN with tile_compression=GZIP for
    the gzipped-JSON metadata archive — the tile bytes must already be gzipped;
    tile_compression only tells the client how to decode them.
    """
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
                "tile_type": tile_type,
                "tile_compression": tile_compression,
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
