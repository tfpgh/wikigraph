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
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

PALETTE_OUTPUT_PATH = Path("intermediates/cluster_palette.parquet")
TILES_OUTPUT_PATH = Path("intermediates/graph_tiles.pmtiles")

WORLD_EXTENT = 2**16
TILE_SIZE = 256

MIN_NODE_TARGET_PX = 1.0
RADIUS_PERCENTILE_FOR_MAX_Z = 0.001

EDGE_ALPHA = 0.2
EDGE_STROKE_WIDTH = 1.0
EDGES_PER_CHUNK = 5_000_000

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


def enrich_edges(edges: pl.DataFrame, nodes_with_color: pl.DataFrame) -> pl.DataFrame:
    """Attach source/target positions and the target's cluster color to each edge.

    Self-loops are dropped. Edges whose endpoints aren't in nodes_with_color
    (orphans / filtered redirects) fall out via inner joins.
    """
    src = nodes_with_color.select(
        pl.col("id").alias("src"),
        pl.col("x").alias("sx"),
        pl.col("y").alias("sy"),
    )
    dst = nodes_with_color.select(
        pl.col("id").alias("dst"),
        pl.col("x").alias("dx"),
        pl.col("y").alias("dy"),
        "r",
        "g",
        "b",
    )
    return (
        edges.filter(pl.col("src") != pl.col("dst"))
        .join(src, on="src", how="inner")
        .join(dst, on="dst", how="inner")
        .select("sx", "sy", "dx", "dy", "r", "g", "b")
    )


def _bucket_edges_chunk(
    chunk: pl.DataFrame, tile_w: float, n_axis: int, half: float
) -> pl.DataFrame:
    """Run the DDA explode/dedupe pipeline on one slice of edges.

    Returns long-form tile-edge rows (one per chunk-edge × tile pair) with
    columns: tx, ty, sx, sy, dx, dy, er, eg, eb. The chunk-local edge id used
    for dedup is dropped from the output so chunks can concat cleanly.
    """
    return (
        chunk.with_row_index("cid")
        .with_columns(
            ((pl.col("sx") + half) / tile_w).floor().cast(pl.Int32).alias("src_tx"),
            ((pl.col("sy") + half) / tile_w).floor().cast(pl.Int32).alias("src_ty"),
            ((pl.col("dx") + half) / tile_w).floor().cast(pl.Int32).alias("dst_tx"),
            ((pl.col("dy") + half) / tile_w).floor().cast(pl.Int32).alias("dst_ty"),
        )
        .with_columns(
            (
                pl.max_horizontal(
                    (pl.col("src_tx") - pl.col("dst_tx")).abs(),
                    (pl.col("src_ty") - pl.col("dst_ty")).abs(),
                )
                * 2
                + 1
            )
            .cast(pl.Int32)
            .alias("n_steps")
        )
        # Drop endpoint-tile columns before exploding — they'd otherwise be
        # duplicated n_steps times per edge and blow up the intermediate.
        .select("cid", "sx", "sy", "dx", "dy", "r", "g", "b", "n_steps")
        .with_columns(pl.int_ranges(0, pl.col("n_steps") + 1).alias("step"))
        .explode("step")
        .with_columns(
            (
                pl.col("step").cast(pl.Float64) / pl.col("n_steps").cast(pl.Float64)
            ).alias("t")
        )
        .with_columns(
            (
                (pl.col("sx") + pl.col("t") * (pl.col("dx") - pl.col("sx")) + half)
                / tile_w
            )
            .floor()
            .cast(pl.Int32)
            .alias("tx"),
            (
                (pl.col("sy") + pl.col("t") * (pl.col("dy") - pl.col("sy")) + half)
                / tile_w
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
        .group_by(["cid", "tx", "ty"], maintain_order=False)
        .agg(
            pl.first("sx"),
            pl.first("sy"),
            pl.first("dx"),
            pl.first("dy"),
            pl.first("r").alias("er"),
            pl.first("g").alias("eg"),
            pl.first("b").alias("eb"),
        )
        .select(["tx", "ty", "sx", "sy", "dx", "dy", "er", "eg", "eb"])
    )


def bucket_edges_by_tile(edges: pl.DataFrame, max_z: int) -> pl.DataFrame:
    """Group edges by every z=MAX tile their line segment crosses.

    Uses a 2x-oversampled DDA-style traversal: sample the parametric line at
    n_steps = 2 * max(|dtx|, |dty|) + 1 points and floor each to a tile coord.
    Dedupe per (edge, tile). Bbox-explode would overshoot diagonals by orders
    of magnitude — DDA stays proportional to line length in tiles.

    Processes edges in chunks of EDGES_PER_CHUNK to cap peak memory: with
    200M+ edges, the global explode produces billions of intermediate rows
    that won't fit at once even on a 1.5TB host. Each chunk independently
    buckets and dedupes; results are concatenated and a single final group_by
    collapses them into one list-row per tile.
    """
    tile_w = WORLD_EXTENT / (2**max_z)
    n_axis = 2**max_z
    half = WORLD_EXTENT / 2

    n_chunks = (len(edges) + EDGES_PER_CHUNK - 1) // EDGES_PER_CHUNK
    logger.info(
        f"Bucketing {len(edges):,} edges in {n_chunks:,} chunks of {EDGES_PER_CHUNK:,}"
    )

    membership_chunks: list[pl.DataFrame] = []
    total_rows = 0
    edges_seen = 0
    pbar = tqdm(
        range(0, len(edges), EDGES_PER_CHUNK),
        total=n_chunks,
        desc="Bucketing edges",
        unit=" chunks",
    )
    for start in pbar:
        chunk_result = _bucket_edges_chunk(
            edges.slice(start, EDGES_PER_CHUNK), tile_w, n_axis, half
        )
        total_rows += len(chunk_result)
        edges_seen = min(start + EDGES_PER_CHUNK, len(edges))
        membership_chunks.append(chunk_result)
        pbar.set_postfix(
            rows=f"{total_rows:,}",
            tiles_per_edge=f"{total_rows / edges_seen:.2f}",
        )

    logger.info(
        f"Bucketed {len(edges):,} edges into {total_rows:,} tile-edge rows "
        f"(avg {total_rows / len(edges):.2f} tiles/edge)"
    )

    logger.info("Collapsing chunks into per-tile list rows")
    result = (
        pl.concat(membership_chunks)
        .group_by(["tx", "ty"], maintain_order=False)
        .agg("sx", "sy", "dx", "dy", "er", "eg", "eb")
    )
    logger.info(
        f"Edge buckets: {len(result):,} tiles "
        f"(avg {total_rows / max(len(result), 1):.1f} edges/tile)"
    )
    return result


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


NodeData = tuple[list[float], list[float], list[float], list[int], list[int], list[int]]
EdgeData = tuple[
    list[float],
    list[float],
    list[float],
    list[float],
    list[int],
    list[int],
    list[int],
]


def render_max_tile(
    tx: int,
    ty: int,
    max_z: int,
    node_data: NodeData | None,
    edge_data: EdgeData | None,
) -> tuple[int, int, bytes]:
    """Render one z=MAX tile to lossless WebP bytes.

    Edges are drawn first at EDGE_ALPHA behind nodes; nodes opaque on top.
    Both are grouped by color in Python so we issue one batched SkPath draw
    per color, not per primitive. Background is transparent — the frontend
    composites it onto whatever background it wants.
    """
    surface = skia.Surface(TILE_SIZE, TILE_SIZE)
    canvas = surface.getCanvas()
    canvas.clear(skia.Color4f(0, 0, 0, 0))

    ppwu = TILE_SIZE * (2**max_z) / WORLD_EXTENT
    origin_x = tx * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2
    origin_y = ty * WORLD_EXTENT / (2**max_z) - WORLD_EXTENT / 2
    edge_alpha_byte = int(EDGE_ALPHA * 255)

    if edge_data is not None:
        sxs, sys, dxs, dys, ereds, egreens, eblues = edge_data
        edge_paths: dict[tuple[int, int, int], skia.Path] = {}
        for sx, sy, dx, dy, red, green, blue in zip(
            sxs, sys, dxs, dys, ereds, egreens, eblues
        ):
            color = (red, green, blue)
            path = edge_paths.get(color)
            if path is None:
                path = skia.Path()
                edge_paths[color] = path
            path.moveTo((sx - origin_x) * ppwu, (sy - origin_y) * ppwu)
            path.lineTo((dx - origin_x) * ppwu, (dy - origin_y) * ppwu)

        for (red, green, blue), path in edge_paths.items():
            canvas.drawPath(
                path,
                skia.Paint(
                    AntiAlias=True,
                    Style=skia.Paint.kStroke_Style,
                    StrokeWidth=EDGE_STROKE_WIDTH,
                    Color=skia.ColorSetARGB(edge_alpha_byte, red, green, blue),
                ),
            )

    if node_data is not None:
        xs, ys, radii, nreds, ngreens, nblues = node_data
        node_paths: dict[tuple[int, int, int], skia.Path] = {}
        for x, y, r, red, green, blue in zip(xs, ys, radii, nreds, ngreens, nblues):
            color = (red, green, blue)
            path = node_paths.get(color)
            if path is None:
                path = skia.Path()
                node_paths[color] = path
            path.addCircle((x - origin_x) * ppwu, (y - origin_y) * ppwu, r * ppwu)

        for (red, green, blue), path in node_paths.items():
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

    nodes_with_color = nodes.join(palette, on="partition", how="inner")
    nodes_bucketed = bucket_nodes_by_tile(nodes_with_color, max_z)
    logger.info(f"{len(nodes_bucketed):,} z={max_z} tiles contain at least one node")

    edges = pl.read_parquet(EDGES_INPUT_PATH)
    logger.info(f"Loaded {len(edges):,} edges")
    edges_enriched = enrich_edges(edges, nodes_with_color)
    del edges
    logger.info(
        f"Enriched edges: {len(edges_enriched):,} after dropping self-loops and orphans"
    )
    edges_bucketed = bucket_edges_by_tile(edges_enriched, max_z)
    del edges_enriched
    logger.info(f"{len(edges_bucketed):,} z={max_z} tiles contain at least one edge")

    tiles = nodes_bucketed.join(
        edges_bucketed, on=["tx", "ty"], how="full", coalesce=True
    )
    logger.info(f"{len(tiles):,} z={max_z} tiles total (node ∪ edge)")

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}

    def dispatch_tiles():
        for row in tiles.iter_rows():
            tx, ty = row[0], row[1]
            n_xs = row[2]
            node_data = (
                (row[2], row[3], row[4], row[5], row[6], row[7])
                if n_xs is not None
                else None
            )
            e_sxs = row[8]
            edge_data = (
                (row[8], row[9], row[10], row[11], row[12], row[13], row[14])
                if e_sxs is not None
                else None
            )
            yield delayed(render_max_tile)(tx, ty, max_z, node_data, edge_data)

    logger.info(f"Rendering z={max_z} (max zoom)")
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        dispatch_tiles()
    )
    pyramid[max_z] = {}
    for tx, ty, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=len(tiles), desc=f"Rendering z={max_z}", unit=" tiles"
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
