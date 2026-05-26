"""Build the node metadata that drives the interactive frontend.

Three artifacts land in output/:

1. node_meta.pmtiles — the tile pyramid. For each (z, x, y) tile we emit a
   gzipped JSON array of the nodes whose radius reaches NODE_META_MIN_PX pixels
   at that zoom *and whose center falls inside the tile*. Center-only placement
   means exactly one entry per node per zoom level (no replication across the
   tiles a big node spans). Each entry is self-contained — geometry, label,
   cluster, pagerank rank, and the node's full in/out adjacency — so the
   frontend never makes a per-node fetch on hover. Neighbor entries carry their
   own position, radius, and cluster id; the frontend resolves cluster id to a
   color via meta.json.

   Tile entry shape (keys kept short; coords rounded to COORD_DECIMALS):
       {
         "id": 12345, "t": "United States",
         "x": 32100.5, "y": 18000.2, "r": 40.0,
         "cl": 7, "pr": 1,
         "out": [[x, y, r, cl], ...],   # every outlink, no cap
         "in":  [[x, y, r, cl], ...]    # every inlink, no cap
       }

2. pages.pmtiles — one entry per page, addressed by mapping the dense page id
   into a (z, x, y) slot. There's no tile-pyramid meaning here: we pick the
   smallest zoom Z whose grid holds every page, then pack id -> tileid densely
   along the PMTiles Hilbert order (tileid = base(Z) + id). Each entry carries
   the page plus its full adjacency, with neighbor id and title included so the
   frontend can render a link panel without a tile lookup.

   Page entry shape:
       {
         "id": 12345, "t": "United States", "cl": 7, "pr": 1,
         "out": [[id, t, x, y, r, cl], ...],
         "in":  [[id, t, x, y, r, cl], ...]
       }

3. meta.json — overarching stats: total page count, total link count, and the
   cluster table (id, color, count, name). name is seeded with the title of the
   cluster's highest-pagerank page, to be hand-edited later.
"""

import gzip
import json
import math
from pathlib import Path

import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from pmtiles.tile import Compression, TileType, tileid_to_zxy
from tqdm import tqdm

from tiles.common import (
    TILE_SIZE,
    WORLD_EXTENT,
    compute_max_zoom,
    write_pmtiles,
)
from tiles.palette import compute_palette

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

META_TILES_OUTPUT_PATH = Path("output/node_meta.pmtiles")
PAGES_OUTPUT_PATH = Path("output/pages.pmtiles")
META_JSON_OUTPUT_PATH = Path("output/meta.json")

# A node gets metadata in a tile only once its radius reaches this many pixels
# at that zoom — keeps tiles small and matches "interactive only when big
# enough." radius_px = radius * TILE_SIZE * 2^z / WORLD_EXTENT.
NODE_META_MIN_PX = 5.0

# World-coordinate rounding in the JSON. 0.01 world units is well under a pixel
# even at max zoom, and trims float repr bloat.
COORD_DECIMALS = 2


def build_records(nodes: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
    """Assemble one self-contained metadata record per node.

    Returns columns: id, t, x, y, radius, cl (cluster id), pr (pagerank rank,
    1 = highest), out and inn (list[struct] of full neighbor adjacency). Each
    neighbor struct holds nid, nt, nx, ny, nr, ncl — the same shape for both
    directions so the two encoders can project whichever fields they need.
    """
    attrs = nodes.select(
        "id",
        "title",
        pl.col("x").round(COORD_DECIMALS),
        pl.col("y").round(COORD_DECIMALS),
        pl.col("radius").round(COORD_DECIMALS),
        pl.col("partition"),
        pl.col("pagerank")
        .rank(method="ordinal", descending=True)
        .cast(pl.Int64)
        .alias("pr"),
    )

    # Neighbor attributes, keyed once per direction by the join column. id is
    # aliased twice (join key + nid) so the struct keeps the neighbor's own id.
    def neighbor(join_col: str) -> pl.DataFrame:
        return attrs.select(
            pl.col("id").alias(join_col),
            pl.col("id").alias("nid"),
            pl.col("title").alias("nt"),
            pl.col("x").alias("nx"),
            pl.col("y").alias("ny"),
            pl.col("radius").alias("nr"),
            pl.col("partition").alias("ncl"),
        )

    nb_struct = pl.struct("nid", "nt", "nx", "ny", "nr", "ncl")

    # Outgoing: for each src, all dst neighbors (full destination attributes).
    out_adj = (
        edges.join(neighbor("dst"), on="dst", how="inner")
        .group_by("src")
        .agg(nb_struct.alias("out"))
        .rename({"src": "id"})
    )

    # Incoming: for each dst, all src neighbors (full source attributes).
    in_adj = (
        edges.join(neighbor("src"), on="src", how="inner")
        .group_by("dst")
        .agg(nb_struct.alias("inn"))
        .rename({"dst": "id"})
    )

    return (
        attrs.select(
            "id",
            pl.col("title").alias("t"),
            "x",
            "y",
            "radius",
            pl.col("partition").alias("cl"),
            "pr",
        )
        .join(out_adj, on="id", how="left")
        .join(in_adj, on="id", how="left")
    )


def bucket_meta_tiles(records: pl.DataFrame, z: int) -> pl.DataFrame:
    """Filter to threshold-passing nodes at zoom z and group them by tile.

    Placement is by node center, so each node lands in exactly one tile. Returns
    one row per non-empty tile: (tx, ty, recs) where recs is a list of the full
    node-record structs.
    """
    ppwu = TILE_SIZE * (2**z) / WORLD_EXTENT
    visible = records.filter(pl.col("radius") * ppwu >= NODE_META_MIN_PX)
    if visible.is_empty():
        # 0-row frame. (Don't select() only literals onto an empty frame — that
        # broadcasts them into a phantom 1-row frame.)
        return pl.DataFrame(
            schema={"tx": pl.Int32, "ty": pl.Int32, "recs": pl.List(pl.Int64)}
        )

    tile_w = WORLD_EXTENT / (2**z)
    n_axis = 2**z

    rec = pl.struct("id", "t", "x", "y", "radius", "cl", "pr", "out", "inn").alias("rec")
    return (
        visible.with_columns(
            ((pl.col("x") + WORLD_EXTENT / 2) / tile_w)
            .floor()
            .cast(pl.Int32)
            .alias("tx"),
            ((pl.col("y") + WORLD_EXTENT / 2) / tile_w)
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
        .with_columns(rec)
        .group_by(["tx", "ty"], maintain_order=False)
        .agg(pl.col("rec").alias("recs"))
    )


def encode_tile(tx: int, ty: int, recs: list[dict]) -> tuple[int, int, bytes]:
    """Reshape a tile's node records into compact JSON and gzip them."""
    entries = []
    for rec in recs:
        out = [[n["nx"], n["ny"], n["nr"], n["ncl"]] for n in (rec["out"] or [])]
        inn = [[n["nx"], n["ny"], n["nr"], n["ncl"]] for n in (rec["inn"] or [])]
        entries.append(
            {
                "id": rec["id"],
                "t": rec["t"],
                "x": rec["x"],
                "y": rec["y"],
                "r": rec["radius"],
                "cl": rec["cl"],
                "pr": rec["pr"],
                "out": out,
                "in": inn,
            }
        )
    data = json.dumps(entries, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    return tx, ty, gzip.compress(data, compresslevel=6)


def build_layer(records: pl.DataFrame, z: int) -> dict[tuple[int, int], bytes]:
    """Bucket and encode every metadata tile at zoom z."""
    bucketed = bucket_meta_tiles(records, z)
    n_tiles = len(bucketed)
    if n_tiles == 0:
        logger.info(f"z={z}: no nodes above {NODE_META_MIN_PX}px, skipping")
        return {}

    layer: dict[tuple[int, int], bytes] = {}
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(encode_tile)(tx, ty, recs) for tx, ty, recs in bucketed.iter_rows()
    )
    for tx, ty, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=n_tiles, desc=f"Encoding z={z}", unit=" tiles"
    ):
        layer[(tx, ty)] = data

    total_bytes = sum(len(b) for b in layer.values())
    logger.info(
        f"z={z}: {n_tiles:,} tiles, {total_bytes / 1e6:.1f} MB gzipped "
        f"(avg {total_bytes / n_tiles / 1024:.1f} KB/tile)"
    )
    return layer


def encode_page(
    rid: int,
    t: str,
    cl: int,
    pr: int,
    out: list[dict] | None,
    inn: list[dict] | None,
    base: int,
) -> tuple[int, int, bytes]:
    """Encode one page's full record to gzipped JSON, addressed by its id.

    The page id is mapped to a (z, x, y) slot via the PMTiles Hilbert order:
    tileid = base + id, where base is the first tileid at the packing zoom.
    """
    _, x, y = tileid_to_zxy(base + rid)
    entry = {
        "id": rid,
        "t": t,
        "cl": cl,
        "pr": pr,
        "out": [
            [n["nid"], n["nt"], n["nx"], n["ny"], n["nr"], n["ncl"]]
            for n in (out or [])
        ],
        "in": [
            [n["nid"], n["nt"], n["nx"], n["ny"], n["nr"], n["ncl"]]
            for n in (inn or [])
        ],
    }
    data = json.dumps(entry, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return x, y, gzip.compress(data, compresslevel=6)


def build_page_archive(
    records: pl.DataFrame,
) -> tuple[dict[int, dict[tuple[int, int], bytes]], int]:
    """Pack one JSON entry per page into a single-zoom PMTiles pyramid.

    Picks the smallest zoom Z whose 2^Z × 2^Z grid holds every page, then writes
    pages in id order so tileids stay ascending (clustered archive).
    """
    n = len(records)
    z = max(1, math.ceil(math.log2(n) / 2))  # 4^z >= n
    base = (4**z - 1) // 3  # first tileid at zoom z
    logger.info(f"Packing {n:,} pages at z={z} ({2**z:,} per side, base tileid {base:,})")

    sel = records.sort("id").select("id", "t", "cl", "pr", "out", "inn")
    layer: dict[tuple[int, int], bytes] = {}
    results = Parallel(n_jobs=-1, return_as="generator", backend="loky")(
        delayed(encode_page)(rid, t, cl, pr, out, inn, base)
        for rid, t, cl, pr, out, inn in sel.iter_rows()
    )
    for x, y, data in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=n, desc="Encoding pages", unit=" pages"
    ):
        layer[(x, y)] = data

    total_bytes = sum(len(b) for b in layer.values())
    logger.info(f"Page archive: {n:,} pages, {total_bytes / 1e6:.1f} MB gzipped")

    pyramid: dict[int, dict[tuple[int, int], bytes]] = {zz: {} for zz in range(z + 1)}
    pyramid[z] = layer
    return pyramid, z


def build_meta_json(
    nodes: pl.DataFrame, edges: pl.DataFrame, palette: pl.DataFrame
) -> dict:
    """Assemble the overarching meta.json: totals + the cluster table.

    Each cluster's name is seeded with the title of its highest-pagerank page.
    """
    counts = nodes.group_by("partition").agg(pl.len().alias("count"))
    names = (
        nodes.sort("pagerank", descending=True)
        .group_by("partition", maintain_order=True)
        .agg(pl.col("title").first().alias("name"))
    )

    clusters = (
        counts.join(names, on="partition", how="inner")
        .join(palette, on="partition", how="inner")
        .sort("count", descending=True)
    )

    cluster_list = [
        {
            "id": int(row["partition"]),
            "color": [int(row["r"]), int(row["g"]), int(row["b"])],
            "count": int(row["count"]),
            "name": row["name"],
        }
        for row in clusters.iter_rows(named=True)
    ]

    return {
        "total_pages": len(nodes),
        "total_links": len(edges),
        "clusters": cluster_list,
    }


if __name__ == "__main__":
    logger.info("Building node metadata → output/")

    nodes = pl.read_parquet(NODES_INPUT_PATH)
    logger.info(f"Loaded {len(nodes):,} nodes")

    edges = pl.read_parquet(EDGES_INPUT_PATH)
    logger.info(f"Loaded {len(edges):,} edges")

    palette = compute_palette(nodes["partition"])

    max_z = compute_max_zoom(nodes["radius"])

    records = build_records(nodes, edges)
    logger.success(f"Built {len(records):,} node records (full in/out adjacency)")

    # 1. Tile pyramid.
    pyramid: dict[int, dict[tuple[int, int], bytes]] = {}
    for z in range(max_z + 1):
        pyramid[z] = build_layer(records, z)

    total_tiles = sum(len(layer) for layer in pyramid.values())
    total_bytes = sum(sum(len(b) for b in layer.values()) for layer in pyramid.values())
    logger.info(
        f"Metadata pyramid: {total_tiles:,} tiles, {total_bytes / 1e6:.1f} MB gzipped"
    )

    write_pmtiles(
        pyramid,
        max_z,
        META_TILES_OUTPUT_PATH,
        tile_type=TileType.UNKNOWN,
        tile_compression=Compression.GZIP,
    )
    logger.success(f"Wrote metadata pyramid to {META_TILES_OUTPUT_PATH}")

    # 2. Per-page archive.
    page_pyramid, page_z = build_page_archive(records)
    write_pmtiles(
        page_pyramid,
        page_z,
        PAGES_OUTPUT_PATH,
        tile_type=TileType.UNKNOWN,
        tile_compression=Compression.GZIP,
    )
    logger.success(f"Wrote per-page archive to {PAGES_OUTPUT_PATH}")

    # 3. Overarching meta.json.
    meta = build_meta_json(nodes, edges, palette)
    META_JSON_OUTPUT_PATH.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.success(
        f"Wrote meta.json ({len(meta['clusters']):,} clusters) to "
        f"{META_JSON_OUTPUT_PATH}"
    )
