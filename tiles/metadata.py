"""Build the node metadata pyramid: per-tile JSON bundled into a PMTiles archive.

For each (z, x, y) tile we emit a gzipped JSON array of the nodes that are at
least NODE_META_MIN_PX pixels in radius at that zoom and whose circle touches
the tile. Each entry is self-contained — geometry, label, the node's color, and
its capped in/out adjacency — so the frontend never makes a per-node fetch on
hover. It drives: outline (x, y, r), name + click-through link (title), hit-test
(x, y, r), and edge highlighting (out neighbors in their target color, in
neighbors in white).

Adjacency is capped to the TOP_K most important neighbors per direction (by the
neighbor's pagerank), with the true totals kept so the frontend can show
"+N more". Entries are replicated into every tile a node spans (placement A):
only nodes larger than a full tile span more than one, so the duplication is
concentrated on the few biggest hubs — bounded by the top-K cap.

Tile entry shape (keys kept short; coords rounded to COORD_DECIMALS):
    {
      "id": 12345, "t": "United States",
      "x": 32100.5, "y": 18000.2, "r": 40.0, "c": [r, g, b],
      "out": [[x, y, r, g, b], ...<=TOP_K],   # target pos + target color
      "in":  [[x, y], ...<=TOP_K],            # source pos (drawn white)
      "no": 1503, "ni": 28411                 # true out/in degree
    }
"""

import gzip
import json
from pathlib import Path

import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from pmtiles.tile import Compression, TileType
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

META_TILES_OUTPUT_PATH = Path("intermediates/node_meta.pmtiles")

# A node gets metadata in a tile only once its radius reaches this many pixels
# at that zoom — keeps tiles small and matches "interactive only when big
# enough." radius_px = radius * TILE_SIZE * 2^z / WORLD_EXTENT.
NODE_META_MIN_PX = 6.0

# Cap on in/out neighbors kept per node, ranked by the neighbor's pagerank.
TOP_K = 100

# World-coordinate rounding in the JSON. 0.01 world units is well under a pixel
# even at max zoom, and trims float repr bloat.
COORD_DECIMALS = 2


def build_records(nodes: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
    """Assemble one self-contained metadata record per node.

    Returns columns: id, t, x, y, radius, c (list[u8] rgb), out (list[struct]),
    inn (list[struct]), no, ni — plus radius kept separate for thresholding.
    """
    palette = compute_palette(nodes["partition"])
    attrs = nodes.join(palette, on="partition", how="inner").select(
        "id",
        "title",
        pl.col("x").round(COORD_DECIMALS),
        pl.col("y").round(COORD_DECIMALS),
        pl.col("radius").round(COORD_DECIMALS),
        "pagerank",
        "r",
        "g",
        "b",
    )

    # Outgoing: for each src, its top-K dst neighbors (target pos + target color).
    out_adj = (
        edges.join(
            attrs.select(
                pl.col("id").alias("dst"),
                pl.col("x").alias("dx"),
                pl.col("y").alias("dy"),
                pl.col("pagerank").alias("dpr"),
                pl.col("r").alias("cr"),
                pl.col("g").alias("cg"),
                pl.col("b").alias("cb"),
            ),
            on="dst",
            how="inner",
        )
        .group_by("src")
        .agg(
            pl.len().alias("no"),
            pl.struct("dx", "dy", "cr", "cg", "cb").top_k_by("dpr", TOP_K).alias("out"),
        )
        .rename({"src": "id"})
    )

    # Incoming: for each dst, its top-K src neighbors (source pos only — white).
    in_adj = (
        edges.join(
            attrs.select(
                pl.col("id").alias("src"),
                pl.col("x").alias("sx"),
                pl.col("y").alias("sy"),
                pl.col("pagerank").alias("spr"),
            ),
            on="src",
            how="inner",
        )
        .group_by("dst")
        .agg(
            pl.len().alias("ni"),
            pl.struct("sx", "sy").top_k_by("spr", TOP_K).alias("inn"),
        )
        .rename({"dst": "id"})
    )

    return (
        attrs.select(
            "id",
            pl.col("title").alias("t"),
            "x",
            "y",
            "radius",
            pl.concat_list("r", "g", "b").alias("c"),
        )
        .join(out_adj, on="id", how="left")
        .join(in_adj, on="id", how="left")
        .with_columns(pl.col("no").fill_null(0), pl.col("ni").fill_null(0))
    )


def bucket_meta_tiles(records: pl.DataFrame, z: int) -> pl.DataFrame:
    """Filter to threshold-passing nodes at zoom z and group records by tile.

    Returns one row per non-empty tile: (tx, ty, recs) where recs is a list of
    the full node-record structs. Explodes on a narrow (id, tx, ty) frame and
    joins the heavy record back afterward so the int-range explode stays cheap.
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

    tiles = (
        visible.select("id", "x", "y", "radius")
        .with_columns(
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
        .select("id", "tx", "ty")
    )

    rec = pl.struct("id", "t", "x", "y", "radius", "c", "out", "inn", "no", "ni").alias(
        "rec"
    )
    return (
        tiles.join(visible.with_columns(rec).select("id", "rec"), on="id", how="inner")
        .group_by(["tx", "ty"], maintain_order=False)
        .agg(pl.col("rec").alias("recs"))
    )


def encode_tile(tx: int, ty: int, recs: list[dict]) -> tuple[int, int, bytes]:
    """Reshape a tile's node records into compact JSON and gzip them."""
    entries = []
    for rec in recs:
        out = [
            [o["dx"], o["dy"], o["cr"], o["cg"], o["cb"]] for o in (rec["out"] or [])
        ]
        inn = [[i["sx"], i["sy"]] for i in (rec["inn"] or [])]
        entries.append(
            {
                "id": rec["id"],
                "t": rec["t"],
                "x": rec["x"],
                "y": rec["y"],
                "r": rec["radius"],
                "c": rec["c"],
                "out": out,
                "in": inn,
                "no": rec["no"],
                "ni": rec["ni"],
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


if __name__ == "__main__":
    logger.info(f"Building node metadata tiles → {META_TILES_OUTPUT_PATH}")

    nodes = pl.read_parquet(NODES_INPUT_PATH)
    logger.info(f"Loaded {len(nodes):,} nodes")

    edges = pl.read_parquet(EDGES_INPUT_PATH)
    logger.info(f"Loaded {len(edges):,} edges")

    max_z = compute_max_zoom(nodes["radius"])

    records = build_records(nodes, edges)
    logger.success(f"Built {len(records):,} node records (top-{TOP_K} in/out)")

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
