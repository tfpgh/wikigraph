"""Build the directed graph in CSR form for the shortest-path backend.

Emits a single flat little-endian binary, `output/graph.csr`, holding both a
forward adjacency (out-edges, expanded from the source) and a reverse adjacency
(in-edges, expanded from the target) so the Rust service can run bidirectional
BFS. Node ids are the dense 0..N-1 ids assigned in extract_graph.py; both ids
and offsets fit in u32 (N≈7.7M, E≈228M, < 2^32).

File layout (all u32, little-endian):
    magic   "WGCS"                (4 bytes)
    version 1                     (u32)
    n_nodes                       (u32)
    n_edges                       (u32)
    fwd_offsets   n_nodes + 1     (u32)   prefix sums into fwd_neighbors
    fwd_neighbors n_edges         (u32)   out-neighbors, grouped by source
    rev_offsets   n_nodes + 1     (u32)
    rev_neighbors n_edges         (u32)   in-neighbors, grouped by target

Neighbor lists are sorted ascending within each node (lexsort secondary key),
which the backend doesn't require but keeps the file deterministic.
"""

import struct
from pathlib import Path

import numpy as np
import polars as pl
from loguru import logger

NODES_INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
EDGES_INPUT_PATH = Path("intermediates/extracted_edges.parquet")

CSR_OUTPUT_PATH = Path("output/graph.csr")

MAGIC = b"WGCS"
VERSION = 1


def build_csr(
    keys: np.ndarray, vals: np.ndarray, n_nodes: int
) -> tuple[np.ndarray, np.ndarray]:
    """Group `vals` by `keys` into (offsets, neighbors), sorted within each group.

    offsets has length n_nodes+1; neighbors[offsets[i]:offsets[i+1]] are the
    values for node i. Both returned as u32.
    """
    # lexsort orders by keys first, then vals — so each node's neighbor run is
    # contiguous and ascending.
    order = np.lexsort((vals, keys))
    neighbors = vals[order].astype(np.uint32)

    counts = np.bincount(keys, minlength=n_nodes)
    offsets = np.zeros(n_nodes + 1, dtype=np.uint32)
    offsets[1:] = np.cumsum(counts)  # cumsum <= n_edges < 2^32, safe in u32
    return offsets, neighbors


if __name__ == "__main__":
    logger.info("Building directed CSR for the shortest-path backend")

    n_nodes = (
        int(pl.read_parquet(NODES_INPUT_PATH, columns=["id"])["id"].max())  # pyright: ignore[reportArgumentType]
        + 1
    )
    logger.info(f"n_nodes = {n_nodes:,}")

    edges = pl.read_parquet(EDGES_INPUT_PATH, columns=["src", "dst"])
    src = edges["src"].to_numpy().astype(np.int64)
    dst = edges["dst"].to_numpy().astype(np.int64)
    n_edges = src.shape[0]
    logger.info(f"n_edges = {n_edges:,}")

    assert src.max() < n_nodes and dst.max() < n_nodes, "edge id exceeds n_nodes"
    assert n_edges < 2**32, "n_edges exceeds u32"

    logger.info("Building forward adjacency (out-edges)")
    fwd_off, fwd_nbr = build_csr(src, dst, n_nodes)

    logger.info("Building reverse adjacency (in-edges)")
    rev_off, rev_nbr = build_csr(dst, src, n_nodes)

    CSR_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing {CSR_OUTPUT_PATH}")
    with open(CSR_OUTPUT_PATH, "wb") as f:
        f.write(MAGIC)
        f.write(struct.pack("<III", VERSION, n_nodes, n_edges))
        for arr in (fwd_off, fwd_nbr, rev_off, rev_nbr):
            f.write(np.ascontiguousarray(arr, dtype="<u4").tobytes())

    size_gb = CSR_OUTPUT_PATH.stat().st_size / 1e9
    logger.success(
        f"Wrote {CSR_OUTPUT_PATH} ({size_gb:.2f} GB): "
        f"{n_nodes:,} nodes, {n_edges:,} edges × 2 directions"
    )
