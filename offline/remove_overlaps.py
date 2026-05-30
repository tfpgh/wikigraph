from pathlib import Path

import numpy as np
import polars as pl
from loguru import logger

INPUT_PATH = Path("intermediates/initial_enriched_nodes.parquet")
OUTPUT_PATH = Path("intermediates/enriched_nodes.parquet")

DAMPING = 1.5

SEPARATION_PADDING = 0.02

MAX_ITERS = 500


def candidate_pairs(
    x: np.ndarray, y: np.ndarray, r: np.ndarray, cell: float
) -> tuple[np.ndarray, np.ndarray]:
    """Unique (i < j) node pairs whose bounding boxes share a grid cell.

    A superset of the overlapping pairs (overlapping circles ⇒ overlapping
    bboxes ⇒ a shared cell, for any cell size). The caller applies the exact
    circle test.
    """
    n = len(x)
    base = pl.DataFrame(
        {"i": np.arange(n, dtype=np.int64), "x": x, "y": y, "r": r}
    ).with_columns(
        ((pl.col("x") - pl.col("r")) / cell).floor().cast(pl.Int32).alias("cx_min"),
        ((pl.col("x") + pl.col("r")) / cell).floor().cast(pl.Int32).alias("cx_max"),
        ((pl.col("y") - pl.col("r")) / cell).floor().cast(pl.Int32).alias("cy_min"),
        ((pl.col("y") + pl.col("r")) / cell).floor().cast(pl.Int32).alias("cy_max"),
    )
    exploded = (
        base.with_columns(
            pl.int_ranges(pl.col("cx_min"), pl.col("cx_max") + 1).alias("cx")
        )
        .explode("cx")
        .with_columns(pl.int_ranges(pl.col("cy_min"), pl.col("cy_max") + 1).alias("cy"))
        .explode("cy")
        .select(["i", "cx", "cy"])
    )
    pairs = (
        exploded.join(exploded, on=["cx", "cy"], suffix="_j")
        .filter(pl.col("i") < pl.col("i_j"))
        .select("i", pl.col("i_j").alias("j"))
        .unique()
    )
    return pairs["i"].to_numpy(), pairs["j"].to_numpy()


def relax(x: np.ndarray, y: np.ndarray, r: np.ndarray) -> None:
    """Push overlapping nodes apart in place until none overlap."""
    n = len(x)

    # The target separation per pair includes the padding, so size the detection
    # grid (and the bbox explode) to the padded radii — no near pair is missed.
    r_pad = r * (1.0 + SEPARATION_PADDING)
    # Small enough that most nodes sit alone in a cell, but floored so the
    # biggest node spans at most ~64 cells across its diameter.
    cell = max(2.0 * float(np.median(r_pad)), float(r_pad.max()) / 32.0)
    logger.info(
        f"{n:,} nodes, grid cell = {cell:.3f} "
        f"(r median={np.median(r):.3f}, max={r.max():.3f}), "
        f"damping={DAMPING}, padding={SEPARATION_PADDING:.0%}"
    )

    n_overlap = 0
    for it in range(1, MAX_ITERS + 1):
        i, j = candidate_pairs(x, y, r_pad, cell)
        dx = x[i] - x[j]
        dy = y[i] - y[j]
        d = np.hypot(dx, dy)

        # True overlap drives the stop condition; the padded penetration drives
        # the push, so nodes are nudged a hair past touching.
        true_pen = (r[i] + r[j]) - d
        n_overlap = int((true_pen > 0).sum())
        if n_overlap == 0:
            logger.success(f"iter {it}: no overlaps remain")
            return
        logger.info(
            f"iter {it}: {n_overlap:,} overlaps, max pen = {true_pen.max():.4f}"
        )

        pad_pen = (r_pad[i] + r_pad[j]) - d
        active = pad_pen > 0
        i, j = i[active], j[active]
        dx, dy, d, pad_pen = dx[active], dy[active], d[active], pad_pen[active]

        # Unit push direction from j toward i. Coincident centers (d≈0) have no
        # defined direction — the analysis finds none, but give them a random
        # one so the relaxation stays robust rather than producing NaNs.
        coincident = d < 1e-9
        if coincident.any():
            theta = np.random.default_rng(it).uniform(
                0, 2 * np.pi, int(coincident.sum())
            )
            dx[coincident], dy[coincident] = np.cos(theta), np.sin(theta)
            d[coincident] = 1.0
        ux, uy = dx / d, dy / d

        # Split the push inversely with radius: the smaller node moves more, the
        # hub barely budges. The two weights sum to 1, so a damping of 1 would
        # separate the pair exactly in one step.
        rs = r_pad[i] + r_pad[j]
        step = DAMPING * pad_pen
        mi = step * (r_pad[j] / rs)
        mj = step * (r_pad[i] / rs)

        # Jacobi update: accumulate every pair's contribution per node, apply once.
        x += np.bincount(i, mi * ux, n) - np.bincount(j, mj * ux, n)
        y += np.bincount(i, mi * uy, n) - np.bincount(j, mj * uy, n)

    logger.warning(f"Hit MAX_ITERS={MAX_ITERS} with {n_overlap:,} overlaps remaining")


def remove_overlaps(nodes: pl.DataFrame) -> pl.DataFrame:
    """Return nodes with x/y nudged until no two node circles overlap.

    Relaxes in float64, then verifies the values are still overlap-free once
    rounded back to the stored x/y dtype (float32 rounding near the world edge
    can be comparable to the smallest gaps).
    """
    x = nodes["x"].cast(pl.Float64).to_numpy().copy()
    y = nodes["y"].cast(pl.Float64).to_numpy().copy()
    r = nodes["radius"].cast(pl.Float64).to_numpy()
    x0, y0 = x.copy(), y.copy()

    relax(x, y, r)

    disp = np.hypot(x - x0, y - y0)
    logger.info(
        f"Displacement (world units): median={np.median(disp):.4f}, "
        f"p99={np.quantile(disp, 0.99):.4f}, max={disp.max():.4f}"
    )

    # Round-trip through the stored dtype and confirm the written bytes are clean.
    dtype = nodes["x"].dtype
    x_out = pl.Series("x", x).cast(dtype)
    y_out = pl.Series("y", y).cast(dtype)
    xf = x_out.cast(pl.Float64).to_numpy()
    yf = y_out.cast(pl.Float64).to_numpy()
    r_pad = r * (1.0 + SEPARATION_PADDING)
    cell = max(2.0 * float(np.median(r_pad)), float(r_pad.max()) / 32.0)
    vi, vj = candidate_pairs(xf, yf, r_pad, cell)
    residual = int(
        ((r[vi] + r[vj]) - np.hypot(xf[vi] - xf[vj], yf[vi] - yf[vj]) > 0).sum()
    )
    if residual:
        logger.warning(f"{residual:,} overlaps remain after {dtype} rounding")
    else:
        logger.success(f"Zero overlaps in the final {dtype} coordinates")

    return nodes.with_columns(x_out, y_out)


if __name__ == "__main__":
    if OUTPUT_PATH.exists():
        logger.info(f"{OUTPUT_PATH} already exists, skipping")
    else:
        logger.info(f"Removing overlaps from {INPUT_PATH}")
        nodes = pl.read_parquet(INPUT_PATH)
        nodes = remove_overlaps(nodes)
        nodes.write_parquet(OUTPUT_PATH, compression="zstd")
        logger.success(f"Wrote {len(nodes):,} de-overlapped nodes to {OUTPUT_PATH}")
