from colorsys import hsv_to_rgb

import polars as pl

GOLDEN_RATIO_CONJUGATE = 0.618033988749895
COLOR_SATURATION = 0.85
COLOR_VALUE = 1.0


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
