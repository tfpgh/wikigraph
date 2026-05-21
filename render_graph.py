from colorsys import hsv_to_rgb
from pathlib import Path

import polars as pl
import skia
from loguru import logger

INPUT_PATH = Path("intermediates/enriched_nodes.parquet")
OUTPUT_PATH = Path("graph_nodes.png")

IMAGE_SIZE = 20_000
PADDING = 100

ZOOM = 1.0

# Clip the fit-bounds to these percentiles of x and y. A few extreme outlier
# nodes won't dominate the scale; they'll just render off-canvas.
CLIP_LOW_PERCENTILE = 0.001
CLIP_HIGH_PERCENTILE = 0.999

RADIUS_SCALING = 0.8

BACKGROUND_COLOR = skia.Color(0x33, 0x33, 0x33)
COLOR_SATURATION = 0.9
COLOR_VALUE = 1.0
GOLDEN_RATIO_CONJUGATE = 0.618033988749895


def load_nodes() -> pl.DataFrame:
    logger.info(f"Reading {INPUT_PATH}")
    return pl.read_parquet(INPUT_PATH, columns=["x", "y", "radius", "partition"])


def scale_nodes(nodes: pl.DataFrame) -> pl.DataFrame:
    lo_x = float(nodes["x"].quantile(CLIP_LOW_PERCENTILE))  # pyright: ignore[reportArgumentType]
    hi_x = float(nodes["x"].quantile(CLIP_HIGH_PERCENTILE))  # pyright: ignore[reportArgumentType]
    lo_y = float(nodes["y"].quantile(CLIP_LOW_PERCENTILE))  # pyright: ignore[reportArgumentType]
    hi_y = float(nodes["y"].quantile(CLIP_HIGH_PERCENTILE))  # pyright: ignore[reportArgumentType]

    extent = max(hi_x - lo_x, hi_y - lo_y)
    if extent <= 0:
        raise ValueError("Layout has no spatial extent")

    scale = ZOOM * (IMAGE_SIZE - PADDING * 2) / extent
    cx_data = (lo_x + hi_x) / 2
    cy_data = (lo_y + hi_y) / 2
    center = IMAGE_SIZE / 2

    logger.info(
        f"Scaling layout by {scale:.4f} "
        f"(zoom={ZOOM}, clip=[{CLIP_LOW_PERCENTILE:.3f}, {CLIP_HIGH_PERCENTILE:.3f}])"
    )
    return nodes.with_columns(
        ((pl.col("x") - cx_data) * scale + center).alias("px"),
        ((cy_data - pl.col("y")) * scale + center).alias("py"),
        (pl.col("radius") * scale * RADIUS_SCALING).alias("pr"),
    )


def color_for_partition(partition: int) -> int:
    hue = (partition * GOLDEN_RATIO_CONJUGATE) % 1.0
    r, g, b = hsv_to_rgb(hue, COLOR_SATURATION, COLOR_VALUE)

    return skia.Color(int(r * 255), int(g * 255), int(b * 255))


def draw_nodes(nodes: pl.DataFrame) -> skia.Surface:
    logger.info(f"Drawing {len(nodes):,} nodes")

    surface = skia.Surface(IMAGE_SIZE, IMAGE_SIZE)
    canvas = surface.getCanvas()
    canvas.clear(BACKGROUND_COLOR)

    paint = skia.Paint(AntiAlias=True)
    colors: dict[int, int] = {}

    for x, y, pr, partition in nodes.select(
        ["px", "py", "pr", "partition"]
    ).iter_rows():
        partition = int(partition)
        color = colors.get(partition)
        if color is None:
            color = color_for_partition(partition)
            colors[partition] = color

        paint.setColor(color)
        canvas.drawCircle(float(x), float(y), float(pr), paint)

    return surface


def write_image(surface: skia.Surface) -> None:
    image = surface.makeImageSnapshot()
    image.save(str(OUTPUT_PATH), skia.kPNG)
    logger.success(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    nodes = load_nodes()
    nodes = scale_nodes(nodes)
    surface = draw_nodes(nodes)
    write_image(surface)
