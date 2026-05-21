from colorsys import hsv_to_rgb
from pathlib import Path

import polars as pl
import skia
from loguru import logger

INPUT_PATH = Path("enriched_nodes.parquet")
OUTPUT_PATH = Path("graph_nodes.png")

IMAGE_SIZE = 10_000
PADDING = 100
MIN_PIXEL_RADIUS = 0.5

BACKGROUND_COLOR = skia.Color(0x33, 0x33, 0x33)
COLOR_SATURATION = 0.9
COLOR_VALUE = 1.0
GOLDEN_RATIO_CONJUGATE = 0.618033988749895


def load_nodes() -> pl.DataFrame:
    logger.info(f"Reading {INPUT_PATH}")
    return pl.read_parquet(INPUT_PATH, columns=["x", "y", "radius", "partition"])


def scale_nodes(nodes: pl.DataFrame) -> pl.DataFrame:
    min_x = float(nodes["x"].min())
    max_x = float(nodes["x"].max())
    min_y = float(nodes["y"].min())
    max_y = float(nodes["y"].max())

    width = max_x - min_x
    height = max_y - min_y
    extent = max(width, height)
    if extent <= 0:
        raise ValueError("Layout has no spatial extent")

    scale = (IMAGE_SIZE - PADDING * 2) / extent
    offset_x = (IMAGE_SIZE - width * scale) / 2
    offset_y = (IMAGE_SIZE - height * scale) / 2

    logger.info(f"Scaling layout by {scale:.4f}")
    return nodes.with_columns(
        ((pl.col("x") - min_x) * scale + offset_x).alias("px"),
        ((max_y - pl.col("y")) * scale + offset_y).alias("py"),
        (pl.col("radius") * scale).clip(lower_bound=MIN_PIXEL_RADIUS).alias("pr"),
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
