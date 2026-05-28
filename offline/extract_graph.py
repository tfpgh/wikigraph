import bz2
import html
import re
from pathlib import Path

import polars as pl
from joblib import Parallel, delayed
from loguru import logger
from tqdm import tqdm

DUMP_INDEX_PATH = Path("dumps/enwiki-20260501-pages-articles-multistream-index.txt.bz2")
DUMP_PATH = Path("dumps/enwiki-20260501-pages-articles-multistream.xml.bz2")

NODES_OUTPUT_PATH = Path("intermediates/extracted_nodes.parquet")
EDGES_OUTPUT_PATH = Path("intermediates/extracted_edges.parquet")

REDIRECT_MAX_HOPS = 10

# Parse XML using regexes instead of a proper XML parser
# This is a lot faster
PAGE_RE = re.compile(r"<page>(.*?)</page>", re.DOTALL)
TITLE_RE = re.compile(r"<title>([^<]*)</title>")
NS_RE = re.compile(r"<ns>(\d+)</ns>")
TEXT_RE = re.compile(r"<text[^>]*>(.*?)</text>", re.DOTALL)
REDIRECT_RE = re.compile(r"#REDIRECT\s*\[\[([^\]|#]+)", re.IGNORECASE)
REF_RE = re.compile(r"<ref[^>]*?(?:/>|>.*?</ref>)", re.DOTALL)
TRUNCATE_RE = re.compile(
    r"==\s*(references|notes|bibliography|external links|"
    r"further reading|sources|citations|works cited|footnotes)\s*==",
    re.IGNORECASE,
)

LINK_RE = re.compile(r"\[\[([^\]|#<>{}\n]+?)(?:[|#][^\]]*)?\]\]")

NON_ARTICLE_PREFIXES = (
    "File:",
    "Image:",
    "Category:",
    "Wikipedia:",
    "WP:",
    "Template:",
    "Help:",
    "Portal:",
    "Draft:",
    "Module:",
    "MediaWiki:",
    "User:",
    "Talk:",
    "Special:",
    "Book:",
    "TimedText:",
    "#",
)


def normalize_title(title: str) -> str:
    """Apply Wikipedia's canonical title rules

    Underscores are spaces and the first character is capitalized.
    """
    title = title.replace("_", " ").strip()

    if not title:
        return title

    return title[0].upper() + title[1:]


def get_stream_offsets() -> list[tuple[int, int | None]]:
    """Read the multistream dump index and return (start, end) byte ranges per stream

    Index is one line per page, each stream has 100 pages so we de-dupe to get unique streams.
    """

    logger.info("Processing stream offsets from dump index")

    offsets: set[int] = set()
    with bz2.open(DUMP_INDEX_PATH, "rt") as f:
        for line in tqdm(f, desc="Processing offsets", unit=" lines"):
            offset = int(line.split(":", 1)[0])
            offsets.add(offset)

    sorted_offsets = sorted(offsets)

    return list(zip(sorted_offsets, sorted_offsets[1:] + [None]))


def process_stream(
    start: int, end: int | None
) -> tuple[list[tuple[str, bool, str | None]], list[tuple[str, str]]]:
    """Decode one bz2 stream and extract page + link records.

    Returns:
        pages: (title, is_redirect, redirect_target) for namespace-0 pages.
        links: (source_title, target_title) for every unique wikilink from an article.
               Titles are resolved to IDs later.
    """
    with open(DUMP_PATH, "rb") as f:
        f.seek(start)
        compressed = f.read(end - start) if end is not None else f.read()

    xml = bz2.decompress(compressed).decode("utf-8", errors="replace")

    pages: list[tuple[str, bool, str | None]] = []
    links: list[tuple[str, str]] = []

    for page_match in PAGE_RE.finditer(xml):
        page_xml = page_match.group(1)

        # Only namespace-0 articles
        ns_match = NS_RE.search(page_xml)
        if not ns_match or ns_match.group(1) != "0":
            continue

        title_match = TITLE_RE.search(page_xml)
        text_match = TEXT_RE.search(page_xml)
        if not title_match or not text_match:
            continue

        # The dump XML-escapes the wikitext inside <text>, so `<ref>` is stored
        # as `&lt;ref&gt;` and titles like "AT&T" as "AT&amp;T". Decode entities
        # before any regex (REF_RE, LINK_RE) or title normalization — otherwise
        # ref-stripping silently no-ops and every cited publisher/journal leaks
        # into the link graph.
        title = normalize_title(html.unescape(title_match.group(1)))
        text = html.unescape(text_match.group(1))

        # Strip inline citations (removes links to publishers, authors, journals)
        text = REF_RE.sub("", text)

        # Truncate at the first reference-style section header
        truncate_match = TRUNCATE_RE.search(text)
        if truncate_match:
            text = text[: truncate_match.start()]

        # Redirect has to be at the top of the text
        redirect_match = REDIRECT_RE.search(text[:300])
        if redirect_match:
            target = normalize_title(redirect_match.group(1))
            pages.append((title, True, target))
            continue

        pages.append((title, False, None))

        # Deduplicate per-article links
        seen: set[str] = set()
        for link_match in LINK_RE.finditer(text):
            target = link_match.group(1).strip()
            if not target or target.startswith(NON_ARTICLE_PREFIXES):
                continue

            target = normalize_title(target)

            if target in seen or target == title:
                continue
            seen.add(target)

            links.append((title, target))

    return pages, links


def resolve_redirect_chains(redirects: dict[str, str]) -> dict[str, str]:
    """Flatten redirect chains: A -> B -> C collapses to A -> C.

    These are officially discouraged by Wikipedia but still exist occassionally.
    """
    resolved: dict[str, str] = {}
    for source in redirects:
        current = source
        for _ in range(REDIRECT_MAX_HOPS):
            next = redirects.get(current)
            if next is None or next == current:
                break
            current = next
        resolved[source] = current

    return resolved


if __name__ == "__main__":
    logger.info("Starting graph extraction from dump")

    offsets = get_stream_offsets()
    logger.info(f"Found {len(offsets)} streams to process")

    all_pages: list[tuple[str, bool, str | None]] = []
    all_links: list[tuple[str, str]] = []

    results = Parallel(n_jobs=-1, return_as="generator_unordered", backend="loky")(
        delayed(process_stream)(start, end) for start, end in offsets
    )
    for pages, links in tqdm(  # pyright: ignore[reportGeneralTypeIssues]
        results, total=len(offsets), desc="Processing streams", unit=" streams"
    ):
        all_pages.extend(pages)
        all_links.extend(links)

    logger.info(f"Raw: {len(all_pages):,} pages, {len(all_links):,} links")

    # Polars post-processing
    logger.info("Loading pages and links into polars")
    pages_df = pl.DataFrame(
        all_pages,
        schema=["title", "is_redirect", "redirect_target"],
        orient="row",
    )
    links_df = pl.DataFrame(
        all_links,
        schema=["source", "target"],
        orient="row",
    )

    logger.info("Resolving redirects")
    redirects_raw = dict(
        pages_df.filter(pl.col("is_redirect"))
        .select(["title", "redirect_target"])
        .iter_rows()
    )
    redirects = resolve_redirect_chains(redirects_raw)
    logger.info(f"Resolved {len(redirects):,} redirects")

    # Give compact node IDs in title-sorted order
    nodes_df = (
        pages_df.filter(~pl.col("is_redirect"))
        .select("title")
        .sort("title")
        .with_row_index("id")
        .with_columns(pl.col("id").cast(pl.UInt32))
    )
    logger.info(f"Articles: {len(nodes_df):,}")

    edges_df = (
        links_df.with_columns(pl.col("target").replace(redirects).alias("target"))
        .join(
            nodes_df.rename({"title": "source", "id": "src"}), on="source", how="inner"
        )
        .join(
            nodes_df.rename({"title": "target", "id": "dst"}), on="target", how="inner"
        )
        .select(["src", "dst"])
        .filter(
            pl.col("src") != pl.col("dst")
        )  # drop self-loops from redirect collapse
        .unique()
    )
    logger.info(f"Final edges: {len(edges_df):,}")

    logger.info("Writing parquets")
    nodes_df.write_parquet(NODES_OUTPUT_PATH, compression="zstd")
    edges_df.write_parquet(EDGES_OUTPUT_PATH, compression="zstd")
    logger.success(
        f"Wrote {len(nodes_df):,} nodes and {len(edges_df):,} edges to parquets"
    )
