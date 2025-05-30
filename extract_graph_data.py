import math
from pathlib import Path

import duckdb
from loguru import logger
from tqdm import tqdm

LINK_CHUNK_SIZE = 100_000

CSV_CHUNK_COUNT = 8  # Probably should just be equal to the number of cores

PAGE_SQL = """SELECT page_id
FROM page
WHERE page_namespace = 0 AND page_is_redirect = FALSE
ORDER BY page_id ASC;"""

DIRECT_LINK_SQL = """SELECT
  pl.pl_from   AS source_id,
  p.page_id    AS target_id
FROM pagelinks pl
JOIN linktarget lt
  ON lt.lt_id = pl.pl_target_id
-- Join to ensure the source is a real, non-redirect article
JOIN page src
  ON src.page_id        = pl.pl_from
 AND src.page_namespace = 0
 AND src.page_is_redirect = FALSE
-- Join to resolve the target title to a page
JOIN page p
  ON p.page_namespace = lt.lt_namespace
 AND p.page_title     = lt.lt_title
WHERE
  pl.pl_from_namespace = 0  -- source in ns 0 (redundant with src join)
  AND lt.lt_namespace   = 0  -- only consider links to ns 0 targets
  AND p.page_is_redirect = FALSE;
"""

REDIRECT_LINK_SQL = """
SELECT
  pl.pl_from        AS source_id,
  rd_target.page_id AS target_id
FROM
  pagelinks AS pl
  JOIN linktarget AS lt
    ON lt.lt_id = pl.pl_target_id

  -- ensure the source is a real (non-redirect) article
  JOIN page AS source_p
    ON source_p.page_id         = pl.pl_from
   AND source_p.page_namespace  = 0
   AND source_p.page_is_redirect = FALSE

  -- only consider links that land on *some* redirect page in ns 0
  JOIN page AS p
    ON p.page_namespace = lt.lt_namespace
   AND p.page_title     = lt.lt_title
   AND p.page_is_redirect = TRUE

  -- but only follow redirects *into* ns 0
  JOIN redirect AS rd
    ON rd.rd_from       = p.page_id
   AND rd.rd_namespace  = 0

  -- and the final target must be a real (non-redirect) ns 0 article
  JOIN page AS rd_target
    ON rd_target.page_namespace  = rd.rd_namespace
   AND rd_target.page_title      = rd.rd_title
   AND rd_target.page_is_redirect = FALSE

WHERE
  pl.pl_from_namespace = 0  -- source in ns 0
  AND lt.lt_namespace   = 0; -- only original links to ns 0"""

duckdb_con = duckdb.connect("dumps/combined_dumps.db")

logger.info("Deleting existing graph data")
for graph_data_file in Path("graph_data/").glob("*.csv"):
    graph_data_file.unlink()

logger.info("Fetching page IDs from DuckDB")
page_ids: list[int] = [row[0] for row in duckdb_con.sql(PAGE_SQL).fetchall()]
# logger.info("Generating page ID -> graph ID map")
# page_id_to_graph_id_map = {
#     page_id: graph_id for graph_id, page_id in enumerate(page_ids)
# }

logger.info("Writing nodes to csv (with header)")

chunk_size = math.ceil(len(page_ids) / CSV_CHUNK_COUNT)

for i in range(0, len(page_ids), chunk_size):
    chunk_num = i // chunk_size
    chunk = page_ids[i : i + chunk_size]

    with open(f"graph_data/page_nodes_{chunk_num}.csv", "w") as f:
        f.write("id")
        for page_id in tqdm(chunk, desc=f"page_nodes_{chunk_num}.csv"):
            f.write(f"\n{page_id}")

link_page_ids: list[tuple[int, int]] = []
# link_graph_ids: list[tuple[int, int]] = []

logger.info("Querying direct links from DuckDB")
direct_link_res = duckdb_con.sql(DIRECT_LINK_SQL)
logger.info(f"{len(direct_link_res)} direct links found")

logger.info("Fetching direct links from DuckDB")
with tqdm(total=len(direct_link_res) // LINK_CHUNK_SIZE) as pbar:
    while chunk := direct_link_res.fetchmany(LINK_CHUNK_SIZE):
        link_page_ids.extend((row[0], row[1]) for row in chunk)
        # link_graph_ids.extend(
        #     (page_id_to_graph_id_map[row[0]], page_id_to_graph_id_map[row[1]])
        #     for row in chunk
        # )
        pbar.update(1)

logger.info("Querying redirect links from DuckDB")
redirect_link_res = duckdb_con.sql(REDIRECT_LINK_SQL)
logger.info(f"{len(redirect_link_res)} redirect links found")

logger.info("Fetching redirect links from DuckDB")
with tqdm(total=len(redirect_link_res) // LINK_CHUNK_SIZE) as pbar:
    while chunk := redirect_link_res.fetchmany(LINK_CHUNK_SIZE):
        link_page_ids.extend((row[0], row[1]) for row in chunk)
        # link_graph_ids.extend(
        #     (page_id_to_graph_id_map[row[0]], page_id_to_graph_id_map[row[1]])
        #     for row in chunk
        # )
        pbar.update(1)

duckdb_con.close()

logger.info("Writing relationships to csv (with header)")

chunk_size = math.ceil(len(link_page_ids) / CSV_CHUNK_COUNT)

for i in range(0, len(link_page_ids), chunk_size):
    chunk_num = i // chunk_size
    chunk = link_page_ids[i : i + chunk_size]

    with open(f"graph_data/page_relationships_{chunk_num}.csv", "w") as f:
        f.write("id_from,id_to")
        for link in tqdm(chunk, desc=f"page_relationships_{chunk_num}.csv"):
            f.write(f"\n{link[0]},{link[1]}")

# logger.info("Creating graph from pages and links")
# graph = Graph(
#     n=len(page_ids),
#     edges=link_graph_ids,
#     directed=True,
# )

# logger.info("Summarizing graph")
# print(igraph.summary(graph))

# logger.info("Clustering graph using the Leiden algorithm")
# leiden_communities = leidenalg.find_partition(
#     graph, leidenalg.CPMVertexPartition, seed=0
# )
# logger.info("Summarizing communities")
# print(leiden_communities.summary())
# logger.info("Summarizing cluser graph")
# print(leiden_communities.sizes())
# print(igraph.summary(leiden_communities.cluster_graph()))
