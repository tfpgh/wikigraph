import multiprocessing
import shutil
from pathlib import Path

from loguru import logger
from neo4j import GraphDatabase

MEMGRAPH_URI = "bolt://localhost:7687"
MEMGRAPH_AUTH = ("", "")

project_graph_data_path = Path("graph_data/")
memgraph_graph_data_path = Path("/usr/lib/memgraph/graph_data/")


def _execute_single_query(query: str) -> None:
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
    with driver.session() as session:
        session.run(query)  # type: ignore[reportArgumentType]


try:
    shutil.rmtree(memgraph_graph_data_path)
except FileNotFoundError:
    logger.info("No memgraph graph data folder found. Creating.")

memgraph_graph_data_path.mkdir()

for graph_data_file in project_graph_data_path.glob("*.csv"):
    logger.info(
        f"Copying {shutil.copy2(graph_data_file, memgraph_graph_data_path)} to memgraph directory"
    )

logger.info("Changing storage mode to IN_MEMORY_ANALYTICAL")
_execute_single_query("STORAGE MODE IN_MEMORY_ANALYTICAL;")
logger.info("Dropping existing graph")
_execute_single_query("DROP GRAPH;")

logger.info("Loading nodes")
node_queries: list[str] = []
for node_data_file in memgraph_graph_data_path.glob("page_nodes_*.csv"):
    node_queries.append(
        f"LOAD CSV FROM '{node_data_file}' with HEADER as row CREATE (p:Page {{id: toInteger(row.id)}});"
    )

with multiprocessing.Pool(len(node_queries)) as pool:
    pool.starmap(_execute_single_query, [(query,) for query in node_queries])

logger.info("Creating index on :Page(id)")
_execute_single_query("CREATE INDEX ON :Page(id);")

logger.info("Loading relationships")
relationship_queries: list[str] = []
for relationship_data_file in memgraph_graph_data_path.glob("page_relationships_*.csv"):
    relationship_queries.append(
        f"LOAD CSV FROM '{relationship_data_file}' with HEADER as row MATCH (p1:Page {{id: toInteger(row.id_from)}}), (p2:Page {{id: toInteger(row.id_to)}}) CREATE (p1)-[:LINKS_TO]->(p2);"
    )

with multiprocessing.Pool(len(relationship_queries)) as pool:
    pool.starmap(_execute_single_query, [(query,) for query in relationship_queries])

shutil.rmtree(memgraph_graph_data_path)
