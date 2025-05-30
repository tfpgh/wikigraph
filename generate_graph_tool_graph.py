from pathlib import Path

import graph_tool
from loguru import logger
from tqdm import tqdm

project_graph_data_path = Path("graph_data/")

logger.info("Creating graph")
graph = graph_tool.Graph(directed=True)

logger.info("Loading page node data from csv")
page_ids: list[int] = []
for node_data_file in tqdm(project_graph_data_path.glob("page_nodes_*.csv"), total=8):
    with open(node_data_file) as f:
        page_ids.extend(int(id) for id in f.read().split()[1:])

logger.info("Generating page ID -> vertex descriptor map + page ID property map")
page_id_to_vertex_map: dict = {}
page_id_property_map = graph.new_vertex_property("int32_t")
for page_id, vertex_descriptor in tqdm(
    zip(page_ids, graph.add_vertex(len(page_ids))), total=len(page_ids)
):
    page_id_to_vertex_map[page_id] = vertex_descriptor
    page_id_property_map[vertex_descriptor] = page_id


logger.info("Loading page node data from csv and adding to graph")
for relationship_data_file in tqdm(
    project_graph_data_path.glob("page_relationships_*.csv"), total=8
):
    new_relationships: list[tuple[int, int]] = []
    with open(relationship_data_file) as f:
        processed_file_data = f.read().split()[1:]
        for relationship_str in tqdm(
            processed_file_data, desc=relationship_data_file.name, leave=False
        ):
            relationship = relationship_str.split(",")
            # graph.add_edge(
            #     page_id_to_vertex_map[int(relationship[0])],
            #     page_id_to_vertex_map[int(relationship[1])],
            #     add_missing=False,
            # )
            new_relationships.append(
                (
                    page_id_to_vertex_map[int(relationship[0])],
                    page_id_to_vertex_map[int(relationship[1])],
                )
            )
    graph.add_edge_list(new_relationships)

logger.info("Printing graph summary")
logger.info(graph)

logger.info("Saving graph as gt file")
graph.save("graph_data/graph_tool_graph.gt")
