from pathlib import Path

import igraph
from loguru import logger

project_graph_data_path = Path("graph_data/")

# logger.info("Loading page node data from csv")
# page_ids: list[int] = []
# for node_data_file in tqdm(project_graph_data_path.glob("page_nodes_*.csv"), total=8):
#     with open(node_data_file) as f:
#         page_ids.extend(int(id) for id in f.read().split()[1:])

# logger.info("Generating page ID -> graph ID map")
# page_id_to_graph_id_map = {
#     page_id: graph_id for graph_id, page_id in enumerate(page_ids)
# }

# logger.info("Creating graph")
# graph = igraph.Graph(n=len(page_ids), directed=True)

# logger.info("Loading page node data from csv and adding to graph")
# for relationship_data_file in tqdm(project_graph_data_path.glob("page_relationships_*.csv"), total=8):
#     new_relationships: list[tuple[int, int]] = []
#     with open(relationship_data_file) as f:
#         processed_file_data = f.read().split()[1:]
#         for relationship_str in processed_file_data:
#             relationship = relationship_str.split(",")
#             new_relationships.append((page_id_to_graph_id_map[int(relationship[0])], page_id_to_graph_id_map[int(relationship[1])]))

#     graph.add_edges(new_relationships)

# logger.info("Printing graph summary")
# igraph.summary(graph)

# logger.info("Writing compressed graphml file")
# graph.write_graphmlz(str(project_graph_data_path / "igraph_graph.graphml.gz"))

# logger.info("Writing page ID -> graph ID map")
# with open(project_graph_data_path / "igraph_page_id_to_graph_id_map.json", "w") as f:
#     json.dump(page_id_to_graph_id_map, f)

logger.info("Loading graph from graphml file")
graph = igraph.Graph.Read_GraphML(str(project_graph_data_path / "igraph_graph.graphml"))

logger.info("Generating drl layout")
drl_layout = graph.layout("drl")
logger.info(f"drl bounding box is: {drl_layout.bounding_box}")
logger.info("Finished drl layout")

logger.info("Generating graphopt layout")
graphopt_layout = graph.layout("graphopt")
logger.info(f"graphopt bounding box is: {graphopt_layout.bounding_box}")
logger.info("Finished graphopt layout")

logger.info("Generating lgl layout")
lgl_layout = graph.layout("lgl")
logger.info(f"lgl bounding box is: {lgl_layout.bounding_box}")
logger.info("Finished lgl layout")
