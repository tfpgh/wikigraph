import graph_tool
import graph_tool.draw
from loguru import logger

print(graph_tool.openmp_enabled())
print(graph_tool.openmp_get_num_threads())

logger.info("Loading graph")
graph = graph_tool.load_graph("graph_data/graph_tool_graph.gt")

logger.info("Laying out graph by SFDP")
pos = graph_tool.draw.sfdp_layout(graph, verbose=True)

logger.info(len(pos))
