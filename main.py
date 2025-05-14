import gzip
import re

from tqdm import tqdm

# Matches tuple of length 12
page_sql_regex = re.compile(
    r"\(\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*'([^']*)'\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*'([^']*)'\s*,\s*(.*?)\s*\)"
)

node_ids: dict[int, int] = {}


with gzip.open("data/enwiki-latest-page.sql.gz", "rt") as f:
    count_a = 0
    count_b = 0
    for line in tqdm(f, total=7364):
        for page_match in page_sql_regex.finditer(line):
            page_match_elements = page_match.group().split(",")
            try:
                page_namespace = int(page_match_elements[1])
                page_is_redirect = int(page_match_elements[3])
                page_id = int(page_match_elements[0][1:])
            except ValueError:
                continue
            if page_namespace == 0:
                count_a += 1
                if page_is_redirect == 0:
                    count_b += 1

print(count_a)
print(count_b)

# link_sql_regex = re.compile(r"\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)")

# # edges: list[tuple[int, int]] = []

# count = 0
# with gzip.open("data/enwiki-latest-pagelinks.sql.gz", "rt") as f:
#     for line in tqdm(f, total=34671):
#         for link_match in link_sql_regex.finditer(line):
#             # link = literal_eval(link_match.group())
#             count += 1
#             # edges.append((link[0], link[2]))

# print(count)

# # print("building graph")
# # graph = ig.Graph(
# #     n=len(node_ids),
# #     edges=edges,
# #     directed=True,
# # )
# # print(ig.summary(graph))
