import bz2
import html
import multiprocessing as mp
import re
import urllib.parse

import lxml.etree
from loguru import logger
from tqdm import tqdm

# TODO: Replace single underscores in links with spaces

TAG_PREFIX = "{http://www.mediawiki.org/xml/export-0.11/}"

REDIRECT_REGEX = re.compile(
    r"#REDIRECT:?\s*\[\[([^\]\n]+(?:\[[^\]\n]*\][^\]\n]*)*)\]\]", re.IGNORECASE
)
LINK_REGEX = re.compile(
    r"\[\[(?!(?:File|Image|Category|Template|Wikipedia|Help|Portal|Special|Media|User|Talk|Commons)(?: talk)?:)([^|\]\n]+)(?:\|[^\]]+)?\]\]",
    re.IGNORECASE,
)


def _remove_element(element: lxml.etree._Element) -> None:
    element.clear()

    parent = element.getparent()
    if parent is not None:
        parent.remove(element)


def _normalize_link_target(link_target: str) -> str:
    # Remove specific target and alias
    link_target = link_target.strip().split("#")[0].split("|")[0]

    # Remove leading or trailing colon
    link_target = link_target.strip(":")

    # Fix possible URL encoding
    link_target = urllib.parse.unquote(link_target)

    # Fix possible HTML encoding
    link_target = html.unescape(link_target)

    # Remove double spaces
    link_target = " ".join(link_target.split())

    return link_target


def continuously_process_page_data(queue: mp.Queue) -> None:
    with (
        open("parsed_graph/pages.txt", "w") as pages_f,
        open("parsed_graph/temp_redirects.txt", "w") as redirects_f,
        open("parsed_graph/temp_links.txt", "w") as links_f,
    ):
        while True:
            page_data = queue.get()
            if page_data == "STOP":
                logger.info("Process stopping")
                break

            if page_data["redirect"]:
                redirect_target_match = REDIRECT_REGEX.search(page_data["text"])
                if redirect_target_match is None:
                    logger.warning(
                        f'Redirect page "{page_data["title"]}" is missing a redirect. Ignoring.'
                    )
                    continue

                redirects_f.write(
                    f"{page_data['title']}<|>{redirect_target_match.group(1)}\n"
                )
                continue

            pages_f.write(f"{page_data['id']}<|>{page_data['title']}\n")

            if page_data["text"] is None:
                continue

            for link_match in LINK_REGEX.findall(page_data["text"]):
                links_f.write(f"{page_data['id']}<|>{link_match}\n")


page_data_queue = mp.Queue(maxsize=1_000_000)

page_data_process = mp.Process(
    target=continuously_process_page_data, args=(page_data_queue,)
)
page_data_process.start()

with bz2.open("dumps/enwiki-latest-pages-articles-multistream.xml.bz2") as f:
    xml_context = lxml.etree.iterparse(f)
    with tqdm(total=6_996_407, desc="Extracting pages from XML", unit="pages") as pbar:
        for action, element in xml_context:
            if element.tag == f"{TAG_PREFIX}page":
                # If it's not a normal or redirect page, we don't need to process it
                if int(element.find(f"{TAG_PREFIX}ns").text) != 0:  # type: ignore[reportOptionalMemberAccess]
                    _remove_element(element)
                    continue

                page_data = {
                    "title": element.find(f"{TAG_PREFIX}title").text,  # type: ignore[reportOptionalMemberAccess]
                    "id": int(element.find(f"{TAG_PREFIX}id").text),  # type: ignore[reportArgumentType]
                    "redirect": element.find(f"{TAG_PREFIX}redirect") is not None,
                    "text": element.find(f"{TAG_PREFIX}revision/{TAG_PREFIX}text").text,  # type: ignore[reportOptionalMemberAccess]
                }

                page_data_queue.put(page_data)

                _remove_element(element)

                if not page_data["redirect"]:
                    pbar.update(1)

    page_data_queue.put("STOP")

logger.info("Waiting for background process to finish")
page_data_process.join()

logger.info("Loading page information")
pages_set: set[str] = set()
pages_set_upper: set[str] = set()
upper_to_proper_capitalization: dict[str, str] = {}

page_id_to_name: dict[int, str] = {}
page_name_to_id: dict[str, int] = {}
with open("parsed_graph/pages.txt") as pages_f:
    for line in tqdm(pages_f, total=6_996_407, unit="pages"):
        id_str, name = line.strip().split("<|>", maxsplit=1)

        pages_set.add(name)
        pages_set_upper.add(name.upper())

        upper_to_proper_capitalization[name.upper()] = name

        page_id_to_name[int(id_str)] = name
        page_name_to_id[name] = int(id_str)

logger.info("Processing redirects")
redirects_set: set[str] = set()
redirect_map: dict[str, str] = {}

skipped_count = 0
with open("parsed_graph/temp_redirects.txt") as temp_redirects_f:
    for line in tqdm(temp_redirects_f, total=11_456_473, unit="redirects"):
        redirect_name, target_name = line.strip().split("<|>", maxsplit=1)

        redirects_set.add(redirect_name)

        target_name = _normalize_link_target(target_name)

        if target_name.upper() in pages_set_upper:
            redirect_map[redirect_name] = upper_to_proper_capitalization[
                target_name.upper()
            ]
        elif target_name.replace("_", " ").upper() in pages_set_upper:
            redirect_map[redirect_name] = upper_to_proper_capitalization[
                target_name.replace("_", " ").upper()
            ]
        else:
            skipped_count += 1

logger.info(
    f"Skipped {skipped_count} redirects or {round((skipped_count / (len(redirect_map) + skipped_count)) * 100, 3)}%"
)

logger.info("Processing links")
with open("parsed_graph/temp_links.txt") as temp_links_f:
    for line in tqdm(temp_links_f, total=334_142_032, unit="links"):
        page_id_str, target_name = line.strip().split("<|>", maxsplit=1)

        page_id = int(page_id_str)

        target_name = _normalize_link_target(target_name)
        print(page_id, target_name)
