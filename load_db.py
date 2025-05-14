from pathlib import Path

import requests
from loguru import logger
from tqdm import tqdm

LOCAL_DUMP_DIRECTORY = Path("dumps/")
DB_DUMP_FILENAMES = [
    "enwiki-latest-linktarget.sql.gz",
    "enwiki-latest-page.sql.gz",
    "enwiki-latest-pagelinks.sql.gz",
    "enwiki-latest-redirect.sql.gz",
]
WIKIPEDIA_DUMP_BASE_URL = "https://dumps.wikimedia.org/enwiki/latest"

for dump_filename in DB_DUMP_FILENAMES:
    local_dump_path = LOCAL_DUMP_DIRECTORY / dump_filename

    # Check if we have already downloaded the dump
    if local_dump_path.exists():
        logger.info(f"{dump_filename} already exists. Skipping download")
        continue

    logger.info(f"Downloading {dump_filename}")
    url = f"{WIKIPEDIA_DUMP_BASE_URL}/{dump_filename}"
    response = requests.get(url, stream=True)
    if response.ok:
        with open(local_dump_path, "wb") as f:
            for chunk in tqdm(
                response.iter_content(chunk_size=10 * 1024),
                desc=f"{dump_filename} progress",
                total=int(response.headers.get("Content-Length", 0)) / (10 * 1024),
            ):
                f.write(chunk)
        logger.info(f"Downloaded {dump_filename}")
    else:
        logger.error(f"Failed to download {dump_filename}")
        raise Exception(f"Failed to download {dump_filename}")
