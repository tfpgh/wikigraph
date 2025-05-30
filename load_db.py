import gzip
import re
from pathlib import Path

import duckdb
import pandas as pd
import requests
from loguru import logger
from tqdm import tqdm

LOCAL_DUMP_DIRECTORY = Path("dumps/")
DB_DUMP_FILENAMES = [
    "enwiki-latest-redirect.sql.gz",
    "enwiki-latest-linktarget.sql.gz",
    "enwiki-latest-page.sql.gz",
    "enwiki-latest-pagelinks.sql.gz",
]
WIKIPEDIA_DUMP_BASE_URL = "https://dumps.wikimedia.org/enwiki/latest"

DUCKDB_TABLE_SQL = """DROP TABLE IF EXISTS redirect;
CREATE TABLE redirect (
  rd_from INTEGER NOT NULL DEFAULT 0,
  rd_namespace INTEGER NOT NULL DEFAULT 0,
  rd_title VARCHAR(255) NOT NULL DEFAULT '',
  rd_interwiki VARCHAR(32) DEFAULT NULL,
  rd_fragment VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (rd_from)
);

DROP TABLE IF EXISTS linktarget;
CREATE TABLE linktarget (
  lt_id BIGINT PRIMARY KEY,
  lt_namespace INTEGER NOT NULL,
  lt_title VARCHAR NOT NULL,
  UNIQUE (lt_namespace, lt_title)
);

DROP TABLE IF EXISTS page;
CREATE TABLE page (
  page_id INTEGER PRIMARY KEY,
  page_namespace INTEGER NOT NULL DEFAULT 0,
  page_title VARCHAR NOT NULL DEFAULT '',
  page_is_redirect BOOLEAN NOT NULL DEFAULT FALSE,
  page_is_new BOOLEAN NOT NULL DEFAULT FALSE,
  page_random DOUBLE NOT NULL DEFAULT 0,
  page_touched VARCHAR(14) NOT NULL,
  page_links_updated VARCHAR(14),
  page_latest INTEGER NOT NULL DEFAULT 0,
  page_len INTEGER NOT NULL DEFAULT 0,
  page_content_model VARCHAR(32),
  page_lang VARCHAR(35),
  UNIQUE (page_namespace, page_title)
);

DROP TABLE IF EXISTS pagelinks;
CREATE TABLE pagelinks (
  pl_from INTEGER NOT NULL DEFAULT 0,
  pl_from_namespace INTEGER NOT NULL DEFAULT 0,
  pl_target_id BIGINT NOT NULL,
  PRIMARY KEY (pl_from, pl_target_id)
);
"""


# Returns a df of insertions and the appropriate DuckDB SQL to insert it
def _get_insertions_df_from_str(
    insertion_sql: str, filename: str
) -> tuple[pd.DataFrame, str]:
    insertions: list[tuple] = []
    if filename == "enwiki-latest-redirect.sql.gz":
        matches = re.findall(
            r"\(((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL))(?:\s*,?)?\)",
            insertion_sql,
        )
        for insertion in matches:
            insertions.append(
                (
                    int(insertion[0]),
                    int(insertion[1]),
                    insertion[2],
                    insertion[3],
                    insertion[4],
                )
            )

        return pd.DataFrame(
            insertions
        ), "INSERT INTO redirect SELECT * FROM insertions_df"
    elif filename == "enwiki-latest-linktarget.sql.gz":
        matches = re.findall(
            r"\(((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL))(?:\s*,?)?\)",
            insertion_sql,
        )
        for insertion in matches:
            insertions.append(
                (
                    int(insertion[0]),
                    int(insertion[1]),
                    insertion[2],
                )
            )

        return pd.DataFrame(
            insertions
        ), "INSERT INTO linktarget SELECT * FROM insertions_df"
    elif filename == "enwiki-latest-page.sql.gz":
        matches = re.findall(
            r"\(((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+(?:\.\d+)?|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL)),\s*((?:'(?:[^'\\]|\\.)*'|NULL))(?:\s*,?)?\)",
            insertion_sql,
        )
        for insertion in matches:
            insertions.append(
                (
                    int(insertion[0]),
                    int(insertion[1]),
                    insertion[2],
                    bool(int(insertion[3])),
                    bool(int(insertion[4])),
                    float(insertion[5]),
                    insertion[6],
                    insertion[7],
                    int(insertion[8]),
                    int(insertion[9]),
                    insertion[10],
                    insertion[11],
                )
            )

        return pd.DataFrame(insertions), "INSERT INTO page SELECT * FROM insertions_df"
    elif filename == "enwiki-latest-pagelinks.sql.gz":
        matches = re.findall(
            r"\(((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL)),\s*((?:[-+]?\d+|NULL))(?:\s*,?)?\)",
            insertion_sql,
        )
        for insertion in matches:
            insertions.append(
                (
                    int(insertion[0]),
                    int(insertion[1]),
                    int(insertion[2]),
                )
            )

        return pd.DataFrame(
            insertions
        ), "INSERT INTO pagelinks SELECT * FROM insertions_df"
    else:
        raise ValueError(f"{filename} is not a valid filename.")


logger.info("Starting dump downloads")
for dump_filename in DB_DUMP_FILENAMES:
    local_dump_path = LOCAL_DUMP_DIRECTORY / dump_filename

    # Check if we already downloaded the dump
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

if Path("dumps/combined_dumps.db").exists():
    logger.info("Are you sure you want to regenerate the DuckDB database?")
    check_input = input("Type yes to continue. Anything else will cancel: ")
    if check_input != "yes":
        raise ValueError("Canceling.")

Path("dumps/combined_dumps.db").unlink(missing_ok=True)
Path("dumps/combined_dumps.db.wal").unlink(missing_ok=True)

logger.info("Starting DB loading")
duckdb_con = duckdb.connect("dumps/combined_dumps.db")
duckdb_con.sql(DUCKDB_TABLE_SQL)

for dump_filename in DB_DUMP_FILENAMES:
    local_dump_path = LOCAL_DUMP_DIRECTORY / dump_filename

    logger.info(f"Determining length of {dump_filename}")
    with gzip.open(local_dump_path) as f:
        count = 0
        for _ in f:
            count += 1

    logger.info(f"Starting insertions from {dump_filename}")
    with gzip.open(local_dump_path, mode="rt") as f:
        transaction_len = 0
        duckdb_con.sql("BEGIN TRANSACTION;")
        for line in tqdm(f, total=count):
            if "INSERT INTO" in line:
                insertions_df, sql = _get_insertions_df_from_str(line, dump_filename)
                duckdb_con.sql(sql)
                transaction_len += 1
            if transaction_len >= 1000:
                duckdb_con.sql("COMMIT;")
                duckdb_con.sql("BEGIN TRANSACTION;")
                transaction_len = 0

        duckdb_con.sql("COMMIT;")
