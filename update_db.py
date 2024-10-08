import argparse
import os
from datetime import datetime

import polars as pl

from client import StackoverflowQuestionsClient

OUTPUT_PATH = "questions.parquet"
TAG = "python-polars"


def update_db(key: str | None = None):
    if key is not None:
        print(f"Using key!")

    client = StackoverflowQuestionsClient(key=key)

    print("Fetching questions...")
    questions = client.get_all_questions(tag=TAG)
    df_questions = pl.DataFrame(questions)

    crawl_date = datetime.now()
    df_questions = df_questions.with_columns(pl.lit(crawl_date).alias("crawl_date"))
    print(f"Done fetching {df_questions.height} questions.")

    print(client.quota_remaining)

    if os.path.exists(OUTPUT_PATH):
        print(f"Storage '{OUTPUT_PATH}' exists. Loading questions...")
        df_storage = pl.read_parquet(OUTPUT_PATH)
        print(f"Done loading {df_storage.height} questions. Concatenating...")
        df_questions = pl.concat([df_storage, df_questions])
        print(f"Done concatenating dataframes.")

    print(f"Writing questions to storage '{OUTPUT_PATH}'...")
    df_questions.write_parquet(OUTPUT_PATH)
    print(f"Done writing questions.")


if __name__ == "__main__":
    # handle command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--key",
        type=str,
        required=False,
        help="An optional stackexchange API key.",
    )
    args = parser.parse_args()

    # update database
    update_db(key=args.key)
