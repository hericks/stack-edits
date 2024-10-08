import os
from datetime import datetime

import polars as pl

from client import StackoverflowQuestionsClient

OUTPUT_PATH = "questions.parquet"
TAG = "python-polars"


if __name__ == "__main__":
    client = StackoverflowQuestionsClient()

    print("Fetching questions...")
    questions = client.get_all_questions(tag=TAG)
    df_questions = pl.DataFrame(questions)

    crawl_date = datetime.now()
    df_questions = df_questions.with_columns(pl.lit(crawl_date).alias("crawl_date"))
    print(f"Done fetching {df_questions.height} questions.")

    if os.path.exists(OUTPUT_PATH):
        print(f"Storage '{OUTPUT_PATH}' exists. Loading questions...")
        df_storage = pl.read_parquet(OUTPUT_PATH)
        print(f"Done loading {df_storage.height} questions. Concatenating...")
        df_questions = pl.concat([df_storage, df_questions])
        print(f"Done concatenating dataframes.")

    print(f"Writing questions to storage '{OUTPUT_PATH}'...")
    df_questions.write_parquet(OUTPUT_PATH)
    print(f"Done writing questions.")
