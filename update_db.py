import json
from datetime import datetime
from typing import Optional

import duckdb
import polars as pl
import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

DB_NAME = "questions.db"
TABLE_NAME = "questions"
TAG = "python-polars"


class Question(BaseModel):
    question_id: int

    title: str
    tags: list[str]
    link: str
    body_markdown: str

    score: int
    view_count: int

    is_answered: bool
    accepted_answer_id: Optional[int] = None
    answer_count: int

    creation_date: datetime
    last_activity_date: datetime
    last_edit_date: Optional[datetime] = None
    protected_date: Optional[datetime] = None
    closed_date: Optional[datetime] = None

    crawl_date: datetime


def requests_retry_session(
    retries=10,
    backoff_factor=2,
    status_forcelist=(400,),
):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def get_questions(tag: str = "python-polars") -> list[Question]:
    session = requests_retry_session()
    crawl_timestamp = datetime.now()
    all_questions, page, has_more = [], 1, True
    with tqdm() as pbar:
        while has_more:
            response = session.get(
                url="https://api.stackexchange.com/2.3/questions",
                params={
                    "site": "stackoverflow",
                    "tagged": tag,
                    "page": page,
                    "pagesize": 100,
                    "filter": "!nNPvSNP4(R",
                },
            )
            response.raise_for_status()
            response_dict = json.loads(response.text)
            questions = [
                Question.model_validate(item | {"crawl_date": crawl_timestamp})
                for item in response_dict["items"]
            ]
            all_questions.extend(questions)
            page += 1
            has_more = response_dict["has_more"]
            pbar.update(1)
    return all_questions


if __name__ == "__main__":
    print("Fetching questions...")
    questions = get_questions(tag=TAG)
    df_questions = pl.DataFrame(questions)
    print(f"Done fetching {df_questions.height} questions.")

    db = duckdb.connect(DB_NAME)
    if TABLE_NAME in db.sql("SHOW tables;").pl().to_series():
        print(
            f"Table '{TABLE_NAME}' in database '{DB_NAME}' already exists. Concatenating dataframe..."
        )
        db.sql(f"INSERT INTO questions (SELECT * FROM df_questions);")
    else:
        db = duckdb.connect(DB_NAME)
        print(
            f"Table '{TABLE_NAME}' in database '{DB_NAME}' does not exist yet. Creating from dataframe..."
        )
        db.sql("CREATE TABLE questions AS (SELECT * FROM df_questions);")

    print("Flushing...")
    db.sql("CHECKPOINT;")
