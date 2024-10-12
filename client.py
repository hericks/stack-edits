import json
import logging
import time
from datetime import datetime, timedelta

import requests

from models import Question, QuestionsResponse

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger()


class StackoverflowQuestionsClient:
    def __init__(self, key: str | None = None):
        self.base_url = "https://api.stackexchange.com"
        self.api_version = "2.3"
        self.site = "stackoverflow"
        self.key = key

        # to be set by last request
        self.quota_max = None
        self.quota_remaining = None
        self.backoff = None
        self.last_request_timestamp = None

    def get_all_questions(self, tag: str) -> list[Question]:
        """Fetches all questions for a given tag."""
        all_questions, page, has_more = [], 1, True
        while has_more:
            response = self.get_questions(tag, page)
            all_questions.extend(response.items)
            page += 1
            has_more = response.has_more
        return all_questions

    def get_questions(
        self, tag: str, page: int = 1, pagesize: int = 100, max_retries: int = 10
    ) -> QuestionsResponse:
        """Fetches all questions for a given tag listed on a given page."""
        logger.info(f"Fetching page {page}...")

        # ensure backoff parameter is respected
        self._ensure_backoff()

        for retry in range(max_retries):
            # make request
            url = "/".join([self.base_url, self.api_version, "questions"])
            response = requests.get(
                url=url,
                params={
                    "site": self.site,
                    "tagged": tag,
                    "page": page,
                    "pagesize": pagesize,
                    "filter": "!nNPvSNP4(R",
                    "key": self.key,
                },
            )

            if response.status_code == 200:
                break

            logger.warning(
                f"Problem encountered (status_code: {response.status_code}, text: {response.text})."
            )

            if retry == max_retries - 1:
                logger.info("Max number of retries reached. Stopping...")
                response.raise_for_status()

            logger.info(f"Waiting {2**retry}s...")
            time.sleep(2**retry)

        # parse output
        response_dict = json.loads(response.text)
        response_model = QuestionsResponse.model_validate(response_dict)

        # update client state
        self.quota_max = response_model.quota_max
        self.quota_remaining = response_model.quota_remaining
        self.backoff = response_model.backoff
        self.last_request_timestamp = datetime.now()

        # TODO: remove
        if self.backoff is not None:
            logger.info(f"{self.backoff}s backoff requested.")

        return response_model

    def _ensure_backoff(self) -> None:
        """Ensures any previously returned backoff request is respected."""
        if self.backoff is None:
            return

        # compute remaining backoff period
        now = datetime.now()
        backoff_timedelta = timedelta(seconds=self.backoff)
        next_request_allowed = self.last_request_timestamp + backoff_timedelta
        remaining_backoff = (next_request_allowed - now).total_seconds()

        if remaining_backoff > 0:
            logger.info(f"{remaining_backoff}s backoff required. Waiting...")
            time.sleep(remaining_backoff)
