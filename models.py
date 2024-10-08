from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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


class QuestionsResponse(BaseModel):
    items: list[Question]

    backoff: Optional[int] = None
    has_more: bool
    quota_max: int
    quota_remaining: int
