from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field


class EmailTasks(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True, max_length=36)
    message_id: str = Field(default=None, max_length=255)
    history_id: str = Field(default=None, max_length=255)
    status: str = Field(default=None, max_length=20)
    email_address: str = Field(default=None, max_length=255)
    created_at: datetime = Field(default_factory=datetime.now, nullable=False)