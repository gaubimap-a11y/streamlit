from __future__ import annotations

from pydantic import BaseModel


class UserRow(BaseModel):
    user_id: str
    username: str
    email: str
    password_hash: str
    is_active: bool

    model_config = {"frozen": True}
