"""UT for pages/users.py - UI layer."""

import pathlib
from datetime import datetime, timedelta, timezone

from streamlit.testing.v1 import AppTest

from src.domain.user import UserRow

_USERS_SCRIPT = str(pathlib.Path(__file__).parent.parent / "pages" / "users.py")


def _set_valid_session(at: AppTest, username: str = "admin") -> None:
    at.session_state["authenticated"] = True
    at.session_state["username"] = username
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)


def test_users_page_shows_table_when_authenticated():
    at = AppTest.from_file(_USERS_SCRIPT, default_timeout=5)
    _set_valid_session(at)
    at.session_state["_users_dataset_cache"] = [
        UserRow(
            user_id="1",
            username="admin",
            email="admin@example.com",
            password_hash="hash",
            is_active=True,
        ),
        UserRow(
            user_id="2",
            username="guest",
            email="guest@example.com",
            password_hash="hash",
            is_active=False,
        ),
    ]
    at.run()

    assert at.title[0].value == "Users"
    assert at.dataframe[0].value.shape[0] == 2
    assert list(at.dataframe[0].value.columns) == ["user_id", "username", "email", "is_active"]


def test_users_page_redirects_when_not_authenticated():
    at = AppTest.from_file(_USERS_SCRIPT, default_timeout=5)
    at.run()

    assert len(at.title) == 0
