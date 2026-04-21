from datetime import datetime, timedelta, timezone
import pathlib

from streamlit.testing.v1 import AppTest

_DASHBOARD = str(pathlib.Path(__file__).parent.parent / "pages" / "dashboard.py")


def _set_valid_session(at: AppTest, username: str = "admin") -> None:
    at.session_state["authenticated"] = True
    at.session_state["username"] = username
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)


def test_n1_saved_filter_sections_are_visible_for_authenticated_user():
    at = AppTest.from_file(_DASHBOARD, default_timeout=5)
    _set_valid_session(at)
    at.run()

    markdown_text = " ".join(str(widget.value) for widget in at.markdown)
    assert "My filters" in markdown_text
    assert "Shared with me" in markdown_text


def test_a1_apply_notice_is_shown_once_after_returning_to_dashboard():
    at = AppTest.from_file(_DASHBOARD, default_timeout=5)
    _set_valid_session(at)
    at.session_state["_product_apply_notice"] = "Mot so field cu khong con hop le da duoc bo qua: legacy"
    at.run()

    info_texts = [str(widget.value) for widget in at.info]
    assert any("field cu khong con hop le" in text for text in info_texts)
    assert "_product_apply_notice" not in at.session_state
