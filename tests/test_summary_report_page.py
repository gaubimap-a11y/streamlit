"""UT for pages/summary_report.py - UI layer."""

from datetime import datetime, timedelta, timezone
import pathlib

from streamlit.testing.v1 import AppTest


_SUMMARY_REPORT_SCRIPT = str(pathlib.Path(__file__).parent.parent / "pages" / "summary_report.py")


def _set_valid_session(at: AppTest, username: str = "admin") -> None:
    at.session_state["authenticated"] = True
    at.session_state["username"] = username
    at.session_state["login_time"] = datetime.now(tz=timezone.utc) - timedelta(hours=1)


def test_summary_report_page_renders_filter_sections_for_authenticated_user():
    at = AppTest.from_file(_SUMMARY_REPORT_SCRIPT, default_timeout=5)
    _set_valid_session(at)
    at.run()

    assert at.title[0].value == "Báo cáo tổng hợp"
    assert any("Bộ lọc báo cáo" in str(item.value) for item in at.markdown)
