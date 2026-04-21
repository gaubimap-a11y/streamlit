from __future__ import annotations

from src.domain.report_filters import SUMMARY_REPORT_CODE
from src.ui.base.base_page import BasePage
from src.ui.pages.summary_report.filter import SummaryReportFilterSection
from src.ui.pages.summary_report.report import render_summary_report_output
from src.ui.styles.loader import inject_css


class SummaryReportPage(BasePage):
    def __init__(self) -> None:
        super().__init__()
        self._filter_section = SummaryReportFilterSection()

    @property
    def page_title(self) -> str:
        return "Báo cáo tổng hợp"

    @property
    def page_icon(self) -> str:
        return "R"

    @property
    def report_id(self) -> str:
        return SUMMARY_REPORT_CODE

    @property
    def report_code(self) -> str:
        return self.report_id

    @property
    def current_route(self) -> str:
        return "/summary_report"

    def render(self) -> None:
        self._require_auth()
        self._render_page_header()
        self._filter_section.render()
        render_summary_report_output()
        

    def _apply_css(self) -> None:
        super()._apply_css()
        inject_css("table.css", "summary_report.css")
