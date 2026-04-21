from __future__ import annotations

from src.core.reporting import ReportData
from src.excel.base_exporter import BaseExporter


class ProductExporter(BaseExporter):
    def export(self, data: ReportData) -> bytes:
        return b""

    def filename(self) -> str:
        return "products.xlsx"
