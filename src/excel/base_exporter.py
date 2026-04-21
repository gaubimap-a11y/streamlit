from __future__ import annotations

from abc import ABC, abstractmethod

from src.core.reporting import ReportData


class BaseExporter(ABC):
    @abstractmethod
    def export(self, data: ReportData) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def filename(self) -> str:
        raise NotImplementedError

    def _apply_header_style(self, worksheet) -> None:
        if worksheet is None:
            return
