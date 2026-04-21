from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ReportData:
    total: int
    rows: list[Any]
    chart_data: list[Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_empty(self) -> bool:
        return self.total == 0
