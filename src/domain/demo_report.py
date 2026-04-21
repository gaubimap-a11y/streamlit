from __future__ import annotations

from dataclasses import asdict

import pandas as pd

from src.domain.models import DemoSummary


def summarize_demo_report(report_df: pd.DataFrame, source: str) -> DemoSummary:
    return DemoSummary(
        total_revenue=float(report_df["Total Revenue"].sum()),
        total_sales=int(report_df["Total Sales"].sum()),
        row_count=int(report_df.shape[0]),
        source=source,
    )


def summary_to_dict(summary: DemoSummary) -> dict[str, int | float | str]:
    return asdict(summary)
