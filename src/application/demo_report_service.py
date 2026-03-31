from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time

import pandas as pd

from domain.demo_report import summarize_demo_report
from domain.models import DemoRequest, DemoSummary
from domain.validation import validate_demo_request
from infrastructure.sql_warehouse_source import fetch_databricks_rows, load_databricks_config, normalize_databricks_rows
from infrastructure.exporters import export_demo_csv, persist_output_sample


@dataclass(frozen=True)
class DemoReportResult:
    request: DemoRequest
    report_df: pd.DataFrame
    summary: DemoSummary
    elapsed_seconds: float


def generate_demo_report(request: DemoRequest) -> DemoReportResult:
    started_at = time.perf_counter()
    validate_demo_request(request)
    config = load_databricks_config()
    rows = fetch_databricks_rows(
        config,
        product_name=request.product_name,
    )
    report_df = normalize_databricks_rows(rows)
    summary = summarize_demo_report(report_df, source=f"databricks_warehouse:{config.warehouse_id}")
    elapsed_seconds = time.perf_counter() - started_at
    return DemoReportResult(
        request=request,
        report_df=report_df,
        summary=summary,
        elapsed_seconds=elapsed_seconds,
    )


def export_demo_report(result: DemoReportResult, persist_sample: bool = False) -> tuple[str, bytes, Path | None]:
    file_name, csv_bytes = export_demo_csv(result.report_df, result.request.product_name)
    sample_path = persist_output_sample(result.report_df, result.request.product_name) if persist_sample else None
    return file_name, csv_bytes, sample_path
