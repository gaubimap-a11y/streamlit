from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time

import pandas as pd

from src.application.auth.authorization_service import require_permission
from src.domain.auth_models import AuthenticatedSession
from src.domain.demo_report import summarize_demo_report
from src.domain.models import DemoRequest, DemoSummary
from src.domain.validation import validate_demo_request
from src.infrastructure.repositories.exporters import export_demo_csv, persist_output_sample
from src.infrastructure.repositories.sql_warehouse_source import (
    fetch_databricks_rows,
    load_databricks_config,
    normalize_databricks_rows,
)


@dataclass(frozen=True)
class DemoReportResult:
    request: DemoRequest
    report_df: pd.DataFrame
    summary: DemoSummary
    elapsed_seconds: float


def generate_demo_report(session: AuthenticatedSession, request: DemoRequest) -> DemoReportResult:
    require_permission(session, "run_report", resource="dashboard", action="run_report")
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


def export_demo_report(
    session: AuthenticatedSession,
    result: DemoReportResult,
    persist_sample: bool = False,
) -> tuple[str, bytes, Path | None]:
    require_permission(session, "export_output", resource="dashboard", action="export_output")
    file_name, csv_bytes = export_demo_csv(result.report_df, result.request.product_name)
    sample_path = persist_output_sample(result.report_df, result.request.product_name) if persist_sample else None
    return file_name, csv_bytes, sample_path
