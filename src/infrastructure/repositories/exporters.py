from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd


OUTPUT_SAMPLE_DIR = Path("docs/output-samples/COOP-0002-Databrick-Connect")


def build_export_filename(product_name: str, timestamp: datetime | None = None) -> str:
    export_time = timestamp or datetime.now()
    safe_product = product_name.strip().replace("/", "-").replace(" ", "-")
    safe_product = safe_product or "all"
    return f"demo-report_{safe_product}_{export_time.strftime('%Y%m%d%H%M%S')}.csv"


def export_demo_csv(report_df: pd.DataFrame, product_name: str) -> tuple[str, bytes]:
    file_name = build_export_filename(product_name)
    csv_bytes = report_df.to_csv(index=False).encode("utf-8")
    return file_name, csv_bytes


def persist_output_sample(report_df: pd.DataFrame, product_name: str) -> Path:
    OUTPUT_SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    file_name = build_export_filename(product_name)
    output_path = OUTPUT_SAMPLE_DIR / file_name
    report_df.to_csv(output_path, index=False)
    return output_path
