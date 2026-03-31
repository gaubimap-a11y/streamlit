from __future__ import annotations

from infrastructure.sql_warehouse_source import DatabricksConfig

from application.demo_report_service import export_demo_report, generate_demo_report
from domain.models import DemoRequest


def test_generate_demo_report_returns_required_columns_and_summary(monkeypatch) -> None:
    perf_counter_values = iter([10.0, 10.75])
    monkeypatch.setattr("application.demo_report_service.time.perf_counter", lambda: next(perf_counter_values))
    monkeypatch.setattr(
        "application.demo_report_service.load_databricks_config",
        lambda: DatabricksConfig(
            host="host",
            token="token",
            warehouse_id="warehouse-123",
        ),
    )
    captured: dict[str, str] = {}
    monkeypatch.setattr(
        "application.demo_report_service.fetch_databricks_rows",
        lambda config, product_name: (
            captured.update({"product_name": product_name})
            or [
                {"product_name": product_name, "total_revenue": 1200.5, "total_sales": 4},
                {"product_name": product_name, "total_revenue": 1800.0, "total_sales": 6},
            ]
        ),
    )

    request = DemoRequest(product_name="Coffee A")

    result = generate_demo_report(request)

    assert list(result.report_df.columns) == ["Product Name", "Total Revenue", "Total Sales"]
    assert result.report_df["Product Name"].tolist() == ["Coffee A", "Coffee A"]
    assert (result.report_df["Total Revenue"] > 0).all()
    assert (result.report_df["Total Sales"] > 0).all()
    assert result.summary.total_revenue == float(result.report_df["Total Revenue"].sum())
    assert result.summary.total_sales == int(result.report_df["Total Sales"].sum())
    assert result.summary.row_count == 2
    assert result.summary.source == "databricks_warehouse:warehouse-123"
    assert result.elapsed_seconds == 0.75
    assert captured["product_name"] == "Coffee A"


def test_export_demo_report_returns_csv_download_payload_without_persisting(monkeypatch) -> None:
    perf_counter_values = iter([20.0, 20.25])
    monkeypatch.setattr("application.demo_report_service.time.perf_counter", lambda: next(perf_counter_values))
    monkeypatch.setattr(
        "application.demo_report_service.load_databricks_config",
        lambda: DatabricksConfig(
            host="host",
            token="token",
            warehouse_id="warehouse-123",
        ),
    )
    monkeypatch.setattr(
        "application.demo_report_service.fetch_databricks_rows",
        lambda config, product_name: [
            {"product_name": product_name, "total_revenue": 500.0, "total_sales": 2}
        ],
    )

    request = DemoRequest(product_name="Coffee A")
    result = generate_demo_report(request)

    file_name, csv_bytes, sample_path = export_demo_report(result, persist_sample=False)

    assert file_name.startswith("demo-report_Coffee-A_")
    assert b"Product Name,Total Revenue,Total Sales" in csv_bytes
    assert sample_path is None


def test_generate_demo_report_with_empty_search_fetches_all(monkeypatch) -> None:
    perf_counter_values = iter([30.0, 30.5])
    monkeypatch.setattr("application.demo_report_service.time.perf_counter", lambda: next(perf_counter_values))
    captured: dict[str, str] = {}
    monkeypatch.setattr(
        "application.demo_report_service.load_databricks_config",
        lambda: DatabricksConfig(
            host="host",
            token="token",
            warehouse_id="warehouse-123",
        ),
    )

    def _fake_fetch(config, product_name):
        captured["product_name"] = product_name
        return [{"product_name": "Coffee A", "total_revenue": 500.0, "total_sales": 2}]

    monkeypatch.setattr("application.demo_report_service.fetch_databricks_rows", _fake_fetch)

    result = generate_demo_report(DemoRequest(product_name=""))

    assert captured["product_name"] == ""
    assert result.summary.row_count == 1
    assert result.elapsed_seconds == 0.5


def test_generate_demo_report_preserves_product_name_without_normalizing(monkeypatch) -> None:
    perf_counter_values = iter([40.0, 40.2])
    monkeypatch.setattr("application.demo_report_service.time.perf_counter", lambda: next(perf_counter_values))
    monkeypatch.setattr(
        "application.demo_report_service.load_databricks_config",
        lambda: DatabricksConfig(
            host="host",
            token="token",
            warehouse_id="warehouse-123",
        ),
    )
    captured: dict[str, str] = {}

    def _fake_fetch(config, product_name):
        captured["product_name"] = product_name
        return [{"product_name": "Coffee A", "total_revenue": 500.0, "total_sales": 2}]

    monkeypatch.setattr("application.demo_report_service.fetch_databricks_rows", _fake_fetch)

    generate_demo_report(DemoRequest(product_name="  Coffee A  "))

    assert captured["product_name"] == "  Coffee A  "


def test_generate_demo_report_passes_partial_case_insensitive_search_text(monkeypatch) -> None:
    perf_counter_values = iter([50.0, 50.2])
    monkeypatch.setattr("application.demo_report_service.time.perf_counter", lambda: next(perf_counter_values))
    monkeypatch.setattr(
        "application.demo_report_service.load_databricks_config",
        lambda: DatabricksConfig(
            host="host",
            token="token",
            warehouse_id="warehouse-123",
        ),
    )
    captured: dict[str, str] = {}

    def _fake_fetch(config, product_name):
        captured["product_name"] = product_name
        return [{"product_name": "Coffee Arabica", "total_revenue": 500.0, "total_sales": 2}]

    monkeypatch.setattr("application.demo_report_service.fetch_databricks_rows", _fake_fetch)

    generate_demo_report(DemoRequest(product_name="coffee"))

    assert captured["product_name"] == "coffee"
