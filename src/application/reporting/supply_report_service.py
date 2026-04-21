from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import pandas as pd

from src.application.auth.audit_service import (
    AuditEventWriter,
    build_audit_event_from_session,
    record_audit_event,
)
from src.application.auth.authorization_service import require_permission
from src.core.exceptions import DataAccessError
from src.domain.auth_models import AuthenticatedSession
from src.domain.auth_validation import PermissionDeniedError
from src.domain.supply_report_filter import AXIS_NONE, KPI_NAMES, SupplyReportFilter
from src.domain.supply_report_row import SupplyReportAggRow
from src.infrastructure.databricks.client import databricks_connection
from src.infrastructure.repositories.supply_report_repository import SupplyReportRepository

logger = logging.getLogger(__name__)

# Nhan dong tong cum (AC-7)
TOTAL_LABEL = "総供給"
# Gia tri hien thi khi mau so = 0 (AC-14)
NA = "N/A"


@dataclass
class MatrixPayload:
    """Ket qua tra ve tu SupplyReportService de renderer hien thi."""

    df: pd.DataFrame
    is_empty: bool
    row_axes: list[str] = field(default_factory=list)
    col_axes: list[str] = field(default_factory=list)
    kpi_cols: list[str] = field(default_factory=list)


class SupplyReportService:
    """Orchestrate report authz, audit, query, va payload cho renderer."""

    def __init__(
        self,
        repository: SupplyReportRepository | None = None,
        *,
        audit_writer: AuditEventWriter | None = None,
    ) -> None:
        self._repository = repository or SupplyReportRepository()
        self._audit_writer = audit_writer

    def get_filter_metadata(self) -> dict[str, list[str]]:
        """Lay danh sach options cho dropdown filter."""
        try:
            with databricks_connection() as conn:
                return self._repository.load_filter_metadata(conn)
        except DataAccessError:
            logger.exception("Khong lay duoc metadata filter supply_report.")
            raise
        except Exception as exc:
            logger.exception("Loi ngoai y khi lay metadata filter supply_report.")
            raise DataAccessError("Khong lay duoc metadata filter.") from exc

    def ensure_page_access(self, session: AuthenticatedSession) -> None:
        """Kiem tra quyen mo trang supply report."""
        try:
            require_permission(
                session,
                "view_dashboard",
                resource="supply_report",
                action="view_dashboard",
            )
        except PermissionDeniedError as exc:
            self._record_audit(
                session,
                event_type="access_denied",
                action="view_dashboard",
                result="denied",
                details={"reason": str(exc) or "missing_view_dashboard"},
            )
            raise

    def run_report(
        self,
        session: AuthenticatedSession,
        supply_filter: SupplyReportFilter,
    ) -> MatrixPayload:
        """Kiem tra quyen, ghi audit, roi thuc thi supply report."""
        self._require_report_access(session)
        self._record_audit(
            session,
            event_type="report_query_requested",
            action="run_report",
            result="requested",
            details={
                "period_id": str(supply_filter.period_id or ""),
                "store_name": str(supply_filter.store_name or ""),
                "row_axis_1": supply_filter.row_axis_1,
                "col_axis_1": supply_filter.col_axis_1,
            },
        )

        try:
            payload = self.get_matrix_payload(supply_filter)
        except DataAccessError as exc:
            self._record_audit(
                session,
                event_type="report_query_failed",
                action="run_report",
                result="failed",
                details={"reason": str(exc)},
            )
            raise

        self._record_audit(
            session,
            event_type="report_query_completed",
            action="run_report",
            result="success",
            details={"is_empty": str(payload.is_empty)},
        )
        return payload

    def get_matrix_payload(self, supply_filter: SupplyReportFilter) -> MatrixPayload:
        """Load data, tinh KPI, tra ve MatrixPayload cho renderer."""
        try:
            with databricks_connection() as conn:
                rows = self._repository.load_dataset(supply_filter, conn)
        except DataAccessError:
            logger.exception("Khong lay duoc dataset supply_report.")
            raise
        except Exception as exc:
            logger.exception("Loi ngoai y khi load dataset supply_report.")
            raise DataAccessError("Khong lay duoc du lieu bao cao.") from exc

        if not rows:
            return MatrixPayload(df=pd.DataFrame(), is_empty=True)

        kpi_cols = [k for k in supply_filter.evaluation_items if k in KPI_NAMES] or KPI_NAMES
        row_axes = [axis for axis in (supply_filter.row_axis_1, supply_filter.row_axis_2) if axis and axis != AXIS_NONE]
        col_axes = [axis for axis in (supply_filter.col_axis_1, supply_filter.col_axis_2) if axis and axis != AXIS_NONE]
        df = pd.DataFrame([self._compute_kpi_record(row) for row in rows])

        return MatrixPayload(
            df=df,
            is_empty=False,
            row_axes=row_axes,
            col_axes=col_axes,
            kpi_cols=kpi_cols,
        )

    @staticmethod
    def calculate_kyokyu_suryo(quantity_sold: int) -> int:
        """供給数量 = SUM(quantity_sold) (AC-6)."""
        return quantity_sold

    @staticmethod
    def calculate_kyokyu_kingaku(sales_amount: Decimal) -> Decimal:
        """供給金額 = SUM(sales_amount) (AC-6)."""
        return sales_amount

    @staticmethod
    def calculate_heikin_tan_ka(
        sales_amount: Decimal,
        quantity_sold: int,
    ) -> Decimal | str:
        """平均売単価 = SUM(sales_amount) / SUM(quantity_sold) (AC-6)."""
        if quantity_sold == 0:
            return NA
        return (sales_amount / Decimal(quantity_sold)).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )

    @staticmethod
    def calculate_suryo_pi(quantity_sold: int, customer_count: int) -> Decimal | str:
        """数量PI = SUM(quantity_sold) / SUM(customer_count) * 1000 (AC-8)."""
        if customer_count == 0:
            return NA
        return (
            Decimal(quantity_sold) / Decimal(customer_count) * Decimal("1000")
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def calculate_kingaku_pi(
        sales_amount: Decimal,
        customer_count: int,
    ) -> Decimal | str:
        """金額PI = SUM(sales_amount) / SUM(customer_count) (AC-9)."""
        if customer_count == 0:
            return NA
        return (sales_amount / Decimal(customer_count)).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )

    @staticmethod
    def dedup_customer_count(group_df: pd.DataFrame) -> int:
        """Deduplicate customer_count theo grain store_id x period_id (AC-8/AC-9).

        customer_count trong daily_customer_count co grain ngay x cua hang,
        khong co product_id. Khi group co nhieu product cung store-period,
        neu SUM truc tiep se nhan N lan. Can dedup theo (store_id, period_id).
        """
        if group_df.empty:
            return 0

        dedup_keys = [key for key in ("_store_id", "period_id") if key in group_df.columns]
        if not dedup_keys:
            return int(group_df["_customer_count"].sum())

        dedup_df = group_df[dedup_keys + ["_customer_count"]].drop_duplicates(subset=dedup_keys)
        return int(dedup_df["_customer_count"].sum())

    @classmethod
    def aggregate_group_metrics(
        cls,
        group_df: pd.DataFrame,
        kpi_cols: list[str],
    ) -> dict[str, Any]:
        """Aggregate KPI cho mot group bat ky (row_dim x col_dim).

        Gop quantity_sold / sales_amount bang SUM tren raw, sau do ap dung
        cong thuc KPI tai grain da group. Khong dung re-aggregate tu df KPI
        vi cac ty so (PI, don gia) khong the cong don lai.
        """
        qty = int(group_df["_quantity_sold"].sum())
        amt = float(group_df["_sales_amount"].sum())
        cust = cls.dedup_customer_count(group_df)

        metrics: dict[str, Any] = {}
        for kpi in kpi_cols:
            if kpi == "供給数量":
                metrics[kpi] = qty
            elif kpi == "供給金額":
                metrics[kpi] = amt
            elif kpi == "平均売単価":
                metrics[kpi] = round(amt / qty, 2) if qty > 0 else NA
            elif kpi == "数量PI":
                metrics[kpi] = round(qty / cust * 1000, 2) if cust > 0 else NA
            elif kpi == "金額PI":
                metrics[kpi] = round(amt / cust, 2) if cust > 0 else NA
        return metrics

    def _compute_kpi_record(self, row: SupplyReportAggRow) -> dict[str, Any]:
        """Tao dict cho mot dong KPI, gom ca cot raw de tinh tong cum."""
        qty = row.quantity_sold
        amt = row.sales_amount
        cust = row.customer_count

        return {
            "_store_id": row.store_id,
            "store_name": row.store_name,
            "product_name": row.product_name,
            "classification": row.classification,
            "period_id": row.period_id,
            "_quantity_sold": qty,
            "_sales_amount": float(amt),
            "_customer_count": cust,
            "供給数量": qty,
            "供給金額": float(amt),
            "平均売単価": self._to_float_or_na(self.calculate_heikin_tan_ka(amt, qty)),
            "数量PI": self._to_float_or_na(self.calculate_suryo_pi(qty, cust)),
            "金額PI": self._to_float_or_na(self.calculate_kingaku_pi(amt, cust)),
        }

    def _require_report_access(self, session: AuthenticatedSession) -> None:
        for permission, action in (("view_data", "view_data"), ("run_report", "run_report")):
            try:
                require_permission(
                    session,
                    permission,
                    resource="supply_report",
                    action=action,
                )
            except PermissionDeniedError as exc:
                self._record_audit(
                    session,
                    event_type="access_denied",
                    action=action,
                    result="denied",
                    details={"reason": str(exc) or f"missing_{permission}"},
                )
                raise

    def _record_audit(
        self,
        session: AuthenticatedSession | None,
        *,
        event_type: str,
        action: str,
        result: str,
        details: dict[str, object] | None = None,
    ) -> bool:
        event = build_audit_event_from_session(
            session,
            event_type=event_type,
            resource="supply_report",
            action=action,
            result=result,
            details=details,
        )
        return record_audit_event(self._audit_writer, event)

    @staticmethod
    def _to_float_or_na(value: Decimal | str) -> float | str:
        if isinstance(value, Decimal):
            return float(value)
        return value
