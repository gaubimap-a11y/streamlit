from __future__ import annotations

from src.domain.supply_report_filter import SupplyReportFilter


class SupplyReportQuery:
    """SQL builder cho dataset nền của report cung ứng sản phẩm đơn lẻ.

    Nguồn dữ liệu: fact.daily_sales JOIN fact.daily_customer_count (AC-5).
    Query được parameterized; không dùng string interpolation cho giá trị filter.

    Grain của customer_count:
        daily_customer_count có grain sale_date × store_id (không có product_id).
        Nếu JOIN trực tiếp rồi SUM, customer_count bị nhân N lần theo số sản phẩm.
        Giải pháp: CTE pre-aggregate customer_count lên grain store_id × period_id
        trước, sau đó JOIN và dùng MAX() để lấy giá trị đã tổng hợp (AC-8/AC-9).
    """

    @staticmethod
    def _catalog() -> str:
        # PoC KOBE-00006 luôn đọc cùng một catalog dữ liệu để tránh lệch nguồn
        # giữa cấu hình môi trường và truy vấn kiểm tra thủ công.
        return "tmn_kobe"

    @staticmethod
    def _qualified_table(catalog: str, table_ref: str) -> str:
        """Chuẩn hóa tên bảng để hỗ trợ nhiều convention đặt tên."""
        if table_ref.count(".") >= 2:
            return table_ref
        return f"{catalog}.{table_ref}"

    @staticmethod
    def build_dataset(
        f: SupplyReportFilter,
        sales_table_ref: str = "fact.daily_sales",
        customer_table_ref: str = "fact.daily_customer_count",
    ) -> tuple[str, list]:
        """Trả về (sql, params) để load dataset aggregate cho report.

        Params theo thứ tự xuất hiện trong mệnh đề WHERE của query chính.
        CTE customer_totals không nhận params (lọc phạm vi chỉ qua JOIN).
        """
        cat = SupplyReportQuery._catalog()
        sales_table = SupplyReportQuery._qualified_table(cat, sales_table_ref)
        customer_table = SupplyReportQuery._qualified_table(cat, customer_table_ref)
        conds: list[str] = []
        params: list = []

        if _is_effective_filter_value(f.product_name):
            conds.append("s.product_name LIKE ?")
            params.append(f"%{f.product_name}%")
        if _is_effective_filter_value(f.store_name):
            conds.append("s.store_name = ?")
            params.append(f.store_name)
        if _is_effective_filter_value(f.period_id):
            conds.append("s.period_id = ?")
            params.append(f.period_id)
        if _is_effective_filter_value(f.classification):
            conds.append("s.classification = ?")
            params.append(f.classification)

        where = ("WHERE " + " AND ".join(conds)) if conds else ""

        # CTE: tổng số khách theo store × period (grain đúng cho PI)
        cte = (
            f"WITH customer_totals AS ("
            f"  SELECT store_id, period_id, SUM(customer_count) AS customer_count"
            f"  FROM {customer_table}"
            f"  GROUP BY store_id, period_id"
            f")"
        )

        # Query chính: aggregate sales, join với CTE, dùng MAX để lấy giá trị CTE
        main = (
            f"SELECT"
            f"  s.store_id,"
            f"  s.store_name,"
            f"  s.product_name,"
            f"  s.classification,"
            f"  s.period_id,"
            f"  SUM(s.quantity_sold)                AS quantity_sold,"
            f"  SUM(s.sales_amount)                 AS sales_amount,"
            f"  COALESCE(MAX(ct.customer_count), 0) AS customer_count"
            f" FROM {sales_table} AS s"
            f" LEFT JOIN customer_totals AS ct"
            f"   ON  s.store_id  = ct.store_id"
            f"   AND s.period_id = ct.period_id"
            f" {where}"
            f" GROUP BY"
            f"   s.store_id,"
            f"   s.store_name,"
            f"   s.product_name,"
            f"   s.classification,"
            f"   s.period_id"
            f" ORDER BY"
            f"   s.store_id,"
            f"   s.store_name,"
            f"   s.product_name,"
            f"   s.classification,"
            f"   s.period_id"
        )

        sql = f"{cte} {main}"
        return sql, params

    @staticmethod
    def build_filter_options(
        sales_table_ref: str = "fact.daily_sales",
    ) -> tuple[str, list]:
        """Lấy distinct values cho các dropdown filter."""
        cat = SupplyReportQuery._catalog()
        sales_table = SupplyReportQuery._qualified_table(cat, sales_table_ref)
        sql = (
            f"SELECT DISTINCT store_name, product_name, classification, period_id"
            f" FROM {sales_table}"
            f" ORDER BY store_name, product_name, classification, period_id"
        )
        return sql, []


def _is_effective_filter_value(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().casefold()
    if not normalized:
        return False
    return normalized not in {"tất cả", "tat ca", "all", "(all)"}
