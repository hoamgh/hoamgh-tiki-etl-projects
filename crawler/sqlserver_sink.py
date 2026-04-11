"""SQL Server sink: full snapshot replace (no SQL-side duplicate checks)."""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class SQLServerSink:
    """
    Đồng bộ full snapshot vào SQL Server.

    Triết lý đơn giản:
    - CHỈ một lớp chống trùng tại storage (product_id)
    - SQL chỉ nhận snapshot đã unique và replace toàn bộ bảng
    """

    def __init__(self, cfg: Dict):
        self.cfg = cfg or {}
        self.enabled = bool(self.cfg.get("enabled", False))
        self.conn = None
        self.cursor = None

        self.categories_table = self.cfg.get("categories_table", "dbo.Categories")
        self.brands_table = self.cfg.get("brands_table", "dbo.Brands")
        self.sellers_table = self.cfg.get("sellers_table", "dbo.Sellers")
        self.products_table = self.cfg.get("products_table", "dbo.Products")

    def connect(self):
        if not self.enabled:
            return

        try:
            import pyodbc  # type: ignore
        except Exception as exc:
            raise RuntimeError("SQL Server enabled nhưng thiếu pyodbc. Cài: pip install pyodbc") from exc

        conn_str = (self.cfg.get("connection_string") or "").strip()
        if not conn_str:
            raise ValueError("Thiếu sql_server.connection_string trong config.json")

        self.conn = pyodbc.connect(conn_str)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        logger.info("SQL Server sink connected")

    def sync_snapshot(self, products: List[Dict], run_id: str):
        if not self.enabled:
            return
        if self.cursor is None or self.conn is None:
            raise RuntimeError("SQL Server sink chưa connect")

        # Build dimensions from unique products snapshot
        categories: Dict[int, str] = {}
        brands_map: Dict[Tuple[str, str], int] = {}
        sellers_map: Dict[Tuple[str, int], int] = {}

        for p in products:
            cid = p.get("category_id")
            if cid is not None:
                categories[int(cid)] = str(p.get("category_name") or "")

            bkey = (str(p.get("brand_name") or ""), str(p.get("brand_type") or ""))
            if bkey not in brands_map:
                brands_map[bkey] = len(brands_map) + 1

            skey = (str(p.get("seller_name") or ""), 1 if p.get("is_tiki_trading") else 0)
            if skey not in sellers_map:
                sellers_map[skey] = len(sellers_map) + 1

        # Full replace, no per-row SQL checks
        self.cursor.execute(f"DELETE FROM {self.products_table};")
        self.cursor.execute(f"DELETE FROM {self.brands_table};")
        self.cursor.execute(f"DELETE FROM {self.sellers_table};")
        self.cursor.execute(f"DELETE FROM {self.categories_table};")

        if categories:
            self.cursor.executemany(
                f"INSERT INTO {self.categories_table} (category_id, category_name) VALUES (?, ?);",
                [(cid, cname) for cid, cname in categories.items()],
            )

        if brands_map:
            self.cursor.execute(f"SET IDENTITY_INSERT {self.brands_table} ON;")
            self.cursor.executemany(
                f"INSERT INTO {self.brands_table} (brand_id, brand_name, brand_type) VALUES (?, ?, ?);",
                [(bid, bname, btype) for (bname, btype), bid in brands_map.items()],
            )
            self.cursor.execute(f"SET IDENTITY_INSERT {self.brands_table} OFF;")

        if sellers_map:
            self.cursor.execute(f"SET IDENTITY_INSERT {self.sellers_table} ON;")
            self.cursor.executemany(
                f"INSERT INTO {self.sellers_table} (seller_id, seller_name, is_tiki_trading) VALUES (?, ?, ?);",
                [(sid, sname, is_tiki) for (sname, is_tiki), sid in sellers_map.items()],
            )
            self.cursor.execute(f"SET IDENTITY_INSERT {self.sellers_table} OFF;")

        product_rows = []
        for p in products:
            bkey = (str(p.get("brand_name") or ""), str(p.get("brand_type") or ""))
            skey = (str(p.get("seller_name") or ""), 1 if p.get("is_tiki_trading") else 0)

            price = p.get("price")
            original_price = p.get("original_price")
            discount_rate = p.get("discount_rate")
            rating_average = p.get("rating_average")
            review_count = p.get("review_count")
            quantity_sold = p.get("quantity_sold")

            purchase_status = p.get("purchase_status") or (
                "has_sales"
                if ((quantity_sold or 0) > 0 or (review_count or 0) > 0)
                else "new_listing"
            )
            is_rating_suspect = (
                int(p.get("is_rating_suspect"))
                if p.get("is_rating_suspect") is not None
                else int((rating_average or 0) > 4.5 and 1 <= (review_count or 0) < 10)
            )
            discount_flag = p.get("discount_flag") or (
                "no_discount"
                if (discount_rate or 0) == 0
                else "fake_discount"
                if (price or 0) >= (original_price or 0)
                else "extreme_discount"
                if (discount_rate or 0) >= 50
                else "normal_discount"
            )

            product_rows.append(
                (
                    p.get("product_id"),
                    p.get("product_name"),
                    p.get("category_id"),
                    brands_map.get(bkey),
                    sellers_map.get(skey),
                    price,
                    original_price,
                    discount_rate,
                    rating_average,
                    review_count,
                    quantity_sold,
                    purchase_status,
                    is_rating_suspect,
                    discount_flag,
                )
            )

        if product_rows:
            self.cursor.executemany(
                f"""
                INSERT INTO {self.products_table} (
                    product_id, product_name, category_id, brand_id, seller_id,
                    price, original_price, discount_rate, rating_average, review_count,
                    quantity_sold, purchase_status, is_rating_suspect, discount_flag
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                product_rows,
            )

        self.conn.commit()
        logger.info(
            "SQL Server snapshot synced: %d products, %d categories, %d brands, %d sellers (%s)",
            len(product_rows),
            len(categories),
            len(brands_map),
            len(sellers_map),
            run_id,
        )

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()
        if self.enabled:
            logger.info("SQL Server sink closed")
