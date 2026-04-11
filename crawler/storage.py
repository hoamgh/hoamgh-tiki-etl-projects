"""In-memory storage for crawl snapshot and downstream sinks."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class InMemoryStorage:
    """Store unique products by product_id for S3/SQL sinks."""

    def __init__(self):
        self.products = {}
        logger.info("In-memory storage initialized.")

    def _now(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save_categories(self, categories, parent_id):
        # Category persistence is not required. Categories are derived from products for SQL sync.
        _ = categories, parent_id

    def upsert_products(self, products, run_id):
        new_count = 0
        updated_count = 0
        now = self._now()

        for p in products:
            pid = p["product_id"]
            existing = self.products.get(pid)
            if existing:
                updated_count += 1
            else:
                new_count += 1

            self.products[pid] = {
                "product_id": p.get("product_id"),
                "product_name": p.get("product_name"),
                "category_id": p.get("category_id"),
                "category_name": p.get("category_name"),
                "brand_name": p.get("brand_name"),
                "brand_type": p.get("brand_type"),
                "price": p.get("price"),
                "original_price": p.get("original_price"),
                "discount_rate": p.get("discount_rate"),
                "rating_average": p.get("rating_average"),
                "review_count": p.get("review_count"),
                "quantity_sold": p.get("quantity_sold"),
                "seller_name": p.get("seller_name"),
                "is_tiki_trading": p.get("is_tiki_trading", 0),
                "last_crawled_at": now,
                "crawl_run_id": run_id,
            }

        logger.info(
            "Upserted %d products (New: %d, Updated: %d)",
            len(products),
            new_count,
            updated_count,
        )
        return new_count, updated_count

    def get_all_products(self):
        """Return unique product snapshot by product_id."""
        return list(self.products.values())

    def close(self):
        logger.info("In-memory storage closed.")

