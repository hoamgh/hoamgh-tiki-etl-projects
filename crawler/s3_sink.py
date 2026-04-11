"""S3 sink: upload crawl snapshot CSV directly to S3."""

import csv
import io
import logging
import os

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


logger = logging.getLogger(__name__)


class S3Sink:
    """Upload in-memory snapshot to S3 as CSV."""

    def __init__(self, s3_config=None):
        s3_config = s3_config or {}

        self.bucket_name = s3_config.get("bucket_name") or os.getenv("S3_BUCKET_NAME")
        self.raw_prefix = (s3_config.get("raw_prefix") or os.getenv("S3_RAW_PREFIX") or "raw").strip("/")
        self.region_name = s3_config.get("region") or os.getenv("AWS_DEFAULT_REGION")

        self.enabled = bool(self.bucket_name)
        self._client = None

        if not self.enabled:
            logger.info("S3 sink disabled (missing bucket_name/S3_BUCKET_NAME).")
            return

        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 upload. Please install requirements.txt")

        self._client = boto3.client("s3", region_name=self.region_name)
        logger.info("S3 sink enabled. Bucket=%s Prefix=%s", self.bucket_name, self.raw_prefix)

    @staticmethod
    def _csv_columns():
        return [
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand_name",
            "brand_type",
            "price",
            "original_price",
            "discount_rate",
            "rating_average",
            "review_count",
            "quantity_sold",
            "seller_name",
            "is_tiki_trading",
            "last_crawled_at",
            "crawl_run_id",
        ]

    def upload_snapshot(self, products, run_id):
        if not self.enabled:
            return 0, None

        if not products:
            logger.warning("No products to upload to S3.")
            return 0, None

        columns = self._csv_columns()
        rows = []
        for p in products:
            rows.append([
                p.get("product_id"),
                p.get("product_name"),
                p.get("category_id"),
                p.get("category_name"),
                p.get("brand_name"),
                p.get("brand_type"),
                p.get("price"),
                p.get("original_price"),
                p.get("discount_rate"),
                p.get("rating_average"),
                p.get("review_count"),
                p.get("quantity_sold"),
                p.get("seller_name"),
                p.get("is_tiki_trading"),
                p.get("last_crawled_at"),
                p.get("crawl_run_id"),
            ])

        rows.sort(key=lambda r: (r[5], r[2], -(r[6] or 0)))

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows)

        key = f"{self.raw_prefix}/{run_id}.csv"
        body = buf.getvalue().encode("utf-8-sig")

        self._client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType="text/csv; charset=utf-8",
        )

        s3_uri = f"s3://{self.bucket_name}/{key}"
        logger.info("Uploaded %d products -> %s", len(rows), s3_uri)
        return len(rows), s3_uri
