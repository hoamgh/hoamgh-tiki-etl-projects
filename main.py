"""Main orchestrator: crawl products and sync SQL Server snapshot."""

import json
import logging
import os
import sys
from datetime import datetime

from crawler.category_mapper import discover_leaf_categories
from crawler.brand_classifier import BrandClassifier
from crawler.scraper import TikiScraper, DEFAULT_HEADERS
from crawler.storage import InMemoryStorage
from crawler.sqlserver_sink import SQLServerSink
from crawler.s3_sink import S3Sink


def setup_logging():
    """
    Cấu hình logging: ghi ra cả console và file log.
    File log lưu theo timestamp để không bị ghi đè.
    """
    os.makedirs("logs", exist_ok=True)

    log_filename = f"logs/crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return log_filename


def load_config(filepath="config.json"):
    """
    Load file cấu hình JSON.

    Args:
        filepath (str): Đường dẫn đến file config

    Returns:
        dict: Nội dung config đã parse
    """
    if not os.path.exists(filepath):
        print(f"ERROR: Không tìm thấy file config: {filepath}")
        print("Hãy tạo file config.json theo hướng dẫn trong README.md")
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_env_file(filepath=".env"):
    """Load key=value pairs from .env into process environment."""
    if not os.path.exists(filepath):
        return

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue

            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def main():
    """Main entry point - Điều phối toàn bộ quy trình."""
    log_file = setup_logging()
    logger = logging.getLogger("main")

    # Load local secrets/config for AWS/S3, SQL, etc.
    load_env_file(".env")

    # ========== PARSE ARGUMENTS ==========
    args = sys.argv[1:]

    # Xác định file config
    config_path = "config.json"
    for arg in args:
        if not arg.startswith("--") and arg.endswith(".json"):
            config_path = arg
            break

    logger.info(f"Loading config: {config_path}")
    config = load_config(config_path)

    # ========== KHỞI TẠO CÁC COMPONENT ==========
    settings = config["scraping_settings"]
    # Unique run ID (timestamp-based)
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logger.info("=" * 60)
    logger.info(f"PROJECT: {config.get('project', {}).get('name', 'Tiki Crawler')}")
    logger.info(f"RUN ID:  {run_id}")
    logger.info(f"LOG:     {log_file}")
    logger.info("=" * 60)

    # Khởi tạo Brand Classifier
    classifier = BrandClassifier(
        global_brands_list=config["global_brands_dict"],
        oem_indicators=config.get("oem_indicators"),
    )

    # Khởi tạo Scraper
    scraper = TikiScraper(settings, classifier)

    # Storage in-memory for one crawl run
    storage = InMemoryStorage()

    # Optional SQL Server sink (local engine)
    sql_sink = SQLServerSink(config.get("sql_server", {}))
    if sql_sink.enabled:
        sql_sink.connect()

    # Optional S3 sink (upload snapshot after crawl)
    s3_sink = S3Sink(config.get("s3", {}))

    try:
        # ==========================================
        # PHASE 1: DEEP CATEGORY DISCOVERY
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 1: Dò tìm tất cả Leaf Categories (Sub-category sâu nhất)")
        logger.info("=" * 60)

        all_leaf_categories = []

        for parent in config["target_parents"]:
            parent_id = parent["id"]
            parent_name = parent["name"]
            parent_desc = parent.get("description", "")

            logger.info(f"")
            logger.info(
                f"Scanning: {parent_name} (ID: {parent_id}) - {parent_desc}"
            )

            leaves = discover_leaf_categories(
                parent_id=parent_id,
                headers=DEFAULT_HEADERS,
                delay=settings.get("delay_between_requests_sec", 2.0),
            )

            # Lưu metadata categories in-memory
            storage.save_categories(leaves, parent_id)
            all_leaf_categories.extend(leaves)

        # De-duplicate categories by ID
        seen_ids = set()
        unique_categories = []
        for cat in all_leaf_categories:
            if cat["id"] not in seen_ids:
                seen_ids.add(cat["id"])
                unique_categories.append(cat)

        logger.info("")
        logger.info(
            f"TỔNG CỘNG: {len(unique_categories)} leaf categories "
            f"(đã loại trùng từ {len(all_leaf_categories)})"
        )
        for cat in unique_categories:
            logger.info(f"  [{cat['id']:>6}] {cat['name']}")

        # ==========================================
        # PHASE 2: CRAWL & CLASSIFY REAL-TIME
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 2: Crawl sản phẩm + Phân loại Brand Type real-time")
        logger.info("=" * 60)

        total_found = 0
        for i, cat in enumerate(unique_categories, 1):
            logger.info("")
            logger.info(
                f"[{i}/{len(unique_categories)}] "
                f"Crawling: {cat['name']} (ID: {cat['id']})"
            )

            # Crawl tất cả trang của category này
            products = scraper.scrape_category(cat["id"], cat["name"])

            if products:
                # One dedup layer at storage by product_id
                storage.upsert_products(products, run_id)

                total_found += len(products)
                logger.info(f"  -> Tìm thấy: {len(products)}")
            else:
                logger.info("  -> Không có sản phẩm")

        # ==========================================
        # PHASE 3: FINALIZE & SINKS
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 3: Lưu trữ dữ liệu")
        logger.info("=" * 60)

        # Upload snapshot directly to S3 (optional)
        s3_count, s3_uri = s3_sink.upload_snapshot(storage.get_all_products(), run_id)

        # SQL sync chạy 1 lần duy nhất từ snapshot đã unique trong storage
        if sql_sink.enabled:
            sql_sink.sync_snapshot(storage.get_all_products(), run_id)

        logger.info("")
        logger.info("=" * 60)
        logger.info("CRAWL HOÀN THÀNH!")
        logger.info("=" * 60)
        logger.info(f"Run ID:             {run_id}")
        logger.info(f"Categories crawled: {len(unique_categories)}")
        logger.info(f"Sản phẩm crawl:     {total_found:,}")

        logger.info(f"")
        logger.info(f"Output files:")
        if s3_uri:
            logger.info(f"  S3:     {s3_uri} ({s3_count:,} records)")
        else:
            logger.info("  S3:     Disabled")
        if sql_sink.enabled:
            logger.info("  SQL:    Synced to local SQL Server")
        logger.info(f"  Log:    {log_file}")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Crawl bị dừng bởi người dùng (Ctrl+C)")
    except Exception as e:
        logger.error(f"Crawl thất bại: {e}", exc_info=True)
        raise
    finally:
        sql_sink.close()
        storage.close()


if __name__ == "__main__":
    main()
