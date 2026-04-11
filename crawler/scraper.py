"""
Scraper Engine - Core Crawler thu thập sản phẩm từ Tiki API

Chịu trách nhiệm:
  1. Gọi API products theo từng category + pagination
  2. Trích xuất ĐÚNG 14 trường tinh gọn (Zero-Garbage Schema)
  3. Phân loại brand_type real-time qua BrandClassifier
  4. Xử lý lỗi, retry, rate limiting tự động

Chiến lược chống bot:
  - Delay giữa các request (configurable)
  - Retry với exponential backoff khi gặp 429/5xx
  - User-Agent thực tế
  - Session reuse (connection pooling)
"""

import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

TIKI_PRODUCTS_API = "https://tiki.vn/api/v2/products"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://tiki.vn/",
}


class TikiScraper:
    """
    Core engine thu thập sản phẩm từ Tiki API.

    Features:
    - Tự động pagination đến hết hoặc max_pages
    - Zero-Garbage extraction: chỉ giữ 15 trường cần thiết
    - Real-time brand classification
    - Retry tự động khi gặp lỗi mạng / rate limit
    - Rate limiting có cấu hình
    """

    def __init__(self, settings, brand_classifier):
        """
        Args:
            settings (dict): scraping_settings từ config.json
            brand_classifier (BrandClassifier): Instance đã khởi tạo
        """
        self.settings = settings
        self.classifier = brand_classifier
        self.session = self._create_session()

        logger.info(
            f"TikiScraper initialized: "
            f"max_pages={settings.get('max_pages_per_category', 50)}, "
            f"delay={settings.get('delay_between_requests_sec', 2.5)}s"
        )

    def _create_session(self):
        """
        Tạo requests.Session với retry logic tự động.

        Retry trên các HTTP status:
        - 429: Too Many Requests (rate limit)
        - 500, 502, 503, 504: Server errors
        """
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)

        retries = Retry(
            total=self.settings.get("max_retries", 3),
            backoff_factor=self.settings.get("retry_backoff_sec", 5),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def scrape_category(self, category_id, category_name=""):
        """
        Thu thập tất cả sản phẩm từ 1 category (tự động phân trang).

        Args:
            category_id (int): ID danh mục Tiki
            category_name (str): Tên danh mục (để ghi log)

        Returns:
            list[dict]: Danh sách sản phẩm đã clean + phân loại.
                Mỗi dict chứa đúng 15 trường của Zero-Garbage Schema.
        """
        products = []
        max_pages = self.settings.get("max_pages_per_category", 50)
        per_page = self.settings.get("products_per_page", 40)
        delay = self.settings.get("delay_between_requests_sec", 2.5)
        timeout = self.settings.get("request_timeout_sec", 15)

        for page in range(1, max_pages + 1):
            logger.info(
                f"  Đang quét category {category_id} "
                f"({category_name}) - Trang {page}"
            )

            params = {
                "limit": per_page,
                "category": category_id,
                "page": page,
                "sort": "default",
            }

            try:
                response = self.session.get(
                    TIKI_PRODUCTS_API, params=params, timeout=timeout
                )
                response.raise_for_status()

                result = response.json()
                data = result.get("data", [])

                if not data:
                    logger.info(
                        f"  Hết sản phẩm ở category {category_id} "
                        f"tại trang {page}"
                    )
                    break

                # Trích xuất + phân loại từng sản phẩm
                for item in data:
                    clean = self._extract_clean_record(
                        item, category_id, category_name
                    )
                    if clean:
                        products.append(clean)

                # Kiểm tra đã đến trang cuối chưa
                paging = result.get("paging", {})
                last_page = paging.get("last_page", max_pages)
                if page >= last_page:
                    logger.info(
                        f"  Đã đến trang cuối ({last_page}) "
                        f"của category {category_id}"
                    )
                    break

                # Rate limiting: chờ giữa các request
                time.sleep(delay)

            except requests.exceptions.HTTPError as e:
                logger.warning(
                    f"  HTTP Error tại category {category_id}, "
                    f"trang {page}: {e}"
                )
                if e.response and e.response.status_code == 403:
                    logger.error("  Bị chặn (403 Forbidden). Dừng category này.")
                    break
                time.sleep(delay * 3)  # Chờ lâu hơn khi lỗi
                continue

            except requests.RequestException as e:
                logger.error(
                    f"  Network Error tại category {category_id}, "
                    f"trang {page}: {e}"
                )
                time.sleep(delay * 2)
                continue

            except (ValueError, KeyError) as e:
                logger.error(
                    f"  Lỗi parse data category {category_id}, "
                    f"trang {page}: {e}"
                )
                break

        return products

    def _extract_clean_record(self, item, category_id, category_name):
        """
        Trích xuất ĐÚNG 15 trường tinh gọn từ raw product data.
        (Zero-Garbage Schema)

        Loại bỏ hoàn toàn: URLs, HTML, ảnh, specs JSON, delivery info,
        installment info, related products, breadcrumbs.

        Args:
            item (dict): Raw product data từ Tiki API
            category_id (int): ID category đang crawl
            category_name (str): Tên category

        Returns:
            dict hoặc None: Clean record với 15 trường, hoặc None nếu
                dữ liệu không hợp lệ (bỏ qua sản phẩm rác)
        """
        # Validate: phải có product_id
        product_id = item.get("id")
        if not product_id:
            return None

        # Validate: phải có giá hợp lệ (bỏ hàng free/lỗi giá)
        price = item.get("price")
        if not price or price <= 0:
            return None

        # === PHÂN LOẠI BRAND REAL-TIME ===
        brand_type, is_tiki_trading = self.classifier.classify(item)

        # Xử lý quantity_sold (có thể là dict hoặc int hoặc None)
        qty_sold = item.get("quantity_sold")
        if isinstance(qty_sold, dict):
            qty_sold = qty_sold.get("value", 0)
        elif not isinstance(qty_sold, (int, float)):
            qty_sold = 0

        # Xử lý original_price (fallback = price nếu không có)
        original_price = item.get("original_price")
        if not original_price or original_price <= 0:
            original_price = price

        # === TRẢ VỀ ĐÚNG 14 TRƯỜNG (ZERO-GARBAGE) ===
        return {
            "product_id": int(product_id),
            "product_name": str(item.get("name") or "").strip(),
            "category_id": int(category_id),
            "category_name": str(category_name).strip(),
            "brand_name": str(item.get("brand_name") or "").strip(),
            "brand_type": brand_type,
            "price": int(price),
            "original_price": int(original_price),
            "discount_rate": round(float(item.get("discount_rate") or 0), 2),
            "rating_average": round(float(item.get("rating_average") or 0), 2),
            "review_count": int(item.get("review_count") or 0),
            "quantity_sold": int(qty_sold or 0),
            "seller_name": str(item.get("seller_name") or "").strip(),
            "is_tiki_trading": is_tiki_trading,
        }
