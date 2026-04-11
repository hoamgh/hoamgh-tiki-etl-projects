"""
Brand Classifier - Thuật toán Phân loại Real-time (Global vs. Local/OEM Generic)

Đây là "linh hồn" của hệ thống. Mỗi sản phẩm được phân loại ngay
tại thời điểm crawl dựa trên Decision Tree 3 tầng:

  ┌─────────────────────────────────────────────────────┐
  │               PRODUCT INPUT                         │
  │  brand_name, badges, seller_name, ...               │
  └──────────────────────┬──────────────────────────────┘
                         │
           ┌─────────────▼──────────────┐
           │ brand_name ∈ global_dict?  │
           └─────┬────────────┬─────────┘
                 │ YES        │ NO
       ┌─────────▼──┐  ┌─────▼──────────────────────────┐
       │GLOBAL_BRAND│  │ LOCAL_OEM_GENERIC (gộp 2 nhóm) │
       └────────────┘  └─────────────────────────────────┘

Mỗi sản phẩm sẽ có thêm 2 flag bổ trợ:
  - is_official: Cửa hàng chính hãng trên Tiki
  - is_tiki_trading: Được Tiki Trading phân phối/fulfillment
"""

import logging

logger = logging.getLogger(__name__)


class BrandClassifier:
    """
        Phân loại sản phẩm thành 2 nhóm thương hiệu:
    - Global_Brand:  Thương hiệu quốc tế đã biết HOẶC Official Store
        - Local/OEM Generic: Hàng OEM/No Brand/Generic/Cross-border
            và thương hiệu nhỏ lẻ/nội địa chưa có trong từ điển
    """

    # Nhãn phân loại (constants)
    GLOBAL_BRAND = "Global_Brand"
    LOCAL_OEM_GENERIC = "Local/OEM Generic"

    # Backward compatibility alias cho code cũ
    OEM_GENERIC = LOCAL_OEM_GENERIC
    LOCAL_GENERIC = LOCAL_OEM_GENERIC

    def __init__(self, global_brands_list, oem_indicators=None):
        """
        Args:
            global_brands_list (list[str]): Danh sách thương hiệu quốc tế
                từ config.json. Các entry bắt đầu bằng "__" sẽ bị bỏ qua
                (đó là comment/separator trong JSON).
            oem_indicators (list[str]): Từ khóa nhận diện hàng OEM/Generic.
        """
        # Lọc bỏ comment entries (bắt đầu bằng __) và normalize thành lowercase set
        self.global_brands = set(
            b.strip().lower()
            for b in global_brands_list
            if not b.startswith("__")
        )

        self.oem_indicators = set(
            k.strip().lower()
            for k in (oem_indicators or [
                "oem", "no brand", "noname", "generic",
                "unbranded", "không thương hiệu", "none", "other", ""
            ])
        )

        logger.info(
            f"BrandClassifier initialized: "
            f"{len(self.global_brands)} global brands, "
            f"{len(self.oem_indicators)} OEM indicators"
        )

    def classify(self, product_item):
        """
        Phân loại 1 sản phẩm dựa trên Decision Tree.

        NOTE: is_official đã bị loại bỏ khỏi logic phân loại.
        API list endpoint Tiki (/api/v2/products) không trả về badge data
        trong response danh sách → field is_official luôn = 0, gây mislead.

        Args:
            product_item (dict): Raw product data từ Tiki API

        Returns:
            tuple: (brand_type: str, is_tiki_trading: int)
                - brand_type: "Global_Brand" | "Local/OEM Generic"
                - is_tiki_trading: 1 nếu Tiki Trading fulfillment, 0 nếu không
        """
        brand_name_raw = str(product_item.get("brand_name") or "").strip()
        brand_name_lower = brand_name_raw.lower()

        # Phát hiện tín hiệu Tiki Trading (detect được qua seller_name)
        is_tiki_trading = self._detect_tiki_trading(product_item)

        # ========== DECISION TREE ==========

        # Ưu tiên 1: Brand nằm trong từ điển Global
        if brand_name_lower in self.global_brands:
            return self.GLOBAL_BRAND, is_tiki_trading

        # Ưu tiên 2: Brand rỗng hoặc khớp OEM indicators
        if not brand_name_lower or brand_name_lower in self.oem_indicators:
            return self.LOCAL_OEM_GENERIC, is_tiki_trading

        # Ưu tiên 3: Hàng cross-border (thường là OEM Trung Quốc)
        if self._detect_cross_border(product_item):
            return self.LOCAL_OEM_GENERIC, is_tiki_trading

        # Mặc định: Thương hiệu nhỏ lẻ/nội địa
        return self.LOCAL_OEM_GENERIC, is_tiki_trading

    def _detect_tiki_trading(self, item):
        """
        Phát hiện sản phẩm được Tiki Trading phân phối/fulfillment.

        Tiki Trading = Tiki mua hàng và tự bán -> độ uy tín cao hơn.

        Returns:
            int: 1 nếu Tiki Trading, 0 nếu không
        """
        # Kiểm tra seller_name
        seller_name = str(item.get("seller_name") or "").lower()
        if "tiki trading" in seller_name:
            return 1

        # Kiểm tra current_seller object
        current_seller = item.get("current_seller", {}) or {}
        if "tiki trading" in str(current_seller.get("name", "")).lower():
            return 1

        # Kiểm tra badges cho tiki_trading / tikinow
        for field in ["badges_new", "badges"]:
            badges = item.get(field, []) or []
            if isinstance(badges, list):
                for badge in badges:
                    if isinstance(badge, dict):
                        code = str(badge.get("code", "")).lower()
                        if code in ("tiki_trading", "tikinow"):
                            return 1

        return 0

    def _detect_cross_border(self, item):
        """
        Phát hiện hàng cross-border (giao từ nước ngoài).
        Thường là hàng OEM/Generic từ Trung Quốc.

        Returns:
            bool: True nếu cross-border
        """
        for field in ["badges_new", "badges"]:
            badges = item.get(field, []) or []
            if isinstance(badges, list):
                for badge in badges:
                    if isinstance(badge, dict):
                        code = str(badge.get("code", "")).lower()
                        if "cross_border" in code or "imported" in code:
                            return True
        return False
