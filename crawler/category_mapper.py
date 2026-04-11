"""
Category Mapper - Thuật toán Định vị Danh mục Sâu (Deep Category Mapper)

Tự động dò tìm tất cả leaf categories (danh mục lá/sâu nhất) từ một
parent category ID. Đảm bảo không bỏ sót bất kỳ sub-category nào
trong cây danh mục của Tiki.

Chiến lược:
  1. Gọi API categories/{parent_id} để lấy thông tin parent
  2. Kiểm tra mảng 'children' trong response
  3. Nếu child không có children -> đó là leaf node
  4. Nếu child có children -> đệ quy sâu hơn
  5. Giới hạn max_depth để tránh infinite recursion
"""

import time
import logging
import requests

logger = logging.getLogger(__name__)

TIKI_CATEGORY_API = "https://tiki.vn/api/v2/categories/{category_id}"


def fetch_category_info(category_id, headers, timeout=15):
    """
    Gọi API Tiki để lấy thông tin chi tiết của 1 category.

    Args:
        category_id (int): ID danh mục Tiki
        headers (dict): HTTP headers
        timeout (int): Request timeout (seconds)

    Returns:
        dict hoặc None: JSON response từ API
    """
    url = TIKI_CATEGORY_API.format(category_id=category_id)
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Không thể fetch category {category_id}: {e}")
        return None


def discover_leaf_categories(parent_id, headers, delay=1.0, max_depth=5):
    """
    Dò tìm đệ quy tất cả leaf categories (danh mục lá) từ parent_id.

    Leaf category = danh mục không có con, là nơi chứa sản phẩm thực tế.
    Ví dụ: "Linh kiện máy tính" (8322) -> children: [RAM, CPU, Mainboard, ...]
    Nếu RAM cũng có children [DDR4, DDR5] thì tiếp tục đào sâu.

    Args:
        parent_id (int): ID danh mục cha
        headers (dict): HTTP headers cho API request
        delay (float): Thời gian chờ giữa các request (giây)
        max_depth (int): Giới hạn độ sâu đệ quy (tránh infinite loop)

    Returns:
        list[dict]: Danh sách leaf categories, mỗi phần tử có:
            - id (int): Category ID
            - name (str): Tên danh mục
            - parent_id (int): ID danh mục cha gốc
    """
    logger.info(f"Bắt đầu dò tìm leaf categories cho parent ID: {parent_id}")

    category_data = fetch_category_info(parent_id, headers)
    if not category_data:
        logger.warning(f"Không fetch được parent category {parent_id}")
        return []

    leaves = []
    _recurse_children(
        category_data=category_data,
        original_parent_id=parent_id,
        headers=headers,
        delay=delay,
        leaves=leaves,
        depth=0,
        max_depth=max_depth
    )

    logger.info(f"Tìm thấy {len(leaves)} leaf categories dưới parent {parent_id}")
    return leaves


def _recurse_children(category_data, original_parent_id, headers, delay,
                      leaves, depth, max_depth):
    """
    Hàm đệ quy nội bộ để duyệt cây danh mục.

    Logic:
    - Nếu node không có children -> thêm vào danh sách leaf
    - Nếu node có children inline (đã có trong response) -> đệ quy trực tiếp
    - Nếu child chưa kèm children -> gọi API kiểm tra xem có sub-children không
    - Giới hạn depth để an toàn
    """
    if depth > max_depth:
        # An toàn: coi node này là leaf để tránh loop vô hạn
        cat_id = category_data.get("id")
        if cat_id:
            leaves.append({
                "id": cat_id,
                "name": category_data.get("name", "Unknown"),
                "parent_id": original_parent_id
            })
        return

    children = category_data.get("children", [])

    if not children:
        # Đây là leaf node (không có con)
        cat_id = category_data.get("id")
        if cat_id:
            leaves.append({
                "id": cat_id,
                "name": category_data.get("name", "Unknown"),
                "parent_id": original_parent_id
            })
        return

    for child in children:
        child_id = child.get("id")
        child_name = child.get("name", "Unknown")

        if not child_id:
            continue

        sub_children = child.get("children", [])

        if sub_children:
            # Child đã có danh sách con inline -> đệ quy trực tiếp
            _recurse_children(
                category_data=child,
                original_parent_id=original_parent_id,
                headers=headers,
                delay=delay,
                leaves=leaves,
                depth=depth + 1,
                max_depth=max_depth
            )
        else:
            # Child chưa rõ có con hay không -> gọi API xác minh
            time.sleep(delay * 0.3)  # Delay nhẹ để tránh rate limit
            child_detail = fetch_category_info(child_id, headers)

            if child_detail and child_detail.get("children"):
                # Có sub-categories ẩn -> đệ quy sâu hơn
                logger.debug(f"  Category {child_id} ({child_name}) có sub-categories ẩn")
                _recurse_children(
                    category_data=child_detail,
                    original_parent_id=original_parent_id,
                    headers=headers,
                    delay=delay,
                    leaves=leaves,
                    depth=depth + 1,
                    max_depth=max_depth
                )
            else:
                # Xác nhận là leaf node
                leaves.append({
                    "id": child_id,
                    "name": child_name,
                    "parent_id": original_parent_id
                })
                logger.debug(f"  Leaf found: [{child_id}] {child_name}")
