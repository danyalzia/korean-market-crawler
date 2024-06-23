# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


HEADLESS: Final[bool] = False
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 300000
DEFAULT_TIMEOUT: Final[int] = 30000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 600
DEFAULT_RATE_LIMIT: Final[int] = 20

CATEGORIES_CHUNK_SIZE: Final[int] = 5
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 20
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 20

USE_CATEGORY_SAVE_STATES: Final[bool] = (
    False  # ? We are not changing pages for categories in this market, so it doesn't make sense to make this True
)

USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "호흡기"
END_CATEGORY: Final[str] = "기타 브랜드"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "scubapro"
