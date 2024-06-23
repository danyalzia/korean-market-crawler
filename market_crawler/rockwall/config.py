# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


HEADLESS: Final[bool] = True
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 300000
DEFAULT_TIMEOUT: Final[int] = 300000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 600
DEFAULT_RATE_LIMIT: Final[int] = 20

CATEGORIES_CHUNK_SIZE: Final[int] = 4
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 20
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 20

USE_CATEGORY_SAVE_STATES: Final[bool] = True
USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "바다낚시"
END_CATEGORY: Final[str] = "세트상품"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "rockwall"
