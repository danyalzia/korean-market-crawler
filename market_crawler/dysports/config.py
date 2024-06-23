# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


LOGIN_URL: Final[str] = (
    "http://www.dy-sports.com/login?url=http%3A%2F%2Fwww.dy-sports.com%2F"
)
ID: Final[str] = "ID"
PW: Final[str] = "PW"

HEADLESS: Final[bool] = True
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 300000
DEFAULT_TIMEOUT: Final[int] = 300000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 60
DEFAULT_RATE_LIMIT: Final[int] = 10

CATEGORIES_CHUNK_SIZE: Final[int] = 5
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 10
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 10

USE_CATEGORY_SAVE_STATES: Final[bool] = True
USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "요가/필라테스"
END_CATEGORY: Final[str] = "복싱/잡화"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "dysports"
