# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


LOGIN_URL: Final[str] = (
    "https://jkussmall.com/member/login.html?noMemberOrder&returnUrl=%2Findex.html"
)
ID: Final[str] = "ID"
PW: Final[str] = "PW"

HEADLESS: Final[bool] = True
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 300000
DEFAULT_TIMEOUT: Final[int] = 30000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 6000
DEFAULT_RATE_LIMIT: Final[int] = 6

CATEGORIES_CHUNK_SIZE: Final[int] = 2
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 6
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 6

USE_CATEGORY_SAVE_STATES: Final[bool] = True
USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "신상>카멜레온"
END_CATEGORY: Final[str] = "특가>맨"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "jkuss"
