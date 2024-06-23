# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


LOGIN_URL: Final[str] = "http://aqusb2b.com/happy_member_login.php"
ID: Final[str] = "ID"
PW: Final[str] = "PW"

HEADLESS: Final[bool] = True
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 300000
DEFAULT_TIMEOUT: Final[int] = 30000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 6000
DEFAULT_RATE_LIMIT: Final[int] = 10

CATEGORIES_CHUNK_SIZE: Final[int] = 2
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 10
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 10

USE_CATEGORY_SAVE_STATES: Final[bool] = True
USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "수영복"
END_CATEGORY: Final[str] = "수상안전"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "aqus"
