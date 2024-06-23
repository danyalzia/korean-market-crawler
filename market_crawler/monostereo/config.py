# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from typing import Final


LOGIN_URL: Final[str] = "https://b2b.monostereo1stop.com/splashpage"
ID: Final[str] = "ID"
PW: Final[str] = "PW"

HEADLESS: Final[bool] = True
DEFAULT_NAVIGATION_TIMEOUT: Final[int] = 3000000
DEFAULT_TIMEOUT: Final[int] = 30000
DEFAULT_ASYNC_TIMEOUT: Final[int] = 12000
DEFAULT_RATE_LIMIT: Final[int] = 20

CATEGORIES_CHUNK_SIZE: Final[int] = 10
MIN_PRODUCTS_CHUNK_SIZE: Final[int] = 15
MAX_PRODUCTS_CHUNK_SIZE: Final[int] = 80

USE_CATEGORY_SAVE_STATES: Final[bool] = True
USE_PRODUCT_SAVE_STATES: Final[bool] = True
SAVE_HTML: Final[bool] = True

START_CATEGORY: Final[str] = "New Releases>In stock Only"
END_CATEGORY: Final[str] = "Speakers>Back Order Only"
START_PAGE: Final[int] = 1

SITENAME: Final[str] = "monostereo"
