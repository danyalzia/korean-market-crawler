# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from functools import cache
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from market_crawler.helpers import igetattr


if TYPE_CHECKING:
    from typing import Any

    from market_crawler.data import CrawlData


# ? Market configuration
class Config(Protocol):
    HEADLESS: bool
    DEFAULT_NAVIGATION_TIMEOUT: int
    DEFAULT_TIMEOUT: int
    DEFAULT_ASYNC_TIMEOUT: int
    DEFAULT_RATE_LIMIT: int
    ID: str
    PW: str
    CATEGORIES_CHUNK_SIZE: int
    MIN_PRODUCTS_CHUNK_SIZE: int
    MAX_PRODUCTS_CHUNK_SIZE: int
    USE_CATEGORY_SAVE_STATES: bool
    USE_PRODUCT_SAVE_STATES: bool
    SAVE_HTML: bool
    START_CATEGORY: str
    END_CATEGORY: str
    START_PAGE: int
    SITENAME: str


@cache
def get_market_config(sitename: str) -> Config:
    return cast(
        "Config",
        (
            import_module(f"market_crawler.{Path(sitename).parent.stem}.config")
            if Path(sitename).stem == "tests"
            else import_module(f"market_crawler.{Path(sitename).stem}.config")
        ),
    )


@cache
def get_market_data(sitename: str):
    data: Any = import_module(f"market_crawler.{sitename}.data")

    crawl_data: type[CrawlData] = igetattr(data, f"{sitename}CrawlData")

    return crawl_data()
