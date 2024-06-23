# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from functools import singledispatch
from typing import TYPE_CHECKING, Protocol

from market_crawler.helpers import chunks


if TYPE_CHECKING:
    from dunia.playwright import PlaywrightBrowser
    from market_crawler.initialization import Category
    from market_crawler.settings import Settings


class Crawl(Protocol):
    async def __call__(
        self,
        category: Category,
        browser: PlaywrightBrowser,
        settings: Settings,
        columns: list[str],
    ): ...


@dataclass(slots=True, frozen=True, kw_only=True)
class SequentialCrawler:
    categories: list[Category]
    start_category: str
    end_category: str
    crawl: Crawl


@dataclass(slots=True, frozen=True, kw_only=True)
class ConcurrentCrawler:
    categories: list[Category]
    start_category: str
    end_category: str
    chunk_size: int
    crawl: Crawl


def categories_range(
    categories: list[Category], start_category: str, end_category: str
):
    start_category_index = 0
    end_category_index = len(categories)
    for idx, cat in enumerate(categories):
        if cat.name == start_category:
            start_category_index = idx

        if cat.name == end_category:
            end_category_index = idx
    return start_category_index, end_category_index


@singledispatch
async def crawl_categories(
    crawler: SequentialCrawler | ConcurrentCrawler,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
): ...


@crawl_categories.register(SequentialCrawler)
async def _(
    crawler: SequentialCrawler,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    start_category_index, end_category_index = categories_range(
        crawler.categories, crawler.start_category, crawler.end_category
    )
    for category in crawler.categories[start_category_index : end_category_index + 1]:
        await crawler.crawl(category, browser, settings, columns)


@crawl_categories.register(ConcurrentCrawler)
async def _(
    crawler: ConcurrentCrawler,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    start_category_index, end_category_index = categories_range(
        crawler.categories, crawler.start_category, crawler.end_category
    )
    categories_subset = crawler.categories[
        start_category_index : end_category_index + 1
    ]
    range_chunks = chunks(range(len(categories_subset)), crawler.chunk_size)
    for categories_chunk in range_chunks:
        tasks = (
            asyncio.create_task(
                crawler.crawl(
                    categories_subset[idx],
                    browser,
                    settings,
                    columns,
                )
            )
            for idx in categories_chunk
        )

        await asyncio.gather(*tasks)
