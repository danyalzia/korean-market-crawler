# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import psutil

from market_crawler import error
from market_crawler.log import logger


if TYPE_CHECKING:
    from dunia.playwright import PlaywrightBrowser, PlaywrightPage


@dataclass(kw_only=True)
class MemoryOptimizer:
    max_products_chunk_size: int
    page_memory: float = field(init=False, repr=False, default=0)
    memory_usage: float = field(init=False, repr=False, default=0)

    async def optimize_products_chunk_sizes(
        self,
        browser: PlaywrightBrowser,
        categories_chunk_size: int,
        products_chunk_size: int,
    ):
        if len(browser.pages) < 1:
            raise ValueError("OPTIMIZER: No page is present in browser")

        if products_chunk_size == 0:
            products_chunk_size = 1
            logger.log(
                "OPTIMIZER", "Products chunk size was 0, therefore it is set to 1"
            )

        if categories_chunk_size == 0:
            categories_chunk_size = 1
            logger.log(
                "OPTIMIZER", "Categories chunk size was 0, therefore it is set to 1"
            )

        self.page_memory = await max_page_memory(browser.pages)

        number_of_concurrent_pages = products_chunk_size * categories_chunk_size

        # ? Estimated memory usage
        self.memory_usage = self.page_memory * number_of_concurrent_pages

        logger.log("OPTIMIZER", f"Page memory: {self.page_memory: 0.1f} MB")
        logger.log(
            "OPTIMIZER",
            f"Number of concurrent pages: {number_of_concurrent_pages} ({products_chunk_size} x {categories_chunk_size})",
        )
        logger.log(
            "OPTIMIZER",
            f"Memory usage: {self.memory_usage: 0.1f} MB",
        )
        logger.log("OPTIMIZER", f"Available memory: {available_memory(): 0.1f} MB")

        # ? Preserve 4 GB for system usage
        while (
            self.memory_usage >= (available_memory() - 4000) and products_chunk_size > 1
        ):
            # ? Decrease the chunk size by 1
            new_products_chunk_size = products_chunk_size - 1

            logger.log(
                "OPTIMIZER",
                f"Decreasing products chunk size from {products_chunk_size} to {new_products_chunk_size}",
            )

            products_chunk_size = new_products_chunk_size

        while (
            self.memory_usage < (available_memory() - 4000)
            and products_chunk_size < self.max_products_chunk_size
        ):
            estimated_memory_budget = (available_memory() - 4000) - self.memory_usage

            new_products_chunk_size = round(
                (estimated_memory_budget / self.page_memory) / categories_chunk_size
            )

            logger.log(
                "OPTIMIZER",
                f"Increasing products chunk size from {products_chunk_size} to {new_products_chunk_size}",
            )

            products_chunk_size = new_products_chunk_size

        if products_chunk_size <= self.max_products_chunk_size:
            new_products_chunk_size = products_chunk_size
        else:
            logger.log(
                "OPTIMIZER",
                f"New products chunk size {products_chunk_size} is larger than the max products chunk size {self.max_products_chunk_size}, therefore changing it back to {self.max_products_chunk_size}",
            )
            new_products_chunk_size = self.max_products_chunk_size

        logger.log("OPTIMIZER", f"New products chunk size: {new_products_chunk_size}")

        return new_products_chunk_size


# ? Adapted from http://code.activestate.com/recipes/578019
def bytes2human(n: int):
    symbols = ("K", "M", "G", "T", "P", "E", "Z", "Y")
    prefix = {s: 1 << (i + 1) * 10 for i, s in enumerate(symbols)}
    return next(
        (f"{float(n) / prefix[s]:.1f}{s}" for s in reversed(symbols) if n >= prefix[s]),
        f"{n}B",
    )


def to_megabytes(n: int):
    return n / float(1 << 20)


def available_memory():
    return to_megabytes(psutil.virtual_memory().available)  # type: ignore


async def page_memory(page: PlaywrightPage):
    return to_megabytes(
        int(
            await page.evaluate(
                "() => { return window.performance.memory.usedJSHeapSize; }"
            )
        )
    )


async def max_page_memory(pages: list[PlaywrightPage]):
    page_memories: list[float] = []
    for idx, page in enumerate(pages, start=1):
        try:
            memory = await page_memory(page)
        except error.PlaywrightError:
            continue
        else:
            logger.log("OPTIMIZER", f"Page # {idx} memory: {memory: 0.1f} MB")
            page_memories.append(memory)

    return max(page_memories)
