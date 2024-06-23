# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import pickle

from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aiofile import AIOFile

from market_crawler.log import debug


if TYPE_CHECKING:
    from typing import Self

    from market_crawler.config import Config


@dataclass(slots=True, kw_only=True)
class CategoryState:
    name: str
    pageno: int
    date: str
    sitename: str
    done: bool = field(init=False, default=False)

    def __post_init__(self):
        self.name = self.name.replace("/", "_").replace(">", "_").replace(":", "_")
        # ? Possible workaround to odd behaviour when deserializing through pickle.loads() when it throws "... object has no attribute 'done'" error
        self.done = False

    @property
    def directory(self) -> str:
        return os.path.join(
            os.path.dirname(__file__), self.sitename, "states", self.date
        )

    @property
    def file(self) -> str:
        return os.path.join(self.directory, f"{self.name}.pkl")

    async def exists(self) -> bool:
        return await asyncio.to_thread(os.path.exists, self.file)

    async def load(self) -> Self:
        try:
            async with AIOFile(self.file, "rb") as f:
                bytes_ = await f.read_bytes()
                self = await asyncio.to_thread(pickle.loads, bytes_)

        except EOFError as err:
            debug(f"Category state ({self.file}) load faild: {err}")

        return self

    async def save(self) -> None:
        with suppress(FileExistsError):
            await asyncio.to_thread(
                os.makedirs,
                self.directory,
                exist_ok=True,
            )

        with suppress(FileNotFoundError):
            try:
                async with AIOFile(self.file, "wb") as f:
                    await f.write(await asyncio.to_thread(pickle.dumps, self))
            except OSError as err:
                debug(f"Category state ({self.file}) save faild: {err}")
                # ? OSError: [Errno 9] Bad file descriptor
                if err.errno != 9:
                    raise err from err


@dataclass(slots=True, kw_only=True)
class ProductState:
    productid: str
    category_name: str
    date: str
    sitename: str
    done: bool = field(init=False, default=False)

    def __post_init__(self):
        self.category_name = (
            self.category_name.replace("/", "_").replace(">", "_").replace(":", "_")
        )
        # ? Possible workaround to odd behaviour when deserializing through pickle.loads() when it throws "... object has no attribute 'done'" error
        self.done = False

    @property
    def directory(self) -> str:
        return os.path.join(
            os.path.dirname(__file__),
            self.sitename,
            "states",
            self.date,
            self.category_name,
        )

    @property
    def file(self) -> str:
        return os.path.join(self.directory, f"{self.productid}.pkl")

    async def exists(self) -> bool:
        return await asyncio.to_thread(os.path.exists, self.file)

    async def load(self) -> Self:
        try:
            async with AIOFile(self.file, "rb") as f:
                bytes_ = await f.read_bytes()
                self = await asyncio.to_thread(pickle.loads, bytes_)
        except EOFError as err:
            debug(f"Product state ({self.file}) load faild: {err}")

        return self

    async def save(self) -> None:
        with suppress(FileExistsError):
            await asyncio.to_thread(
                os.makedirs,
                self.directory,
                exist_ok=True,
            )

        with suppress(FileNotFoundError):
            try:
                async with AIOFile(self.file, "wb") as f:
                    await f.write(await asyncio.to_thread(pickle.dumps, self))
            except OSError as err:
                debug(f"Product state ({self.file}) save faild: {err}")
                # ? OSError: [Errno 9] Bad file descriptor
                if err.errno != 9:
                    raise err from err


async def get_category_state(
    config: Config,
    category_name: str,
    date: str,
) -> CategoryState | None:
    """
    Create new category state object for serialization

    If the category has already been crawled, then it returns None
    """
    if config.USE_CATEGORY_SAVE_STATES:
        pageno = config.START_PAGE

        category_state = CategoryState(
            name=category_name,
            pageno=pageno,
            date=date,
            sitename=config.SITENAME,
        )

        if await category_state.exists():
            category_state = await category_state.load()

            # ? Let's check if state file is present in folder different than its date, if it is, then change the date to current date
            if category_state.date != date:
                debug(
                    f"Category state for {category_state.name} was saved on {category_state.date}, but it is present in current date ({date}) directory, so its date will be changed to current date ({date})."
                )
                category_state.date = date

            if category_state.done:
                debug(
                    f"Category {category_state.name} was crawled already, so skipping it."
                )

                return None

        return category_state

    pageno = config.START_PAGE

    return CategoryState(
        name=category_name,
        pageno=pageno,
        date=date,
        sitename=config.SITENAME,
    )


async def get_product_state(
    config: Config,
    productid: str,
    category_name: str,
    date: str,
) -> ProductState | None:
    """
    Create new product state object for serialization

    If the product has already been crawled, then it returns None
    """
    if not config.USE_PRODUCT_SAVE_STATES:
        return ProductState(
            productid=productid,
            category_name=category_name,
            date=date,
            sitename=config.SITENAME,
        )

    product_state = ProductState(
        productid=productid,
        category_name=category_name,
        date=date,
        sitename=config.SITENAME,
    )

    if await product_state.exists():
        product_state = await product_state.load()

        # ? Let's check if state file is present in folder different than its date, if it is, then change the date to current date
        if product_state.date != date:
            debug(
                f"Product state for {product_state.productid} was saved on {product_state.date}, but it is present in current date ({date}) directory, so its date will be changed to current date ({date})."
            )
            product_state.date = date

        if product_state.done:
            debug(
                f"Product {product_state.productid} was crawled already, so skipping it."
            )

            return None

    return product_state
