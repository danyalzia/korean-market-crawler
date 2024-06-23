# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from typing import cast

from aiofile import AIOFile


@dataclass(slots=True, kw_only=True)
class CategoryHTML:
    name: str
    pageno: int
    date: str
    sitename: str

    def __post_init__(self):
        self.name = self.name.replace("/", "_").replace(">", "_").replace(":", "_")

    @property
    def directory(self) -> str:
        return os.path.join(os.path.dirname(__file__), self.sitename, "html", self.date)

    @property
    def file(self) -> str:
        return os.path.join(
            self.directory,
            f"{self.name}-{self.pageno}.html",
        )

    async def save(self, content: str, encoding: str = "utf-8-sig") -> None:
        await asyncio.to_thread(
            os.makedirs,
            self.directory,
            exist_ok=True,
        )
        async with AIOFile(
            self.file,
            "w",
            encoding=encoding,
        ) as afp:
            await afp.write(content)

    async def load(self, encoding: str = "utf-8-sig") -> str:
        async with AIOFile(
            self.file,
            "r",
            encoding=encoding,
        ) as afp:
            html = cast("str", await afp.read())

        return html

    async def exists(self) -> bool:
        return await asyncio.to_thread(os.path.exists, self.file)


@dataclass(slots=True, kw_only=True)
class ProductHTML:
    category_name: str
    pageno: int
    productid: str
    date: str
    sitename: str

    def __post_init__(self):
        self.category_name = (
            self.category_name.replace("/", "_").replace(">", "_").replace(":", "_")
        )

    @property
    def directory(self) -> str:
        return os.path.join(
            os.path.dirname(__file__),
            self.sitename,
            "html",
            self.date,
            self.category_name,
            str(self.pageno),
        )

    @property
    def file(self) -> str:
        return os.path.join(self.directory, f"{self.productid}.html")

    async def save(self, content: str, encoding: str = "utf-8-sig") -> None:
        await asyncio.to_thread(
            os.makedirs,
            self.directory,
            exist_ok=True,
        )
        async with AIOFile(
            self.file,
            "w",
            encoding,
        ) as afp:
            await afp.write(content)

    async def load(self, encoding: str = "utf-8-sig") -> str:
        async with AIOFile(
            self.file,
            "r",
            encoding=encoding,
        ) as afp:
            html = cast("str", await afp.read())

        return html

    async def exists(self) -> bool:
        return await asyncio.to_thread(os.path.exists, self.file)
