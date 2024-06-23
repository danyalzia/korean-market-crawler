# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from glob import glob
from multiprocessing import cpu_count, freeze_support
from typing import Any, cast

import pandas as pd

from market_crawler import log
from market_crawler.daiwa.app import get_productid
from market_crawler.excel import get_column_mapping


sys.path.insert(0, "..")
sys.path.insert(0, "../..")


async def main():
    date: str = datetime.now().strftime("%Y%m%d")
    temp = os.path.join(os.path.dirname(__file__), "temp", date)
    save_dir = os.path.join(temp, "constructed")

    dfs = await concat_df_from_dir2(temp)
    df: pd.Series[Any] = pd.concat(dfs)  # type: ignore

    column_mapping = get_column_mapping(
        "column_mapping.json",
    )

    urls: list[str] = list(df[column_mapping["product_url"]])  # type: ignore
    images: list[str] = list(df[column_mapping["detailed_images_html_source"]])  # type: ignore

    log.warning(f"Total urls: {len(urls)}")

    for product_url, image in zip(urls, images, strict=True):
        if "not present" in image:
            continue

        if not (productid := get_productid(product_url).ok()):
            continue

        predicate: Any = df[column_mapping["product_url"]] == product_url

        save_html = os.path.join(save_dir, f"{productid}.html")

        if os.path.exists(save_html):
            with open(save_html, encoding="utf-8-sig") as f:
                html_source = f.read()

            df.loc[  # type: ignore
                predicate, column_mapping["detailed_images_html_source"]
            ] = html_source

    df.to_excel("reconstructed_temporary.xlsx", index=False)


async def concat_df_from_dir2(directory: str):
    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        results = [
            executor.submit(
                cast(Any, pd.read_csv),
                os.path.join(directory, filename),  # type: ignore
                dtype=str,
                encoding="utf-8-sig",
            )
            for filename in sorted(glob(os.path.join(directory, "*_temporary.csv")))
        ]

    return [r.result() for r in results]


if __name__ == "__main__":
    freeze_support()
    asyncio.run(main())
