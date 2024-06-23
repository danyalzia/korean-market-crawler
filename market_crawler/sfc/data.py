# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class SFCCrawlData(CrawlData):
    category: str = ""
    product_url: str = ""
    thumbnail_image_url: str = ""
    price2: int | str = 0
    product_name: str = ""
    model_name: str = ""
    detailed_images_html_source: str = ""
