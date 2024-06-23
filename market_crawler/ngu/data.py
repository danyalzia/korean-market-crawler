# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class NGUCrawlData(CrawlData):
    product_name: str = ""
    manufacturing_country: str = ""
    price3: int = 0
    price2: int = 0
    quantity: str = ""
    category: str = ""
    option1: str = ""
    thumbnail_image_url: str = ""
    detailed_images_html_source: str = ""
    product_url: str = ""
    sold_out_text: str = ""
