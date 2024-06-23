# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class YongSungCrawlData(CrawlData):
    category: str = ""
    product_url: str = ""
    thumbnail_image_url: str = ""
    price2: int = 0
    price3: int = 0
    percent: str = ""
    manufacturing_country: str = ""
    product_name: str = ""
    product_code: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    sold_out_text: str = ""
    detailed_images_html_source: str = ""
