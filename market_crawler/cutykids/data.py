# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class CutyKidsCrawlData(CrawlData):
    product_name: str = ""
    brand: str = ""
    price3: int | str = 0
    percent: str = ""
    price2: int = 0
    message1: str = ""
    message2: str = ""
    category: str = ""
    option1: str = ""
    thumbnail_image_url: str = ""
    detailed_images_html_source: str = ""
    product_url: str = ""
    sold_out_text: str = ""
