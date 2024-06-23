# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class PurefishingCrawlData(CrawlData):
    category: str = ""
    product_url: str = ""
    product_name: str = ""
    thumbnail_image_url: str = ""
    option1: str = ""
    model_name: str = ""
    price3: int = 0
    price1: int = 0
    price2: int = 0
    quantity: str = ""
    sold_out_text: str = ""
    detailed_images_html_source: str = ""
    message1: str = ""
