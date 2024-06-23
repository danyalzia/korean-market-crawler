# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class DysportsCrawlData(CrawlData):
    category: str = ""
    product_url: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    product_name: str = ""
    model_name: str = ""
    brand: str = ""
    model_name2: str = ""
    option1: str = ""
    message1: str = ""
    message2: str = ""
    manufacturing_country: str = ""
    quantity: str = ""
    price2: int = 0
    price3: str = ""
    detailed_images_html_source: str = ""
