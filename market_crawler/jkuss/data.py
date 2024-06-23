# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class JkussCrawlData(CrawlData):
    sold_out_text: str = ""
    product_name: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    manufacturing_country: str = ""
    manufacturer: str = ""
    delivery_fee: str = ""
    brand: str = ""
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    price1: int | str = 0
    price2: int | str = 0
    price3: int | str = 0
