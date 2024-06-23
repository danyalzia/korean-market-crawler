# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class TusaCrawlData(CrawlData):
    product_name: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    detailed_images_html_source: str = ""
    price2: int | str = 0
    manufacturing_country: str = ""
    manufacturer: str = ""
    brand: str = ""
    option1: str = ""
