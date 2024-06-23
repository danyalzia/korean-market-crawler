# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class KiganismCrawlData(CrawlData):
    model_name: str = ""
    brand: str = ""
    delivery_fee: str = ""
    sold_out_text: str = ""
    product_name: str = ""
    product_url: str = ""
    category: str = ""
    message1: str = ""
    manufacturer: str = ""
    thumbnail_image_url: str = ""
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    price2: int | str = 0
