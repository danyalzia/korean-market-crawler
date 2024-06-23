# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class DanharooCrawlData(CrawlData):
    product_name: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    detailed_images_html_source: str = ""
    price2: int | str = 0
    price3: int | str = 0
    model_name: str = ""
    quantity: str = ""
    manufacturing_country: str = ""
    option1: str = ""
    option2: str = ""
