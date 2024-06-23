# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class XeeonCrawlData(CrawlData):
    model_name: str = ""
    sold_out_text: str = ""
    product_name: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    delivery_fee: int | str = ""
    brand: str = ""
    detailed_images_html_source: str = ""
    price3: int | str = 0
    consumer_fee: int | str = 0
    option1: str = ""
    option2: str = ""
    option3: str = ""
    option1_title: str = ""
