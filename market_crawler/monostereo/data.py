# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class MonostereoCrawlData(CrawlData):
    product_url: str = ""
    category: str = ""
    thumbnail_image_url: str = ""
    product_name: str = ""
    model_name: str = ""
    option3: str = ""
    quantity: str = ""
    price2: int | str = 0
    price3: int | str = 0
    option2: str = ""
    model_name2: str = ""
    period: str = ""
    manufacturer: str = ""
    percent: str = ""
    brand: str = ""
    option4: str = ""
    message1: str = ""
    message2: str = ""
    message3: str = ""
    message4: str = ""
    delivery_fee: str = ""
    detailed_images_html_source: str = ""
    option1: str = ""
