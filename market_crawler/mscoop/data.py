# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class MscoopCrawlData(CrawlData):
    product_name: str = ""
    model_name: str = ""
    manufacturing_country: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    price3: int = 0
    price2: int = 0
    delivery_fee: int | str = 0
    detailed_images_html_source: str = ""
    sold_out_text: str = ""
    text_other_than_sold_out: str = ""
