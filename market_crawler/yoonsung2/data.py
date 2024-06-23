from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class YoonSung2CrawlData(CrawlData):
    product_name: str = ""
    model_name: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    price2: int = 0
    price3: int | str = ""
    sold_out_text: str = ""
    quantity: str = ""
    manufacturing_country: str = ""
    detailed_images_html_source: str = ""
