from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class YoonSung1CrawlData(CrawlData):
    product_name: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    brand: str = ""
    manufacturing_country: str = ""
    price3: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
    option4: str = ""
