from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class PGRGolfCrawlData(CrawlData):
    product_url: str = ""
    category: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    price3: int = 0
    price2: int = 0
    model_name: str = ""
    quantity: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    detailed_images_html_source: str = ""
