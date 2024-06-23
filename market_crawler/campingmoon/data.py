from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class CampingmoonCrawlData(CrawlData):
    category: str = ""
    model_name: str = ""
    sold_out_text: str = ""
    product_name: str = ""
    manufacturing_country: str = ""
    product_url: str = ""
    thumbnail_image_url: str = ""
    price3: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
