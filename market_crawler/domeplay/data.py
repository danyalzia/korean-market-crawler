from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class DomeplayCrawlData(CrawlData):
    sold_out_text: str = ""
    product_name: str = ""
    model_name: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    price3: int = 0
    delivery_fee: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
