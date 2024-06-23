from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class ShulineCrawlData(CrawlData):
    sold_out_text: str = ""
    product_name: str = ""
    model_name: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    price2: int = 0
    period: str = ""
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
