from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class SafetecCrawlData(CrawlData):
    product_name: str = ""
    delivery_fee: int = 0
    price2: int = 0
    quantity: str = ""
    category: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    detailed_images_html_source: str = ""
    product_url: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
