from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class ApisCrawlData(CrawlData):
    product_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    delivery_fee: int = 0
    model_name: str = ""
    model_name2: str = ""
    percent: str = ""
    price2: int = 0
    price3: int = 0
    message1: str = ""
    category: str = ""
    option1: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    detailed_images_html_source: str = ""
    product_url: str = ""
