from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class Campingb2bCrawlData(CrawlData):
    category: str = ""
    model_name: str = ""
    sold_out_text: str = ""
    product_name: str = ""
    manufacturing_country: str = ""
    product_url: str = ""
    thumbnail_image_url: str = ""
    manufacturer: str = ""
    price3: int = 0
    text_other_than_price: str = ""
    price2: int = 0
    delivery_fee: int = 0
    message1: str = ""
    detailed_images_html_source: str = ""
