from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class HituzenCrawlData(CrawlData):
    product_name: str = ""
    model_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    delivery_fee: int = 0
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    price2: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    sold_out_text: str = ""
