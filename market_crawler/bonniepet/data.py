from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class BonniePetCrawlData(CrawlData):
    product_name: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    sold_out_text: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    price2: int = 0
    price3: int = 0
    delivery_fee: int = 0
    model_name: str = ""
    quantity: str = ""
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
