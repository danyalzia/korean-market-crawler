from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class ImacCrawlData(CrawlData):
    category: str = ""
    product_url: str = ""
    product_name: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
    model_name: str = ""
    price2: int = 0
    delivery_fee: str = ""
    sold_out_text: str = ""
    detailed_images_html_source: str = ""
