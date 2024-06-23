from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class OssenbergCrawlData(CrawlData):
    product_url: str = ""
    category: str = ""
    product_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    model_name: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    detailed_images_html_source: str = ""
    price2: int = 0
    price3: int = 0
    option1: str = ""
    option2: str = ""
    option3: str = ""
