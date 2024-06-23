from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class BanaxCrawlData(CrawlData):
    product_name: str = ""
    model_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    product_url: str = ""
    category: str = ""
    price2: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
