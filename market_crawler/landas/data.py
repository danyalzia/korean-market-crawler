from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class LandasCrawlData(CrawlData):
    product_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    brand: str = ""
    option1: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    detailed_images_html_source: str = ""
    price2: int = 0
    price3: int = 0
    product_url: str = ""
    category: str = ""
