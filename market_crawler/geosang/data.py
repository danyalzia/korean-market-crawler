from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class GeosangCrawlData(CrawlData):
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
    price2: int = 0
    price3: int | str = ""
    manufacturing_country: str = ""
    manufacturer: str = ""
    brand: str = ""
    detailed_images_html_source: str = ""
