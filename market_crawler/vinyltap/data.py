from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class VinyltapCrawlData(CrawlData):
    product_url: str = ""
    product_name: str = ""
    model_name: str = ""
    message3: str = ""
    message2: str = ""
    message4: str = ""
    price2: str = ""
    category: str = ""
    thumbnail_image_url: str = ""
    option1: str = ""
    model_name2: str = ""
    percent: str = ""
    manufacturing_country: str = ""
    quantity: str = ""
    period: str = ""
    manufacturer: str = ""
    price1: str = ""
    message1: str = ""
    detailed_images_html_source: str = ""
