from dataclasses import dataclass

from market_crawler.data import CrawlData


@dataclass(slots=True, frozen=True)
class BlackrhinoCrawlData(CrawlData):
    product_name: str = ""
    model_name: str = ""
    brand: str = ""
    manufacturing_country: str = ""
    sold_out_text: str = ""
    thumbnail_image_url: str = ""
    thumbnail_image_url2: str = ""
    thumbnail_image_url3: str = ""
    thumbnail_image_url4: str = ""
    thumbnail_image_url5: str = ""
    product_url: str = ""
    category: str = ""
    price3: int = 0
    release_date: int = 0
    detailed_images_html_source: str = ""
    option1: str = ""
    option2: str = ""
    option3: str = ""
