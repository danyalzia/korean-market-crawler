# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class CrawlData:
    product_name: str | None = field(
        default=None, repr=False, metadata={"help": "Actual product name"}
    )
    model_name: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Model name (it is usually in uppercase)"},
    )
    model_name2: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Second model name (for markets which have more than one)"},
    )
    category: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Category of the product being crawled"},
    )
    sold_out_text: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Text that is present when the product is sold out"},
    )
    text_other_than_sold_out: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Some markets have additional information besides sold out"},
    )
    product_url: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "The URL of the cralwed product. We may use it to later to verify the products or it could be useful to scrap the data again using HTTP based methods"
        },
    )
    thumbnail_image_url: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "URL of the thumbnail image. It is usually present in the left side of the product information table on the product page"
        },
    )
    thumbnail_image_url2: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "URL of the second thumbnail image (for markets which have more than one)"
        },
    )
    thumbnail_image_url3: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "URL of the third thumbnail image (for markets which have more than two)"
        },
    )
    thumbnail_image_url4: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "URL of the fourth thumbnail image (for markets which have more than three)"
        },
    )
    thumbnail_image_url5: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "URL of the fifth thumbnail image (for markets which have more than four)"
        },
    )
    detailed_images_html_source: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Product detail images' URLs that is enclosed inside our HTML source template"
        },
    )
    detailed_images_html_source2: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Second product detail images' URLs that is enclosed inside our HTML source template (for markets which have more one)"
        },
    )
    detailed_images_html_source_problem_text: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Information to store if product detailed images have not been found on the page or they cannot be open or they are corrupt but is required for crawling"
        },
    )
    price3: int | str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Price in which the product is being sold"},
    )
    text_other_than_price: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Some markets have additional information besides the price"},
    )
    price2: int | str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Price in which the product is being supplied"},
    )
    manufacturer: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Manufacturer or company name of the product"},
    )
    manufacturing_country: str | None = field(
        default=None,
        repr=False,
        metadata={"help": 'Manufacturing country. Some markets call it "origin"'},
    )
    brand: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Brand name. This is different from Manufacturer when present"
        },
    )
    product_code: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Product code which is different Model name when present"},
    )
    release_date: int | None = field(
        default=None, repr=False, metadata={"help": "Release date"}
    )
    delivery_fee: int | str | None = field(
        default=None, repr=False, metadata={"help": "Delivery or shipping fee"}
    )
    delivery_fee2: int | str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Second delivery or shipping fee (for markets which have more than one)"
        },
    )
    delivery_fee_classification: int | str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Delivery or shipping fee classification"},
    )
    tax_division: int | str | None = field(
        default=None, repr=False, metadata={"help": "Tax division"}
    )
    price1: int | str | None = field(
        default=None, repr=False, metadata={"help": "Can be any kind of price"}
    )
    price2: int | str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Can be any kind of price but the second kind of price if present on the page"
        },
    )
    price3: int | str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Can be any kind of price but the third kind of price if present on the page"
        },
    )
    discount_price: int | str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Discount price or amount of discount"},
    )
    percent: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Price percentage difference between two prices"},
    )
    option1_title: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Name of the options dropdown/combobox, usually color or size"
        },
    )
    option1: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "The first options dropdown/combobox"},
    )
    option2: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "The second options dropdown/combobox if present on the page"
        },
    )
    option3: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "The third options dropdown/combobox if present on the page"},
    )
    option4: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "The fourth options dropdown/combobox if present on the page"
        },
    )
    message1: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Additional information (first kind)"},
    )
    message2: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Additional information (second kind)"},
    )
    message3: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Additional information (third kind)"},
    )
    message4: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Additional information (fourth kind)"},
    )
    quantity: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Quantity of the product"},
    )
    period: str | None = field(
        default=None,
        repr=False,
        metadata={"help": "Discount period"},
    )
    item_number_code: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Code that is written manually to the crawled file during the registration of the product. This will be used for all product entries that may or may not have different option1 or other texts."
        },
    )
    single_item_code: str | None = field(
        default=None,
        repr=False,
        metadata={
            "help": "Similar to Item number code. However, this is used only for the specific product entry containing unique texts."
        },
    )

    def __post_init__(self):
        # ? Make sure that the subclass contains only the attributes present here and not its own attributes
        # ? See: https://stackoverflow.com/questions/7136154/python-how-to-get-subclasss-new-attributes-name-in-base-classs-method
        subclass_attributes = set(dir(self.__class__)) - set(dir(CrawlData))

        if subclass_attributes and "__slotnames__" not in subclass_attributes:
            raise AttributeError(
                f"Derived class should have same attributes as Base class (got extra attributes: {', '.join(subclass_attributes)})"
            )
