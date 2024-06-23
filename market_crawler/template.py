# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import json
import os

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pandas as pd

from excelsheet.dataframe import remove_existing_rows
from excelsheet.utils import col_to_excel
from market_crawler.data import CrawlData
from market_crawler.path import csv_template_file


if TYPE_CHECKING:
    from collections.abc import Iterable


# # * Reserved Columns
# # * According to TEMPLATE_DB.xlsx
@dataclass(slots=False, frozen=True)
class TemplateColumns:
    BRAND_COLUMN: str = "브랜드명\n[필수]"
    CATEGORY_COLUMN: str = "물류처ID"
    DELIVERY_FEE2_COLUMN: str = "배송비"
    DELIVERY_FEE_CLASSIFICATION_COLUMN: str = "배송비구분\n[필수]"
    DELIVERY_FEE_COLUMN: str = "공급처\n지불\n배송비"
    DETAILED_IMAGES_HTML_SOURCE2_COLUMN: str = "상품상세설명\n[사방넷]"
    DETAILED_IMAGES_HTML_SOURCE_COLUMN: str = "상품상세설명\n[필수]"
    DETAILED_IMAGES_HTML_SOURCE_PROBLEM_TEXT_COLUMN: str = "추가상품그룹코드"
    DISCOUNT_PRICE_COLUMN: str = "반품지구분"
    ITEM_NUMBER_CODE_COLUMN: str = "Item number code"
    MANUFACTURER_COLUMN: str = "제조사\n[필수]"
    MANUFACTURING_COUNTRY_COLUMN: str = "원산지(제조국)\n[필수]"
    MESSAGE1_COLUMN: str = "message1"
    MESSAGE2_COLUMN: str = "message2"
    MESSAGE3_COLUMN: str = "message3"
    MESSAGE4_COLUMN: str = "message4"
    MODEL_NAME2_COLUMN: str = "모델명2"
    MODEL_NAME_COLUMN: str = "모델명"
    OPTION1_COLUMN: str = "옵션상세명칭(1)"
    OPTION1_TITLE_COLUMN: str = "옵션제목(1)"
    OPTION2_COLUMN: str = "옵션상세명칭(2)"
    OPTION3_COLUMN: str = "option3"
    OPTION4_COLUMN: str = "option4"
    PERCENT_COLUMN: str = "percent"
    PERIOD_COLUMN: str = "period"
    PRICE1_COLUMN: str = "price1"
    PRICE2_COLUMN: str = "원가\n[필수]"
    PRICE3_COLUMN: str = "판매가\n[필수]"
    PRODUCT_CODE_COLUMN: str = "자체상품코드"
    PRODUCT_NAME_COLUMN: str = "원본 상품명"
    PRODUCT_URL_COLUMN: str = "모델NO"
    QUANTITY_COLUMN: str = "quantity"
    RELEASE_DATE_COLUMN: str = "제조일"
    SINGLE_ITEM_CODE_COLUMN: str = "Single item code"
    SOLD_OUT_TEXT_COLUMN: str = "상품약어"
    TAX_DIVISION_COLUMN: str = "세금구분\n[필수]"
    TEXT_OTHER_THAN_PRICE_COLUMN: str = "반품지구분"
    TEXT_OTHER_THAN_SOLD_OUT_COLUMN: str = "반품지구분"
    THUMBNAIL_IMAGE_URL2_COLUMN: str = "부가이미지6"
    THUMBNAIL_IMAGE_URL3_COLUMN: str = "부가이미지7"
    THUMBNAIL_IMAGE_URL4_COLUMN: str = "부가이미지8"
    THUMBNAIL_IMAGE_URL5_COLUMN: str = "부가이미지9"
    THUMBNAIL_IMAGE_URL_COLUMN: str = "대표이미지\n[필수]"
    # SELLING_PRICE_COLUMN: str = "원가\n[필수]"
    # SHIPPING_FEE2_COLUMN: str = "배송비"
    # SHIPPING_FEE_CLASSIFICATION_COLUMN: str = "배송비구분\n[필수]"
    # SHIPPING_FEE_COLUMN: str = "공급처\n지불\n배송비"
    # SUPPLY_PRICE_COLUMN: str = "판매가\n[필수]"


def get_template_columns(filename: str) -> list[str]:
    if not filename:
        raise ValueError("Tempalte filename cannot be empty")

    template_file = csv_template_file(filename)

    template_dataframe = pd.read_csv(template_file, encoding="utf-8-sig")

    return cast("list[str]", remove_existing_rows(template_dataframe).columns)


def dump_template_column_mapping_to_json(sitename: str):
    columns = get_template_columns("TEMPLATE_DB.xlsx")
    col_dict: dict[str, str] = {
        columns[x - 1]: col_to_excel(x) for x in range(1, len(columns) + 1)
    }
    excel_columns = list(col_dict)
    template_columns = TemplateColumns()

    template_columns_attrs = [
        i for i in dir(template_columns) if not i.startswith("__")
    ]

    crawl_data_attrs = set(dir(CrawlData))

    json_mapping: dict[Any, str] = {}
    for column in excel_columns:
        for attr in template_columns_attrs:
            if template_columns.__getattribute__(attr) == column:
                attr_name = attr.lower().replace("_column", "")
                assert attr_name in crawl_data_attrs, f"{attr_name}"
                json_mapping[attr_name] = column

    with open(
        os.path.join(os.path.dirname(__file__), sitename, "column_mapping.json"),
        "w",
        encoding="utf-8",
    ) as f:
        f.write(json.dumps(json_mapping, ensure_ascii=False))


def build_detailed_images_html(
    urls: Iterable[str], html_source_top: str, html_source_bottom: str
):
    """
    Build HTML of detailed images from the URLs
    """

    # ? Remove duplicate urls (if any) but preserve the insertion order
    html = "".join(f"<img src='{url}' /><br />" for url in dict.fromkeys(urls))

    html = "".join(
        [
            html_source_top,
            html,
            html_source_bottom,
        ],
    )

    return html.strip()
