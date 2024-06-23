# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import os

from contextlib import suppress
from os.path import join

import pandas as pd


def csv_template_file(filename: str) -> str:
    """
    .CSV template file that will be used to save the crawled data

    If .CSV is not present but .XLSX is present, then it will also convert .XLSX file and save it as .CSV
    """
    return to_csv(filename)


def to_csv(filename: str) -> str:
    """
    Convert .XLSX to .CSV format
    """
    if not filename.endswith(".xlsx"):
        raise OSError("File is not in .XLSX extension")

    csv_file = filename.replace(".xlsx", ".csv")
    with suppress(OSError):
        os.remove(csv_file)

    pd.read_excel(filename, dtype=str).to_csv(
        csv_file, encoding="utf-8-sig", index=False
    )
    return csv_file


def temporary_xlsx_file(
    *,
    sitename: str,
    date: str,
    category_name: str,
    page_no: int,
) -> str:
    """
    Per page of the category .XLSX file in the temporary directory
    """
    return join(
        os.path.dirname(__file__),
        sitename,
        "temp",
        date,
        f"products_{sitename}_{date}_{category_name.replace('/', '_').replace('>', '_').replace(':', '_')}_{page_no}_temporary.xlsx",
    )


def temporary_csv_file(
    *,
    sitename: str,
    date: str,
    category_name: str,
    page_no: int,
) -> str:
    """
    Per page of the category .CSV file in the temporary directory
    """
    return join(
        os.path.dirname(__file__),
        sitename,
        "temp",
        date,
        f"products_{sitename}_{date}_{category_name.replace('/', '_').replace('>', '_').replace(':', '_')}_{page_no}_temporary.csv",
    )


def temporary_custom_urls_csv_file(
    *,
    sitename: str,
    date: str,
) -> str:
    """
    Custom URLs .CSV file in the temporary directory
    """
    return join(
        os.path.dirname(__file__),
        sitename,
        "temp",
        date,
        f"products_{sitename}_{date}_CUSTOM_URLS_temporary.csv",
    )


def temporary_xlsx_per_product_file(
    *,
    sitename: str,
    date: str,
    category_name: str,
    page_no: int,
    idx: int,
) -> str:
    """
    Per product .XLSX file in the temporary directory
    """
    return join(
        os.path.dirname(__file__),
        sitename,
        "temp",
        date,
        f"products_{sitename}_{date}_{category_name.replace('/', '_').replace('>', '_').replace(':', '_')}_{page_no}_{idx}_temporary.xlsx",
    )


def temporary_csv_per_product_file(
    *,
    sitename: str,
    date: str,
    category_name: str,
    page_no: int,
    idx: int,
) -> str:
    """
    Per product .CSV file in the temporary directory
    """
    return join(
        os.path.dirname(__file__),
        sitename,
        "temp",
        date,
        f"products_{sitename}_{date}_{category_name.replace('/', '_').replace('>', '_').replace(':', '_')}_{page_no}_{idx}_temporary.csv",
    )
