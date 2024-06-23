# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import os
import sys

from typing import TYPE_CHECKING

from loguru import logger


if TYPE_CHECKING:
    from typing import Final

__all__ = ["action", "info", "debug", "warning", "error", "success", "logger"]

logger.remove()

LOGGER_FORMAT_STR: Final[str] = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line} [Process ID {extra[id]}]</cyan> - <level>{message}</level>"
)

logger.add(
    sys.stderr,
    format=LOGGER_FORMAT_STR,
    level="INFO",
    colorize=True,
    enqueue=True,
)

PROCESS_ID: Final[int] = os.getpid()

logger = logger.bind(id=PROCESS_ID).opt(colors=True)

logger.level("ACTION", no=38, color="<yellow><dim>")
# ? We will set its severity value between DEBUG and INFO
# ? See: https://loguru.readthedocs.io/en/stable/api/logger.html
logger.level("OPTIMIZER", no=15, color="<green><dim>")

info = logger.info
debug = logger.debug
warning = logger.warning
error = logger.error
success = logger.success


# ? Some common logging patterns so that we don't have to write boilerplate logging code in other markets
class Detail:
    @staticmethod
    def total_categories(total_categories: int):
        logger.info(f"Total categories: <yellow>{total_categories}</>")

    @staticmethod
    def total_pages(total_pages: int):
        logger.info(f"Total pages: <yellow>{total_pages}</>")

    @staticmethod
    def total_products_in_category(category_name: str, number_of_products: int):
        logger.info(
            f"Total products: <magenta>{number_of_products}</> <CYAN><white>(Category: {category_name})</></>",
        )

    @staticmethod
    def total_products_on_page(number_of_products: int, page_no: int):
        logger.info(
            f"Total products: <magenta>{number_of_products}</> <CYAN><white>(Page # {page_no})</></>",
        )

    @staticmethod
    def page_url(category_url: str):
        logger.info(f"Page URL: <blue>{category_url}</>")


class Action:
    @staticmethod
    def visit_category(category_name: str, url: str):
        logger.log(
            "ACTION", f"Visit category: <cyan>{category_name}</> <blue>| {url}</>"
        )

    @staticmethod
    def products_not_present_on_page(page_url: str, page_no: int):
        logger.log(
            "ACTION",
            f"As there are no products are present on page # {page_no}, therefore stopping the crawling <blue>| {page_url}</>",
        )

    @staticmethod
    def category_page_crawled(category: str, page_no: int):
        logger.log(
            "ACTION",
            f"<CYAN><white>Page # {page_no} has been crawled</></><cyan> | C: {category}</>",
        )

    @staticmethod
    def page_crawled(page_no: int):
        logger.log("ACTION", f"<CYAN><white>Page # {page_no} has been crawled</></>")

    @staticmethod
    def product_crawled(
        idx: int,
        category: str,
        page_no: int,
        current_url: str,
    ):
        logger.log(
            "ACTION",
            f"<magenta>No: {idx + 1}</><cyan> | C: {category}</><light-yellow> | Page: {page_no}</><blue> | {current_url}</> has been crawled",
        )

    @staticmethod
    def product_crawled_with_options(
        idx: int,
        category: str,
        page_no: int,
        current_url: str,
        total_options: int,
    ):
        logger.log(
            "ACTION",
            f"<magenta>No: {idx + 1}</><cyan> | C: {category}</><light-yellow> | Page: {page_no}</><blue> | {current_url}</> has been crawled <BLUE><w>({total_options} options are present)</w></BLUE>",
        )

    @staticmethod
    def product_custom_url_crawled(
        idx: int,
        current_url: str,
    ):
        logger.log(
            "ACTION",
            f"<magenta>No: {idx + 1}</><blue> | {current_url}</> has been crawled",
        )

    @staticmethod
    def product_custom_url_crawled_with_options(
        idx: int,
        current_url: str,
        total_options: int,
    ):
        logger.log(
            "ACTION",
            f"<magenta>No: {idx + 1}</><blue> | {current_url}</> has been crawled <BLUE><w>({total_options} options are present)</w></BLUE>",
        )


action = Action()
detail = Detail()
