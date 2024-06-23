# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib

from enum import Enum, auto
from functools import cache
from typing import cast
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, load_page, parse_document, visit_link
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.hyperinc import config
from market_crawler.hyperinc.data import HyperincCrawlData
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright
        ).create()
        categories = await get_categories(sitename=config.SITENAME)
        log.detail.total_categories(len(categories))

        columns = list(settings.COLUMN_MAPPING.values())
        crawler = ConcurrentCrawler(
            categories=categories,
            start_category=config.START_CATEGORY,
            end_category=config.END_CATEGORY,
            chunk_size=config.CATEGORIES_CHUNK_SIZE,
            crawl=crawl,
        )
        await crawl_categories(crawler, browser, settings, columns)


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    category_url = category.url
    category_name = category.name

    if not (
        category_state := await get_category_state(
            config=config,
            category_name=category_name,
            date=settings.DATE,
        )
    ):
        return None

    category_html = CategoryHTML(
        name=category_name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
    )
    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    page = await load_page(
        browser=browser,
        url=category_page_url,
        html=category_html,
        on_failure="visit",
        rate_limit=config.DEFAULT_RATE_LIMIT,
        async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
        wait_until="networkidle",
    )
    content = await page.content()
    if config.SAVE_HTML and not await category_html.exists():
        await category_html.save(content)

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    if total_products_text := await document.text_content(
        "#contents > div.xans-element-.xans-product.xans-product-normalpackage > div.xans-element-.xans-product.xans-product-normalmenu > div > p > span"
    ):
        products_len = parse_int(total_products_text)

        log.info(
            f"Total products on category <blue>{category_name}</>: <light-green>{products_len}</>",
        )

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        await page.close()
        page = await load_page(
            browser=browser,
            url=category_page_url,
            html=category_html,
            on_failure="visit",
            rate_limit=config.DEFAULT_RATE_LIMIT,
            async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
            wait_until="networkidle",
        )
        content = await page.content()
        if config.SAVE_HTML and not await category_html.exists():
            await category_html.save(content)

        if not (document := await parse_document(content, engine="lexbor")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        if not (number_of_products := await has_products(document)):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        log.detail.total_products_on_page(number_of_products, category_state.pageno)

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        for chunk in chunks(range(number_of_products), config.MAX_PRODUCTS_CHUNK_SIZE):
            tasks = (
                extract_product(
                    idx,
                    browser,
                    category_page_url,
                    category_state,
                    category_html,
                    filename,
                    settings,
                    columns,
                    number_of_products,
                )
                for idx in chunk
            )

            await asyncio.gather(*tasks)

        log.action.category_page_crawled(category_state.name, category_state.pageno)

        category_state.pageno += 1
        category_html.pageno = category_state.pageno

        if config.USE_CATEGORY_SAVE_STATES:
            await category_state.save()

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    category_page_url: str,
    category_state: CategoryState,
    category_html: CategoryHTML,
    filename: str,
    settings: Settings,
    columns: list[str],
    number_of_products: int,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    content = await load_content(
        browser=browser,
        url=category_page_url,
        html=category_html,
        on_failure="fetch",
        wait_until="networkidle",
        async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
        rate_limit=config.DEFAULT_RATE_LIMIT,
    )
    if config.SAVE_HTML and not await category_html.exists():
        await category_html.save(content)

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
        page = await browser.new_page()
        await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()
        if config.SAVE_HTML and not await category_html.exists():
            await category_html.save(content)
        await page.close()

        if not (document := await parse_document(content, engine="lxml")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        match await get_products(document):
            case Ok(products):
                product = products[idx]
            case Err(err):
                raise error.ProductsNotFound(err, url=category_page_url)

        match await get_product_link(product, category_page_url):
            case Ok(product_url):
                pass
            case Err(err):
                raise error.ProductLinkNotFound(err, url=category_page_url)

    assert (
        len(products) == number_of_products
    ), "Total number of products on the page seems to have been changed"

    productid = get_productid(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name=category_state.name,
            date=category_state.date,
        )
    ):
        return None

    sold_out_text = ""

    if soldout_icon := await product.query_selector(
        "div.description > div.icon > div.promotion > img"
    ):
        if soldout_icon_img := await soldout_icon.get_attribute("src"):
            if "soldout" in soldout_icon_img:
                sold_out_text = "품절"
                log.warning(f"Product # {idx+1} is soldout")

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await extract_product_name(document):
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match await extract_thumbnail_image(document, product_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match await extract_table(document, product_url):
        case Ok(table):
            price2, manufacturing_country, delivery_fee = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    detailed_images_html_source = await extract_html(
        page, product_url, html_top, html_bottom
    )

    options = (await extract_options(page)).split(",")

    if options:
        for option1 in options:
            crawl_data = HyperincCrawlData(
                category=category_state.name,
                product_url=product_url,
                sold_out_text=sold_out_text,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                price2=price2,
                manufacturing_country=manufacturing_country,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
            )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_crawled_with_options(
            idx,
            category_state.name,
            category_state.pageno,
            product_url,
            len(options),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    crawl_data = HyperincCrawlData(
        category=category_state.name,
        product_url=product_url,
        sold_out_text=sold_out_text,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        price2=price2,
        manufacturing_country=manufacturing_country,
        delivery_fee=delivery_fee,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )

    return None


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\/(\d*)\/category\/")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


async def get_products(document: Document):
    query = "li[id^='anchorBoxId_']"
    return (
        Ok(products)
        if (products := await document.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#main > div.sub_all > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.do_detile_item_info > div.dm_detail_title"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document, product_url: str):
    manufacturing_country = ""
    delivery_fee = 0

    if price2 := await document.text_content("#span_product_price_text"):
        price2 = parse_int(price2)
    else:
        raise error.SupplyPriceNotFound(product_url)

    if delivery_fee := await document.text_content("span[class='delv_price_B'] strong"):
        delivery_fee = parse_int(delivery_fee)
    else:
        delivery_fee = 0

    query = "div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    th_elements = await table_tbody.query_selector_all("tr > th")
    td_elements = await table_tbody.query_selector_all("tr > td")

    assert len(th_elements) == len(
        td_elements
    ), f"Not equal {len(th_elements)} vs {len(td_elements)}"

    for th, td in zip(
        th_elements,
        td_elements,
    ):
        th_str = cast(str, await th.text_content())
        td_str = cast(str, await td.text_content())

        if "국내·해외배송" in th_str:
            manufacturing_country = td_str

    if delivery_fee or delivery_fee == 0:
        return price2, manufacturing_country, delivery_fee
    else:
        raise error.DeliveryFeeNotFound(product_url)


class OptionsCase(Enum):
    ONLY_OPTIONS1_IS_PRESENT = auto()
    OPTIONS1_AND_OPTIONS2_IS_PRESENT = (
        auto()
    )  # ? Logic is just like Xeeon; color and size options are present
    ONLY_CHECKS_PRESENT = auto()
    OPTIONS1_AND_ONLY_CHECKS_IS_PRESENT = auto()
    OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT = (
        auto()
    )  # ? https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%80%B8%ED%81%AC%EB%A3%A8%EC%A6%88-nv-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/45/category/175/display/1/
    OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_8_OPTIONS = (
        auto()
    )  # ? https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EB%84%A4%EC%98%A8%EC%95%84%EC%9D%B4%EB%94%94idyl-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1%EC%9A%A9/75/category/175/display/1/#none
    OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_6_OPTIONS = (
        auto()
    )  # ? https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%B9%B4%EB%B0%94%EC%98%88%EB%A1%9Cylrd-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1sp%EC%9A%A9/66/category/175/dilay/1/


async def extract_options(page: PlaywrightPage):
    options_case = OptionsCase.ONLY_OPTIONS1_IS_PRESENT

    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option",
    )

    options2_selectors = await page.query_selector_all(
        "#product_option_id2 > option, #product_option_id2 > optgroup > option",
    )

    options3_selectors = await page.query_selector_all(
        "#product_option_id3 > option, #product_option_id3 > optgroup > option",
    )

    options4_selectors = await page.query_selector_all(
        "#product_option_id4 > option, #product_option_id4 > optgroup > option",
    )

    options5_selectors = await page.query_selector_all(
        "#product_option_id5 > option, #product_option_id5 > optgroup > option",
    )

    options6_selectors = await page.query_selector_all(
        "#product_option_id6 > option",
    )

    options7_selectors = await page.query_selector_all(
        "#product_option_id7 > option",
    )

    options8_selectors = await page.query_selector_all(
        "#product_option_id8 > option",
    )

    options2_present = bool(options2_selectors)
    if options2_present:
        options3_present = bool(options3_selectors)
        options4_present = bool(options4_selectors)
        options5_present = bool(options5_selectors)

        if not options3_present and not options4_present and not options5_present:
            options_case = OptionsCase.OPTIONS1_AND_OPTIONS2_IS_PRESENT

        elif (
            options3_present
            and options4_present
            and options5_present
            and not options6_selectors
            and not options7_selectors
            and not options8_selectors
        ):
            options_case = OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT
        elif options6_selectors and options7_selectors and options8_selectors:
            options_case = (
                OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_8_OPTIONS
            )
        elif (
            options3_present
            and options4_present
            and options5_present
            and options6_selectors
            and not options7_selectors
            and not options8_selectors
        ):
            options_case = (
                OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_6_OPTIONS
            )
        elif options3_present and options4_present:
            options_case = OptionsCase.OPTIONS1_AND_ONLY_CHECKS_IS_PRESENT

    log.info(f"Options case is: {options_case}")

    final_str_option1: str = ""

    if options_case == OptionsCase.ONLY_OPTIONS1_IS_PRESENT:
        for opt1_idx in range(len(options1_selectors)):
            option1_value: str = cast(
                str,
                await options1_selectors[opt1_idx].get_attribute("value"),
            )

            options1_str: str = cast(
                str, await options1_selectors[opt1_idx].text_content()
            )

            if option1_value not in ["*", "**"]:
                try:
                    await page.select_option(
                        "#product_option_id1", option1_value, timeout=5000
                    )
                # ? See: https://hyperinc.kr/product/%EB%B6%88%EC%8A%A4-%EB%B2%8C%EB%A0%88%EC%9B%8C%ED%84%B0%EB%B2%A0%EC%8A%A4%ED%8A%B8/225/category/27/display/1/
                except error.PlaywrightTimeoutError:
                    if opt1_idx == 0:
                        log.warning(f"Unusual table | {page.url}")
                    color_selectors = await page.query_selector_all(
                        "#infoArea_fixed > table.xans-element-.xans-product.xans-product-option.xans-record- > tbody.xans-element-.xans-product.xans-product-option.xans-record- > tr > td > ul a, #infoArea_fixed > table.xans-element-.xans-product.xans-product-option.xans-record- > tbody:nth-child(3) > tr > td > ul a"
                    )
                    if opt1_idx < len(color_selectors):
                        if color_checker := color_selectors[opt1_idx]:
                            await color_checker.click()

                final_str_option1 += f"{options1_str},"

    elif options_case == OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT:
        final_str_option1 = await extract_options1_and_checks_and_options2(page)

    elif (
        options_case
        == OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_8_OPTIONS
    ):
        final_str_option1 = (
            await extract_options1_and_checks_and_options2_but_total_8_options(page)
        )
    elif (
        options_case
        == OptionsCase.OPTIONS1_AND_CHECKS_AND_OPTIONS2_PRESENT_BUT_TOTAL_6_OPTIONS
    ):
        final_str_option1 = (
            await extract_options1_and_checks_and_options2_but_total_6_options(page)
        )
    elif options_case == OptionsCase.OPTIONS1_AND_ONLY_CHECKS_IS_PRESENT:
        final_str_option1 = await extract_options1_and_only_checks_but_not_options2(
            page
        )

    elif options_case == OptionsCase.OPTIONS1_AND_OPTIONS2_IS_PRESENT:
        final_str_option1 = await extract_options1_and_options2(page)

    return "".join(final_str_option1.removesuffix(",").split())


async def extract_options1_and_checks_and_options2(page: PlaywrightPage):
    final_str_option1: str = ""

    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option",
    )

    options2_selectors = await page.query_selector_all(
        "#product_option_id2 > option",
    )

    options3_selectors = await page.query_selector_all(
        "#product_option_id3 > option",
    )

    options4_selectors = await page.query_selector_all(
        "#product_option_id4 > option",
    )

    options5_selectors = await page.query_selector_all(
        "#product_option_id5 > option",
    )

    for opt5_idx in range(len(options5_selectors)):
        option5_value: str = cast(
            str,
            await options5_selectors[opt5_idx].get_attribute("value"),
        )

        options5_str: str = cast(
            str,
            await options5_selectors[opt5_idx].text_content(),
        )

        # print(f"{options5_str = }")

        for j in range(len(options1_selectors)):
            option1_value: str = cast(
                str,
                await options1_selectors[j].get_attribute("value"),
            )

            # print(f"{options1_str = }")

            if option1_value not in ["*", "**"]:
                try:
                    await page.select_option(
                        "#product_option_id1", option1_value, timeout=5000
                    )
                except error.PlaywrightTimeoutError:
                    if j == 0:
                        log.warning(f"Unusual table | {page.url}")
                    color_selectors = await page.query_selector_all(
                        "#infoArea_fixed > table.xans-element-.xans-product.xans-product-option.xans-record- > tbody:nth-child(3) > tr > td > ul a"
                    )
                    if j < len(color_selectors):
                        if color_checker := color_selectors[j]:
                            await color_checker.click()

                if option5_value not in ["*", "**"]:
                    options1_str: str = cast(
                        str, await options1_selectors[j].text_content()
                    )

                    # ? We don't need to select this one if we have already selected quantity option
                    final_str_option1 += f"{options1_str}_{options5_str},"

    for opt2_idx in range(len(options2_selectors)):
        option2_value: str = cast(
            str,
            await options2_selectors[opt2_idx].get_attribute("value"),
        )

        # print(f"{options2_str = }")

        if option2_value not in ["*", "**"]:
            options2_str: str = cast(
                str,
                await options2_selectors[opt2_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options2_str},"

    for opt3_idx in range(len(options3_selectors)):
        option3_value: str = cast(
            str,
            await options3_selectors[opt3_idx].get_attribute("value"),
        )

        # print(f"{options3_str = }")

        if option3_value not in ["*", "**"]:
            options3_str: str = cast(
                str,
                await options3_selectors[opt3_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options3_str},"

    for opt4_idx in range(len(options4_selectors)):
        option4_value: str = cast(
            str,
            await options4_selectors[opt4_idx].get_attribute("value"),
        )

        # print(f"{options4_str = }")

        if option4_value not in ["*", "**"]:
            options4_str: str = cast(
                str,
                await options4_selectors[opt4_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options4_str},"

    return final_str_option1


async def extract_options1_and_checks_and_options2_but_total_8_options(
    page: PlaywrightPage,
):
    final_str_option1: str = ""

    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option",
    )

    options2_selectors = await page.query_selector_all(
        "#product_option_id2 > option",
    )

    options3_selectors = await page.query_selector_all(
        "#product_option_id3 > option",
    )

    options4_selectors = await page.query_selector_all(
        "#product_option_id4 > option",
    )

    options5_selectors = await page.query_selector_all(
        "#product_option_id5 > option",
    )

    options6_selectors = await page.query_selector_all(
        "#product_option_id6 > option",
    )

    options7_selectors = await page.query_selector_all(
        "#product_option_id7 > option",
    )

    options8_selectors = await page.query_selector_all(
        "#product_option_id8 > option",
    )

    # ? See: https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EB%84%A4%EC%98%A8%EC%95%84%EC%9D%B4%EB%94%94idyl-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1%EC%9A%A9/75/category/175/display/1/#none
    log.warning("(first check) Total 8 options are present including checks")
    for opt8_idx in range(len(options8_selectors)):
        option8_value: str = cast(
            str,
            await options8_selectors[opt8_idx].get_attribute("value"),
        )

        options8_str: str = cast(
            str,
            await options8_selectors[opt8_idx].text_content(),
        )

        # print(f"{options8_str = }")

        for opt2_idx in range(len(options2_selectors)):
            options2_selectors = await page.query_selector_all(
                "#product_option_id2 > option",
            )

            option2_value: str = cast(
                str,
                await options2_selectors[opt2_idx].get_attribute("value"),
            )

            # print(f"{options1_str = }")

            if option8_value not in ["*", "**"] and option2_value not in [
                "*",
                "**",
            ]:
                options2_str: str = cast(
                    str, await options2_selectors[opt2_idx].text_content()
                )

                # ? We don't need to select this one if we have already selected quantity option
                final_str_option1 += f"{options2_str}_{options8_str},"

    for opt1_idx in range(len(options1_selectors)):
        option1_value: str = cast(
            str,
            await options1_selectors[opt1_idx].get_attribute("value"),
        )

        # print(f"{options1_str = }")

        if option1_value not in ["*", "**"]:
            options1_str: str = cast(
                str,
                await options1_selectors[opt1_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options1_str},"

    for opt3_idx in range(len(options3_selectors)):
        option3_value: str = cast(
            str,
            await options3_selectors[opt3_idx].get_attribute("value"),
        )

        # print(f"{options3_str = }")

        if option3_value not in ["*", "**"]:
            options3_str: str = cast(
                str,
                await options3_selectors[opt3_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options3_str},"

    for opt4_idx in range(len(options4_selectors)):
        option4_value: str = cast(
            str,
            await options4_selectors[opt4_idx].get_attribute("value"),
        )

        # print(f"{options4_str = }")

        if option4_value not in ["*", "**"]:
            options4_str: str = cast(
                str,
                await options4_selectors[opt4_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options4_str},"

    log.warning("(Second check) Total 8 options are present including checks")
    for opt5_idx in range(len(options5_selectors)):
        option5_value: str = cast(
            str,
            await options5_selectors[opt5_idx].get_attribute("value"),
        )

        # print(f"{options5_str = }")

        if option5_value not in ["*", "**"]:
            options5_str: str = cast(
                str,
                await options5_selectors[opt5_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options5_str},"

    for opt6_idx in range(len(options6_selectors)):
        option6_value: str = cast(
            str,
            await options6_selectors[opt6_idx].get_attribute("value"),
        )

        # print(f"{options6_str = }")

        if option6_value not in ["*", "**"]:
            options6_str: str = cast(
                str,
                await options6_selectors[opt6_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options6_str},"

    for opt7_idx in range(len(options7_selectors)):
        option7_value: str = cast(
            str,
            await options7_selectors[opt7_idx].get_attribute("value"),
        )

        # print(f"{options7_str = }")

        if option7_value not in ["*", "**"]:
            options7_str: str = cast(
                str,
                await options7_selectors[opt7_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options7_str},"

    return final_str_option1


async def extract_options1_and_checks_and_options2_but_total_6_options(
    page: PlaywrightPage,
):
    final_str_option1: str = ""

    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option",
    )

    options2_selectors = await page.query_selector_all(
        "#product_option_id2 > option",
    )

    options3_selectors = await page.query_selector_all(
        "#product_option_id3 > option",
    )

    options4_selectors = await page.query_selector_all(
        "#product_option_id4 > option",
    )

    options5_selectors = await page.query_selector_all(
        "#product_option_id5 > option",
    )

    options6_selectors = await page.query_selector_all(
        "#product_option_id6 > option",
    )

    # ? See: https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EB%84%A4%EC%98%A8%EC%95%84%EC%9D%B4%EB%94%94idyl-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1%EC%9A%A9/75/category/175/display/1/#none
    log.warning("(first check) Total 6 options are present including checks")
    for opt6_idx in range(len(options6_selectors)):
        option6_value: str = cast(
            str,
            await options6_selectors[opt6_idx].get_attribute("value"),
        )

        options6_str: str = cast(
            str,
            await options6_selectors[opt6_idx].text_content(),
        )

        # print(f"{options6_str = }")

        for opt1_idx in range(len(options1_selectors)):
            options1_selectors = await page.query_selector_all(
                "#product_option_id1 > option",
            )

            option1_value: str = cast(
                str,
                await options1_selectors[opt1_idx].get_attribute("value"),
            )

            # print(f"{options1_str = }")

            if option6_value not in ["*", "**"] and option1_value not in [
                "*",
                "**",
            ]:
                options1_str: str = cast(
                    str, await options1_selectors[opt1_idx].text_content()
                )

                # ? We don't need to select this one if we have already selected quantity option
                final_str_option1 += f"{options1_str}_{options6_str},"

    for opt2_idx in range(len(options2_selectors)):
        option2_value: str = cast(
            str,
            await options2_selectors[opt2_idx].get_attribute("value"),
        )

        # print(f"{options2_str = }")

        if option2_value not in ["*", "**"]:
            options2_str: str = cast(
                str,
                await options2_selectors[opt2_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options2_str},"

    for opt3_idx in range(len(options3_selectors)):
        option3_value: str = cast(
            str,
            await options3_selectors[opt3_idx].get_attribute("value"),
        )

        # print(f"{options3_str = }")

        if option3_value not in ["*", "**"]:
            options3_str: str = cast(
                str,
                await options3_selectors[opt3_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options3_str},"

    for opt4_idx in range(len(options4_selectors)):
        option4_value: str = cast(
            str,
            await options4_selectors[opt4_idx].get_attribute("value"),
        )

        # print(f"{options4_str = }")

        if option4_value not in ["*", "**"]:
            options4_str: str = cast(
                str,
                await options4_selectors[opt4_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options4_str},"

    log.warning("(Second check) Total 6 options are present including checks")
    for opt5_idx in range(len(options5_selectors)):
        option5_value: str = cast(
            str,
            await options5_selectors[opt5_idx].get_attribute("value"),
        )

        # print(f"{options5_str = }")

        if option5_value not in ["*", "**"]:
            options5_str: str = cast(
                str,
                await options5_selectors[opt5_idx].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options5_str},"

    return final_str_option1


async def extract_options1_and_only_checks_but_not_options2(page: PlaywrightPage):
    final_str_option1: str = ""

    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option",
    )

    options2_selectors = await page.query_selector_all(
        "#product_option_id2 > option",
    )

    options3_selectors = await page.query_selector_all(
        "#product_option_id3 > option",
    )

    options4_selectors = await page.query_selector_all(
        "#product_option_id4 > option",
    )

    for j in range(len(options1_selectors)):
        option1_value: str = cast(
            str,
            await options1_selectors[j].get_attribute("value"),
        )

        # print(f"{options1_str = }")

        if option1_value not in ["*", "**"]:
            await page.select_option(
                "#product_option_id1",
                option1_value,
            )

            options1_str: str = cast(str, await options1_selectors[j].text_content())

            final_str_option1 += f"{options1_str},"

    for k in range(len(options2_selectors)):
        option2_value: str = cast(
            str,
            await options2_selectors[k].get_attribute("value"),
        )

        # print(f"{options2_str = }")

        if option2_value not in ["*", "**"]:
            options2_str: str = cast(
                str,
                await options2_selectors[k].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options2_str},"

    for k in range(len(options3_selectors)):
        option3_value: str = cast(
            str,
            await options3_selectors[k].get_attribute("value"),
        )

        # print(f"{options3_str = }")

        if option3_value not in ["*", "**"]:
            options3_str: str = cast(
                str,
                await options3_selectors[k].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options3_str},"

    for k in range(len(options4_selectors)):
        option4_value: str = cast(
            str,
            await options4_selectors[k].get_attribute("value"),
        )

        # print(f"{options4_str = }")

        if option4_value not in ["*", "**"]:
            options4_str: str = cast(
                str,
                await options4_selectors[k].text_content(),
            )

            # ? We don't need to select this one if we have already selected quantity option
            final_str_option1 += f"{options4_str},"

    return final_str_option1


# ? Its rule is similar to Xeeon for extracting both options1 and options2
async def extract_options1_and_options2(page: PlaywrightPage):
    final_str_option1 = ""
    options1_selectors = await page.query_selector_all(
        "#product_option_id1 > option, #product_option_id1 > optgroup > option"
    )

    options2_selectors = await page.query_selector_all("#product_option_id2 > option")

    for j in range(len(options1_selectors)):
        option1_value: str = cast(
            str, await options1_selectors[j].get_attribute("value")
        )

        options1_str: str = cast(str, await options1_selectors[j].text_content())
        if option1_value not in ["*", "**"]:
            try:
                await page.select_option(
                    "#product_option_id1", option1_value, timeout=5000
                )
            except error.PlaywrightTimeoutError:
                color_selectors = await page.query_selector_all(
                    "#infoArea_fixed > table.xans-element-.xans-product.xans-product-option.xans-record- > tbody:nth-child(3) > tr > td > ul a"
                )

                if j < len(color_selectors):
                    if color_checker := color_selectors[j]:
                        await color_checker.click()
                        options2_selectors = await page.query_selector_all(
                            "#product_option_id2 > option"
                        )

            for k in range(len(options2_selectors)):
                option2_value: str = cast(
                    str, await options2_selectors[k].get_attribute("value")
                )

                options2_str: str = cast(
                    str, await options2_selectors[k].text_content()
                )
                if option2_value not in ["*", "**"] and options2_str not in ["empty"]:
                    final_str_option1 += f"{options1_str}_{options2_str},"

    return final_str_option1


@cache
def image_quries():
    return "#prdDetail div[align='center'] img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    elements = await page.query_selector_all(query)

    urls: list[str] = []
    for el in elements:
        action = lambda: get_srcs(el)
        focus = lambda: focus_element(page, el)
        is_base64 = isin("base64")
        match await do(action).retryif(
            predicate=is_base64,
            on_retry=focus,
            max_tries=5,
        ):
            case Ok(src):
                urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    urls = list(map(lambda url: urljoin(product_url, url), urls))

    html_source = html_top

    if urls:
        if table_selector := await page.query_selector("#edinfo-studio"):
            table_html = await table_selector.inner_html()

            for image_url in urls:
                html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

            html_source = "".join([html_source, table_html]).replace('"', "'")

        else:
            for image_url in urls:
                html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

        html_source = "".join(
            [
                html_source,
                html_bottom,
            ],
        )

        return html_source.strip()
    else:
        log.warning(
            f"Product detail images are not present at all <blue>| {page.url}</>"
        )
        return "NOT PRESENT"


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)


async def extract_html(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(images):
            return images
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            return "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)
