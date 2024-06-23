# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import platform
import re

from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from functools import cache, partial, reduce
from glob import glob
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any, cast

import pandas as pd

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, load_page, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.bot import copy_dataframe_cells_to_excel_template
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.yongsung import config
from market_crawler.yongsung.data import YongSungCrawlData
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def change_page_strategy2(category_url: str, page_no: int):
    if "?com_board_page" in category_url:
        return category_url.replace(
            f"?com_board_page={page_no-1}", f"?com_board_page={page_no}"
        )
    elif "&com_board_page" in category_url:
        return category_url.replace(
            f"&com_board_page={page_no-1}", f"&com_board_page={page_no}"
        )

    return f"{category_url}?com_board_page={page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_selector("text='로그아웃'")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query='input[name="user_id"]',
        password_query='input[name="password"]',
        login_button_query="div.login_btn > input[type=submit]",
        login_button_strategy=login_button_strategy,
    )


# ? Unlike other markets, YONGSUNG requires two files to be crawled separately
async def run(settings: Settings):
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = get_login_info()
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()

        columns = list(settings.COLUMN_MAPPING.values())

        if settings.URLS:
            await crawl_urls(
                list(dict.fromkeys(settings.URLS)),
                browser,
                settings,
                columns,
            )
            return None

        categories = await get_categories(sitename=config.SITENAME)
        log.detail.total_categories(len(categories))

        crawler = ConcurrentCrawler(
            categories=categories,
            start_category=config.START_CATEGORY,
            end_category=config.END_CATEGORY,
            chunk_size=config.CATEGORIES_CHUNK_SIZE,
            crawl=crawl,
        )
        await crawl_categories(crawler, browser, settings, columns)

    # ???????????????????????????????????????????????????????????????

    log.info("Crawling DETAIL page now ...")

    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright
        ).create()

        page = await browser.new_page()
        await visit_link(
            page,
            "https://www.yong-sung.co.kr/default/product/all_product.php",
            wait_until="networkidle",
        )

        page_no = config.START_PAGE

        while True:
            current_page_url = change_page_strategy2(page.url, page_no)
            log.detail.page_url(current_page_url)

            await visit_link(
                page,
                current_page_url,
                wait_until="networkidle",
            )

            try:
                products = await get_products2(page)
            except error.ProductsNotFound:
                log.action.products_not_present_on_page(page.url, page_no)
                break

            number_of_products = len(products)

            log.detail.total_products_on_page(number_of_products, page_no)

            filename: str = temporary_csv_file(
                sitename=config.SITENAME,
                date=settings.DATE,
                category_name="DETAIL",
                page_no=page_no,
            ).replace("_temporary.csv", ".csv")

            for chunk in chunks(
                range(number_of_products), config.MAX_PRODUCTS_CHUNK_SIZE
            ):
                tasks = (
                    extract_product2(
                        idx,
                        browser,
                        current_page_url,
                        page_no,
                        filename,
                        columns,
                        settings,
                        number_of_products,
                    )
                    for idx in chunk
                )

                await asyncio.gather(*tasks)

            log.action.category_page_crawled("DETAIL", page_no)

            page_no += 1

    # ***************************************************************

    process_detail_files(settings)

    # ***************************************************************

    # ???????????????????????????????????????????????????????????????


async def crawl_urls(
    urls: list[str], browser: PlaywrightBrowser, settings: Settings, columns: list[str]
):
    series: list[dict[str, str | int]] = []
    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(
                idx,
                browser,
                settings,
                urls[idx],
                series,
            )
            for idx in chunk
        )

        await asyncio.gather(*tasks)

    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )
    await asyncio.to_thread(
        pd.DataFrame(series, columns=columns).to_csv,  # type: ignore
        filename,  # type: ignore
        encoding="utf-8-sig",
        index=False,
    )


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    settings: Settings,
    product_url: str,
    series: list[dict[str, str | int]],
):
    page = await browser.new_page()
    await visit_link(page, product_url)

    product_name = await extract_product_name(page)

    table = await extract_table(page)

    thumbnail_image = await extract_thumbnail_image(page)

    product_url = product_url
    thumbnail_image_url = thumbnail_image
    sold_out_text = table.sold_out_text
    percent = table.percent
    price3 = table.price3
    manufacturing_country = table.manufacturing_country
    product_code = table.product_code
    price2 = table.price2

    option_list = await extract_options(page)

    await page.close()

    if option_list:
        for soldout_flag, option in option_list:
            match split_options_text(option, price3):
                case Ok((option1, option2, option3, price3_)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            if soldout_flag == "not soldout":
                crawl_data = YongSungCrawlData(
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    product_name=product_name,
                    manufacturing_country=manufacturing_country,
                    percent=percent,
                    product_code=product_code,
                    price3=price3_,
                    price2=price2,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2=option2,
                    option3=str(option3),
                )
            else:
                crawl_data = YongSungCrawlData(
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    product_name=product_name,
                    manufacturing_country=manufacturing_country,
                    percent=percent,
                    product_code=product_code,
                    price3=price3_,
                    price2=price2,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2="품절",
                    option3=str(option3),
                )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(option_list),
        )
        return None

    crawl_data = YongSungCrawlData(
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        manufacturing_country=manufacturing_country,
        percent=percent,
        product_code=product_code,
        price3=price3,
        price2=price2,
        sold_out_text=sold_out_text,
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(idx, crawl_data.product_url)

    return None


def process_detail_files(settings: Settings):
    log.info("Processing DETAIL file ...")

    detail_csv_files_dir = os.path.join(
        os.path.dirname(__file__), "temp", settings.DATE
    )

    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        results = [
            executor.submit(
                cast(Any, pd.read_csv),
                os.path.join(detail_csv_files_dir, filename),
                encoding="utf-8-sig",
            )
            for filename in sorted(
                glob(os.path.join(detail_csv_files_dir, "*_DETAIL*.csv"))
            )
        ]

    df_list = [r.result() for r in results]

    try:
        assert df_list
    except AssertionError as err:
        from colorama import Back, Fore

        raise AssertionError(
            "".join(
                [
                    Fore.RED,
                    f"There are no *_DETAIL.CSV files in {Back.RED}{Fore.WHITE}{settings.DATE}{Fore.RED}{Back.RESET} folder",
                    Fore.RESET,
                ]
            )
        ) from err

    df = pd.concat(df_list)

    detail_products_csv_file: Path = Path(
        os.path.dirname(__file__),
        f"{config.SITENAME.upper()}_{settings.DATE}_DETAIL.csv",
    )

    # ? We need to remove the already existing file if present, otherwise shutil.copy fails
    if os.path.exists(detail_products_csv_file):
        os.remove(detail_products_csv_file)

    df.drop_duplicates(keep="first").to_csv(
        detail_products_csv_file,
        index=False,
        encoding="utf-8-sig",
    )

    log.info(f"Formatting {Path(detail_products_csv_file).name} ...")
    if platform.system() == "Windows":
        copy_dataframe_cells_to_excel_template(
            output_file=str(detail_products_csv_file),
            template_file=settings.TEMPLATE_FILE,
            column_mapping=settings.COLUMN_MAPPING,
            crawl_data=YongSungCrawlData(),
        )
    log.success(f"File saved to <CYAN><white>{detail_products_csv_file}</></>")

    # if production_run:
    #     # ? DETAIL csv file
    #     log.info(
    #         f"Copying <green>{Path(detail_products_csv_file).name}</> to Google Drive directory {os.path.join(GOOGLE_DRIVE_DIR, config.SITENAME.upper())}"
    #     )

    #     shutil.copy(
    #         detail_products_csv_file,
    #         os.path.join(GOOGLE_DRIVE_DIR, config.SITENAME.upper()),
    #     )

    #     log.success(
    #         f"{Path(detail_products_csv_file).name} saved to <CYAN><white>{os.path.join(GOOGLE_DRIVE_DIR, config.SITENAME.upper())}</></>"
    #     )


async def extract_product2(
    idx: int,
    browser: PlaywrightBrowser,
    category_url: str,
    page_no: int,
    filename: str,
    columns: list[str],
    settings: Settings,
    number_of_products: int,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, category_url, wait_until="networkidle")

    if not (products := await get_products2(page)):
        raise error.ProductsNotFound("Products are not present at all")

    if not (
        product_url := await get_product2_link(
            products, idx, category_url, number_of_products
        )
    ):
        return None

    productid = get_productid2(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name="DETAIL",
            date=settings.DATE,
        )
    ):
        return None

    await visit_link(page, product_url, wait_until="networkidle")

    product_name = cast(
        str, await page.text_content("#post_area > p.readpg_txt")
    ).strip()

    html_source = await extract_images(page, html_top, html_bottom)

    if "NOT PRESENT" in html_source:
        raise error.ProductDetailImageNotFound(page.url)

    crawl_data = YongSungCrawlData(
        product_name=product_name,
        detailed_images_html_source=html_source,
        product_url=page.url,
        category="제품소개 > 전체보기",
    )

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, crawl_data.category, page_no, crawl_data.product_url
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await page.close()


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    category_page_url = category.url
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

    log.action.visit_category(category_name, category_page_url)

    page = await browser.new_page()
    await visit_link(page, category_page_url)

    page_nav_selectors = await page.query_selector_all(
        '#innerPage > div:nth-child(7) > div > div > div > ul > li[name="paginateNum"]'
    )

    total_pages = len(page_nav_selectors) if page_nav_selectors else 1

    while category_state.pageno <= total_pages:
        page_nav_selectors = await page.query_selector_all(
            '#innerPage > div:nth-child(7) > div > div > div > ul > li[name="paginateNum"]'
        )

        total_pages = len(page_nav_selectors) if page_nav_selectors else 1

        if category_state.pageno != 1 and page_nav_selectors:
            async with page.expect_navigation():
                await page_nav_selectors[category_state.pageno - 1].click()

        category_page_url = page.url

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
                    columns,
                    settings,
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


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?spec=(\w+)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


def get_productid2(url: str) -> Result[str, str]:
    regex = compile_regex(r"com_board_idx=(\w*)")
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


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "tr[id^='tr_'] > td.item-name"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element):
    if not (product_link := await product.query_selector("span.underTXT")):
        raise error.QueryNotFound("Product link not found", "a")

    if not (onlick := await product_link.get_attribute("onclick")):
        raise error.QueryNotFound("Product link not found", "onclick")

    regex = compile_regex(r"\((.*)\)")
    onlick = regex.findall(onlick)[0].replace("'", "").replace('"', "")

    return f"http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec={onlick}"


async def get_products2(page: PlaywrightPage):
    try:
        products = await page.query_selector_all(
            "table.table_03 > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(1) > td"
        )
        assert products
    except AssertionError as err:
        raise error.ProductsNotFound(
            "There are no products on the current url", page.url
        ) from err

    return products


async def get_product2_link(
    products: list[PlaywrightElementHandle],
    idx: int,
    category_url: str,
    number_of_products: int,
) -> str | None:
    try:
        product_selector = (await products[idx].query_selector_all("a"))[0]
        product_href = "http://www.yong-sung.co.kr" + cast(
            str, await product_selector.get_attribute("href")
        )
    except TimeoutError as err:
        raise error.ProductLinkNotFound(
            f"Product link is not present for product no {idx + 1}"
        ) from err

    except IndexError as err:
        raise error.ProductLinkNotFound(
            f"Number of calculated products don't match and seem to have been changed: {len(products)} vs {number_of_products} | {category_url}"
        ) from err

    else:
        return product_href


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    category_page_url: str,
    category_state: CategoryState,
    category_html: CategoryHTML,
    filename: str,
    columns: list[str],
    settings: Settings,
):
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

    if not (product_url := (await get_product_link(product)).ok()):
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

        match await get_product_link(product):
            case Ok(product_url):
                pass
            case Err(err):
                raise error.ProductLinkNotFound(err, url=category_page_url)

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

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    product_name = await extract_product_name(page)

    table = await extract_table(page)

    thumbnail_image = await extract_thumbnail_image(page)

    product_url = product_url
    thumbnail_image_url = thumbnail_image
    sold_out_text = table.sold_out_text
    percent = table.percent
    price3 = table.price3
    manufacturing_country = table.manufacturing_country
    product_code = table.product_code
    price2 = table.price2

    option_list = await extract_options(page)

    if option_list:
        for soldout_flag, option in option_list:
            match split_options_text(option, price3):
                case Ok((option1, option2, option3, price3_)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            if soldout_flag == "not soldout":
                crawl_data = YongSungCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    product_name=product_name,
                    manufacturing_country=manufacturing_country,
                    percent=percent,
                    product_code=product_code,
                    price3=price3_,
                    price2=price2,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2=option2,
                    option3=str(option3),
                )
            else:
                crawl_data = YongSungCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    product_name=product_name,
                    manufacturing_country=manufacturing_country,
                    percent=percent,
                    product_code=product_code,
                    price3=price3_,
                    price2=price2,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2="품절",
                    option3=str(option3),
                )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_crawled_with_options(
            idx,
            category_state.name,
            category_state.pageno,
            product_url,
            len(option_list),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    crawl_data = YongSungCrawlData(
        category=category_state.name,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        manufacturing_country=manufacturing_country,
        percent=percent,
        product_code=product_code,
        price3=price3,
        price2=price2,
        sold_out_text=sold_out_text,
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


async def extract_options(page: PlaywrightPage):
    option1_selectors = await page.query_selector_all(
        "#div_Option_Select > span > ul > li > ul > li"
    )

    texts_list: list[str] = [
        text.strip()
        for selector in option1_selectors
        if (text := await selector.text_content())
        and await selector.get_attribute("onclick") != "soldoutAlert()"
    ]

    soldout_texts_list: list[str] = [
        text.strip()
        for selector in option1_selectors
        if (text := await selector.text_content())
        and (attrib := await selector.get_attribute("onclick"))
        and attrib == "soldoutAlert()"
    ]

    all_crawl_data: list[tuple[str, str]] = [
        ("not soldout", text) for text in texts_list
    ]

    all_crawl_data.extend(("soldout", text) for text in soldout_texts_list)
    return all_crawl_data


async def extract_thumbnail_image(page: PlaywrightPage):
    regex = compile_regex("background-image: url(((.*)));")

    if (
        not (style := await page.get_attribute("#mainImageFrameMain", "style"))
        or "background-image: url" not in style
    ):
        raise ValueError("Thumbnail url is not present inside style attribute")

    if regex_match := regex.search(
        style,
    ):
        thumbnail_image = regex_match.group(1).replace("('", "").replace("')", "")
    else:
        raise ValueError(f"Regex not found in {style}")

    return thumbnail_image


async def extract_product_name(page: PlaywrightPage):
    try:
        product_name = cast(
            str, await page.text_content('ul[class="item-info"] div[class="Name"]')
        ).strip()  # ? There are often newlines or whitespaces at start and end of the product name text
        assert product_name
    except (TimeoutError, AssertionError) as err:
        raise error.ProductNameNotFound(page.url) from err

    return product_name


regex_between_parenthesis = r"\((.*)\)"


def take_between(regex: re.Pattern[Any], s: str):
    if match := regex.search(s):
        return match.group(1)


def take_outside(regex: re.Pattern[Any], s: str):
    return regex.sub("", s)


# * Between parenthesis and whitespace at closing parenthesis
# r"\((.*)(\s)\)" e.g. (xyz ) => xyz # ? This won't work if whitespace isn't present before closing parenthesis (that is why we are not using it here)

take_between_parenthesis = cache(
    partial(take_between, compile_regex(regex_between_parenthesis))
)
take_outside_parenthesis = cache(
    partial(take_outside, compile_regex(regex_between_parenthesis))
)


async def extract_table(page: PlaywrightPage):
    product_code = ""
    percent = ""
    price2 = 0
    price3 = 0
    sold_out_text = ""
    manufacturing_country = ""

    if not (
        table_body := await page.query_selector_all(
            'div.block.detail-page > table > tbody > tr > td:nth-child(2) > ul[class="item-info"]'
        )
    ):
        # ? Some products don't have the table at all
        # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=47548&category=048
        log.warning(f"Table is not present <blue>| {page.url}</>")
        raise error.TableNotFound(page.url)

    # ? As we have already checked the truthy for table_body list above, we won't write it in try except block
    item_headings = await table_body[0].query_selector_all(
        # 'li > div[class="table"] > div.th'
        ", ".join(
            [
                "li:nth-child(2) > div > div.th",
                "li:nth-child(3) > div > div.th",
                "li:nth-child(4) > div > div.th",
                "li:nth-child(5) > div > div.th",
                "li:nth-child(6) > div > div.th",
            ]
        )
    )
    item_values = await table_body[0].query_selector_all(
        # 'li > div[class="table"] > div.td'
        ", ".join(
            [
                "li:nth-child(2) > div > div.td",
                "li:nth-child(3) > div > div.td",
                "li:nth-child(4) > div > div.td",
                "li:nth-child(5) > div > div.td",
                "li:nth-child(6) > div > div.td",
            ]
        )
    )

    assert len(item_headings) == len(
        item_values
    ), f"Not equal {len(item_headings)} vs {len(item_values)}"

    for key, val in zip(
        item_headings, item_values
    ):  # ? First th is for product name and doesn't contain any text
        key_str = cast(str, await key.text_content()).strip()
        val_str = cast(str, await val.text_content()).strip()

        if "모델코드" in key_str:
            product_code = val_str

        if "판매가격" in key_str:
            if percent_element := await val.query_selector('span[class="percent"]'):
                percent = cast(str, await percent_element.text_content())

            if price_element := await val.query_selector('div[class="Price"]'):
                full_price = cast(str, await price_element.text_content())
                price2 = parse_int(take_between_parenthesis(full_price))
                price3 = parse_int(take_outside_parenthesis(full_price))

        if "재고" in key_str:
            sold_out_text = val_str

        if "규격/원산지" in key_str:
            manufacturing_country = val_str

    if not product_code:
        log.warning(f"Product code is not present <blue>| {page.url}</>")
        raise error.ProductCodeNotFound(page.url)

    if not percent:
        log.warning(f"Percent is not present <blue>| {page.url}</>")
        raise error.PercentNotFound(page.url)

    if not price2:
        log.warning(f"Price2 is not present <blue>| {page.url}</>")
        raise error.Price2NotFound(page.url)

    if not price3:
        log.warning(f"Price3 is not present <blue>| {page.url}</>")
        raise error.Price3NotFound(page.url)

    if not sold_out_text:
        log.warning(f"Sold out text is not present <blue>| {page.url}</>")
        raise error.SoldOutNotFound(page.url)

    # ? See: http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=5-SEI-1052
    if not manufacturing_country:
        log.warning(f"Manufacturing country is not present <blue>| {page.url}</>")

    return Table(
        product_code, percent, price2, price3, sold_out_text, manufacturing_country
    )


@dataclass(slots=True)
class Table:
    product_code: str
    percent: str
    price2: int
    price3: int
    sold_out_text: str
    manufacturing_country: str


@returns(IndexError, ValueError)
def split_options_text(option1: str, price3: int):
    option3 = ""
    if "(+" in option1 and "원" in option1:
        regex = compile_regex(r"\s*?\(\s*?\+\w+[,]?\w*원?.*\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            option3 = parse_int(additional_price)
            price3 += option3

    if "(-" in option1 and "원" in option1:
        regex = compile_regex(r"\s*?\(\s*?\-\w+[,]?\w*원?.*\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            option3 = parse_int(additional_price)
            price3 = option3 - price3

    if match := compile_regex(r"\[.*\]").findall(option1):
        return option1.replace(match[0], "").strip(), match[0], option3, price3
    if "[재고 △]" in option1:
        return option1.replace("[재고 △]", "").strip(), "[재고 △]", option3, price3
    if "[품절]" in option1:
        return option1.replace("[품절]", "").strip(), "[품절]", option3, price3
    if "품절" in option1:
        return option1.replace("품절", "").strip(), "품절", option3, price3

    return option1.strip(), "", option3, price3


def replace_all(string: str, to_replace: Iterable[str], replace_with: str):
    return reduce(lambda s, r: s.replace(r, replace_with), to_replace, string)


def remove_all(string: str, to_replace: Iterable[str]):
    return replace_all(string, to_replace, "")


async def extract_images(page: PlaywrightPage, html_top: str, html_bottom: str) -> str:
    query = "#post_area"

    if not (
        detail_img_div_root := await page.wait_for_selector(query, state="visible")
    ):
        log.warning(
            f"Product detail images are not present at all <blue>| {page.url}</>"
        )
        raise error.ProductDetailImageNotFound(page.url)

    html_source = await detail_img_div_root.inner_html()

    html_source = (
        html_source.strip()
        .replace("\n", "")
        .replace('src="/', 'src="http://www.yong-sung.co.kr/')
    )

    img_start = '<img class="disnone"'
    regex: Any = compile_regex(f"""(.*){img_start}""")
    if match := regex.search(html_source):
        html_source = html_source.replace(match.group(1), "")

    html_source = "".join(
        [
            html_top,
            html_source,
            html_bottom,
        ],
    )

    return html_source.strip()
