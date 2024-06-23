# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from functools import cache
from typing import NamedTuple, cast
from urllib.parse import urljoin

import pandas as pd

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError, TimeoutException
from dunia.extraction import load_content, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from market_crawler.tentwentybag import config
from market_crawler.tentwentybag.data import TentwentybagCrawlData
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "https://www.1020bag.com/", wait_until="load")

    for category in await page.query_selector_all(
        "#contents > div > div.contents > div.main_side > div > div.sub_menu_box.layer_type > ul > li"
    ):
        if not (category_page_url := await category.query_selector("a")):
            continue

        if not (category_text := await category_page_url.text_content()):
            continue

        category_text = category_text.strip()

        if not category_text or category_text in [
            "전체상품보기",
            "가격인하",
            "가격준수",
        ]:
            continue

        # ? Not all main categories have sub categories
        if not (
            subcategories := await category.query_selector_all(
                'ul[class="sub_depth1"] > li'
            )
        ):
            url = urljoin(
                page.url,
                await category_page_url.get_attribute("href"),
            )
            full_subcategories.append(Category(category_text, url))

        for subcategory in subcategories:
            if not (subcategory_url := await subcategory.query_selector("a")):
                continue

            if not (subcategory_text := await subcategory_url.text_content()):
                continue

            subcategory_text = subcategory_text.strip()

            # ? Not all sub categories have sub sub categories
            if not (
                sub_subcategories := await subcategory.query_selector_all(
                    'ul[class="sub_depth2"] > li'
                )
            ):
                url = urljoin(
                    page.url,
                    await subcategory_url.get_attribute("href"),
                )
                full_text = f"{category_text}>{subcategory_text}"
                full_subcategories.append(Category(full_text, url))

            for subsubcategory in sub_subcategories:
                if not (subsubcategory_url := await subsubcategory.query_selector("a")):
                    continue

                if not (subsubcategory_text := await subsubcategory_url.text_content()):
                    continue

                subsubcategory_text = subsubcategory_text.strip()
                url = urljoin(
                    page.url,
                    await subsubcategory_url.get_attribute("href"),
                )
                full_text = f"{category_text}>{subcategory_text}>{subsubcategory_text}"
                full_subcategories.append(Category(full_text, url))

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    await page.click(login_button_query)

    await page.wait_for_selector(
        'a[href="../member/logout.php?returnUrl="]', state="visible"
    )


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#loginId",
        password_query="#loginPwd",
        login_button_query="#formLogin > div.member_login_box > div.login_input_sec > button[type='submit']",
        login_button_strategy=login_button_strategy,
    )


async def run(settings: Settings) -> None:
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

        if not settings.URLS:
            if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
                subcategories = await get_categories(
                    sitename=config.SITENAME, filename="subcategories.txt"
                )
            else:
                subcategories = await find_subcategories(browser)

            log.detail.total_categories(len(subcategories))

            crawler = ConcurrentCrawler(
                categories=subcategories,
                start_category=config.START_CATEGORY,
                end_category=config.END_CATEGORY,
                chunk_size=config.CATEGORIES_CHUNK_SIZE,
                crawl=crawl,
            )
            await crawl_categories(crawler, browser, settings, columns)
            return None

        await crawl_urls(
            list(dict.fromkeys(settings.URLS)),
            browser,
            settings,
            columns,
        )


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
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    try:
        await visit_link(page, product_url, wait_until="networkidle")
    except TimeoutException:
        await visit_link(page, product_url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    await page.close()

    # ? Model name is same as Product ID for SINWOOD, so we don't need to extract model name separately
    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        model_name,
        message1,
        price3,
        price2,
        model_name2,
        manufacturing_country,
        delivery_fee,
        options,
        detailed_images_html_source,
    ) = await extract_data(browser, document, product_url, html_top, html_bottom)

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok((option1, price2, option2, option3)):
                    pass
                case Err(err):
                    raise error.OptionsNotFound(err, url=product_url)

            crawl_data = TentwentybagCrawlData(
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                message1=message1,
                model_name2=model_name2,
                price3=price3,
                price2=price2,
                manufacturing_country=manufacturing_country,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
            )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )
        return None

    crawl_data = TentwentybagCrawlData(
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        message1=message1,
        model_name2=model_name2,
        price3=price3,
        price2=price2,
        manufacturing_country=manufacturing_country,
        delivery_fee=delivery_fee,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(idx, product_url)

    return None


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
        name=category.name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
    )
    log.action.visit_category(category_name, category_page_url)

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
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
        log.detail.page_url(category_page_url)

        if not (document := await parse_document(content, engine="lxml")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        if not (products_len := await has_products(document)):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        log.detail.total_products_on_page(products_len, category_state.pageno)

        products_chunk = chunks(range(products_len), config.MAX_PRODUCTS_CHUNK_SIZE)

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        for chunk in products_chunk:
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

        log.action.category_page_crawled(category_name, category_state.pageno)

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
    columns: list[str],
    settings: Settings,
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
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
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
                raise error.ProductsNotFound(err, url=category_page_url)

    productid = get_productid(product_url).expect(
        f"Product ID not found: {product_url}"
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

    match await extract_brand(product):
        case Ok(brand):
            pass
        case Err(_):
            brand = ""

    match await extract_soldout_text(product):
        case Ok(sold_out_text):
            pass
        case Err(_):
            sold_out_text = ""

    page = await browser.new_page()
    try:
        await visit_link(page, product_url, wait_until="networkidle")
    except TimeoutException:
        await visit_link(page, product_url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    await page.close()

    # ? Model name is same as Product ID for SINWOOD, so we don't need to extract model name separately
    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        model_name,
        message1,
        price3,
        price2,
        model_name2,
        manufacturing_country,
        delivery_fee,
        options,
        detailed_images_html_source,
    ) = await extract_data(browser, document, product_url, html_top, html_bottom)

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok((option1, price2, option2, option3)):
                    pass
                case Err(err):
                    raise error.OptionsNotFound(err, url=product_url)

            crawl_data = TentwentybagCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                message1=message1,
                model_name2=model_name2,
                price3=price3,
                price2=price2,
                manufacturing_country=manufacturing_country,
                delivery_fee=delivery_fee,
                brand=brand,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
                sold_out_text=sold_out_text,
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

    crawl_data = TentwentybagCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        message1=message1,
        model_name2=model_name2,
        price3=price3,
        price2=price2,
        manufacturing_country=manufacturing_country,
        delivery_fee=delivery_fee,
        brand=brand,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
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


class Data(NamedTuple):
    thumbnail_image_url: str
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    product_name: str
    model_name: str
    message1: str
    price3: int
    price2: int
    model_name2: str
    manufacturing_country: str
    delivery_fee: int
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    browser: PlaywrightBrowser,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_images(document, product_url),
        extract_product_name(document),
        extract_model_name(document),
        extract_message1(document),
        extract_price3(document),
        extract_price2(document),
        extract_model_name2(document),
        extract_manufacturing_country(document),
        extract_delivery_fee(document),
        extract_options(document),
        extract_images(document, product_url, html_top, html_bottom),
    )

    # ? Unfortuntely, asyncio.gather() doesn't provide type checking for more than 5 tasks, so we have to cast the types
    (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11) = cast(
        tuple[
            Ok[tuple[str, str, str, str, str]] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[int] | Err[error.QueryNotFound | ValueError],
            Ok[int] | Err[error.QueryNotFound | ValueError],
            Ok[str] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[int] | Err[error.QueryNotFound | ValueError],
            list[str],
            Ok[str] | Err[error.QueryNotFound | error.InvalidImageURL],
        ],
        await asyncio.gather(*tasks),
    )

    match R1:
        case Ok(thumbnail_images):
            (
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
            ) = thumbnail_images
        case Err(err):
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=14462
            thumbnail_image_url = thumbnail_image_url2 = thumbnail_image_url3 = (
                thumbnail_image_url4
            ) = thumbnail_image_url5 = ""

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            page = await browser.new_page()
            await visit_link(page, product_url, wait_until="networkidle")
            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document2
            await page.close()

            tasks = (
                extract_thumbnail_images(document, product_url),
                extract_product_name(document),
                extract_model_name(document),
                extract_message1(document),
                extract_price3(document),
                extract_price2(document),
                extract_model_name2(document),
                extract_manufacturing_country(document),
                extract_delivery_fee(document),
                extract_options(document),
                extract_images(document, product_url, html_top, html_bottom),
            )
            (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11) = cast(  # type: ignore
                tuple[
                    Ok[tuple[str, str, str, str, str]] | Err[error.QueryNotFound],
                    Ok[str] | Err[error.QueryNotFound],
                    Ok[str] | Err[error.QueryNotFound],
                    Ok[str] | Err[error.QueryNotFound],
                    Ok[int] | Err[error.QueryNotFound | ValueError],
                    Ok[int] | Err[error.QueryNotFound | ValueError],
                    Ok[str] | Err[error.QueryNotFound],
                    Ok[str] | Err[error.QueryNotFound],
                    Ok[int] | Err[error.QueryNotFound | ValueError],
                    list[str],
                    Ok[str] | Err[error.QueryNotFound | error.InvalidImageURL],
                ],
                await asyncio.gather(*tasks),
            )
            match R1:
                case Ok(thumbnail_images):
                    (
                        thumbnail_image_url,
                        thumbnail_image_url2,
                        thumbnail_image_url3,
                        thumbnail_image_url4,
                        thumbnail_image_url5,
                    ) = thumbnail_images
                case Err(err):
                    # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=14462
                    thumbnail_image_url = thumbnail_image_url2 = (
                        thumbnail_image_url3
                    ) = thumbnail_image_url4 = thumbnail_image_url5 = ""

            match R2:
                case Ok(product_name):
                    pass
                case Err(err):
                    raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R4:
        case Ok(message1):
            pass
        case Err(err):
            # ? Some products don't have message1
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=7929
            log.debug(f"Message1 is not present: <blue>{product_url}</>")
            message1 = ""

    match R5:
        case Ok(price3):
            pass
        case Err(err):
            # ? Some products don't have price3
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=4094
            log.debug(f"Price3 is not present: <blue>{product_url}</>")
            price3 = 0

    match R6:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R7:
        case Ok(model_name2):
            pass
        case Err(err):
            # ? Some products don't have mdeol name 2
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=12022
            log.debug(f"Model name 2 is not present: <blue>{product_url}</>")
            model_name2 = ""

    match R8:
        case Ok(manufacturing_country):
            pass
        case Err(err):
            # ? Some products don't have manufacturing country
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=11873
            log.debug(f"Manufacturing country is not present: <blue>{product_url}</>")
            manufacturing_country = ""

    match R9:
        case Ok(delivery_fee):
            pass
        case Err(err):
            # ? Some products have text '무료' (Free) as delivery fee
            # ? See: https://www.1020bag.com/goods/goods_view.php?goodsNo=7183
            log.debug(f"Delivery fee is not present: <blue>{product_url}</>")
            delivery_fee = 0

    options = R10

    match R11:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"Detailed images are not present: <blue>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        model_name,
        message1,
        price3,
        price2,
        model_name2,
        manufacturing_country,
        delivery_fee,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?goodsNo=(\w*)")
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
    query = "#contents > div > div.content > div.goods_list_item > div.goods_list > div > div.item_gallery_type.normal_goods > ul > li > div[class='item_cont']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_soldout_text(product: Element):
    query = "div.item_info_cont > div.item_icon_box > img"

    if not (icon := await product.query_selector(query)):
        raise error.QueryNotFound(
            "Sold out icon not found",
            query,
        )

    if (text := await icon.get_attribute("src")) and "soldout" in text:
        return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_brand(product: Element):
    query = "span.item_brand"

    if not (brand := await product.query_selector(query)):
        raise error.QueryNotFound(
            "Brand not found",
            query,
        )

    if not (brand := await brand.text_content()):
        raise error.QueryNotFound("Brand not found", query=query)

    return brand.strip()


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#frmView > div > div > div.item_detail_tit > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "//dt[text()='상품코드']/following-sibling::dd[1]"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query=query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document):
    query = "//dt[text()='짧은설명']/following-sibling::dd[1]"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Message1 not found", query=query)

    return model_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "//dt[text()='판매가']/following-sibling::dd[1]"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return parse_int(price2)


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document):
    query = "//dt[text()='정가']/following-sibling::dd[1]"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price3 not found", query=query)

    return parse_int(price3)


@returns_future(error.QueryNotFound)
async def extract_model_name2(document: Document):
    query = "//dt[text()='자체상품코드']/following-sibling::dd[1]"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name 2 not found", query=query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturing_country(document: Document):
    query = "//dt[text()='원산지']/following-sibling::dd[1]"
    if not (manufacturing_country := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturing country not found", query=query)

    return manufacturing_country.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_delivery_fee(document: Document):
    query = "//dt[text()='배송비']/following-sibling::dd[1]/strong"
    if not (delivery_fee := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    return parse_int(delivery_fee)


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = ".item_photo_info_sec div.item_photo_slide > ul > div > div > li.slick-slide > a > img"
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := (await document.query_selector_all(query))[1:]:
        thumbnail_images = [
            urljoin(product_url, await image.get_attribute("src"))
            for image in thumbnail_images
        ]
        N = 4
        thumbnail_images += [""] * (N - len(thumbnail_images))
        (
            thumbnail_image_url2,
            thumbnail_image_url3,
            thumbnail_image_url4,
            thumbnail_image_url5,
            *_,
        ) = thumbnail_images

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    )


@returns(ValueError, IndexError)
def split_options_text(option1: str, price2: int):
    option3 = ""
    if "(+" in option1:
        regex = compile_regex(r"\(\+\w+[,]?\w*원\)")
        try:
            additional_price = regex.findall(option1)[0]
        except IndexError:
            pass
        else:
            option1 = regex.sub("", option1).strip()
            option3 = parse_int(additional_price)
            price2 += option3

    if "+" in option1 and ":" in option1:
        regex = compile_regex(r":?\s*?\+\w+[,]?\w*원")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1).strip()
        option3 = parse_int(additional_price)
        price2 += option3

    if "(-" in option1:
        regex = compile_regex(r"\(\-\w+[,]?\w*원\)")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1).strip()
        option3 = parse_int(additional_price)
        price2 -= option3

    option2 = ""

    if "[품절]" in option1:
        option1 = option1.replace("[품절]", "").replace("-", "").strip()
        option2 = "[품절]"

    if "[일시품절/재입고미정]" in option1:
        option1 = option1.replace("[일시품절/재입고미정]", "").replace("-", "").strip()
        option2 = "[일시품절/재입고미정]"

    if "[품절/재입고없음]" in option1:
        option1 = option1.replace("[품절/재입고없음]", "").replace("-", "").strip()
        option2 = "[품절/재입고없음]"

    if matches := compile_regex(r"(\d*개남음)").findall(option1):
        quantity_left = matches[0]
        option1 = option1.replace(quantity_left, "").replace("-", "").strip()
        option2 = quantity_left

    return option1, price2, option2, option3


async def extract_options(document: Document):
    query = "select[name='optionSnoInput']"

    return [
        text.strip()
        for option in await document.query_selector_all(
            f"{query} > option, {query} > optgroup > option"
        )
        if (text := await option.text_content())
        and await option.get_attribute("value") not in ["", "*", "**"]
    ]


@cache
def image_queries():
    return "#detail div.detail_cont div.detail_explain_box div.txt-manual img"


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_queries()

    urls = [
        src
        for image in await document.query_selector_all(query)
        if (src := await image.get_attribute("src"))
    ]

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    if any("base64" in url for url in urls):
        raise error.InvalidImageURL("Base64 is present in images")

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )
