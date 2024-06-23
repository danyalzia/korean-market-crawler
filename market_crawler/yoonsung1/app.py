# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from functools import cache
from urllib.parse import urljoin

import pandas as pd

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import fetch_content, load_content, parse_document, visit_link
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
from market_crawler.initialization import Category, get_categories
from market_crawler.log import logger
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.yoonsung1 import config
from market_crawler.yoonsung1.data import YoonSung1CrawlData
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(current_url: str, next_page_no: int) -> str:
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
    if not (
        category_state := await get_category_state(
            config=config,
            category_name=category.name,
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
    category_page_url = page_url(
        current_url=category.url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category.name, category_page_url)

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

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


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_id: str) -> str:
    if not (product_selector := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    if not (product_url := await product_selector.get_attribute("href")):
        raise error.QueryNotFound("Product link not found", "href")

    # ? For YOONSUNG, product url is not the actual url but a JavaScript function (i.e., "javascript:GotoViewPage('3299');", etc.) which contains page code that we can use to build the product url
    if not (match := compile_regex(r"Page[(]'(\d+)'[)]").search(product_url)):
        if not (match := compile_regex(r"ProductSeqNo=(\d*\w*)").search(product_url)):
            raise ValueError(f'Product ID not found in "{product_url}"')
    product_id = match.group(1)

    return f"https://www.yoonsunginc.kr/products/product_view.php?Mode=I&page=1&hc={category_id}&cn=1&sc=0&ProductSeqNo={product_id}&keyword="


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

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    regex = compile_regex(r"hc=(\d+)")
    if match := regex.search(category_page_url):
        category_id = match.group(1)
    else:
        raise ValueError(f'Category ID not found in "{category_page_url}"')

    match await get_product_link(product, category_id):
        case Ok(product_url):
            pass
        case _:
            content = await fetch_content(
                browser=browser,
                url=category_page_url,
                rate_limit=config.DEFAULT_RATE_LIMIT,
            )
            if not (document := await parse_document(content, engine="lexbor")):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=category_page_url
                )
            match await get_products(document):
                case Ok(products):
                    product = products[idx]
                case Err(err):
                    raise error.ProductsNotFound(err, url=category_page_url)

            if match := regex.search(category_page_url):
                category_id = match.group(1)
            else:
                raise Exception(f'Category ID not found in "{category_page_url}"')

            match await get_product_link(product, category_id):
                case Ok(product_url):
                    pass
                case Err(err):
                    raise error.ProductLinkNotFound(err, url=category_page_url)

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

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        category_text,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        options,
        manufacturing_country,
        detailed_images_html_source,
    ) = await extract_data(browser, page, document, product_url, html_top, html_bottom)

    await page.close()

    regex = compile_regex(r"\((\w+)\)")
    brand = regex.findall(category_state.name)[0]

    option4 = ""

    # ? Truncate the HTML if it's beyong the character limit of Excel
    if len(detailed_images_html_source) > 32767:
        detailed_images_html_source = detailed_images_html_source[:32767]
        option4 = "too long"

    if options:
        for option in options:
            crawl_data = YoonSung1CrawlData(
                category=category_text,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturing_country=manufacturing_country,
                brand=brand,
                option1=option.option1,
                price3=option.price3,
                detailed_images_html_source=detailed_images_html_source,
                option4=option4,
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

    logger.warning(f"No Products list is found: <blue>{product_url}</>")

    crawl_data = YoonSung1CrawlData(
        category=category_text,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturing_country=manufacturing_country,
        brand=brand,
        detailed_images_html_source=detailed_images_html_source,
        option4=option4,
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


async def extract_data(
    browser: PlaywrightBrowser,
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    match await extract_product_name(document):
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match await extract_category_text(document):
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match await extract_thumbnail_images(document, product_url):
        case Ok(thumbnail_images):
            (
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
            ) = thumbnail_images
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match await extract_option1_and_prices(product_url):
        case Ok(options):
            pass
        case _:
            logger.warning(f"Unusual dataframes table <blue>{product_url}</>")
            content = await fetch_content(
                browser=browser,
                url=product_url,
                rate_limit=config.DEFAULT_RATE_LIMIT,
            )
            if not (document2 := await parse_document(content, engine="lxml")):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document2

            match await extract_option1_and_prices(product_url):
                case Ok(options):
                    pass
                case Err(err):
                    raise error.ProductNameNotFound(err, url=product_url)

    match await extract_manufacturing_country(document):
        case Ok(manufacturing_country):
            pass
        case Err(err):
            manufacturing_country = ""

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return (
        product_name,
        category_text,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        options,
        manufacturing_country,
        detailed_images_html_source,
    )


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"ProductSeqNo=(\d*\w*)")
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
    query = "#content > div > ul.product_list > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div.details > div.namebar > h5"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "#frm > div.clearfix.mT50 > div.pvL > ul > li"
    if not (category_text := await document.query_selector_all(query)):
        raise error.QueryNotFound("Category text not found", query)

    return ">".join(
        [text.strip() for el in category_text if (text := await el.text_content())]
    )


@returns_future(error.QueryNotFound, ValueError)
async def extract_manufacturing_country(document: Document):
    query = "#frm > div.clearfix.mT50 > div.details > div.taR.sotitle2.pR20"
    if not (text := await document.text_content(query)):
        raise error.QueryNotFound(
            "Text containing possible manufacturing country not found", query
        )

    text = text.strip()

    regex = compile_regex(r"제조국\s*[:]?\s*(\w*\d*)")

    if not (match := regex.findall(text)):
        raise ValueError("Regex for manufacturing country is not matched")

    return match[0]


@dataclass(slots=True, frozen=True)
class Product:
    option1: str
    price3: int


@returns_future(Exception)
async def extract_option1_and_prices(page_url: str):
    products: list[Product] = []
    df_list = pd.read_html(page_url)

    match df_list:
        case [pd.DataFrame() as df, *_] if (
            df.columns.isin(["품명", "판매가격", "부품도"])
        ).all() or (df.columns.isin(["품명", "판매가격"])).all():
            option1_values: list[str] = df["품명"].to_list()
            prices3_values: list[str] = df["판매가격"].to_list()

            for option1, price3 in zip(option1_values, prices3_values, strict=True):
                regex = compile_regex(r"(\d+,?\d+)원")
                if price3 == "0원":
                    products.append(Product(option1, 0))
                else:
                    if not (match := regex.search(price3)):
                        raise Exception(
                            f'Regex couldn\'t find the price in "{price3}" -> {page_url}'
                        )

                    products.append(Product(option1, parse_int(match.group(1))))
        case [pd.DataFrame()]:
            logger.error(f"Only one table is found in {page_url}")
            raise Exception(f"Only one table is found in {page_url}")
        case _:
            logger.error(page_url)
            raise Exception(f"Unusual dataframes: {page_url}")

    return products


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div.bigimg > span > span > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img[id^=Simg]"
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := (await document.query_selector_all(query)):
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


async def extract_html_table(page: PlaywrightPage, product_url: str):
    full_html = ""

    query = "#frm > div.EditText table"

    tbody_elements = await page.query_selector_all(query)

    for tbody in tbody_elements:
        if not (table := await tbody.query_selector("xpath=..")):
            raise error.QueryNotFound(
                f"HTML Table not found: {product_url}", query=query
            )

        html = (await table.inner_html()).strip()
        assert "<table" in html, f"HTML: {html}"
        full_html += html + "\n"

    query = "#frm > div.prod_wrap > ul"

    if not (detail_information := await page.query_selector(query)):
        log.warning(f"Detail information HTML not found: {product_url}")
        return full_html

    if not (html := await detail_information.query_selector("xpath=..")):
        raise error.QueryNotFound(
            f"Detail information HTML not found: {product_url}", query=query
        )

    html = (await html.inner_html()).strip()

    html = html.replace('src="/', 'src="https://www.yoonsunginc.kr/').replace(
        "src='/", "src='https://www.yoonsunginc.kr/"
    )
    full_html += "\n" + html + "\n"

    return full_html


@cache
def image_quries():
    return ", ".join(
        [
            "#frm > div.taC > div > div > img",
            "#frm > div.taC > div > div img",
            "#frm > div.taC > div img",
        ]
    )


@returns_future(error.QueryNotFound, MaxTriesReached)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    table_html = await extract_html_table(page, product_url)
    query = image_quries()

    if elements := await page.query_selector_all(query):
        await page.click(query)

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
            f"Product detail images are not present at all: {product_url}", query=query
        )

    return build_html(
        table_html,
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)
        await page.wait_for_timeout(1000)


def build_html(
    table_html: str, images_construction: Iterable[str], html_top: str, html_bottom: str
):
    html_source = html_top

    for image_url in images_construction:
        html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

    html_source = "".join(
        [
            html_source,
            table_html,
            html_bottom,
        ],
    )

    return html_source.strip()
