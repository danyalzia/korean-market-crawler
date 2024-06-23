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

import lxml.html as lxml
import pandas as pd

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
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
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.sfc import config
from market_crawler.sfc.data import SFCCrawlData
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


async def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsNo=(\w+)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "#content > div > div > div.cg-main > div.goods-list > div.item-display.type-cart > div > ul > li > div"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("div.thumbnail > a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


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

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
        page = await browser.new_page()
        await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()
        if config.SAVE_HTML and not await category_html.exists():
            await category_html.save(content)
        await page.close()

        if not (document := await parse_document(content, engine="lexbor")):
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

    productid = (await get_productid(product_url)).expect(
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

    match await extract_product_name(product):
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match await extract_thumbnail_image(product, product_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(_):
            thumbnail_image_url = ""

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

    match await extract_table(page):
        case Ok(table_list):
            for table in table_list:
                crawl_data = SFCCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    detailed_images_html_source=detailed_images_html_source,
                    model_name=table.model_name,
                    price2=table.price2,
                )

                await save_series_csv(
                    to_series(crawl_data, settings.COLUMN_MAPPING),
                    columns,
                    filename,
                )

            log.action.product_crawled_with_options(
                idx,
                category_state.name,
                category_state.pageno,
                product_url,
                len(table_list),
            )

        case Err(err):
            log.warning(
                f"Table not found <blue>| {page.url}</>. Skipping the product ..."
            )

    await page.close()

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()


@dataclass(frozen=True, slots=True)
class TextChangingPrice:
    text: str
    price2: int


async def is_options_changing_price2_present(page: PlaywrightPage):
    if options_dropdown_list := (
        await page.query_selector_all("#div_Option_Select > span > ul > li > ul > li")
    ):
        for options_dropdown in options_dropdown_list:
            if options_dropdown_text := (await options_dropdown.text_content()):
                options_dropdown_text = options_dropdown_text.strip()

                if "(+" in options_dropdown_text or "(-" in options_dropdown_text:
                    return True

    return False


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(element: Element, product_url: str):
    query = "div.thumbnail > a > img"
    if not (el := await element.query_selector(query)) or not (
        thumbnail_image := await el.get_attribute("src")
    ):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_name(element: Element):
    query = "div.txt > a > strong"
    if not (el := await element.query_selector(query)) or not (
        product_name := await el.text_content()
    ):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


async def extract_table(page: PlaywrightPage):
    model_name = ""
    price2 = 0
    table_list: list[Table] = []

    try:
        df = pd.read_html(await page.content(), flavor="lxml")
    except ValueError:
        return Err(error.TableNotFound(page.url))

    try:
        df[1].columns = df[1].iloc[0, :]  # type: ignore
    except IndexError:
        return Err(error.TableNotFound(page.url))
    else:
        df[1] = df[1].drop([0])

    try:
        model_name_list: list[str] = df[0]["모델"].tolist()
        price2_list: list[str] = df[0]["판매가"].tolist()
    except KeyError:
        try:
            model_name_list: list[str] = df[1]["모델"].tolist()
        except KeyError:
            return Err(error.ModelNameNotFound(page.url))

        try:
            price2_list: list[str] = df[1]["판매가"].tolist()
        except KeyError:
            return Err(error.Price2NotFound(page.url))

    model_names_len = len(model_name_list)
    price2_len = len(price2_list)

    assert (
        model_names_len == price2_len
    ), f"Model names and price2 rows are not same length: {model_names_len} vs {price2_len}"

    for model_name, price2 in zip(model_name_list, price2_list):
        model_name = model_name.strip()
        try:
            price2 = parse_int(price2)
        except ValueError:
            log.warning(
                f"Unusual price2 text found ({price2}) which could not be converted into integer"
            )

        table_list.append(Table(model_name, price2))

    return Ok(table_list)


@dataclass(frozen=True, slots=True)
class Table:
    model_name: str
    price2: int | str


@cache
def image_quries():
    return "#detail > div img"


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
            max_tries=10,
        ):
            case Ok(src):
                urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    return await build_html2(
        map(lambda url: urljoin(product_url, url), urls), page, html_top, html_bottom
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)


async def build_html2(
    urls: Iterable[str], page: PlaywrightPage, html_top: str, html_bottom: str
):
    """
    Build HTML from the URLs of the images based on our template
    """

    html_source = html_top

    for image_url in urls:
        html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

    if (table := await page.query_selector("#detail > div table.__se_tbl_ext")) and (
        div := await table.query_selector("xpath=..")
    ):  # ? Parent of <table>
        table_html_source = (await div.inner_html()).strip()

        html_source = "".join(
            [
                html_source,
                await asyncio.to_thread(remove_styles_in_table, table_html_source),
            ]
        )
    else:
        log.warning(f"Specifications Table HTML not found <blue>| {page.url}</>")

    html_source = "".join(
        [
            html_source,
            html_bottom,
        ],
    )

    return html_source.strip()


def remove_styles_in_table(html_content: str) -> str:
    document: lxml.HtmlElement = lxml.fromstring(html_content)
    td = document.xpath("//*/td")
    for p in td:
        if "style" in p.attrib:  # type: ignore
            del p.attrib["style"]  # type: ignore

    return lxml.tostring(document, pretty_print=True).decode("euc-kr").replace("\n", "").replace("  ", "")  # type: ignore
