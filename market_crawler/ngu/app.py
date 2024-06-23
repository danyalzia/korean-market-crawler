# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib

from dataclasses import dataclass
from functools import cache
from typing import cast
from urllib.parse import urljoin

import backoff

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import fetch_content, load_content, parse_document, visit_link
from dunia.login import LoginInfo
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
from market_crawler.ngu import config
from market_crawler.ngu.data import NGUCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
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


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='member_id']",
        password_query="input[name='member_passwd']",
        login_button_query='a[class="cboth btnLogin_bk"]:has-text("LOGIN")',
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

        match await get_product_link(product, category_page_url):
            case Ok(product_url):
                pass
            case Err(err):
                raise error.ProductsNotFound(err, url=category_page_url)

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
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        table,
        thumbnail_image_url,
        detailed_images_html_source,
        options,
    ) = await extract_data(document, page, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            crawl_data = NGUCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                detailed_images_html_source=detailed_images_html_source,
                manufacturing_country=table.manufacturing_country,
                price2=table.price2,
                price3=table.price3,
                sold_out_text=table.sold_out_text,
                quantity=table.quantity,
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

    crawl_data = NGUCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        detailed_images_html_source=detailed_images_html_source,
        manufacturing_country=table.manufacturing_country,
        price2=table.price2,
        price3=table.price3,
        sold_out_text=table.sold_out_text,
        quantity=table.quantity,
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


async def extract_data(
    document: Document,
    page: PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_product_name(document),
        extract_table(page),
        extract_thumbnail_image(document, product_url),
        extract_images(page, product_url, html_top, html_bottom),
        extract_options(page),
    )

    R1, R2, R3, R4, R5 = await asyncio.gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(table):
            pass
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            raise error.ProductDetailImageNotFound(err, url=product_url)

    match R5:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    return (
        product_name,
        table,
        thumbnail_image_url,
        detailed_images_html_source,
        options,
    )


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"[/](\d+\w+)/category")
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
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    query = "div > div.thumbnail > div > a"
    if not (product_link := await product.query_selector(query)):
        raise error.QueryNotFound("Product link not found", query)

    return urljoin(category_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div.detailArea > div.infoArea > div.cboth.headingArea > h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.keyImg > div > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


async def extract_soldout_text(page: PlaywrightPage):
    if (
        soldout_icon_src := await page.get_attribute(
            "#contents div.detailArea > div.infoArea > div.cboth.headingArea > div.cboth.icon > img.icon_img",
            "src",
        )
    ) and "product_soldout" in soldout_icon_src:
        return "품절"

    return ""


@dataclass(slots=True, kw_only=True)
class Table:
    manufacturing_country: str
    price3: int
    price2: int
    quantity: str
    sold_out_text: str


async def extract_table(page: PlaywrightPage):
    await page.wait_for_load_state("networkidle", timeout=300000)

    try:
        manufacturing_country = cast(
            str,
            await page.text_content(
                "table > tbody > tr:nth-child(2) > td > span:right-of(th:has-text('원산지'))"
            ),
        ).strip()
        price3 = parse_int(
            cast(
                str,
                await page.text_content("#span_product_price_custom"),
            )
        )
        price2 = parse_int(
            cast(
                str,
                await page.text_content("#span_product_price_text"),
            )
        )

        quantity = cast(
            str,
            await page.text_content(
                "div.detailArea > div.infoArea > div.guideArea > p"
            ),
        )

        sold_out_text = await extract_soldout_text(page)
    except Exception as ex:
        return Err(Exception(ex))

    return Ok(
        Table(
            manufacturing_country=manufacturing_country,
            price3=price3,
            price2=price2,
            quantity=quantity,
            sold_out_text=sold_out_text,
        )
    )


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    options: list[str] = []

    option1_query = "select[id='product_option_id1']"
    option2_query = "select[id='product_option_id2']"

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1_texts = [
        "".join((text).split())
        for option1 in option1_elements
        if (text := await option1.text_content())
    ]
    option1_values = [
        await option1.get_attribute("value") for option1 in option1_elements
    ]

    for option1_text, option1_value in zip(option1_texts, option1_values):
        if option1_value in ["", "*", "**"]:
            continue

        # ? When there are a lot of requests at once, select_option() throws TimeoutError, so let's backoff here
        try:
            await page.select_option(
                option1_query,
                value=option1_value,
            )
        except error.PlaywrightTimeoutError as err:
            await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for select_option()"
            ) from err

        try:
            await page.wait_for_load_state("networkidle")
        except error.PlaywrightTimeoutError as err:
            await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for wait_for_load_state()"
            ) from err

        if option2_elements := await page.query_selector_all(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        ):
            option2_texts = [
                "".join((text).split())
                for option2 in option2_elements
                if (text := await option2.text_content())
            ]
            option2_values = [
                await option2.get_attribute("value") for option2 in option2_elements
            ]

            options.extend(
                f"{option1_text}/{option2_text}"
                for option2_text, option2_value in zip(option2_texts, option2_values)
                if option2_value and option2_value not in ["", "*", "**"]
            )
        else:
            options.append(option1_text)

    return options


@cache
def image_quries():
    return "#prdDetail > div img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()
    if elements := await page.query_selector_all(query):
        await page.click(query)
    else:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

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

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
