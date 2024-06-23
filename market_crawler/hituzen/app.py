# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib

from collections.abc import Iterable
from functools import cache, partial, singledispatch
from re import Pattern
from typing import Any, NamedTuple
from urllib.parse import urljoin

import backoff
import pandas as pd

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
from market_crawler.hituzen import config
from market_crawler.hituzen.data import HituzenCrawlData
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


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

    sold_out_text = await extract_soldout_text(product)

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        manufacturer,
        manufacturing_country,
        price2,
        model_name,
        delivery_fee,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok(result):
                    option1_, price2_, option2, option3 = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = HituzenCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                price2=price2_,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1_,
                option2=option2,
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
            len(options),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    crawl_data = HituzenCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        price2=price2,
        delivery_fee=delivery_fee,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
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
    manufacturer: str
    manufacturing_country: str
    price2: int
    model_name: str
    delivery_fee: int
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    (R1, R2, R3, R4) = await asyncio.gather(
        extract_thumbnail_images(document, product_url),
        extract_table(page),
        extract_options(document, page),
        extract_images(document, product_url, html_top, html_bottom),
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
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(table):
            (
                product_name,
                manufacturer,
                manufacturing_country,
                price2,
                model_name,
                delivery_fee,
            ) = table
        case Err(err):
            raise error.SellingPriceNotFound(err, url=product_url)

    match R3:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(error.InvalidImageURL(err)):
            # ? Use Playwright's Page for parsing in case of Base64
            match await extract_images(page, product_url, html_top, html_bottom):
                case Ok(detailed_images_html_source):
                    pass
                case Err(error.QueryNotFound(err)):
                    log.debug(f"{err}: <yellow>{product_url}</>")
                    detailed_images_html_source = "NOT PRESENT"
                case Err(err):
                    raise error.ProductDetailImageNotFound(err, product_url)

        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        manufacturer,
        manufacturing_country,
        price2,
        model_name,
        delivery_fee,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?product_no=(\w+)")
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
    query = "div.xans-element-.xans-product.xans-product-listnormal > ul > li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element) -> str:
    for icon in await product.query_selector_all("div > div > div.icon > img"):
        if (icon_src := await icon.get_attribute("src")) and "soldout" in icon_src:
            return "품절"

    return ""


regex_between_parenthesis = r"\[(.*)\]"


def take_between(regex: Pattern[Any], s: str):
    if match := regex.search(s):
        return match.group(1)


def take_outside(regex: Pattern[Any], s: str):
    return regex.sub("", s)


take_between_brackets = cache(
    partial(take_between, compile_regex(regex_between_parenthesis))
)
take_outside_brackets = cache(
    partial(take_outside, compile_regex(regex_between_parenthesis))
)


class Table(NamedTuple):
    product_name: str = ""
    manufacturer: str = ""
    manufacturing_country: str = ""
    price2: int = 0
    model_name: str = ""
    delivery_fee: int = 0


async def extract_table(page: PlaywrightPage):
    product_name = ""
    manufacturer = ""
    manufacturing_country = ""
    price2 = 0
    model_name = ""
    delivery_fee = 0

    try:
        df = await asyncio.to_thread(pd.read_html, page.url)  # type: ignore
    except ValueError:
        return Err(error.TableNotFound(page.url))

    table_df = df[0].T
    columns: list[str] = list(table_df.iloc[0])  # type: ignore
    table_df.columns = columns  # type: ignore
    table_df = table_df.drop([0]).reset_index(drop=True)

    try:
        product_name = str(table_df.loc[0, "상품명"])
    except KeyError as err:
        raise error.ProductNameNotFound(page.url) from err
    try:
        manufacturer = str(table_df.loc[0, "제조사"])
    except KeyError as err:
        raise error.ManufacturerNotFound(page.url) from err

    try:
        manufacturing_country = str(table_df.loc[0, "원산지"])
    except KeyError as err:
        raise error.ManufacturingCountryNotFound(page.url) from err

    try:
        price2 = parse_int(str(table_df.loc[0, "판매가"]))
    except KeyError as err:
        raise error.Price2NotFound(page.url) from err

    try:
        model_name = str(table_df.loc[0, "상품코드"])
    except KeyError as exception:
        raise error.ModelNameNotFound(page.url) from exception

    regex = compile_regex(r"\d+,?\d+원")
    delivery_fee = parse_int(regex.findall(str(table_df.loc[0, "배송비"]))[0])

    return Ok(
        Table(
            product_name,
            manufacturer,
            manufacturing_country,
            price2,
            model_name,
            delivery_fee,
        )
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div.keyImg > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        log.warning(f"Thumbnail image not found: {product_url}")
        thumbnail_image_url = ""
    else:
        thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img.ThumbImage"
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


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(document: Document, page: PlaywrightPage):
    option1_query = "#product_option_id1"
    option2_query = "#product_option_id2"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := (await option1.text_content()))
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    # ? If option2 is not present, then we don't need to use Page methods
    if not (
        await document.query_selector(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        )
    ):
        return list(option1.keys())

    options: list[str] = []

    for option1_text, option1_value in option1.items():
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

        option2 = {
            "".join(text.split()): option2_value
            for option2 in await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )
            if (text := (await option2.text_content()))
            and (option2_value := await option2.get_attribute("value"))
            not in ["", "*", "**"]
        }

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if "(+" in option1:
        regex = compile_regex(r"\s?\(\+\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if "(-" in option1:
        regex = compile_regex(r"\s?\(\-\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 -= additional_price

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return ", ".join(
        [
            "#prdDetail > div > p > img",
            "#prdDetail > div > img",
            "#prdDetail > div > iframe",
        ]
    )


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage, product_url: str, settings: Settings
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    parent = (
        frame
        if (frame := await document.query_selector("iframe[name='contents_frame']"))
        else document
    )

    query = image_quries()

    urls = [
        src
        for image in await parent.query_selector_all(query)
        if (src := await image.get_attribute("src"))
    ]

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    if any("base64" in url for url in urls):
        raise error.InvalidImageURL("Base64 is present in images")

    return build_html(
        map(lambda url: urljoin(product_url, url), urls), html_top, html_bottom
    )


@extract_images.register
@returns_future(error.QueryNotFound, error.Base64Present)
async def _(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    if frame := page.frame(name="contents_frame"):
        elements = await frame.query_selector_all(query)
    else:
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

    return build_html(
        map(lambda url: urljoin(product_url, url), urls), html_top, html_bottom
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)


def build_html(urls: Iterable[str], html_top: str, html_bottom: str):
    html_source = html_top

    for image_url in urls:
        if "youtube.com" in image_url:
            html_source = "".join(
                [
                    html_source,
                    f"""<iframe width="800" height="600" src="{image_url}" frameborder="0" allowfullscreen="" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"></iframe><br />""",
                ]
            )
        else:
            html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

    html_source = "".join(
        [
            html_source,
            html_bottom,
        ],
    )

    return html_source.strip()
