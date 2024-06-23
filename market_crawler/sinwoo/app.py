# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from contextlib import suppress
from functools import cache, singledispatch
from typing import NamedTuple, overload
from urllib.parse import urljoin

import backoff

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
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
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.sinwoo import config
from market_crawler.sinwoo.data import SinwooCrawlData
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


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "https://www.sinwoo.com/", wait_until="load")

    categories = await page.query_selector_all(
        "#header > div.fullContainer > div > div.naviArea > div.topMenu > ul > li > a"
    )

    for category in categories:
        category_text = await category.text_content()
        category_page_url = urljoin(page.url, await category.get_attribute("href"))

        page2 = await browser.new_page()
        await visit_link(page2, category_page_url, wait_until="load")

        subcategories = await page2.query_selector_all(
            "#contents > table > tbody > tr > td > table > tbody > tr > td > div.container > div.productCategory > div.detailCate > div > p > a"
        )

        for subcategory in subcategories:
            # ? Skip the ones that don't have any text (because their links are empty anyway)
            if subcategory_text := (await subcategory.text_content()):
                url = urljoin(page.url, await subcategory.get_attribute("href"))
                full_text = f"{category_text}>{subcategory_text.strip()}"
                full_subcategories.append(Category(full_text, url))

        await page2.close()

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_selector('a[href="/member/logout.php"]')


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#loginbox form input[name='Uid']",
        password_query="#loginbox form input[name='Pw']",
        login_button_query="#loginbox form a[class='b_red2']",
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

        if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
            subcategories = await get_categories(
                sitename=config.SITENAME, filename="subcategories.txt"
            )
        else:
            subcategories = await find_subcategories(browser)

        log.detail.total_categories(len(subcategories))

        columns = list(settings.COLUMN_MAPPING.values())
        crawler = ConcurrentCrawler(
            categories=subcategories,
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
        if not (document := await parse_document(content, engine="lxml")):
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
                # ? Some products are only for placeholder and don't have any link
                # ? See: https://www.sinwoo.com/shop/socks.php?cat_code=48/739/&page=2
                log.warning(
                    "Placeholder/Empty product found without any link, therefore skipping this entry."
                )
                return None

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

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    # ? Model name is same as Product ID for SINWOO, so we don't need to extract model name separately
    model_name = productid

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        price3,
        delivery_fee,
        message1,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            crawl_data = SinwooCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                price2=price2,
                price3=price3,
                delivery_fee=delivery_fee,
                message1=message1,
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

    crawl_data = SinwooCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        price2=price2,
        price3=price3,
        delivery_fee=delivery_fee,
        message1=message1,
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


class Data(NamedTuple):
    thumbnail_image_url: str
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    product_name: str
    price2: int
    price3: int
    delivery_fee: str
    message1: str
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_images(document, product_url),
        extract_product_name(document),
        extract_price2(document),
        extract_price3(document),
        extract_delivery_fee(document),
        extract_message1(document),
        extract_options(page),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
        R6,
        R7,
        R8,
    ) = await gather(*tasks)

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
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R4:
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    match R5:
        case Ok(delivery_fee):
            pass
        case Err(err):
            raise error.DeliveryFeeNotFound(err, url=product_url)

    match R6:
        case Ok(message1):
            pass
        case Err(err):
            raise error.Message1NotFound(err, url=product_url)

    match R7:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R8:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
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
        price2,
        price3,
        delivery_fee,
        message1,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?brandcode=(\w*-?\w*)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(tree: Document) -> int | None:
    match await get_products(tree):
        case Ok(products):
            return len(products)
        case _:
            return None


@overload
async def get_products(
    tree: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    tree: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(tree: Document | PlaywrightPage):
    query = "#contents > table > tbody > tr > td > table > tbody > tr > td > div.container > div.sub_itemList > div > form > ul > li[class^='item']"
    return (
        Ok(products)
        if (products := await tree.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div[class='itemTit']"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "dd[class='f_price']"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return parse_int(price2)


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document):
    # ? This requires either LXML or Playwright's parser because Selectolax doesn't support XPATH
    query = "//dt[./font[text()='오픈마켓 가격']]/following-sibling::dd[1]"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price3 not found", query=query)

    return parse_int(price3)


@returns_future(error.QueryNotFound)
async def extract_delivery_fee(document: Document):
    # ? This requires either LXML or Playwright's parser because Selectolax doesn't support XPATH
    query = "//dt[text()='배 송 비']/following-sibling::dd[1]"
    if not (delivery_fee := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    return delivery_fee


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document):
    # ? This requires either LXML or Playwright's parser because Selectolax doesn't support XPATH
    query = "//dd[starts-with(text(), '상세정')]"
    if not (message1 := await document.text_content(query)):
        raise error.QueryNotFound("Message1 not found", query=query)

    return message1


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div[class='imgArea'] > div.img #bigimage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        log.warning(f"Thumbnail image not found: {product_url}")
        thumbnail_image_url = ""
    else:
        thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "div.img_small.fl_r > li > div > img"
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
async def extract_options(page: PlaywrightPage):
    options: list[str] = []

    option1_query = "select[name='size']"
    option2_query = "select[name='color']"

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1_texts = [
        "".join(text.split())
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
                "".join(text.split())
                for option2 in option2_elements
                if (text := await option2.text_content())
            ]
            option2_values = [
                await option2.get_attribute("value") for option2 in option2_elements
            ]

            options.extend(
                f"{option1_text}_{option2_text}"
                for option2_text, option2_value in zip(option2_texts, option2_values)
                if option2_value and option2_value != "*" and option2_value != "**"
            )

        else:
            options.append(option1_text)

    return options


@cache
def image_queries():
    return "#contents > div.container > div.detail_con img"


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage, product_url: str, settings: Settings
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document_or_page: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_queries()

    urls = [
        src
        for image in await document_or_page.query_selector_all(query)
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


@extract_images.register
@returns_future(error.QueryNotFound, error.Base64Present)
async def _(
    document_or_page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_queries()
    if not (elements := await document_or_page.query_selector_all(query)):
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    urls: list[str] = []
    for el in elements:
        action = lambda: get_srcs(el)
        focus = lambda: focus_element(document_or_page, el)
        is_base64 = isin("base64")
        match await do(action).retry_if(
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

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)
