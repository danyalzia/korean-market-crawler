# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
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
from market_crawler.memory import MemoryOptimizer
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from market_crawler.thehouse import config
from market_crawler.thehouse.data import TheHouseCrawlData
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)

    await page.wait_for_selector('text="LOGOUT"')


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
        user_id_query="#loginId",
        password_query="#loginPwd",
        login_button_query="button:has-text('로그인')",
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

    memory_optimizer = MemoryOptimizer(
        max_products_chunk_size=config.MAX_PRODUCTS_CHUNK_SIZE,
    )
    category_chunk_size = config.CATEGORIES_CHUNK_SIZE
    products_chunk_size = config.MIN_PRODUCTS_CHUNK_SIZE

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

        products_chunk_size = await memory_optimizer.optimize_products_chunk_sizes(
            browser, category_chunk_size, products_chunk_size
        )

        for chunk in chunks(range(number_of_products), products_chunk_size):
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


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "div.goods_list_item > div.goods_list > div > div > ul > li > div[class='item_cont']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsNo=(\d+\w+)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all(
        "div.item_info_cont > div.item_icon_box > img"
    ):
        if (src := await icon.get_attribute("src")) and "soldout" in src:
            return "품절"

    return ""


async def extract_message2(
    idx: int, products: list[Element]
) -> Result[bool, IndexError | TimeoutError]:
    message2 = False
    try:
        icons = await products[idx].query_selector_all(
            "div.item_info_cont > div.item_icon_box > img"
        )
    except IndexError as err:
        return Err(err)
    try:
        for icon in icons:
            icon_alt = str(await icon.get_attribute("alt"))
            if "판매가준수" in icon_alt:
                message2 = True
    except TimeoutError as err:
        return Err(err)

    return Ok(message2)


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

    sold_out_text = await extract_soldout_text(product)

    message2 = ""
    match await extract_message2(idx, products):
        case Ok(message2_present) if message2_present:
            log.info(f"Sales compliance icon is present: Product no: {idx+1}")
            message2 = "sales compliance"
        case Err(err):
            raise error.Message2NotFound(err, url=product_url)
        case _:
            pass

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    if not (product_name := (await extract_product_name(document)).ok()):
        await visit_link(page, product_url, wait_until="networkidle")

        if not (document := await parse_document(await page.content(), engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        match await extract_product_name(document):
            case Ok(product_name):
                pass
            case Err(err):
                raise error.ProductNameNotFound(err, url=product_url)

    (
        thumbnail_image_url,
        price2,
        message1,
        options,
        detailed_images_html_source,
    ) = await extract_data(document, page, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            match split_options_text(option, price2):
                case Ok(result):
                    (option1, price2_, option2, option3) = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = TheHouseCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                message1=message1,
                message2=message2,
                price2=price2_,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1,
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

    crawl_data = TheHouseCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        message1=message1,
        message2=message2,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
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


async def extract_data(
    document: Document,
    page: PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_image(document, product_url),
        extract_price2(document),
        extract_message1(document),
        extract_options(page),
    )

    (R1, R2, R3, R4) = await asyncio.gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R3:
        case Ok(message1):
            pass
        case Err(err):
            raise error.Message1NotFound(err, url=product_url)

    match R4:
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

    return thumbnail_image_url, price2, message1, options, detailed_images_html_source


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#frmView > div > div > div.item_detail_tit > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "div.item_detail_list > dl.item_price > dd"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query)

    return parse_int(price2)


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(error.QueryNotFound, IndexError)
async def extract_options(page: PlaywrightPage):
    options: list[str] = []

    query = "#frmView > div > div > div.item_detail_list > div > dl > dd > div"

    if not (option_div := await page.query_selector(query)):
        return options

    try:
        await option_div.click()
    except error.PlaywrightTimeoutError as err:
        raise err from err

    query = "#frmView > div > div > div.item_detail_list > div > dl > dd > select > option, #frmView > div > div > div.item_detail_list > div > dl > dd > select > optgroup > option"

    if not (
        option1_elements := await page.query_selector_all(
            query,
        )
    ):
        raise error.QueryNotFound("Options not found", query)

    for j in range(len(option1_elements)):
        try:
            option1_value: str = cast(
                str,
                await option1_elements[j].get_attribute("value"),
            )

            option1_text: str = cast(str, await option1_elements[j].text_content())
        except error.PlaywrightTimeoutError as err:
            raise err from err

        if option1_value not in ["", "*", "**"]:
            options.append(option1_text.strip())

    return options


async def extract_message1(document: Document) -> Result[str, error.QueryNotFound]:
    query = "#detail > div.detail_cont > div > div.txt-manual > p"
    paragraphs: list[str] = []
    paragraph_selectors = await document.query_selector_all(query)

    if not paragraph_selectors:
        return Ok("")

    for p in paragraph_selectors:
        if not (text := await p.text_content()):
            continue

        if (
            "* 최저판매가 *" not in (paragraph := text.replace("\xa0", " "))
            and not paragraph.isspace()
            and "----------------------------------------" not in paragraph
        ):
            paragraphs.append(paragraph.strip())
    return Ok("\n".join(paragraphs))


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if "+" in option1 and "원" in option1:
        regex = compile_regex(r"\s?:\s?\+\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if "-" in option1 and "원" in option1:
        regex = compile_regex(r"\s?:\s?\-\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 -= additional_price

    option1 = option1.strip()

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return ", ".join(
        [
            "#detail > div.detail_cont > div > div.txt-manual > p > img",
            "#detail > div.detail_cont > div > div.txt-manual > p > font > span > b > img",
            "#detail > div.detail_cont > div > div.txt-manual img",
        ]
    )


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
        await element.click()
        await page.wait_for_timeout(1000)
