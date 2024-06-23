# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache, singledispatch
from typing import NamedTuple, overload
from urllib.parse import urljoin

import backoff

from fuzzywuzzy import fuzz
from playwright.async_api import async_playwright

from dunia.aio import gather
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
from market_crawler.numberonesports import config
from market_crawler.numberonesports.data import NumberOneSportsCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
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


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    await page.press(login_button_query, "Enter")

    await page.wait_for_selector("text='로그아웃'")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query='input[name="m_id"]',
        password_query='input[name="password"]',
        login_button_query="#form2 > table > tbody > tr > td.noline > input[type=image]",
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

        subcategories = await get_categories(sitename=config.SITENAME)
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
    await visit_link(page, product_url, wait_until="load")

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        product_name,
        product_code,
        brand,
        manufacturing_country,
        price3,
        price2,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if not product_code:
        return None

    if options:
        for option in options:
            if isinstance(price2, int):
                match split_options_text(option, price2):
                    case Ok(data):
                        option1, _price2, option2 = data
                    case Err(err):
                        raise error.IncorrectData(
                            f"Could not split option text ({option}) into price2 due to an error -> {err}",
                            url=product_url,
                        )

                crawl_data = NumberOneSportsCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    product_code=product_code,
                    brand=brand,
                    manufacturing_country=manufacturing_country,
                    price3=price3,
                    price2=_price2,
                    detailed_images_html_source=detailed_images_html_source,
                    option1=option1,
                    option2=option2,
                )
            else:
                crawl_data = NumberOneSportsCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    product_code=product_code,
                    brand=brand,
                    manufacturing_country=manufacturing_country,
                    price3=price3,
                    price2=price2,
                    detailed_images_html_source=detailed_images_html_source,
                    option1=option,
                    option2="",
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

    crawl_data = NumberOneSportsCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        product_code=product_code,
        brand=brand,
        manufacturing_country=manufacturing_country,
        price3=price3,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
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
    product_name: str
    product_code: str
    brand: str
    manufacturing_country: str
    price3: int | str
    price2: int | str
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    (R1, R2, R3, R4, R5, R6) = await gather(
        extract_product_name(document),
        extract_price2(document),
        extract_thumbnail_image(document, product_url),
        extract_table(document),
        extract_options(document),
        extract_images(document, product_url, html_top, html_bottom),
    )

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            await visit_link(page, product_url, wait_until="networkidle")
            if not (
                lxml_document := await parse_document(
                    await page.content(), engine="lxml"
                )
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            (R1, R2, R3, R4, R5, R6) = await gather(  # type: ignore
                extract_product_name(lxml_document),
                extract_price2(lxml_document),
                extract_thumbnail_image(lxml_document, product_url),
                extract_table(lxml_document),
                extract_options(lxml_document),
                extract_images(lxml_document, product_url, html_top, html_bottom),
            )
            match R1:
                case Ok(product_name):
                    pass
                case Err(err):
                    raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R4:
        case Ok(Table(product_code, brand, manufacturing_country)):
            if not product_code:
                # ? Some products don't have product code
                # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48309&category=048
                log.debug(f"Product code is not present <blue>| {product_url}</>")

            if not brand:
                # ? Some products don't have brand
                # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48302&category=048
                log.debug(f"Brand is not present <blue>| {product_url}</>")

            if not manufacturing_country:
                # ? Some products don't have manufacturing country
                # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48309&category=048
                log.debug(
                    f"Manufacturing country is not present <blue>| {product_url}</>"
                )
        case Err(err):
            log.debug(f"Table is not present <blue>| {product_url}</>")
            product_code, brand, manufacturing_country = ""

    if not (options := R5):
        log.debug(f"Options not present <blue>| {product_url}</>")

    match R6:
        case Ok(detailed_images_html_source):
            pass
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

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    price3 = await extract_price3(page, product_code, product_name, product_url)

    return Data(
        thumbnail_image_url,
        product_name,
        product_code,
        brand,
        manufacturing_country,
        price3,
        price2,
        options,
        detailed_images_html_source,
    )


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsno=(\d*\w*)")
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
    # ? We don't want "Best Products" links because they are already included in general products
    query = "#content > div > form:nth-child(4) > table div.goodsBOX"
    return (
        Ok(products)
        if (products := await tree.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "form[name='frmView'] span[style='font-size:23px; color:#2c2c2c;']"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "#objImg"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_price2(document: Document):
    if not (price := await document.text_content("#price")):
        # ? When sale price isn't present, it means they are out of stock
        # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=8686&category=048
        query = (
            "#goods_spec > form > table:nth-child(7) > tbody > tr:nth-child(2) > td > b"
        )
        if await document.text_content(query) != "일시품절":
            raise error.QueryNotFound("Price2 not found", query=query)

        return "일시품절"  # Temporarily out of stock

    return parse_int(price)


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def extract_price3(
    page: PlaywrightPage, product_code: str, product_name: str, product_url: str
):
    if "1sports.co.kr" not in product_url:
        await visit_link(page, "https://www.1sports.co.kr/", wait_until="networkidle")

    if search_bar := await page.query_selector("#sword.search_input"):
        await page.evaluate(
            """document.querySelector("#sword.search_input").value = ''"""
        )  # ? Clear all the existing input text

        try:
            await search_bar.type(product_code)
        except error.PlaywrightTimeoutError as err:
            raise error.TimeoutException("Timed out waiting for type()") from err

        try:
            await search_bar.press("Enter")
        except error.PlaywrightTimeoutError as err:
            raise error.TimeoutException(
                "Timed out waiting for select_option()"
            ) from err

    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_selector("div.goodsBOX")

    for product in await page.query_selector_all("div.goodsBOX"):
        if not (el := await product.query_selector("a.pname")) or not (
            searched_product_name := await el.text_content()
        ):
            continue

        searched_product_name = searched_product_name.strip()

        if searched_product_name.startswith("["):
            searched_product_name = "".join(
                searched_product_name.split("]")[1:]
            ).strip()

        if fuzz.ratio(product_name, searched_product_name) > 94 and (
            price_element := await product.query_selector("a.pprice")
        ):
            return parse_int(await price_element.text_content())

    return "not searched"


class Table(NamedTuple):
    product_code: str
    brand: str
    manufacturing_country: str


@returns_future(error.QueryNotFound)
async def extract_table(document: Document):
    table_body = await document.query_selector_all(
        ", ".join(
            [
                "#goods_spec > form > table:nth-child(8) > tbody",
                "#goods_spec > form > table:nth-child(9) > tbody",
                "#goods_spec > form > table:nth-child(10) > tbody",
            ]
        ),
    )

    if not table_body:
        # ? Some products don't have the table at all
        # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=47548&category=048
        return Table("", "", "")

    # ? As we have already checked the truthy for table_body list above, we won't write it in try except block
    item_headings, item_values = await gather(
        table_body[0].query_selector_all("tr > th"),
        table_body[0].query_selector_all("tr > td"),
    )

    if not item_headings or not item_values:
        try:
            item_headings, item_values = await gather(
                table_body[1].query_selector_all("tr > th"),
                table_body[1].query_selector_all("tr > td"),
            )
        except IndexError as err:
            raise error.QueryNotFound(
                "Table not found",
                query="['tr > th', 'tr > td']",
            ) from err

    if not item_headings or not item_values:
        try:
            item_headings, item_values = await gather(
                table_body[2].query_selector_all("tr > th"),
                table_body[2].query_selector_all("tr > td"),
            )
        except IndexError as err:
            raise error.QueryNotFound(
                "Table not found",
                query="['tr > th', 'tr > td']",
            ) from err

    if not item_headings or not item_values:
        raise error.QueryNotFound(
            "Table not found",
            query="['tr > th', 'tr > td']",
        )

    try:
        assert len(item_headings) == len(
            item_values
        ), f"Not equal {len(item_headings)} vs {len(item_values)}"
    # ? Some products use second table query for product code, brand and manufacturing country
    # ? See: http://www.1sports.kr/shop/goods/goods_view.php?goodsno=46346&category=048
    except AssertionError:
        log.debug("Second rule for table is being used")

        item_headings, item_values = await gather(
            table_body[1].query_selector_all("tr > th"),
            table_body[1].query_selector_all("tr > td"),
        )

        try:
            assert len(item_headings) == len(
                item_values
            ), f"Not equal {len(item_headings)} vs {len(item_values)}"
        except AssertionError:
            log.warning("Third rule for table is being used")

            item_headings, item_values = await gather(
                table_body[2].query_selector_all("tr > th"),
                table_body[2].query_selector_all("tr > td"),
            )

    table = {
        key.strip(): value.strip()
        for (k, v) in zip(item_headings, item_values)
        if (key := await k.text_content()) and (value := await v.text_content())
    }

    product_code = table.get("제품코드", "")
    brand = table.get("브랜드", "")
    manufacturing_country = table.get("제조국", "")

    return Table(product_code, brand, manufacturing_country)


async def extract_options(document: Document):
    option1_query = "#goods_spec > form > table.sub > tbody > tr > td > div > select"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): option1_value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (option1_value := await option1.get_attribute("value"))
        and option1_value not in ["", "*", "**"]
    }

    return list(option1.keys())


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    if "원" in option1:
        regex = compile_regex(r"\s?\(\w+[,]?\w*원\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            price2 = parse_int(additional_price)

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]"
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절"

    return option1, price2, ""


@cache
def image_quries():
    return ", ".join(
        [
            "#contents > center > img",
            "#contents > table > tbody > tr > td > img",
            "#contents > img",
            "#contents > div > center > img",
            "#contents > center > div > img" "#contents > p > img",
            "#contents img",
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
    document_or_page: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = [
        src
        for image in await document_or_page.query_selector_all(query)
        if (src := await image.get_attribute("src")) and is_valid_urls(src)
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
    query = image_quries()
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
                if is_valid_urls(src):
                    urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


@cache
def is_valid_urls(src: str):
    return all(
        url not in src
        for url in {
            "http://iama2072.filelink.cafe24.com/topmain.jpg",
            "http://iama2605.filelink.cafe24.com/b1.jpg",
            "http://iama2072.filelink.cafe24.com/main.jpg",
        }
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
