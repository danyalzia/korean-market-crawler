# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

import backoff

from playwright.async_api import async_playwright

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
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.safetec import config
from market_crawler.safetec.data import SafetecCrawlData
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def next_page_url(*, current_url: str, next_page_no: int) -> str:
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
        login_button_query='fieldset > a > img[alt="로그인"]',
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
            return None

        # ? Don't exceed 10 if custom url is being crawled otherwise the website becomes very flaky
        config.MAX_PRODUCTS_CHUNK_SIZE = min(config.MAX_PRODUCTS_CHUNK_SIZE, 10)
        await crawl_urls(
            list(dict.fromkeys(settings.URLS)),
            browser,
            settings,
            columns,
        )


async def crawl_urls(
    urls: list[str], browser: PlaywrightBrowser, settings: Settings, columns: list[str]
):
    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )

    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(idx, browser, urls[idx], filename, settings, columns)
            for idx in chunk
        )

        await asyncio.gather(*tasks)


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    product_url: str,
    filename: str,
    settings: Settings,
    columns: list[str],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    productid = get_productid(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name="CUSTOM",
            date=settings.DATE,
        )
    ):
        return None

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        sold_out_text,
        product_name,
        price2,
        delivery_fee,
        quantity,
        thumbnail_image_url,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            match split_options_text(option, price2):
                case Ok(data):
                    option1, _price2, option2, option3 = data
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = SafetecCrawlData(
                product_url=product_url,
                product_name=product_name,
                sold_out_text=sold_out_text,
                thumbnail_image_url=thumbnail_image_url,
                detailed_images_html_source=detailed_images_html_source,
                delivery_fee=delivery_fee,
                price2=_price2,
                quantity=quantity,
                option1=option1,
                option2=option2,
                option3=str(option3),
            )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    crawl_data = SafetecCrawlData(
        product_url=product_url,
        product_name=product_name,
        sold_out_text=sold_out_text,
        thumbnail_image_url=thumbnail_image_url,
        detailed_images_html_source=detailed_images_html_source,
        delivery_fee=delivery_fee,
        price2=price2,
        quantity=quantity,
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

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )

    return None


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

    category_page_url = next_page_url(
        current_url=category_url, next_page_no=category_state.pageno
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

    log.action.visit_category(category_name, category_page_url)

    page_navigations = await document.query_selector_all(
        "#contents > div.xans-element-.xans-product.xans-product-normalpaging.ec-base-paginate > ol > li > a"
    )
    total_pages = len(page_navigations) if page_navigations else 1
    log.detail.total_pages(total_pages)

    while True:
        if category_state.pageno > total_pages:
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        category_page_url = next_page_url(
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

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        sold_out_text,
        product_name,
        price2,
        delivery_fee,
        quantity,
        thumbnail_image_url,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            match split_options_text(option, price2):
                case Ok(data):
                    option1, _price2, option2, option3 = data
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = SafetecCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                sold_out_text=sold_out_text,
                thumbnail_image_url=thumbnail_image_url,
                detailed_images_html_source=detailed_images_html_source,
                delivery_fee=delivery_fee,
                price2=_price2,
                quantity=quantity,
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

    crawl_data = SafetecCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        sold_out_text=sold_out_text,
        thumbnail_image_url=thumbnail_image_url,
        detailed_images_html_source=detailed_images_html_source,
        delivery_fee=delivery_fee,
        price2=price2,
        quantity=quantity,
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
    product_name: str
    price2: int
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
        extract_soldout_text(document),
        extract_product_name(document),
        extract_table(document),
        extract_thumbnail_image(document, product_url),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
    ) = await asyncio.gather(*tasks)

    sold_out_text = R1

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(Table(0, _, _)):
            raise error.Price2NotFound("Price 2 is not found", url=product_url)
        case Ok(Table(price2, 0 as delivery_fee, quantity)):
            log.warning(f"Delivery fee is not found: <blue>{product_url}</>")
        case Ok(Table(_, _, "")):
            raise error.QuantityNotFound("Quantity is not found", url=product_url)
        case Ok(Table(price2, delivery_fee, quantity)):
            pass
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R4:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            thumbnail_image_url = ""

    match R5:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    match await extract_options(page):
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    return (
        sold_out_text,
        product_name,
        price2,
        delivery_fee,
        quantity,
        thumbnail_image_url,
        detailed_images_html_source,
        options,
    )


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\/(\w+)\/category")
    if not (match := regex.search(url)):
        return Err(f"Regex Not Matched: {url}")

    return Ok(str(match.group(1)))


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


async def extract_soldout_text(document: Document):
    query = "img.icon_img"
    if not (icons := await document.query_selector_all(query)):
        return ""

    for icon in icons:
        if (alt := await icon.get_attribute("alt")) and "품절" in alt:
            return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    # ? XPATH is not suppported for selectolax
    query = "//tr[contains(.//span, '상품명')]/td/span"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.keyImg > div > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


def extract_price_fromstring(string: str):
    regex = compile_regex(r"[|]?\s+?(\d+[,]?\d+)원")
    if match := regex.search(string):
        return parse_int(match.group(1))

    return None


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
            option3 = f"-{additional_price}"

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_load_state("networkidle")

    options: list[str] = []

    option1_query = 'select[id="product_option_id1"]'
    option2_query = 'select[id="product_option_id2"]'

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

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

        if option2_elements := await page.query_selector_all(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        ):
            option2 = {
                "".join(text.split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                and value not in ["", "*", "**"]
            }

            options.extend(f"{option1_text},{option2_text}" for option2_text in option2)
        else:
            options.append(option1_text)

    return options


class Table(NamedTuple):
    price2: int
    delivery_fee: int
    quantity: str


@returns_future(ValueError, error.IncorrectData)
async def extract_table(document: Document):
    query = "#span_product_price_text"
    if not (price2 := await document.text_content(query)):
        return Table(0, 0, "")

    price2 = parse_int(price2)

    query = "#contents div.detailArea > div.infoArea div[class='differentialShipping layerTheme'] > div.content > ul > li:nth-child(1) > span"
    if not (delivery_fee_text := await document.text_content(query)):
        delivery_fee = 0
    else:
        try:
            assert "원" in delivery_fee_text
        except AssertionError as err:
            raise error.IncorrectData(
                f"Delivery fee text ({delivery_fee_text}) is invalid"
            ) from err

        delivery_fee = parse_int(delivery_fee_text)

    query = "div.detailArea > div.infoArea > div.guideArea > p"
    if not (quantity := await document.text_content(query)):
        return Table(price2, delivery_fee, "")

    return Table(price2, delivery_fee, quantity)


@cache
def image_quries():
    return ", ".join(
        [
            "#prdDetail > div > p > img",
            "#prdDetail > div > p img",
            "#prdDetail > div p img",
            "#prdDetail > div > img",
            "#prdDetail > div img",
        ]
    )


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    # ? This may help getting rid of base64 encoded images
    await page.mouse.wheel(delta_x=0, delta_y=1000)
    await page.mouse.wheel(delta_x=0, delta_y=-1000)

    with suppress(error.PlaywrightTimeoutError):
        await page.click("#prdInfo > ul > li:nth-child(1) > a")
        await page.wait_for_load_state("domcontentloaded")
        await page.click("#prdDetail > ul[class='menu'] > li > a")
        await page.wait_for_load_state("domcontentloaded")

    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    query = image_quries()
    if elements := await page.query_selector_all(query):
        with suppress(error.PlaywrightTimeoutError):
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
    await page.mouse.wheel(delta_x=0, delta_y=1000)
    await page.mouse.wheel(delta_x=0, delta_y=-1000)

    with suppress(error.PlaywrightTimeoutError):
        await page.click("#prdInfo > ul > li:nth-child(1) > a")
        await page.wait_for_load_state("domcontentloaded")
        await page.click("#prdDetail > ul[class='menu'] > li > a")
        await page.wait_for_load_state("domcontentloaded")

    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()
