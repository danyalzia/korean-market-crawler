# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

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
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.campingmoon import config
from market_crawler.campingmoon.data import CampingmoonCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    await page.click(login_button_query)
    await page.wait_for_selector('a[href="/exec/front/Member/logout/"]')


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def run(settings: Settings):
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

    log.action.visit_category(category.name, category_page_url)

    if not (
        total_products_text := await document.text_content(
            "#contents > div.xans-element-.xans-product.xans-product-normalpackage > div.xans-element-.xans-product.xans-product-normalmenu > div.function > p > strong"
        )
    ):
        raise error.TotalProductsTextNotFound(
            "Total products text is not found on the page", url=category_page_url
        )

    log.detail.total_products_in_category(category.name, parse_int(total_products_text))

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


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "#contents > div.xans-element-.xans-product.xans-product-normalpackage > div.xans-element-.xans-product.xans-product-listnormal > ul [id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


async def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"product_no=(\d+\w+)&")
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


async def extract_soldout_text(document: Element):
    if icon := await document.query_selector(
        'div[class="icon"] > img[class="icon_img"]'
    ):
        if (alt := await icon.get_attribute("alt")) and "품절" in alt:
            return "품절"

    return ""


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

    sold_out_text = await extract_soldout_text(product)

    (
        thumbnail_image_url,
        product_name,
        price3,
        manufacturing_country,
        model_name,
        options,
        detailed_images_html_source,
    ) = await extract_data(browser, document, product_url, html_top, html_bottom)

    if len(options) > 1:
        for option1 in options:
            match split_options_text(option1, price3):
                case Ok((option1_, price3_, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = CampingmoonCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                price3=price3_,
                manufacturing_country=manufacturing_country,
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

    try:
        option1 = options[0]
    except IndexError:
        option1 = ""
        option2 = ""
        option3 = ""
    else:
        match split_options_text(option1, price3):
            case Ok((option1, price3, option2, option3)):
                pass
            case Err(err):
                raise error.IncorrectData(
                    f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                    url=product_url,
                )

    crawl_data = CampingmoonCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        price3=price3,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1=option1,
        option2=option2,
        option3=str(option3),
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )
    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )
    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    return None


async def extract_data(
    browser: PlaywrightBrowser,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    tasks = (
        extract_thumbnail_image(document, product_url),
        extract_table(document),
        extract_options(page),
    )

    result = await asyncio.gather(*tasks)

    R1, R2, R3, R4 = *result, await extract_images(
        page, product_url, html_top, html_bottom
    )

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(table):
            (product_name, price3, manufacturing_country, model_name) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R3:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            raise error.ProductDetailImageNotFound(err, url=product_url)

    await page.close()

    return (
        thumbnail_image_url,
        product_name,
        price3,
        manufacturing_country,
        model_name,
        options,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div[class=infoArea] > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price(document: Document):
    query = "#span_product_price_text"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Selling name not found", query)

    return parse_int(price3)


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.keyImg > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


class Table(NamedTuple):
    product_name: str
    price3: int
    manufacturing_country: str
    model_name: str


@returns_future(error.QueryNotFound, AssertionError, ValueError)
async def extract_table(document: Document):
    tasks = (extract_product_name(document), extract_price(document))

    _product_name, _price3 = await asyncio.gather(*tasks)

    match _product_name:
        case Ok(product_name):
            pass
        case Err(err):
            raise err

    match _price3:
        case Ok(price3):
            pass
        case Err(err):
            raise err

    manufacturing_country = ""
    model_name = ""

    query = "div[class=infoArea] > div > table > tbody"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table is not present", query=query)

    tr_elements = await table_tbody.query_selector_all("tr")
    th_elements = await table_tbody.query_selector_all("tr > th")
    td_elements = await table_tbody.query_selector_all("tr > td")

    assert tr_elements, "<tr> elements not found"
    assert th_elements, "<th> elements not found"
    assert td_elements, "<td> elements not found"

    for th, td in zip(th_elements, td_elements, strict=True):
        if not (th_str := await th.text_content()):
            continue

        if not (td_str := await td.text_content()):
            continue

        if "원산지" in th_str:
            manufacturing_country = td_str
        if "상품코드" in th_str:
            model_name = td_str

    return Table(product_name, price3, manufacturing_country, model_name)


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
    option1_query = "select[id='product_option_id1']"
    option2_query = "select[id='product_option_id2']"

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join((text).split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    if not (
        await page.query_selector(
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
            "".join(text.split()): value
            for option2 in await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )
            if (text := await option2.text_content())
            and (value := await option2.get_attribute("value"))
            and value not in ["", "*", "**"]
        }

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = "#prdDetail > div img"
    elements = await page.query_selector_all(query)

    urls: list[str] = []
    for el in elements:
        action = lambda: get_src(el)
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


async def get_src(el: PlaywrightElementHandle):
    return attr if (attr := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.mouse.wheel(delta_x=0, delta_y=500)
    await page.mouse.wheel(delta_x=0, delta_y=-500)

    await element.scroll_into_view_if_needed()
    await element.focus()
