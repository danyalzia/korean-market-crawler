# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import time

from contextlib import suppress
from functools import cache
from typing import Any
from urllib.parse import urljoin

from aiofile import AIOFile
from playwright.async_api import async_playwright, expect

from dunia.aio import gather
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
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.kiganism import config
from market_crawler.kiganism.data import KiganismCrawlData
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


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "https://www.rodall.co.kr/")

    categories = await page.query_selector_all(".tcat_area > dl > dd:nth-child(-n+7)")

    for category in categories:
        if not (el := await category.query_selector("a")):
            continue

        if not (category_text := await el.text_content()):
            continue

        if not (category_page_url := await el.get_attribute("href")):
            continue

        category_page_url = urljoin(page.url, category_page_url)

        full_subcategories.append(Category(category_text, category_page_url))

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


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

    await page.close()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

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

        await page.close()

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


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsNo=(\w+)")
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
    query = ".item-display.type-gallery ul > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("div.txt > div > img"):
        alt = str(await icon.get_attribute("src"))
        if "soldout" in alt:
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
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        product_name,
        message1,
        price2,
        delivery_fee,
        model_name,
        brand,
        manufacturer,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            match split_options_text(option, price2):
                case Ok((option1, _price2, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = KiganismCrawlData(
                category=category_state.name,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                product_name=product_name,
                message1=message1,
                price2=_price2,
                delivery_fee=delivery_fee,
                model_name=model_name,
                brand=brand,
                manufacturer=manufacturer,
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

    crawl_data = KiganismCrawlData(
        category=category_state.name,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        message1=message1,
        price2=price2,
        delivery_fee=delivery_fee,
        model_name=model_name,
        brand=brand,
        manufacturer=manufacturer,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
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


async def extract_data(
    page: PlaywrightPage,
    document: Any,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_image(document, product_url),
        extract_product_name(document),
        extract_message1(document),
        extract_table(document),
        extract_html(page, product_url, html_top, html_bottom),
    )

    (R1, R2, R3, R4, R5) = await gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(message1):
            pass
        case Err(err):
            raise error.Message1NotFound(err, url=product_url)

    match R4:
        case Ok(table):
            price2, delivery_fee, model_name, brand, manufacturer = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    detailed_images_html_source = R5

    match await extract_options(page):
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    return (
        thumbnail_image_url,
        product_name,
        message1,
        price2,
        delivery_fee,
        model_name,
        brand,
        manufacturer,
        detailed_images_html_source,
        options,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = ".tit > h2"
    return (
        product_name.strip()
        if (product_name := await document.text_content(query))
        else ""
    )


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document):
    query = ".tit p"
    return message1.strip() if (message1 := await document.text_content(query)) else ""


@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_load_state("networkidle")

    options: list[str] = []

    # Checking option1 and option2 is present or not, if just option1 is present, no need to click on options then.
    chk = await page.query_selector_all(".chosen-single span")

    # if just option1 present
    if len(chk) == 1:
        option1_query = "select[name='optionSnoInput']"

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

        for option1_text, _ in option1.items():
            options.append(option1_text)

    # if both option1 and 2 present
    if len(chk) == 2:
        # Open drop down
        option1_query = "div:nth-of-type(5) .chosen-single"
        if locator := await page.query_selector(option1_query):
            await locator.scroll_into_view_if_needed()
            await locator.focus()
            await expect(page.locator(option1_query)).to_be_visible()
            await locator.click()

        #
        with suppress(error.PlaywrightTimeoutError):
            await page.wait_for_load_state("load")
        time.sleep(3)

        # loop through option1 options
        option1_elements_query = "div:nth-of-type(5) div .chosen-container .chosen-drop ul > li:nth-child(n+2)"
        for counter in range(
            len(await page.query_selector_all(option1_elements_query))
        ):
            option1_trick = (await page.query_selector_all(option1_elements_query))[
                counter
            ]

            # click on options one by one
            await option1_trick.scroll_into_view_if_needed()
            await option1_trick.focus()
            await option1_trick.click()

            # get the text of option1
            option1_text = await option1_trick.text_content()

            #
            with suppress(error.PlaywrightTimeoutError):
                await page.wait_for_load_state("networkidle")
            time.sleep(3)

            # get all the text of option2
            option2_query = "select[name='optionNo_1']"
            option2_elements = await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )
            option2 = {
                "".join(text.split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                and value not in ["", "*", "**"]
            }

            options.extend(f"{option1_text},{option2_text}" for option2_text in option2)

            # Open drop down again so that click on next option
            if locator := await page.query_selector(option1_query):
                await locator.scroll_into_view_if_needed()
                await locator.focus()
                await locator.click()

            #
            with suppress(error.PlaywrightTimeoutError):
                await page.wait_for_load_state("load")
            time.sleep(3)

    return options


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document):
    delivery_fee = model_name = brand = manufacturer = ""
    price2 = 0
    ths: list[Element] = []
    tds: list[Element] = []

    query = ".item > ul"
    if not (table_tbodys := await document.query_selector_all(query)):
        raise error.QueryNotFound("Table not found", query=query)

    for table_tbody in table_tbodys:
        ths.extend(
            [el for el in await table_tbody.query_selector_all("li > strong") if el]
        )
        tds.extend(
            [el for el in await table_tbody.query_selector_all("li > div") if el]
        )

    for th, td in zip(ths, tds):
        if not (heading := await th.text_content()):
            continue

        if not (text := await td.text_content()):
            continue

        # price2
        if "판매가" in heading:
            price2_str = text
            try:
                price2 = parse_int(price2_str)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price2 text {price2_str} to number"
                ) from err

        # delivery_fee
        if "배송비" in heading:
            if (el := await td.query_selector("span")) and (
                delivery_fee_text := await el.text_content()
            ):
                delivery_fee = delivery_fee_text.strip()

        # model_name
        if "상품번호" in heading:
            model_name = text.strip()

        # brand
        if "브랜드" in heading:
            brand = text.strip()

        # manufacturer
        if "제조사" in heading:
            manufacturer = text.strip()

    return price2, delivery_fee, model_name, brand, manufacturer


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "#mainImage img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0
    option2 = ""

    if ":+" in option1:
        regex = compile_regex(r"\:\s?\+\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if ":-" in option1:
        regex = compile_regex(r"\:\s?\-\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    if "개" in option1:
        regex = compile_regex(r"\:\s?\w+[,]?\w*개")
        for stock_quan in regex.findall(option1):
            option1 = regex.sub("", option1)
            option2 = stock_quan.replace(":", "")

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "품절", option3 or ""

    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, option2, option3 or ""


@cache
def image_quries():
    return ".txt-manual img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(page: PlaywrightPage, product_url: str) -> list[str]:
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
            max_tries=50,
        ):
            case Ok(src):
                urls.append(src)
            case Err(MaxTriesReached(_)):
                pass

    return list(map(lambda url: urljoin(product_url, url), urls))


async def extract_html(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_images(page, product_url):
        case Ok(images):
            images = list(dict.fromkeys(images))
            return build_detailed_images_html(
                images,
                html_top,
                html_bottom,
            )
        case Err(err):
            log.debug(f"{err} -> {product_url}")
            return "NOT PRESENT"


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()
