# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

from aiofile import AIOFile
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
from market_crawler.dysports import config
from market_crawler.dysports.data import DysportsCrawlData
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
from robustify.result import Err, Ok, Result, returns_future


@cache
def next_page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}?page={next_page_no}"


async def find_subcategories(browser: PlaywrightBrowser, categories: list[Category]):
    full_subcategories: list[Category] = []

    for category in categories:
        category_text = category.name
        category_url = category.url

        page = await browser.new_page()
        await visit_link(page, category_url, wait_until="load")

        subcategories = await page.query_selector_all(
            "body > div.wrapper > section:nth-child(5) > div > div.category-contents > ul > li > a"
        )

        for subcategory in subcategories:
            if (subcategory_text := await subcategory.text_content()) and (
                url := await subcategory.get_attribute("href")
            ):
                full_text = f"{category_text}>{subcategory_text.strip()}"
                full_subcategories.append(Category(full_text, url))

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
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='mem_userid']",
        password_query="input[name='mem_password']",
        login_button_query="#flogin > div button",
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()

        if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
            subcategories = await get_categories(
                sitename=config.SITENAME, filename="subcategories.txt"
            )
        else:
            categories = await get_categories(
                sitename=config.SITENAME, filename="categories.txt"
            )
            subcategories = await find_subcategories(browser, categories)

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

    category_page_url = next_page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    while True:
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


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"item\/(.*)")
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
    query = "body > div.wrapper > section > div > div.cmall-list > div.row > div.items > div"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (el := await product.query_selector("a")) or not (
        product_link := await el.get_attribute("href")
    ):
        raise error.QueryNotFound("Product link not found", "a")

    if not product_link.startswith("http"):
        product_link = urljoin(category_page_url, product_link)

    return product_link


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("a > div.label-box > span"):
        text = str(await icon.text_content())
        if "품절" in text:
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

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    tasks = (
        extract_product_name(document),
        extract_model_name(document),
        extract_thumbnail_image(document, product_url),
        extract_table(document),
        extract_images(page, product_url, html_top, html_bottom),
    )

    R1, R2, R3, R4, R5 = await asyncio.gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_image_url):
            pass
        case Err(_):
            thumbnail_image_url = ""

    match R4:
        case Ok(table):
            (
                brand,
                model_name2,
                option1,
                message1,
                message2,
                manufacturing_country,
                quantity,
                price2,
                price3,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R5:
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

    await page.close()

    crawl_data = DysportsCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        model_name=model_name,
        thumbnail_image_url=thumbnail_image_url,
        brand=brand,
        model_name2=model_name2,
        option1=option1,
        message1=message1,
        message2=message2,
        manufacturing_country=manufacturing_country,
        quantity=quantity,
        price2=price2,
        price3=price3,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
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


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div.product-title"
    if not (product_name := await document.query_selector(query)):
        raise error.QueryNotFound("Product name not found", query)

    if not (el := await product_name.query_selector("div")) or not (
        brand := await el.text_content()
    ):
        raise error.QueryNotFound("Product name (brand part) not found", query)

    if not (el := await product_name.query_selector("strong")) or not (
        name := await el.text_content()
    ):
        raise error.QueryNotFound("Product name (main part) not found", query)

    brand = brand.strip()
    name = name.strip()

    product_name = " ".join([brand, name])

    return product_name


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "#copy_text1"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "body > div.wrapper > section.default.market-section > div > div > div > div.product-box > div > div:nth-child(1) > div > div.slider-for.slick-initialized.slick-slider > div > div > div > div > div > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    if not thumbnail_image.startswith("http"):
        thumbnail_image = urljoin(product_url, thumbnail_image)

    return thumbnail_image


class Table(NamedTuple):
    brand: str
    model_name2: str
    option1: str
    message1: str
    message2: str
    manufacturing_country: str
    quantity: str
    price2: int
    price3: str


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document):
    brand = model_name2 = option1 = message1 = message2 = manufacturing_country = (
        quantity
    ) = ""
    price2 = 0

    query = "body > div.wrapper > section.default.market-section > div > div > div > div.product-box > div > div:nth-child(2) > div > div.product-no-box > div > div"
    if not (table_items := await document.query_selector_all(query)):
        raise error.QueryNotFound("Table not found", query=query)

    for item in table_items:
        if not (el := await item.query_selector("div:nth-child(1)")) or not (
            heading := await el.text_content()
        ):
            continue

        if not (el := await item.query_selector("div:nth-child(2)")) or not (
            text := await el.text_content()
        ):
            continue

        if "브랜드" in heading:
            brand = text

        if "모델명" in heading:
            model_name2 = text

        if "색상" in heading:
            option1 = text

        if "재질" in heading:
            message1 = text

        if "사이즈/중량" in heading:
            message2 = text

        if "원산지" in heading:
            manufacturing_country = text

        if "배송가능수량(택배1건기준)" in heading:
            quantity = text

        if "판매가" in heading:
            price2_str = text
            try:
                price2 = parse_int(price2_str)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price2 text {price2_str} to number"
                ) from err

    if not (price3 := await document.text_content("div.product-box div.product-desc")):
        price3 = ""

    return Table(
        brand,
        model_name2,
        option1,
        message1,
        message2,
        manufacturing_country,
        quantity,
        price2,
        price3,
    )


@cache
def image_quries():
    return "#product_content > div.product-detail img"


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
                if not src.startswith("http"):
                    src = urljoin(product_url, src)
                urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    return build_detailed_images_html(
        urls,
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()
