# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from contextlib import suppress
from functools import cache
from typing import NamedTuple, cast, overload
from urllib.parse import urljoin

import pandas as pd

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
from market_crawler.landas import config
from market_crawler.landas.data import LandasCrawlData
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
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


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)
    await page.wait_for_selector(
        "#headerInner > div.top_right_navi > a:nth-child(3) > img"
    )


async def visit_subcategory(page: PlaywrightPage):
    subcategories = await page.query_selector_all("#leftNav > div > ul > li")
    if total_subcategories := len(subcategories):
        for idx in range(total_subcategories):
            async with page.expect_navigation():
                await (await page.query_selector_all("#leftNav > div > ul > li"))[
                    idx
                ].click()

            yield cast(
                str,
                await (await page.query_selector_all("#leftNav > div > ul > li"))[
                    idx
                ].text_content(),
            ).strip(), page.url
    else:
        yield "", page.url


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='m_id']",
        password_query="input[name='password']",
        login_button_query="a.btn_login_t:text('로그인')",
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

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            categories = await get_categories(sitename=config.SITENAME)

            categories: list[Category] = []
            if os.path.exists("subcategories.txt"):
                categories = await get_categories(
                    sitename=config.SITENAME, filename="subcategories.txt"
                )
            else:
                page = await browser.new_page()
                for category in categories:
                    await visit_link(page, category.url)
                    async for subcategory, url in visit_subcategory(page):
                        if subcategory:
                            categories.append(
                                Category(f"{category.name}_{subcategory}", url)
                            )
                        else:
                            categories.append(Category(category.name, url))

                await page.close()

                async with AIOFile(
                    "subcategories.txt", "w", encoding="utf-8-sig"
                ) as afp:
                    await afp.write(
                        "{}".format(
                            "\n".join(f"{cat.name}, {cat.url}" for cat in categories)
                        )
                    )

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

        # ? Don't exceed 15 if custom url is being crawled otherwise the website becomes very flaky
        config.MAX_PRODUCTS_CHUNK_SIZE = min(config.MAX_PRODUCTS_CHUNK_SIZE, 15)
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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    tasks = (
        extract_product_name(document),
        extract_thumbnail_images(document, product_url),
        extract_table(page),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (
        R1,  # type: ignore
        R2,  # type: ignore
        table,
        R4,
    ) = await gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
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

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            # detailed_images_html_source = "NOT PRESENT"
            raise error.ProductDetailImageNotFound(err, product_url)
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    await page.close()

    product_name = product_name
    manufacturing_country = table.manufacturing_country
    manufacturer = table.manufacturer
    brand = table.brand

    if len(table.option1_soldout) > 1:
        for option1, soldout in table.option1_soldout.items():
            sold_out_text = "품절" if str(soldout) == "0" else ""
            crawl_data = LandasCrawlData(
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturing_country=manufacturing_country,
                manufacturer=manufacturer,
                brand=brand,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1,
            )
            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(table.option1_soldout),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    try:
        option1, soldout = (
            list(table.option1_soldout.keys())[0],
            list(table.option1_soldout.values())[0],
        )
    except IndexError:
        option1 = ""
        sold_out_text = ""
    else:
        sold_out_text = "품절" if str(soldout) == "0" else ""
    crawl_data = LandasCrawlData(
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturing_country=manufacturing_country,
        manufacturer=manufacturer,
        brand=brand,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1=option1,
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

    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    category_html = CategoryHTML(
        name=category.name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
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
    if category_state.name.count("_") == 1:
        category_name = category_state.name.split("_")[-1]
    else:
        category_name = category_state.name

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

    tasks = (extract_price3(product), extract_price2(product))
    (R1, R2) = await gather(*tasks)

    match R1:
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    match R2:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    tasks = (
        extract_product_name(document),
        extract_thumbnail_images(document, product_url),
        extract_table(page),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (
        R1,  # type: ignore
        R2,  # type: ignore
        table,
        R4,
    ) = await gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
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

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            # detailed_images_html_source = "NOT PRESENT"
            raise error.ProductDetailImageNotFound(err, product_url)
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    await page.close()

    product_name = product_name
    manufacturing_country = table.manufacturing_country
    manufacturer = table.manufacturer
    brand = table.brand
    price3 = price3
    price2 = price2

    if len(table.option1_soldout) > 1:
        for option1, soldout in table.option1_soldout.items():
            sold_out_text = "품절" if str(soldout) == "0" else ""
            crawl_data = LandasCrawlData(
                category=category_name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturing_country=manufacturing_country,
                manufacturer=manufacturer,
                brand=brand,
                price3=price3,
                price2=price2,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
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
            len(table.option1_soldout),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    try:
        option1, soldout = (
            list(table.option1_soldout.keys())[0],
            list(table.option1_soldout.values())[0],
        )
    except IndexError:
        option1 = ""
        sold_out_text = ""
    else:
        sold_out_text = "품절" if str(soldout) == "0" else ""
    crawl_data = LandasCrawlData(
        category=category_name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturing_country=manufacturing_country,
        manufacturer=manufacturer,
        brand=brand,
        price3=price3,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1=option1,
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


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsno=(\d+)\&")
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


@overload
async def get_products(
    document_or_page: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    document_or_page: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(document_or_page: Document | PlaywrightPage):
    query = "#subSection > div.tabCon.mcateTabCon > div"
    return (
        Ok(products)
        if (products := await document_or_page.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("div.txt > a")):
        raise error.QueryNotFound("Product link not found", "div.txt > a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(product: Element):
    query = "div.txt > a > dl > dd:nth-child(4)"
    if not (el := await product.query_selector(query)) or not (
        price3_text := await el.text_content()
    ):
        raise error.QueryNotFound("Price3 not found", query=query)

    if (price3 := extract_price_fromstring(price3_text)) is not None:
        return price3

    raise ValueError(f"Could not extract price3 from text ({price3_text})")


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(product: Element):
    query = "div.txt > a > dl > dd:nth-child(5)"
    if not (el := await product.query_selector(query)) or not (
        price2_text := await el.text_content()
    ):
        raise error.QueryNotFound("Price2 not found", query=query)

    if (price2 := extract_price_fromstring(price2_text)) is not None:
        return price2

    raise ValueError(f"Could not extract price2 from text ({price2_text})")


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#prdt_start > div > p"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#detailSlider > li:nth-child(1) > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "#gallery-thumbs > li:not(li[aria-hidden='true']) > div.thumb > a > img"
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


def extract_price_fromstring(string: str):
    regex = compile_regex(r"[|]?\s+?(\d*[,]?\d*)원")
    return parse_int(match.group(1)) if (match := regex.search(string)) else None


class Table(NamedTuple):
    manufacturer: str
    manufacturing_country: str
    brand: str
    option1_soldout: dict[str, str]


async def extract_table(page: PlaywrightPage):
    manufacturing_country = manufacturer = brand = ""

    if not (
        table_tbody := await page.query_selector(
            "#prdt_start > div > ul",
        )
    ):
        raise error.TableNotFound("Table not found")

    li_elements = await table_tbody.query_selector_all("li")
    dt_elements = await table_tbody.query_selector_all("dl > dt")
    dd_elements = await table_tbody.query_selector_all("dl > dd")

    assert li_elements
    assert dt_elements
    assert dd_elements

    total_dt_elements = len(dt_elements)
    total_dd_elements = len(dd_elements)

    if (total_dd_elements - 1) == total_dt_elements:
        dd_elements = dd_elements[:-1]
    elif (total_dd_elements - 2) == total_dt_elements:
        dd_elements = dd_elements[:-2]
    elif (total_dd_elements - 3) == total_dt_elements:
        dd_elements = dd_elements[:-3]
    elif (total_dd_elements - 4) == total_dt_elements:
        dd_elements = dd_elements[:-4]
    elif (total_dd_elements - 5) == total_dt_elements:
        dd_elements = dd_elements[:-5]
    elif (total_dd_elements - 6) == total_dt_elements:
        dd_elements = dd_elements[:-6]
    elif (total_dd_elements - 7) == total_dt_elements:
        dd_elements = dd_elements[:-7]
    elif (total_dd_elements - 8) == total_dt_elements:
        dd_elements = dd_elements[:-8]
    elif (total_dd_elements - 9) == total_dt_elements:
        dd_elements = dd_elements[:-9]
    elif (total_dd_elements - 10) == total_dt_elements:
        dd_elements = dd_elements[:-10]
    elif (total_dd_elements - 11) == total_dt_elements:
        dd_elements = dd_elements[:-11]
    elif (total_dd_elements - 12) == total_dt_elements:
        dd_elements = dd_elements[:-12]
    elif (total_dd_elements - 13) == total_dt_elements:
        dd_elements = dd_elements[:-13]
    elif (total_dd_elements - 14) == total_dt_elements:
        dd_elements = dd_elements[:-14]
    elif (total_dd_elements - 15) == total_dt_elements:
        dd_elements = dd_elements[:-15]
    elif total_dd_elements != total_dt_elements:
        print(f"{total_dt_elements = }")
        print(f"{total_dd_elements = }")

    for dt, dd in zip(dt_elements, dd_elements, strict=True):
        dt_str = "".join(cast(str, await dt.text_content()).split())
        dd_str = cast(str, await dd.text_content())

        if "제조사" in dt_str:
            manufacturer = dd_str

        if "원산지" in dt_str:
            manufacturing_country = dd_str

        if "브랜드" in dt_str:
            brand = dd_str

    await page.wait_for_load_state("networkidle", timeout=300000)
    await page.mouse.wheel(delta_x=0, delta_y=1000)

    query = "#iSub > div:nth-child(1) > form > div > div:nth-child(2) > div > div.infos > table"
    if not (table_child_element := await page.query_selector(query)):
        raise error.QueryNotFound(f"Table not found: {page.url}", query=query)

    if not (table := await table_child_element.query_selector("xpath=..")):
        raise error.TableNotFound(f"Table not found: {page.url}")

    html = (await table.inner_html()).strip()

    try:
        df = pd.read_html(html)[0]
    except Exception as ex:
        print(ex)
        raise

    try:
        dictionary = (
            df.droplevel(0, axis=1)
            .drop(["SIZE", "총수량"], axis=1)
            .dropna()
            .astype(int)
            .to_dict()
        )
    except Exception as ex:
        print(ex)
        print(df)
        raise

    option1_soldout: dict[str, str] = {k: v[0] for (k, v) in dictionary.items()}  # type: ignore

    return Table(manufacturer, manufacturing_country, brand, option1_soldout)


@cache
def image_quries():
    return "#iSub > div > div.detailViewTabInfos img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

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
