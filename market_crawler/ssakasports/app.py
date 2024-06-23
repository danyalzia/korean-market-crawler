# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from functools import cache
from urllib.parse import urljoin

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError, TimeoutException
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
from market_crawler.settings import Settings
from market_crawler.ssakasports import config
from market_crawler.ssakasports.data import SsakasportsCrawlData
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
    await visit_link(page, "https://www.ssakasports.co.kr/index.php")

    categories = await page.query_selector_all(
        "#gnb > ul > li:not(li:nth-child(1)):not(li:nth-last-child(1))"
    )

    for category in categories:
        if not (
            category_text := (
                await (await category.query_selector_all("a"))[0].text_content()
            )
        ):
            continue

        category_text = category_text.strip()

        for c in await category.query_selector_all(
            "div > ul[class=depth2] > li > h3 > a"
        ):
            if (
                await c.get_attribute("href")
                and (t := await c.text_content())
                and not bool(t.strip())
            ):
                if not (
                    url := (
                        await (await category.query_selector_all("a"))[0].get_attribute(
                            "href"
                        )
                    )
                ):
                    continue

                url = urljoin(
                    page.url,
                    url.removeprefix("javascript:location.href='").removesuffix("';"),
                )
                full_subcategories.append(Category(f"{category_text}", url))
                break

        subcategories = [
            c
            for c in await category.query_selector_all(
                "div > ul[class=depth2] > li > h3 > a"
            )
            if await c.get_attribute("href")
            and (t := await c.text_content())
            and bool(t.strip())
            and t != ""
        ]

        for subcategory in subcategories:
            if not (subcategory_text := (await subcategory.text_content())):
                continue

            subcategory_text = subcategory_text.strip()
            print(f"category_text => {subcategory_text = }")
            url = urljoin(page.url, await subcategory.get_attribute("href"))
            full_subcategories.append(
                Category(f"{category_text}>{subcategory_text}", url)
            )

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#id01",
        password_query="#pwd01",
        login_button_query="#loginBtn",
    )


async def run(settings: Settings):
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
            if os.path.exists("subcategories.txt"):
                subcategories = await get_categories(
                    sitename=config.SITENAME, filename="subcategories.txt"
                )
            else:
                subcategories = await find_subcategories(browser)

            log.detail.total_categories(len(subcategories))

            crawler = ConcurrentCrawler(
                categories=subcategories,
                start_category=config.START_CATEGORY,
                end_category=config.END_CATEGORY,
                chunk_size=config.CATEGORIES_CHUNK_SIZE,
                crawl=crawl,
            )
            await crawl_categories(crawler, browser, settings, columns)
            return None

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
    try:
        await visit_link(page, product_url, wait_until="networkidle")
    except TimeoutException as err:
        # ? Product URL may be invalid (i.e., soldout or no longer available)
        if "Navigation failed because page crashed!" in str(err):
            return None

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        category_text,
        product_name,
        model_name,
        price2,
        price3,
        manufacturer,
        manufacturing_country,
        message2,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        if isinstance(options, str):
            crawl_data = SsakasportsCrawlData(
                category=category_text,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                model_name=model_name,
                price2=price2,
                price3=price3,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                message2=message2,
                detailed_images_html_source=detailed_images_html_source,
                option1="",
                option2="",
                option4="",
                sold_out_text=options,
            )

            product_state.done = True
            if config.USE_PRODUCT_SAVE_STATES:
                await product_state.save()

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

            log.action.product_custom_url_crawled_with_options(
                idx,
                product_url,
                len(options),
            )

        else:
            for option1, option4, option2 in zip(*options):
                crawl_data = SsakasportsCrawlData(
                    category=category_text,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    product_name=product_name,
                    model_name=model_name,
                    price2=price2,
                    price3=price3,
                    manufacturer=manufacturer,
                    manufacturing_country=manufacturing_country,
                    message2=message2,
                    detailed_images_html_source=detailed_images_html_source,
                    option1=option1,
                    option2=option2,
                    option4=option4,
                    sold_out_text="",
                )

                await save_series_csv(
                    to_series(crawl_data, settings.COLUMN_MAPPING),
                    columns,
                    filename,
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

    crawl_data = SsakasportsCrawlData(
        category=category_text,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        model_name=model_name,
        price2=price2,
        price3=price3,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        message2=message2,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option4="",
        sold_out_text="",
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
    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        page = await browser.new_page()
        await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()
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
    regex = compile_regex(r"id=(\d*)")
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
    query = "div[id='sProlistArea'] li[class='proitem']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_message1(product: Element, category_url: str):
    if icon := await product.query_selector("img[class='img_size70']"):
        if src := await icon.get_attribute("src"):
            return urljoin(category_url, src)

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

    message1 = await extract_message1(product, category_page_url)

    page = await browser.new_page()
    try:
        await visit_link(page, product_url, wait_until="networkidle")
    except TimeoutException as err:
        # ? Product URL may be invalid (i.e., soldout or no longer available)
        if "Navigation failed because page crashed!" in str(err):
            return None

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        category_text,
        product_name,
        model_name,
        price2,
        price3,
        manufacturer,
        manufacturing_country,
        message2,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        if isinstance(options, str):
            crawl_data = SsakasportsCrawlData(
                category=category_text,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                model_name=model_name,
                price2=price2,
                price3=price3,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                message2=message2,
                detailed_images_html_source=detailed_images_html_source,
                message1=message1,
                option1="",
                option2="",
                option4="",
                sold_out_text=options,
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

        else:
            for option1, option4, option2 in zip(*options):
                crawl_data = SsakasportsCrawlData(
                    category=category_text,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    product_name=product_name,
                    model_name=model_name,
                    price2=price2,
                    price3=price3,
                    manufacturer=manufacturer,
                    manufacturing_country=manufacturing_country,
                    message2=message2,
                    detailed_images_html_source=detailed_images_html_source,
                    message1=message1,
                    option1=option1,
                    option2=option2,
                    option4=option4,
                    sold_out_text="",
                )

                await save_series_csv(
                    to_series(crawl_data, settings.COLUMN_MAPPING),
                    columns,
                    filename,
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

    crawl_data = SsakasportsCrawlData(
        category=category_text,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        model_name=model_name,
        price2=price2,
        price3=price3,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        message2=message2,
        detailed_images_html_source=detailed_images_html_source,
        message1=message1,
        option1="",
        option2="",
        option4="",
        sold_out_text="",
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
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_images(document, product_url),
        extract_category_text(document),
        extract_product_name(document),
        extract_table(document),
        extract_options(document),
        extract_html(page, product_url, html_top, html_bottom),
    )

    (R1, R2, R3, R4, R5, R6) = await gather(*tasks)

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
            await visit_link(page, product_url, wait_until="networkidle")
            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            tasks = (
                extract_thumbnail_images(document2, product_url),
                extract_category_text(document2),
                extract_product_name(document2),
                extract_table(document2),
                extract_options(document2),
                extract_html(page, product_url, html_top, html_bottom),
            )
            (R1, R2, R3, R4, R5, R6) = await gather(*tasks)  # type: ignore
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
        case Ok(category_text):
            pass
        case Err(err):
            await visit_link(page, product_url, wait_until="networkidle")
            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            tasks = (
                extract_thumbnail_images(document2, product_url),
                extract_category_text(document2),
                extract_product_name(document2),
                extract_table(document2),
                extract_options(document2),
                extract_html(page, product_url, html_top, html_bottom),
            )
            (R1, R2, R3, R4, R5, R6) = await gather(*tasks)  # type: ignore
            match R2:
                case Ok(category_text):
                    pass
                case Err(err):
                    raise error.CategoryTextNotFound(err, url=product_url)

    match R3:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R4:
        case Ok(table):
            (
                model_name,
                price2,
                price3,
                manufacturer,
                manufacturing_country,
                message2,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R5:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    detailed_images_html_source = R6

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        category_text,
        product_name,
        model_name,
        price2,
        price3,
        manufacturer,
        manufacturing_country,
        message2,
        options,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#orderform > div.sproTA > div.sproList > div.sprodeTit"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(AssertionError)
async def extract_options(document: Document):
    if sold_out_text := await document.text_content(
        "div[class='option_box'] table[class='op_table'] font[color='red'] > b"
    ):
        return sold_out_text

    option1_query = "#orderform > div.option_box > table > tbody > tr:nth-child(1) td"
    option4_query = "#orderform > div.option_box > table > tbody > tr:nth-child(2) td"
    option2_query = "#orderform > div.option_box > table > tbody > tr:nth-child(3) td"

    option1 = [
        "".join(text.split()) if (text := await option1.text_content()) else ""
        for option1 in (await document.query_selector_all(option1_query))[1:]
    ]

    option4 = [
        "".join(text.split()) if (text := await option4.text_content()) else ""
        for option4 in (await document.query_selector_all(option4_query))[1:]
    ]

    option2 = [
        "".join(text.split()) if (text := await option2.text_content()) else ""
        for option2 in (await document.query_selector_all(option2_query))[1:]
    ]

    assert (
        len(option1) == len(option4) == len(option2)
    ), "Options1, Options4 and Options2 are not equal in length"

    assert (
        len(option1) == len(option4) == len(option2)
    ) != 0, "Options1, Options4 and Options2 are empty"

    return option1, option4, option2


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document | PlaywrightPage):
    price2 = price3 = 0
    model_name = manufacturer = manufacturing_country = message2 = ""

    query = "#orderform > div.sproTA > div.sproList > ul"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    headings = await table_tbody.query_selector_all("li > dl > dt")
    values = await table_tbody.query_selector_all("li > dl > dd")

    for heading, values in zip(headings, values):
        if not (heading := await heading.text_content()):
            continue

        if not (text := await values.text_content()):
            continue

        heading = "".join(heading.split())

        # model_name
        if "제품코드" in heading:
            model_name = text

        # price 2
        if "대리점가격" in heading:
            price2_str = text
            try:
                price2 = parse_int(price2_str)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price2 text {price2_str} to number"
                ) from err

        # price 3
        if "소비자가격" in heading:
            price3_str = text
            try:
                price3 = parse_int(price3_str)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price3 text {price3_str} to number"
                ) from err

        # manufacturer
        if "제조사" in heading:
            manufacturer = text

        # manufacturing country
        if "원산지" in heading:
            manufacturing_country = text

        # message2
        if "색상" in heading:
            message2 = text

    return (
        model_name,
        price2,
        price3,
        manufacturer,
        manufacturing_country,
        message2,
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "img[id='detailImg']"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "div.sproUimg > ul > li > img"
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


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "#container > div.sublinlk span"
    if not (categories := await document.query_selector_all(query)):
        raise error.QueryNotFound("Categories text not found", query)

    return ">".join(
        [
            text.strip()
            for category in categories
            if (text := await category.text_content())
        ]
    )


@cache
def image_quries():
    return "#container > div.spbunder > div.sdetail > div.sdetail img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
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
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    return list(map(lambda url: urljoin(product_url, url), urls))


async def extract_html(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(images):
            images = list(dict.fromkeys(images))
            return build_detailed_images_html(
                images,
                html_top,
                html_bottom,
            )
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            return "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()
