# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import re

from dataclasses import dataclass
from functools import cache, singledispatch
from urllib.parse import urljoin

import backoff
import pandas as pd

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.domejjim import config
from market_crawler.domejjim.data import DomejjimCrawlData
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.result import Err, Ok, Result, UnwrapError, returns_future


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
        user_id_query="#login > div > div.leftArea > form > fieldset > label.id.ePlaceholder > input",
        password_query="#login > div > div.leftArea > form > fieldset > label.password.ePlaceholder > input",
        login_button_query="div[class='btn_login']",
        keep_logged_in_check_query="#login > div > div.leftArea > form > fieldset > p > input",
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

        await crawl_urls(
            list(dict.fromkeys(settings.URLS)),
            browser,
            settings,
            columns,
        )


async def crawl_urls(
    urls: list[str], browser: PlaywrightBrowser, settings: Settings, columns: list[str]
):
    series: list[dict[str, str | int]] = []
    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(
                idx,
                browser,
                settings,
                urls[idx],
                series,
            )
            for idx in chunk
        )

        await asyncio.gather(*tasks)

    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )
    await asyncio.to_thread(
        pd.DataFrame(series, columns=columns).to_csv,  # type: ignore
        filename,  # type: ignore
        encoding="utf-8-sig",
        index=False,
    )


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    settings: Settings,
    product_url: str,
    series: list[dict[str, str | int]],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        discount_price,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            try:
                option1, option2, option3, price3 = split_options_text(option, price3)
            except IndexError as err:
                raise error.OptionsNotFound(err, url=product_url) from err

            crawl_data = DomejjimCrawlData(
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                manufacturing_country=manufacturing_country,
                manufacturer=manufacturer,
                price3=price3,
                price2=price2,
                discount_price=discount_price,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=option3,
            )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )
        return None

    crawl_data = DomejjimCrawlData(
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        manufacturing_country=manufacturing_country,
        manufacturer=manufacturer,
        price3=price3,
        price2=price2,
        discount_price=discount_price,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(idx, product_url)

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

    log.action.visit_category(category_name, category_url)

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

        products_chunk = chunks(
            range(number_of_products), config.MAX_PRODUCTS_CHUNK_SIZE
        )

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        for chunk in products_chunk:
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

        log.action.category_page_crawled(category_name, category_state.pageno)

        category_state.pageno += 1
        category_html.pageno = category_state.pageno

        if config.USE_CATEGORY_SAVE_STATES:
            await category_state.save()

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"branduid=(\d+\w+)&")
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
    query = "#prdBrand > div[class=item-wrap] > div.item-list > table > tbody > tr > td > ul"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for price in await product.query_selector_all("div > li.prd_price.sold"):
        if (soldout_text := await price.text_content()) and "SOLD OUT" in soldout_text:
            return "품절"

    return ""


@cache
def get_options_edge_cases():
    return ["-일시품절", "-품절", "(품절)"]


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

    if "품절" in sold_out_text:
        log.debug(f"Sold out text is present: Product no: {idx+1}")

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        discount_price,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            try:
                option1, option2, option3, price3 = split_options_text(option, price3)
            except IndexError as err:
                raise error.OptionsNotFound(err, url=product_url)

            crawl_data = DomejjimCrawlData(
                category=category_state.name,
                sold_out_text=sold_out_text,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                manufacturing_country=manufacturing_country,
                manufacturer=manufacturer,
                price3=price3,
                price2=price2,
                discount_price=discount_price,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=option3,
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

    crawl_data = DomejjimCrawlData(
        category=category_state.name,
        sold_out_text=sold_out_text,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        manufacturing_country=manufacturing_country,
        manufacturer=manufacturer,
        price3=price3,
        price2=price2,
        discount_price=discount_price,
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


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_product_name(document),
        extract_thumbnail_image(document, product_url),
        extract_table(document, product_url),
        extract_images(document, product_url, html_top, html_bottom),
    )

    (
        R1,
        R2,
        R3,
        R4,
    ) = await asyncio.gather(*tasks)

    if not (product_name := R1.ok()):
        # ? Second attempt with lexbor parser
        await asyncio.sleep(1)
        await visit_link(page, product_url, wait_until="load")

        if not (
            document2 := await parse_document(await page.content(), engine="lexbor")
        ):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        document = document2

        tasks = (
            extract_product_name(document),
            extract_thumbnail_image(document, product_url),
            extract_table(document, product_url),
            extract_images(document, product_url, html_top, html_bottom),
        )

        (
            R1,  # type: ignore
            R2,  # type: ignore
            R3,  # type: ignore
            R4,  # type: ignore
        ) = await asyncio.gather(*tasks)

        if not (product_name := R1.ok()):
            # ? Third attempt with lxml parser
            await asyncio.sleep(2)
            await visit_link(page, product_url, wait_until="load")

            if not (
                document3 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document3

            tasks = (
                extract_product_name(document),
                extract_thumbnail_image(document, product_url),
                extract_table(document, product_url),
                extract_images(document, product_url, html_top, html_bottom),
            )

            (
                R1,  # type: ignore
                R2,  # type: ignore
                R3,  # type: ignore
                R4,  # type: ignore
            ) = await asyncio.gather(*tasks)

            if not (product_name := R1.ok()):
                # ? Fourth attempt with Playwright's Page
                await asyncio.sleep(3)
                await visit_link(page, product_url, wait_until="load")

                tasks = (
                    extract_product_name(page),
                    extract_thumbnail_image(page, product_url),
                    extract_table(page, product_url),
                    extract_images(page, product_url, html_top, html_bottom),
                )

                (
                    R1,  # type: ignore
                    R2,  # type: ignore
                    R3,  # type: ignore
                    R4,  # type: ignore
                ) = await asyncio.gather(*tasks)

                if not (product_name := R1.ok()):
                    # ? Fifth attempt with Playwright's Page but without asyncio.gather()
                    # ? Repeat it 10 times
                    for idx in range(1, 11):
                        await asyncio.sleep(idx)
                        await visit_link(page, product_url, wait_until="load")

                        R1 = await extract_product_name(page)  # type: ignore
                        try:
                            R1.unwrap()
                        except UnwrapError:
                            pass
                        else:
                            break

                    R2 = await extract_thumbnail_image(page, product_url)  # type: ignore
                    R3 = await extract_table(page, product_url)  # type: ignore
                    R4 = await extract_images(page, product_url, html_top, html_bottom)  # type: ignore

                    match R1:
                        case Ok(product_name):
                            pass
                        case Err(err):
                            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R3:
        case Ok(
            Table(
                manufacturer,
                manufacturing_country,
                price3,
                price2,
                discount_price,
            )
        ):
            pass
        case Err(error.SupplyPriceNotFound(err)):
            await visit_link(page, product_url, wait_until="load")
            # ? Second attempt with lexbor parser
            if not (
                document2 := await parse_document(await page.content(), engine="lexbor")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document2

            match await extract_table(document, product_url):
                case Ok(
                    Table(
                        manufacturer,
                        manufacturing_country,
                        price3,
                        price2,
                        discount_price,
                    )
                ):
                    pass
                case Err(error.SupplyPriceNotFound(err)):
                    await visit_link(page, product_url, wait_until="load")
                    # ? Third attempt with lxml parser
                    if not (
                        document3 := await parse_document(
                            await page.content(), engine="lxml"
                        )
                    ):
                        raise HTMLParsingError(
                            "Document is not parsed correctly", url=product_url
                        )

                    document = document3

                    match await extract_table(document, product_url):
                        case Ok(
                            Table(
                                manufacturer,
                                manufacturing_country,
                                price3,
                                price2,
                                discount_price,
                            )
                        ):
                            pass
                        case Err(error.SupplyPriceNotFound(err)):
                            await visit_link(
                                page, product_url, wait_until="networkidle"
                            )

                            # ? Fourth attempt with Playwright's Page
                            match await extract_table(page, product_url):
                                case Ok(
                                    Table(
                                        manufacturer,
                                        manufacturing_country,
                                        price3,
                                        price2,
                                        discount_price,
                                    )
                                ):
                                    pass
                                case Err(error.SupplyPriceNotFound(err)):
                                    raise error.SupplyPriceNotFound(
                                        err, url=product_url
                                    )
                                case Err(err):
                                    raise error.TableNotFound(err, url=product_url)

                        case Err(err):
                            raise error.TableNotFound(err, url=product_url)

                case Err(err):
                    raise error.TableNotFound(err, url=product_url)

        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    match await extract_options(page):
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    return (
        product_name,
        thumbnail_image_url,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        discount_price,
        detailed_images_html_source,
        options,
    )


def split_options_text(option1: str, price3: int):
    option2 = option3 = ""
    if any((s1 := s) in option1 for s in get_options_edge_cases()):
        option1 = re.sub(s1, "", option1)
        option2 = s1
    else:
        option1 = option1

    if "(+" in option1:
        regex = compile_regex(r"\(\+\w+[,]?\w*\)")
        option3 = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        price3 += parse_int(option3)

    if "(-" in option1:
        regex = compile_regex(r"\(\-\w+[,]?\w*\)")
        option3 = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        price3 -= parse_int(option3)

    return option1, option2, option3, price3


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    await page.wait_for_load_state("networkidle")

    options: list[str] = []

    option1_query = "#form1 > div > div.table-opt > table > tbody > tr > td > div > dl:nth-child(1) > dd > select"
    option2_query = "#form1 > div > div.table-opt > table > tbody > tr > td > div > dl:nth-child(2) > dd > select"

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


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(
    document: Document | PlaywrightPage, product_url: str
):
    query = (
        "#productDetail > div.page-body > div.thumb-info > div.thumb-wrap > div > img"
    )
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document | PlaywrightPage):
    query = "#productDetail > div.page-body > div.thumb-info > div.titleArea > h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@dataclass(slots=True, frozen=True)
class Table:
    manufacturer: str
    manufacturing_country: str
    price3: int
    price2: int
    discount_price: int


@singledispatch
async def extract_table(
    document_or_page: Document | PlaywrightPage, product_url: str
) -> Result[Table, error.QueryNotFound | error.SupplyPriceNotFound]:
    raise NotImplementedError(f"Wrong first argument: {type(document_or_page)}")


@extract_table.register
async def _(
    document: Document, product_url: str
) -> Result[Table, error.QueryNotFound | error.SupplyPriceNotFound]:
    manufacturer = manufacturing_country = ""
    price3 = price2 = discount_price = 0

    query = "#form1 > div > div.table-opt > table > tbody"
    if not (table_body := await document.query_selector(query)):
        return Err(error.QueryNotFound("Table not found", query))

    headings = await table_body.query_selector_all("tr > th")
    values = await table_body.query_selector_all("tr > td")

    assert len(headings)
    assert len(values)

    # ? The last two <th> don't contain <td>
    assert len(headings) + 2 == len(
        values
    ), f"Table <tr> and <td> not equal {len(headings)} vs {len(values)}"

    for key, val in zip(
        headings,
        values,
    ):
        if not (heading := await key.text_content()):
            continue

        if not (value := await val.text_content()):
            continue

        heading = "".join(heading.split())
        value = value.strip()

        if "소비자가" in heading or "공급가" in heading:
            price3_str = value
            try:
                price3 = parse_int(price3_str)
            except ValueError:
                log.warning(
                    f"Unique Sell Price <magenta>({price3_str})</> is present <blue>| {product_url}</>"
                )

        if "판매가" in heading or "도매가" in heading:
            price2_str = value
            try:
                price2 = parse_int(price2_str)
            except ValueError:
                log.warning(
                    f"Unique Supply Price <magenta>({price2_str})</> is present <blue>| {product_url}</>"
                )

        if "할인가" in heading:
            discount_price_str = value
            try:
                discount_price = parse_int(discount_price_str)
            except ValueError:
                log.warning(
                    f"Unique Discount Price <magenta>({discount_price_str})</> is present <blue>| {product_url}</>"
                )

        if "제조회사" in heading:
            manufacturer = value

        if "제조국" in heading:
            manufacturing_country = value

    if not manufacturer:
        log.warning(f"Manufacturer is not present <blue>| {product_url}</>")

    if not manufacturing_country:
        log.warning(f"Manufacturing country is not present <blue>| {product_url}</>")

    # ? Some products don't have sell price
    # ? See: http://www.domejjim.com/shop/shopdetail.html?branduid=606436&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=a2l3Ug%3D%3D
    # if not price3:
    #     warning(f"Selling price is not present <blue>| {product_url}</>")

    if not price2:
        return Err(error.SupplyPriceNotFound("Supply price is empty"))

    # if not discount_price:
    #     warning(f"Discount price is not present <blue>| {product_url}</>")

    return Ok(
        Table(
            manufacturer,
            manufacturing_country,
            price3,
            price2,
            discount_price,
        )
    )


@extract_table.register
async def _(
    page: PlaywrightPage, product_url: str
) -> Result[Table, error.QueryNotFound | error.SupplyPriceNotFound]:
    manufacturer = manufacturing_country = ""
    price3 = price2 = discount_price = 0

    # ? Sometimes when we are on the product page, the sidebar (that can be toggled on and off) automatically opens, which hides the elements behind it (so they become non-clickable)
    if sidebar_btn := await page.query_selector(
        "div[id='btn_close'][style='display: inline-block;']"
    ):
        await sidebar_btn.click()

    await page.wait_for_load_state("load")

    query = "#form1 > div > div.table-opt > table > tbody"
    try:
        table_body = (
            await page.query_selector_all(
                query,
            )
        )[0]
    except IndexError:
        return Err(error.QueryNotFound("Table not found", query))

    headings = await table_body.query_selector_all("tr > th")
    values = await table_body.query_selector_all("tr > td")

    assert headings
    assert values

    # if len(headings) == len(values[1:]):
    #     values = values[1:]

    # ? The last two <th> don't contain <td>
    assert len(headings) + 2 == len(
        values
    ), f"Table <tr> and <td> not equal {len(headings)} vs {len(values)}"

    for key, val in zip(
        headings,
        values,
    ):
        if not (heading := await key.text_content()):
            continue

        if not (value := await val.text_content()):
            continue

        heading = "".join(heading.split())
        value = value.strip()

        if "소비자가" in heading or "공급가" in heading:
            price3_str = value
            try:
                price3 = parse_int(price3_str)
            except ValueError:
                log.warning(
                    f"Unique Sell Price <magenta>({price3_str})</> is present <blue>| {product_url}</>"
                )

        if "판매가" in heading or "도매가" in heading:
            price2_str = value
            try:
                price2 = parse_int(price2_str)
            except ValueError:
                log.warning(
                    f"Unique Supply Price <magenta>({price2_str})</> is present <blue>| {product_url}</>"
                )

        if "할인가" in heading:
            discount_price_str = value
            try:
                discount_price = parse_int(discount_price_str)
            except ValueError:
                log.warning(
                    f"Unique Discount Price <magenta>({discount_price_str})</> is present <blue>| {product_url}</>"
                )

        if "제조회사" in heading:
            manufacturer = value

        if "제조국" in heading:
            manufacturing_country = value

    if not manufacturer:
        log.warning(f"Manufacturer is not present <blue>| {product_url}</>")

    if not manufacturing_country:
        log.warning(f"Manufacturing country is not present <blue>| {product_url}</>")

    # ? Some products don't have sell price
    # ? See: http://www.domejjim.com/shop/shopdetail.html?branduid=606436&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=a2l3Ug%3D%3D
    # if not price3:
    #     warning(f"Selling price is not present <blue>| {product_url}</>")

    if not price2:
        return Err(error.SupplyPriceNotFound("Supply price is empty"))

    # if not discount_price:
    #     warning(f"Discount price is not present <blue>| {product_url}</>")

    return Ok(
        Table(
            manufacturer,
            manufacturing_country,
            price3,
            price2,
            discount_price,
        )
    )


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document | PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
) -> str:
    query = "#productDetail > div.page-body > div.prd-detail img"

    urls = [
        src
        for image in await document.query_selector_all(query)
        if (src := await image.get_attribute("src"))
        and "http://www.domejjim.com/design/lya6214/info.jpg" not in src
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
