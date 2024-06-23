# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import shutil

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from functools import cache, singledispatch
from glob import glob
from typing import Any, NamedTuple
from urllib.parse import urljoin

import backoff
import numpy as np
import pandas as pd

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
from market_crawler.shuline import config
from market_crawler.shuline.data import ShulineCrawlData
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
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="a.btnSubmit",
        login_button_strategy=login_button_strategy,
    )
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

    remove_duplicate_with_lower_period(settings)


def remove_duplicate_with_lower_period(settings: Settings):
    temp_dir = os.path.join(os.path.dirname(__file__), "temp")
    final_temporary_file = os.path.join(temp_dir, settings.DATE, "final_temporary.csv")
    backup_dir = os.path.join(temp_dir, settings.DATE, "backup")

    os.makedirs(backup_dir, exist_ok=True)

    with suppress(OSError):
        os.remove(final_temporary_file)

    # ? Copy back existing backup directory files to main temp directory
    for file in os.listdir(backup_dir):
        shutil.copy(
            os.path.join(temp_dir, settings.DATE, "backup", file),
            os.path.join(temp_dir, settings.DATE),
        )
        os.remove(os.path.join(temp_dir, settings.DATE, "backup", file))

    crawled_files = glob(os.path.join(temp_dir, settings.DATE, "*.csv"))

    with ThreadPoolExecutor(max_workers=len(crawled_files)) as executor:
        results: list[Any] = [
            executor.submit(pd.read_csv, file) for file in crawled_files  # type: ignore
        ]

    df = pd.concat([r.result() for r in results])
    df = df.drop_duplicates(
        subset=[
            settings.COLUMN_MAPPING["product_name"],
            settings.COLUMN_MAPPING["option1"],
            settings.COLUMN_MAPPING["period"],
        ],
        keep="first",
    )

    for file in crawled_files:
        shutil.copy(file, backup_dir)
        os.remove(file)

    df = df[df[settings.COLUMN_MAPPING["period"]] != "5/523"]

    # ? Consider period column as datetime
    # ? See: https://stackoverflow.com/questions/68830931/create-dataframe-and-set-column-as-datetime
    # df[settings.COLUMN_MAPPING["period"]] = pd.to_datetime(
    #     df[settings.COLUMN_MAPPING["period"]], errors="coerce"
    # )
    with suppress(ValueError):
        df.loc[:, settings.COLUMN_MAPPING["period"]] = df[  # type: ignore
            settings.COLUMN_MAPPING["period"]
        ].apply(
            lambda x: np.datetime64(x)  # type: ignore
        )
    # ? Only retain entries from overlapping products that have highest datetime
    # ? See: https://stackoverflow.com/questions/12497402/remove-duplicates-by-columns-a-keeping-the-row-with-the-highest-value-in-column
    period_grouped = df.groupby(  # type: ignore
        [
            settings.COLUMN_MAPPING["product_name"],
            settings.COLUMN_MAPPING["option1"],
        ],
    ).apply(
        lambda x: max(x[settings.COLUMN_MAPPING["period"]])  # type: ignore
    )

    df2 = df.set_index(
        [
            settings.COLUMN_MAPPING["product_name"],
            settings.COLUMN_MAPPING["option1"],
        ]
    )

    df2.loc[period_grouped.index, settings.COLUMN_MAPPING["period"]] = period_grouped

    df = df2.reset_index().drop_duplicates(
        subset=[
            settings.COLUMN_MAPPING["product_name"],
            settings.COLUMN_MAPPING["option1"],
            settings.COLUMN_MAPPING["period"],
        ]
    )

    df.to_csv(final_temporary_file, index=False)


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


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


def get_productid(url: str) -> Result[str, str]:
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


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("img.icon_img"):
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

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        product_name,
        model_name,
        period,
        price2,
        options,
        detailed_images_html_source,
    ) = await extract_data(
        page, await page.content(), document, product_url, html_top, html_bottom
    )

    await page.close()

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok(result):
                    (option1_, price2_, option2, option3) = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = ShulineCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                price2=price2_,
                period=period,
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

    crawl_data = ShulineCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        price2=price2,
        period=period,
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


class Data(NamedTuple):
    thumbnail_image_url: str
    model_name: str
    product_name: str
    period: str
    price2: int
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    page: PlaywrightPage,
    content: str,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_thumbnail_image(document, product_url),
        extract_table(content),
        extract_options(document, page),
        extract_images(document, product_url),
    )

    (R1, R2, R3, R4) = await gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            # ? We are not doing it concurrently as not get deadlock
            tasks = (
                await extract_thumbnail_image(page, product_url),
                await extract_table(await page.content()),
                await extract_options(page, page),
                await extract_images(page, product_url, html_top, html_bottom),
            )

            (R1, R2, R3, R4) = tasks  # type: ignore

            match R1:
                case Ok(thumbnail_image_url):
                    pass
                case Err(err):
                    raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(Table(product_name, model_name, period, price2)):
            if not model_name:
                log.warning(f"Model name is not found: <blue>{product_url}</>")
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

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

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

        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        thumbnail_image_url,
        product_name,
        model_name,
        period,
        price2,
        options,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(
    document_or_page: Document | PlaywrightPage, product_url: str
):
    query = "img.BigImage"
    if not (thumbnail_image := await document_or_page.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


class Table(NamedTuple):
    product_name: str
    model_name: str
    period: str
    price2: int


@returns_future(IndexError, ValueError, KeyError)
async def extract_table(content: str):
    dfs = await asyncio.to_thread(
        pd.read_html, content, keep_default_na=False, flavor="lxml"  # type: ignore
    )

    try:
        df = dfs[0].T
    except IndexError as err:
        raise IndexError("DataFrame not found") from err
    else:
        df.columns = df.iloc[0]  # type: ignore
        df = df.drop([0])

    try:
        product_name: str = df["상품명"].iloc[0]  # type: ignore
    except KeyError:
        try:
            df = dfs[1].T
        except IndexError as err:
            raise IndexError("DataFrame not found") from err
        else:
            df.columns = df.iloc[0]  # type: ignore
            df = df.drop([0])

        product_name: str = df["상품명"].iloc[0]  # type: ignore

    try:
        model_name: str = df["상품간략설명"].iloc[0]  # type: ignore
    except KeyError:
        model_name = ""

    try:
        period: str = df["등록일"].iloc[0]  # type: ignore
    except KeyError:
        period = ""

    price2 = parse_int(df["도매가(부가세별도)"].iloc[0])  # type: ignore

    return Table(product_name, model_name, period, price2)


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
async def extract_options(
    document_or_page: Document | PlaywrightPage, page: PlaywrightPage
):
    option1_query = "select[id='product_option_id1']"
    option2_query = "select[id='product_option_id2']"

    option1_elements = await document_or_page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): option1_value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (option1_value := await option1.get_attribute("value"))
        and option1_value not in ["", "*", "**"]
    }

    # ? If option2 is not present, then we don't need to use Page methods
    if not (
        await document_or_page.query_selector(
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
            "".join(text.split()): option2_value
            for option2 in await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )
            if (text := await option2.text_content())
            and (option2_value := await option2.get_attribute("value"))
            and option2_value not in ["", "*", "**"]
        }

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


@cache
def image_quries():
    return "div.cont img"


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = [
        src
        for image in await document.query_selector_all(query)
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
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    elements = await page.query_selector_all(query)

    for el in elements:
        with suppress(error.PlaywrightTimeoutError):
            await el.click()

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
