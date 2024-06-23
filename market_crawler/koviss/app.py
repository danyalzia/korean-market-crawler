# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib

from functools import cache
from typing import NamedTuple, cast
from urllib.parse import urljoin

import pandas as pd

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
from market_crawler.koviss import config
from market_crawler.koviss.data import KovissCrawlData
from market_crawler.path import temporary_csv_file
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


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="fieldset a > img[alt='로그인']",
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
            browser_config=browser_config,
            playwright=playwright,
            login_info=login_info,
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


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"(\d*\w*)/category/(\d*\w*)")
    return (
        Ok(str(match.group(1) + "_" + match.group(2)))
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
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def extract_soldout_text(product: Element):
    query = "div.thumbnail > div.icon > div.promotion > img"
    icons = await product.query_selector_all(query)

    if not icons:
        raise error.QueryNotFound(
            "Sold out icon not found",
            query,
        )

    for icon in icons:
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

    match await extract_soldout_text(product):
        case Ok(sold_out_text):
            pass
        case Err(error.QueryNotFound(err)):
            sold_out_text = ""

    if "품절" in sold_out_text:
        log.debug(f"Sold out text is present: Product no: {idx+1}")

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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        manufacturer,
        manufacturing_country,
        model_name,
        quantity,
        price3,
        price2,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            option1, price2, option2 = split_options_text(option, price2)

            crawl_data = KovissCrawlData(
                category=category_state.name,
                sold_out_text=sold_out_text,
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=thumbnail_image_url,
                manufacturing_country=manufacturing_country,
                manufacturer=manufacturer,
                price2=price2,
                price3=price3,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
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

    crawl_data = KovissCrawlData(
        category=category_state.name,
        sold_out_text=sold_out_text,
        product_url=product_url,
        product_name=product_name,
        model_name=model_name,
        thumbnail_image_url=thumbnail_image_url,
        manufacturing_country=manufacturing_country,
        manufacturer=manufacturer,
        price2=price2,
        price3=price3,
        quantity=quantity,
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
    )

    R1, R2 = await asyncio.gather(*tasks)

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

    tasks = (
        extract_table(page, product_url),
        extract_images(page, product_url, html_top, html_bottom),
        extract_options(page, product_url),
    )

    (
        (manufacturer, manufacturing_country, model_name, quantity, price3, price2),
        _detailed_images_html_source,
        options,
    ) = await asyncio.gather(*tasks)

    match _detailed_images_html_source:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return (
        product_name,
        thumbnail_image_url,
        manufacturer,
        manufacturing_country,
        model_name,
        quantity,
        price3,
        price2,
        detailed_images_html_source,
        options,
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#dbox > div > div.headingArea > h2"
    if not (selector := await document.query_selector(query)):
        raise error.QueryNotFound("Product name not found", query)

    return cast(str, await selector.text_content()).strip()


class Table(NamedTuple):
    maker: str
    country: str
    model_name: str
    quantity: str
    price3: int
    price2: int


async def extract_table(page: PlaywrightPage, product_url: str):
    maker = country = model_name = quantity = ""
    price2 = price3 = 0
    query = "#dbox > div > div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table"
    if not (table := await page.query_selector(query)):
        raise error.QueryNotFound("Table is not present", query)

    if not (table := await table.query_selector("xpath=..")):
        raise error.QueryNotFound("Table is not present", query)

    html = (await table.inner_html()).strip()

    dfs = pd.read_html(html)

    try:
        df = dfs[0].T
    except IndexError as err:
        raise error.TableNotFound("DataFrame is not present", product_url) from err
    else:
        df.columns = df.iloc[0]  # type: ignore
        df = df.drop([0])

    try:
        maker = str(df["제조사"].iloc[0])  # type: ignore
    except KeyError:
        log.warning(f"Manufacturer country not found: <blue>{product_url}</>")

    try:
        country = str(df["원산지"].iloc[0])  # type: ignore
    except KeyError:
        log.warning(f"Manufacturing country not found: <blue>{product_url}</>")

    try:
        price3_text = str(df["소비자가"].iloc[0])  # type: ignore
    except KeyError:
        # ? See: https://www.kovissb2b.com/product/%EC%BD%94%EB%B9%84%EC%8A%A4b2b-%EA%B3%A8%ED%94%84%EC%9A%A9%ED%92%88-%EC%98%A4%EA%B4%91-%EB%B3%BC%EB%A7%88%EC%BB%A4-%EB%B3%B4%EC%84%9D%ED%95%A8-%EC%84%A0%EB%AC%BC%EC%84%B8%ED%8A%B8-gs7903%EB%A7%88%EC%BB%A42%ED%99%80%EB%8D%941/2556/category/53/display/1/
        log.warning(f"Price3 not found: <blue>{product_url}</>")
    else:
        try:
            price3 = parse_int(price3_text)
        except ValueError as err:
            raise ValueError(f"Unusual price3 presnet: {product_url}") from err
    try:
        price2_text = str(df["판매가"].iloc[0])  # type: ignore
    except KeyError:
        # ? See: https://www.kovissb2b.com/product/%EC%BD%94%EB%B9%84%EC%8A%A4b2b-%EA%B3%A8%ED%94%84-%ED%95%84%EB%93%9C%EC%9A%A9%ED%92%88-%EC%83%9D%ED%99%94%EB%B3%BC%EB%A7%88%EC%BB%A4-%EC%84%A0%EB%AC%BC%EC%84%B8%ED%8A%B8-gs7908-%ED%95%B8%EB%93%9C%EB%A9%94%EC%9D%B4%EB%93%9C-%EA%B0%80%EC%A3%BD%ED%99%80%EB%8D%94-%EB%B3%B4%EC%84%9D%ED%95%A8/2847/category/37/display/1/
        log.warning(f"Price2 not found: <blue>{product_url}</>")
    else:
        try:
            price2 = parse_int(price2_text)
        except ValueError as err:
            raise ValueError(f"Unusual price2 presnet: {product_url}") from err
    try:
        model_name = str(df["상품코드"].iloc[0])  # type: ignore
    except KeyError:
        log.warning(f"Model name not found: <blue>{product_url}</>")

    quantity = cast(
        str,
        await page.text_content(
            "#dbox > div > div.detailArea > div.infoArea > div.guideArea > p"
        ),
    )

    return Table(maker, country, model_name, quantity, price3, price2)


def split_options_text(option1: str, price2: int):
    if "(+" in option1:
        regex = compile_regex(r"\(\+\w+[,]?\w*\)")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        price2 += parse_int(additional_price)

    if "(-" in option1:
        regex = compile_regex(r"\(\-\w+[,]?\w*\)")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        price2 -= parse_int(additional_price)

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]"

    return option1, price2, ""


async def extract_options(page: PlaywrightPage, product_url: str):
    await page.wait_for_load_state("networkidle", timeout=300000)

    options1_list: list[str] = []

    option1_query = "select[id='product_option_id1']"
    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    for i in range(len(option1_elements)):
        option1_elements = await page.query_selector_all(
            f"{option1_query} > option, {option1_query} > optgroup > option"
        )
        option1_value: str = cast(str, await option1_elements[i].get_attribute("value"))
        options1_str: str = "".join(
            cast(str, await option1_elements[i].text_content()).split()
        )

        if "(+" in options1_str:
            log.warning(f"(+ is present: {product_url}")

        if "(-" in options1_str:
            log.warning(f"(- is present: {product_url}")

        # log.info(f"{options1_str = }")

        if option1_value not in ["", "*", "**"]:
            await page.select_option(
                option1_query,
                value=option1_value,
            )
            await page.wait_for_load_state("networkidle", timeout=300000)

            options1_list.append(options1_str)

    return options1_list


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = "#prdDetail > div.cont > img, #prdDetail > div.cont > div > p > img, #prdDetail > div.cont img"

    if elements := await page.query_selector_all(query):
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
        match await do(action).retryif(
            predicate=is_base64,
            on_retry=focus,
            max_tries=10,
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

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
