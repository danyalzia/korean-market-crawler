# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib
import os

from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

import backoff

from aiofile import AIOFile
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
from market_crawler.leadersdome import config
from market_crawler.leadersdome.data import LeadersdomeCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, category_page_url: str, next_page_no: int) -> str:
    if "?page" in category_page_url:
        return category_page_url.replace(
            f"?page={next_page_no-1}", f"?page={next_page_no}"
        )
    if "&page" in category_page_url:
        return category_page_url.replace(
            f"&page={next_page_no-1}", f"&page={next_page_no}"
        )

    return f"{category_page_url}&page={next_page_no}"


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "http://leadersdome.co.kr/index.html", wait_until="load")

    for idx in range(len(await page.query_selector_all("#category > div > ul > li"))):
        category = (await page.query_selector_all("#category > div > ul > li"))[idx]

        if not (category_page_url := await category.query_selector("a")):
            continue

        if not (category_text := await category_page_url.text_content()):
            continue

        category_text = category_text.strip()

        if not category_text or category_text in [
            "ALL (즉시출고)",
            "Drug store",
            "Furniture",
        ]:
            continue

        category_page_url = urljoin(
            page.url, await category_page_url.get_attribute("href")
        )
        await visit_link(page, category_page_url, wait_until="load")

        subcategories = await page.query_selector_all(
            "#contents > div.xans-element-.xans-product.xans-product-menupackage > ul > li"
        )

        # ? Not all main categories have sub categories
        if not subcategories:
            full_subcategories.append(Category(category_text, category_page_url))

        for subcategory in subcategories:
            if not (subcategory_page_url := await subcategory.query_selector("a")):
                continue

            if not (subcategory_text := await subcategory_page_url.text_content()):
                continue

            subcategory_text = subcategory_text.strip().removesuffix("()").strip()

            url = urljoin(
                page.url,
                await subcategory_page_url.get_attribute("href"),
            )
            full_text = f"{category_text}>{subcategory_text}"
            full_subcategories.append(Category(full_text, url))

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
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="fieldset > p.loginbtn",
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
        category_page_url=category.url, next_page_no=category_state.pageno
    )
    log.action.visit_category(category.name, category_page_url)

    while True:
        category_page_url = page_url(
            category_page_url=category_page_url, next_page_no=category_state.pageno
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
                raise error.ProductsNotFound(err, url=category_page_url)

    match await extract_soldout_text(product):
        case Ok(sold_out_text):
            pass
        case Err(_):
            sold_out_text = ""

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
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        model_name,
        quantity,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            option1, price2, option2, option3 = split_options_text(option1, price2)

            crawl_data = LeadersdomeCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                price2=price2,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
                sold_out_text=sold_out_text,
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

    crawl_data = LeadersdomeCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        price2=price2,
        quantity=quantity,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
        sold_out_text=sold_out_text,
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
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    product_name: str
    price2: int
    model_name: str
    quantity: str
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
        extract_thumbnail_images(document, product_url),
        extract_product_name(document),
        extract_price2(document),
        extract_model_name(document),
        extract_quantity(document),
    )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
    ) = await asyncio.gather(*tasks)

    tasks = (
        extract_options(page),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (
        R6,
        R7,
    ) = await asyncio.gather(*tasks)

    await page.close()

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
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R4:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R5:
        case Ok(quantity):
            pass
        case Err(err):
            raise error.QuantityNotFound(err, url=product_url)

    match R6:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R7:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        model_name,
        quantity,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"product\/(\w*-?\d*)")
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
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_soldout_text(product: Element):
    query = "div.icon div.promotion > img"

    if not (icon := await product.query_selector(query)):
        raise error.QueryNotFound(
            "Sold out icon not found",
            query,
        )

    if (text := await icon.get_attribute("src")) and "soldout" in text:
        return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#contents > div.xans-element-.xans-product.xans-product-detail > div.headingArea > h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "select[id='product_option_id1']"
    options = await document.query_selector_all(
        f"{query} > option, {query} > optgroup > option"
    )

    if not options:
        raise error.QueryNotFound("Model name not found", query=query)

    model_name = [
        (text).strip()
        for option in options
        if (text := await option.text_content())
        and await option.get_attribute("value") not in ["", "*", "**"]
    ][0]

    return model_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "strong[id='span_product_price_text']"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return parse_int(price2)


@returns_future(error.QueryNotFound)
async def extract_quantity(document: Document):
    query = "div.infoArea > div.guideArea > p[class^='info']"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query=query)

    return quantity.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div.keyImg img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img.ThumbImage"
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


def split_options_text(option1: str, price2: int):
    option3 = ""
    if "(+" in option1:
        regex = compile_regex(r"\(\+\w+[,]?\w*\)")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = parse_int(additional_price)
        price2 += option3

    if "(-" in option1:
        regex = compile_regex(r"\(\-\w+[,]?\w*\)")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = parse_int(additional_price)
        price2 -= option3

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3

    return option1, price2, "", option3


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    options: list[str] = []

    # ? Options are only displayed after selecting the model name from dropdown
    model_names_query = "select[name='option1']"

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await page.click(model_names_query)
    model_names_elements = await page.query_selector_all(
        f"{model_names_query} > option, {model_names_query} > optgroup > option"
    )

    if not model_names_elements:
        await page.close()
        return options

    model_name_values = [
        value
        for model_name in model_names_elements
        if (value := await model_name.get_attribute("value")) not in ["", "*", "**"]
    ]

    # ? When there are a lot of requests at once, select_option() throws TimeoutError, so let's backoff here
    try:
        await page.select_option(
            model_names_query,
            value=model_name_values[0],  # ? We need to select only the first option
        )
    except error.PlaywrightTimeoutError as err:
        await page.reload()
        raise error.TimeoutException("Timed out waiting for select_option()") from err

    try:
        await page.wait_for_load_state("networkidle")
    except error.PlaywrightTimeoutError as err:
        await page.reload()
        raise error.TimeoutException(
            "Timed out waiting for wait_for_load_state()"
        ) from err

    option1_query = "select[id='product_option_id2']"
    option2_query = "select[id='product_option_id3']"

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1_texts = [
        "".join((text).split())
        for option1 in option1_elements
        if (text := await option1.text_content())
    ]
    option1_values = [
        await option1.get_attribute("value") for option1 in option1_elements
    ]

    for option1_text, option1_value in zip(option1_texts, option1_values):
        if option1_value in ["", "*", "**"]:
            continue

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
            option2_texts = [
                "".join((text).split())
                for option2 in option2_elements
                if (text := await option2.text_content())
            ]
            option2_values = [
                await option2.get_attribute("value") for option2 in option2_elements
            ]

            options.extend(
                f"{option1_text}_{option2_text}"
                for option2_text, option2_value in zip(option2_texts, option2_values)
                if option2_value and option2_value not in ["", "*", "**"]
            )

        else:
            options.append(option1_text)

    return options


@cache
def image_queries():
    return "#prdDetail > div img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    query = image_queries()

    if not (elements := await page.query_selector_all(query)):
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
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
