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

from aiofile import AIOFile
from playwright.async_api import async_playwright

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
from market_crawler.imac import config
from market_crawler.imac.data import ImacCrawlData
from market_crawler.initialization import Category, get_categories
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
    await visit_link(page, "https://www.imacmall.co.kr/main/index")

    categories = await page.query_selector_all("#mcate > tbody > tr")

    for category in categories:
        category_page_url = urljoin(
            page.url,
            (
                await (await category.query_selector_all("td > a"))[0].get_attribute(
                    "href"
                )
            ),
        )
        page2 = await browser.new_page()
        await visit_link(page2, category_page_url)

        category_text = cast(
            str,
            await page2.text_content(
                "#layout_body > table > tbody > tr > td:nth-child(2) > div:nth-child(4) > a"
            ),
        )
        await page2.close()

        if category_text in ["스포츠용품", "해외구매대행/공동구매"]:
            continue

        subcategories = await category.query_selector_all("td > table > tbody > tr a")
        if subcategories:
            for subcategory in subcategories:
                if not (subcategory_text := await subcategory.text_content()):
                    continue

                subcategory_text = subcategory_text.strip()

                url = urljoin(page.url, await subcategory.get_attribute("href"))
                full_subcategories.append(
                    Category(f"{category_text}>{subcategory_text}", url)
                )
        else:
            full_subcategories.append(Category(category_text, category_page_url))

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def run(settings: Settings) -> None:
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

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        await page.close()
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

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        model_name,
        product_name,
        price2,
        delivery_fee,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, content, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok((_option1, _price2, sold_out_text, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = ImacCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                price2=_price2,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                option1=_option1,
                option2=sold_out_text,
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

    crawl_data = ImacCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
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


class Data(NamedTuple):
    thumbnail_image_url: str
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    model_name: str
    product_name: str
    price2: int
    delivery_fee: str
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
    # ? For parsing XPATH expression
    if not (lxml_document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    tasks = (
        extract_thumbnail_images(document, product_url),
        extract_model_name(document),
        extract_product_name(document),
        extract_price2(document),
        extract_delivery_fee(lxml_document),
        extract_options(document),
        extract_images(page, product_url, html_top, html_bottom),
    )

    # ? Unfortuntely, asyncio.gather() doesn't provide type checking for more than 5 tasks , so we have to cast the types
    (
        R1,
        R2,
        R3,
        R4,
        R5,
        R6,
        R7,
    ) = cast(
        tuple[
            Ok[tuple[str, str, str, str, str]] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[str] | Err[error.QueryNotFound],
            Ok[int] | Err[error.QueryNotFound | ValueError],
            Ok[str] | Err[error.QueryNotFound],
            list[str],
            Ok[str] | Err[error.QueryNotFound | error.Base64Present],
        ],
        await asyncio.gather(*tasks),
    )

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
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R3:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R4:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R5:
        case Ok(delivery_fee):
            pass
        case Err(err):
            raise error.DeliveryFeeNotFound(err, url=product_url)

    options = R6

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
        model_name,
        product_name,
        price2,
        delivery_fee,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?no=(\w+)")
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
    query = "span[class='goodsDisplayImageWrap']"
    return (
        Ok(products)
        if (products := await document_or_page.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "span[class='short_desc']"
    # ? Some products have the query, but don't have any text in it
    # ? See: https://www.imacmall.co.kr/goods/view?no=308

    # ? We should return empty string in such cases instead of returning an exception
    if (model_name := await document.text_content(query)) is None:
        raise error.QueryNotFound("Model name not found", query=query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "span[class='goods_name']"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "b[class='price']"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return parse_int(price2)


@returns_future(error.QueryNotFound)
async def extract_delivery_fee(document: Document):
    query = "xpath=//tbody/tr[contains(./th, '배송')]/td"
    if not (delivery_fee := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    return delivery_fee.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#goods_thumbs div.slides_container.hide > div > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "#goods_thumbs ul > li > a > img"
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


async def extract_options(document: Document):
    option1_query = "select[name='viewOptions[]']"
    option2_query = "select[name='viewSuboption[]']"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )
    option2_elements = await document.query_selector_all(
        f"{option2_query} > option, {option2_query} > optgroup > option"
    )

    if option1_elements:
        if option2_elements:
            return [
                "_".join([option1_text, option2_text])
                for option1 in option1_elements
                for option2 in option2_elements
                if (option1_text := await option1.text_content())
                and (option2_text := await option2.text_content())
                and await option1.get_attribute("value")
            ]
        else:
            return [
                option1_text
                for option1 in option1_elements
                if (option1_text := await option1.text_content())
                and await option1.get_attribute("value")
            ]

    return [
        option2_text
        for option2 in option2_elements
        if (option2_text := await option2.text_content())
    ]


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0
    if "추가" in option1 and option1.endswith("원)"):
        regex = compile_regex(r"\s?\(?추가\s*?\w+[,]?\w*원\)?")
        additional_price = parse_int(regex.findall(option1)[0])
        option1 = regex.sub("", option1)
        price2 += additional_price
        option3 += additional_price

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


@cache
def image_quries():
    return "#countents_body img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = "contents_frame"
    if not (frame := page.frame(name=query)):
        raise error.QueryNotFound("Frame not found", query)

    query = image_quries()
    elements = await frame.query_selector_all(query)

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
