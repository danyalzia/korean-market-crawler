# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache, singledispatch
from typing import cast, overload
from urllib.parse import urljoin

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
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.daytime import config
from market_crawler.daytime.data import DaytimeCrawlData
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

    query = "#basisBody > tbody > tr > td > table > tbody > tr:nth-child(12) > td > a"
    pages_nav = await document.query_selector_all(query)
    if not (last_page_text := await pages_nav[-1].text_content()):
        raise error.QueryNotFound(
            "Last page text not found",
            query=query,
        )

    total_pages = int(last_page_text)

    log.detail.total_products_in_category(category_name, total_pages)

    while True:
        if category_state.pageno > total_pages:
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

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


async def visit_product_link(page: PlaywrightPage, product_url: str):
    await visit_link(page, product_url, wait_until="load")

    if (
        text := await page.text_content("#content > div > fieldset > h2")
    ) and "500 - 내부 서버 오류" in text:
        log.warning("Error page found, reloading the page ...")
        await visit_link(page, product_url, wait_until="load")

    if (
        text := await page.text_content("#content > div > fieldset > h3")
    ) and "찾고 있는 리소스에 문제가 있어 표시할 수 없습니다" in text:
        log.warning("Error page found, reloading the page ...")
        await visit_link(page, product_url, wait_until="load")


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
    await visit_product_link(page, product_url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    if not (product_name := (await extract_product_name(document)).ok()):
        await visit_product_link(page, product_url)

        if not (document := await parse_document(await page.content(), engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        match await extract_product_name(document):
            case Ok(product_name):
                pass
            case Err(err):
                raise error.ProductNameNotFound(err, url=product_url)

    (
        model_name,
        price3,
        thumbnail_image_url,
        detailed_images_html_source,
        brand,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            crawl_data = DaytimeCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                sold_out_text=sold_out_text,
                price3=price3,
                detailed_images_html_source=detailed_images_html_source,
                brand=brand,
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
            len(options),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    crawl_data = DaytimeCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        sold_out_text=sold_out_text,
        price3=price3,
        detailed_images_html_source=detailed_images_html_source,
        brand=brand,
        option1="",
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
    (
        R1,
        R2,
        R3,
        R4,
        R5,
    ) = await asyncio.gather(
        extract_model_name(document),
        extract_price3(document),
        extract_thumbnail_image(document, product_url),
        extract_images(document, product_url),
        extract_options(document),
    )

    match R1:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R2:
        case Ok(price3):
            pass
        case Err(err):
            raise error.SellingPriceNotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
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

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    match R5:
        case Ok(result):
            brand, options = result
        case Err(err):
            if "Brand" in str(err):
                raise error.BrandNotFound(err, product_url)
            else:
                raise error.OptionsNotFound(err, product_url)

    return (
        model_name,
        price3,
        thumbnail_image_url,
        detailed_images_html_source,
        brand,
        options,
    )


@returns_future(error.QueryNotFound)
async def extract_options(document: Document):
    option1_query = "#goodsOption"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): option1_value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (option1_value := await option1.get_attribute("value"))
        and option1_value not in ["", "*", "**", "선택하세요.", "0"]
    }

    if len(option1_elements) > 0:
        query = (
            "xpath=//*[@id='basisBody']//tr[contains(./td/text(), '브랜드')]/td/span"
        )
        if not (brand := await document.text_content(query)):
            raise error.QueryNotFound("Brand not present", query=query)

    else:
        try:
            brand = cast(
                str,
                await (
                    await document.query_selector_all(
                        "#basisBody > tbody > tr > td > form > table:nth-child(11) > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(5) > td > table > tbody > tr:nth-child(6) > td:nth-child(3) > span",
                    )
                )[0].text_content(),
            )
        except IndexError:
            try:
                # ? Sometimes even when colors option is not present, selector is the same as the one with color option
                brand = cast(
                    str,
                    await (
                        await document.query_selector_all(
                            "#basisBody > tbody > tr > td > form > table:nth-child(11) > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(5) > td > table > tbody > tr:nth-child(7) > td:nth-child(3) > span",
                        )
                    )[0].text_content(),
                )
            except IndexError:
                query = "#basisBody > tbody > tr > td > form > table:nth-child(11) > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(5) > td > table > tbody > tr:nth-child(8) > td:nth-child(3) > span"
                try:
                    brand = cast(
                        str,
                        await (
                            await document.query_selector_all(
                                query,
                            )
                        )[0].text_content(),
                    )
                except IndexError as err:
                    raise error.QueryNotFound("Brand not present", query=query) from err

    brand = "".join(brand.split())

    return brand, list(option1.keys())


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"guid=(\w+)&")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(tree: Document) -> int | None:
    match await get_products(tree):
        case Ok(products):
            return len(products)
        case _:
            return None


@overload
async def get_products(
    tree: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    tree: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(tree: Document | PlaywrightPage):
    # ? We don't want "Best Products" links because they are already included in general products
    query = "#basisBody > tbody > tr > td > table > tbody > tr > td > form > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(1) > td"
    return (
        Ok(products)
        if (products := await tree.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    query = "#basisBody > tbody > tr > td > table > tbody > tr > td > form > table > tbody > tr > td > table > tbody > tr > td > table > tbody > tr:nth-child(4) > td"
    if el := await product.query_selector(query):
        for icon in await el.query_selector_all("img"):
            if (
                src_text := await icon.get_attribute("src")
            ) and "soldout.jpg" in src_text:
                return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "#mainImg"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "#basisBody > tbody > tr > td > form > table:nth-child(11) > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(5) > td > table > tbody"

    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    model_name = ""
    for tr in await table_tbody.query_selector_all("tr"):
        if not (td := await tr.text_content()):  # The first index is for key
            continue

        if "상품코드" in td:
            try:
                if not (
                    model_name := await (await tr.query_selector_all("td"))[
                        2
                    ].text_content()
                ):  # The third index (2) is for value
                    continue
            except IndexError as err:
                raise error.QueryNotFound("Model name not found", "td") from err

            break
    else:
        raise error.QueryNotFound("Model name not found", query)

    return model_name


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#basisBody > tbody > tr > td > form > table:nth-child(11) > tbody > tr:nth-child(1) > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(1) > td"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_price3(document: Document):
    query = "#saleprice"
    if not (price := await document.text_content(query)):
        raise error.QueryNotFound("Selling price not found", query=query)

    return parse_int(price)


@cache
def image_quries():
    return ", ".join(
        [
            "#wyditor_img",
            "#tbContent > tbody > tr > td > div > table > tbody > tr > td > img",
            "#tbContent > tbody > tr > td > div > img",
            "#__01 > tbody > tr > td > img",
        ]
    )


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage, product_url: str, settings: Settings
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document_or_page: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = [
        src
        for image in await document_or_page.query_selector_all(query)
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
    document_or_page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()
    if not (elements := await document_or_page.query_selector_all(query)):
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    urls: list[str] = []
    for el in elements:
        action = lambda: get_srcs(el)
        focus = lambda: focus_element(document_or_page, el)
        is_base64 = isin("base64")
        match await do(action).retry_if(
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

    await element.scroll_into_view_if_needed()
    await element.focus()
