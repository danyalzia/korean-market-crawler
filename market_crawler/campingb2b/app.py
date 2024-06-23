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
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.campingb2b import config
from market_crawler.campingb2b.data import Campingb2bCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
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


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)
    await page.wait_for_selector(login_button_query, state="detached")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#loginId",
        password_query="#loginPwd",
        login_button_query='button:has-text("로그인")',
        keep_logged_in_check_query="text=아이디 저장",
        login_button_strategy=login_button_strategy,
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
        name=category.name,
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

    if total_products_text := await document.text_content(
        "#contents > div > div.content > div > div.goods_pick_list > form > div > h5 > span > strong"
    ):
        products_len = parse_int(total_products_text)

        log.detail.total_products_in_category(category_name, products_len)

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

    if (product_name := (await extract_product_name(document)).ok()) is None:
        raise error.ProductNameNotFound(product_url)

    (
        product_name,
        thumbnail_image_url,
        model_name,
        manufacturer,
        origin,
        delivery_fee,
        price2,
        price3,
        text_other_than_price3,
        message1,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    crawl_data = Campingb2bCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        manufacturer=manufacturer,
        manufacturing_country=origin,
        delivery_fee=delivery_fee,
        price2=price2,
        price3=price3,
        text_other_than_price=text_other_than_price3,
        message1=message1,
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
        extract_images(document, product_url),
    )

    (
        R1,
        R2,
        R3,
        R4,
    ) = await asyncio.gather(*tasks)

    if (product_name := R1.ok()) is None:
        raise error.ProductNameNotFound(product_url)

    match R2:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    (
        model_name,
        manufacturer,
        origin,
        delivery_fee,
        price2,
        price3,
        text_other_than_price3,
        message1,
    ) = R3

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.InvalidImageURL(err)):
            match await extract_images(page, product_url, html_top, html_bottom):
                case Ok(detailed_images_html_source):
                    pass
                case Err(err):
                    raise error.ProductDetailImageNotFound(err, url=product_url)

        case Err(err):
            raise error.ProductDetailImageNotFound(err, url=product_url)

    return (
        product_name,
        thumbnail_image_url,
        model_name,
        manufacturer,
        origin,
        delivery_fee,
        price2,
        price3,
        text_other_than_price3,
        message1,
        detailed_images_html_source,
    )


@cache
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


@overload
async def get_products(
    document_or_page: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    document_or_page: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(document_or_page: Document | PlaywrightPage):
    query = "#contents > div > div.content > div > div.goods_list > div > div > ul > li"
    return (
        Ok(products)
        if (products := await document_or_page.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("div > a")):
        raise error.QueryNotFound("Product link not found", "div > a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div[class=item_detail_tit] > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


async def extract_soldout_text(product: Element):
    if (el := await product.query_selector("a > strong")) and (
        item_class := await el.get_attribute("class")
    ):
        if "soldout" in item_class:
            return "품절"

    return ""


async def extract_table(document: Document, product_url: str):
    model_name = manufacturer = origin = ""

    dl_items = await document.query_selector_all("div[class=item_detail_list] > dl")

    if not len(dl_items):
        raise error.TableNotFound(
            url=product_url, message="Table is not present at all"
        )

    for dl in dl_items:
        dt = (await dl.query_selector_all("dt"))[0]
        dd = (await dl.query_selector_all("dd"))[0]
        if await dt.text_content() == "자체상품코드":
            model_name = cast(str, await dd.text_content())
            try:
                assert model_name
            except AssertionError as e:
                raise error.ModelNameNotFound(
                    url=product_url, message="Model Name is empty"
                ) from e

        if await dt.text_content() == "제조사":
            manufacturer = cast(str, await dd.text_content())
            try:
                assert manufacturer
            except AssertionError as e:
                raise error.ManufacturerNotFound(
                    url=product_url, message="Manufacturer is empty"
                ) from e

        if await dt.text_content() == "원산지":
            origin = cast(str, await dd.text_content())
            try:
                assert origin
            except AssertionError as e:
                raise error.ManufacturingCountryNotFound(
                    url=product_url, message="Manufacturing Country is empty"
                ) from e

    tasks = (
        extract_delivery_fee(document),
        extract_price2(document),
        extract_price3(document),
        extract_text_other_than_price3(document),
        extract_message1(document),
    )
    (
        R1,
        R2,
        R3,
        R4,
        R5,
    ) = await asyncio.gather(*tasks)

    match R1:
        case Ok(delivery_fee):
            pass
        case Err(err):
            raise error.DeliveryFeeNotFound(err, url=product_url)

    match R2:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R3:
        case Ok(price3):
            pass
        case Err(err):
            raise error.SellingPriceNotFound(err, url=product_url)

    match R4:
        case Ok(text_other_than_price3):
            pass
        case Err(err):
            raise error.TextOtherThanSellingPriceNotFound(err, url=product_url)

    match R5:
        case Ok(message1):
            pass
        case Err(err):
            # ? Some products don't have message1
            # ? See: https://www.campingb2b.com/goods/goods_view.php?goodsNo=1000001131
            log.warning(f"Message1 not found: {product_url}")
            message1 = ""

    return (
        model_name,
        manufacturer,
        origin,
        delivery_fee,
        price2,
        price3,
        text_other_than_price3,
        message1,
    )


@returns_future(error.QueryNotFound, ValueError)
async def extract_message1(document: Document):
    query = (
        "#frmView > div > div > div.item_detail_list > dl > dt[style^='background:red']"
    )
    if not (message1 := await document.inner_text(query)):
        raise error.QueryNotFound("Message1 not found", query=query)

    return message1.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document):
    query = "xpath=//dl[contains(./dt/text(), '온라인판매가')]/dd/span"
    if not (price3_text := await document.inner_text(query)):
        raise error.QueryNotFound("Price3 not found", query=query)

    return parse_int(price3_text)


@returns_future(error.QueryNotFound)
async def extract_text_other_than_price3(document: Document):
    query = (
        "xpath=//dl[contains(./dt/text(), '온라인판매가')]/dd/span[@style='color:red;']"
    )
    if not (text_other_than_price3 := await document.text_content(query)):
        raise error.QueryNotFound("Text other than price3 not found", query=query)

    return text_other_than_price3


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "xpath=//dl[contains(./dt/text(), '도매가')]/dd/strong"
    if not (price2_text := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return parse_int(price2_text)


@returns_future(error.QueryNotFound, ValueError)
async def extract_delivery_fee(document: Document):
    query = "div[class=item_detail_list] > dl[class=item_delivery] > dd > strong"
    if not (delivery_fee_text := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    return parse_int(delivery_fee_text)


@cache
def image_quries():
    return "#detail > div.detail_cont > div > div.txt-manual img"


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
