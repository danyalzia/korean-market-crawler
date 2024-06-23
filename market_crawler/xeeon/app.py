# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import cast
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, load_page, parse_document, visit_link
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
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from market_crawler.xeeon import config
from market_crawler.xeeon.data import XeeonCrawlData
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


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="div[class=login] a[class='loginBtn -mov']",
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
    regex = compile_regex(r"product_no=(\w+)")
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
    query = "ul[class='prdList grid4'] > li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("img.icon_img"):
        alt = str(await icon.text_content())
        if "품절" in alt:
            return "품절"

    return ""


async def is_not_crawlable_image_present(document: Document):
    if not (
        image_to_check_selectors := await document.query_selector_all(
            "#prdDetail > div.cont img"
        )
    ):
        log.warning(r"\<src> attribute not found in images")

    for image_to_check_selector in image_to_check_selectors:
        image_to_check = cast(
            str,
            await image_to_check_selector.get_attribute(
                "src",
            ),
        )

        if "top_banner_02.jpg" in image_to_check:
            return True

    return False


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

    if await is_not_crawlable_image_present(document):
        log.warning(
            f"Product contains top_banner_02.jpg, so won't crawl it: No: {idx+1} | {product_url}"
        )
        return None

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

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        product_name,
        price,
        model_name,
        brand,
        delivery_fee,
        consumer_fee,
        option1_title,
        option1,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    options = option1.split(",")

    if len(options) > 1:
        for option in options:
            match split_options_text(option):
                case Ok((option1, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = XeeonCrawlData(
                category=category_state.name,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                product_name=product_name,
                model_name=model_name,
                brand=brand,
                price3=price,
                consumer_fee=consumer_fee,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1_title=option1_title,
                option1=option1,
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

    crawl_data = XeeonCrawlData(
        category=category_state.name,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        model_name=model_name,
        brand=brand,
        price3=price,
        consumer_fee=consumer_fee,
        delivery_fee=delivery_fee,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1_title=option1_title,
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
        extract_thumbnail_image(document, product_url),
        extract_product_name(document),
        extract_table(document, product_url),
        extract_html(page, product_url, html_top, html_bottom),
    )

    (R1, R2, R3, R4) = await gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(table):
            (price, model_name, brand, delivery_fee, consumer_fee) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    detailed_images_html_source = R4

    option1_title, option1 = await extract_options(page, product_name)

    return (
        thumbnail_image_url,
        product_name,
        price,
        model_name,
        brand,
        delivery_fee,
        consumer_fee,
        option1_title,
        option1,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div[class=infoArea] > h3, div[class=infoArea] > h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.keyImg img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document, product_url: str):
    try:
        price = cast(str, await document.text_content("#span_product_price_text"))
        price = parse_int(price)
    except (TimeoutError, ValueError) as err:
        raise error.SellingPriceNotFound(product_url) from err

    model_name = brand = delivery_fee = consumer_fee = ""

    query = "div[class=infoArea] > div > table > tbody"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    tr_elements = await table_tbody.query_selector_all("tr")

    for tr in tr_elements:
        if not (el := await tr.query_selector("th")) or not (
            th := await el.text_content()
        ):
            continue

        if "상품코드" in th:
            if not (td := await tr.query_selector("td")) or not (
                model_name := await td.text_content()
            ):
                raise error.ModelNameNotFound(product_url)

            model_name = model_name.strip()

        if "브랜드" in th:
            if not (td := await tr.query_selector("td")) or not (
                brand := await td.text_content()
            ):
                raise error.BrandNotFound(product_url)

            brand = brand.strip()

        if "배송비" in th:
            # ? We need to crawl <strong> because rest of text includes (50,000원 이상 구매 시 무료), etc.
            # ? See: https://xeeon.co.kr/product/detail.html?product_no=1169&cate_no=72&display_group=1
            if not (td := await tr.query_selector("td strong")) or not (
                delivery_fee := await td.text_content()
            ):
                raise error.DeliveryFeeNotFound(product_url)

            delivery_fee = delivery_fee.strip()

            delivery_fee = parse_int(delivery_fee)  # type: ignore

        if "소비자가" in th:
            if not (td := await tr.query_selector("td")) or not (
                consumer_fee := await td.text_content()
            ):
                raise error.SupplyPriceNotFound(product_url)

            consumer_fee = parse_int(consumer_fee)  # type: ignore

    return price, model_name, brand, delivery_fee, consumer_fee


async def extract_options(page: PlaywrightPage, product_name: str):
    color_present = size_present = color_size_present = False

    option1_title = option1 = color_size_text = color_text = size_text = ""

    table_th_selector = "#prdOptionTable > tbody > tr:nth-child(1) > th"
    try:
        assert await page.query_selector(table_th_selector)

        color_text = cast(
            str,
            await page.text_content(table_th_selector),
        )

        assert color_text == "색상"

        assert await page.query_selector("#product_option_id1")

        color_present = True
    except (AssertionError, TimeoutError):
        color_present = False

    table_th_selector = "#prdOptionTable > tbody > tr:nth-child(2) > th"
    try:
        assert await page.query_selector(table_th_selector)

        size_text = cast(
            str,
            await page.text_content(table_th_selector),
        )

        assert size_text == "사이즈"

        assert await page.query_selector("#product_option_id2")

        size_present = True
    except (AssertionError, TimeoutError):
        size_present = False

    table_th_selector = "#prdOptionTable > tbody > tr:nth-child(1) > th"
    try:
        assert await page.query_selector(table_th_selector)

        color_size_text = cast(str, await page.text_content(table_th_selector))

        assert color_size_text in ["색상-사이즈", "사이즈", "색상"]  # Color-size

        assert await page.query_selector("#product_option_id1")

        color_size_present = True
    except (AssertionError, TimeoutError):
        color_size_present = False

    if not color_present and not size_present and not color_size_present:
        option1_title = "상품"
        option1 = (
            product_name.replace(",", "")
            .replace(" ", "")
            .replace(" ", "")
            .replace("/", "")
        )  # Product name without , and space

        # log.warning(
        #     "Found no color options, size options and also color_size options"
        # )

    if color_present:
        option1_title, option1 = await extract_color(
            page, size_present, color_text, size_text
        )

    if color_size_present:
        option1_title, option1 = await extract_color_size(page, color_size_text)

    return option1_title, option1


async def extract_color(
    page: PlaywrightPage,
    size_present: bool,
    color_text: str,
    size_text: str,
):
    final_str = ""

    # ? When only color is present, options are inside "optgroup"
    if size_present:
        color_options = await page.query_selector_all("#product_option_id1 > option")
    else:
        color_options = await page.query_selector_all(
            "#product_option_id1 > optgroup > option",
        )

    for j in range(len(color_options)):
        color_value_ = cast(str, await color_options[j].get_attribute("value"))
        color_options_str = cast(str, await color_options[j].text_content())

        if color_value_ not in ["*", "**"]:
            await page.select_option("#product_option_id1", color_value_)

            if size_present:
                size_options = await page.query_selector_all(
                    "#product_option_id2 > option",
                )
                for k in range(len(size_options)):
                    size_value_ = cast(
                        str, await size_options[k].get_attribute("value")
                    )
                    size_options_str = cast(str, await size_options[k].text_content())
                    if size_value_ not in ["*", "**"]:
                        # ? We don't need to select this one if we have already selected color option
                        # await page.select_option("#product_option_id2", size_value_)
                        final_str += f"{color_options_str}_{size_options_str},"

            else:
                final_str += f"{color_options_str},"

    final_str = final_str.removesuffix(",")
    option1 = final_str
    option1_title = f"{color_text}_{size_text}" if size_present else color_text
    return option1_title, option1


async def extract_color_size(page: PlaywrightPage, color_size_text: str):
    final_str = ""

    # ? When only color-size (single element) is present, options are inside "optgroup"
    color_size_options = await page.query_selector_all(
        "#product_option_id1 > optgroup > option",
    )

    for j in range(len(color_size_options)):
        color_size_value_ = cast(
            str, await color_size_options[j].get_attribute("value")
        )
        color_size_options_str = cast(str, await color_size_options[j].text_content())

        if color_size_value_ not in ["*", "**"]:
            # ? We don't need to select this one because it's the ONLY one present on the website
            # await page.select_option("#product_option_id1", color_size_value_)
            final_str += f"{color_size_options_str},"

    final_str = final_str.removesuffix(",")
    option1 = final_str
    option1_title = color_size_text  # type: ignore

    return option1_title, option1


@returns(IndexError, ValueError)
def split_options_text(option1: str):
    option3 = 0

    if "(+" in option1:
        regex = compile_regex(r"\s?\(\+\w+[,]?\w*원\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            option3 += additional_price

    if "(-" in option1:
        regex = compile_regex(r"\s?\(\-\w+[,]?\w*원\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            option3 = f"-{additional_price}"

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), "[품절]", option3 or ""

    if "품절" in option1:
        return option1.replace("품절", ""), "품절", option3 or ""

    return option1, "", option3 or ""


@cache
def image_quries():
    return "#prdDetail > div.cont center img"


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

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)
        await page.wait_for_timeout(1000)
