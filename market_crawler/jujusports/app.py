# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from contextlib import suppress
from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

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
from market_crawler.jujusports import config
from market_crawler.jujusports.data import JujuSportsCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify import MaxTriesReached
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
    await visit_link(page, "https://www.jujub2b.co.kr/index.html", wait_until="load")

    query = "#category-lnb > div.position > ul > li > a"
    for idx in range(len(await page.query_selector_all(query))):
        category = (await page.query_selector_all(query))[idx]

        if not (category_page_url := await category.get_attribute("href")):
            continue

        if not (category_text := await category.text_content()):
            continue

        category_text = category_text.strip()

        category_page_url = urljoin(page.url, category_page_url)
        await visit_link(page, category_page_url, wait_until="load")

        subcategories = await page.query_selector_all(
            "#contents > div.xans-element-.xans-product.xans-product-menupackage.product_menupackage > ul > li"
        )

        # ? Not all main categories may have sub categories
        if not subcategories:
            full_subcategories.append(Category(category_text, category_page_url))

        for subcategory in subcategories:
            if not (subcategory_page_url := await subcategory.query_selector("a")):
                continue

            if not (subcategory_text := await subcategory_page_url.text_content()):
                continue

            subcategory_text = subcategory_text.strip()
            if split := subcategory_text.split("("):
                subcategory_text = split[0].strip()

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


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    await page.click(login_button_query)
    await page.wait_for_selector("a:text('로그아웃')")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="div > fieldset > a > img",
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

    total_pages = 0
    query = "#contents > div.xans-element-.xans-product.xans-product-normalpaging.ec-base-paginate > a.last"
    if (el := await document.query_selector(query)) and (
        href := await el.get_attribute("href")
    ):
        regex = compile_regex(r"page=(\d*)")
        total_pages = parse_int(match[0]) if (match := regex.findall(href)) else 1

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        if category_state.pageno > total_pages:
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

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


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"product\/(.*)\/.*\/category")
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
async def get_products(document: Document | PlaywrightPage):
    query = "ul[class='prdList grid5'] li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (el := await product.query_selector("a")) or not (
        product_link := await el.get_attribute("href")
    ):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, product_link)


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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    tasks = (
        extract_product_name(document),
        extract_category_text(document),
        extract_thumbnail_images(document, product_url),
        extract_table(document),
        extract_options(document),
    )

    R1, R2, R3, R4, R5 = await gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_images):
            (
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
            ) = thumbnail_images
        case Err(_):
            thumbnail_image_url = thumbnail_image_url2 = thumbnail_image_url3 = (
                thumbnail_image_url4
            ) = thumbnail_image_url5 = ""

    match R4:
        case Ok(table):
            (
                price2,
                price3,
                maker,
                manufacturing_country,
                delivery_fee,
                quantity,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    options = R5

    R6, R7 = await gather(
        extract_images(page, product_url, html_top, html_bottom),
        extract_price1(browser, product_name),
    )

    match R6:
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

    match R7:
        case Ok(price1):
            pass
        case Err(err):
            raise error.Price1NotFound(err, url=product_url)

    await page.close()

    if options:
        for option in options:
            match split_options_text(option, price2):
                case Ok(result):
                    option1, _price2, option2, option3 = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = JujuSportsCrawlData(
                category=category_text,
                product_url=product_url,
                sold_out_text=sold_out_text,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturer=maker,
                manufacturing_country=manufacturing_country,
                price1=price1,
                price2=_price2,
                price3=price3,
                delivery_fee=delivery_fee,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
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

    crawl_data = JujuSportsCrawlData(
        category=category_text,
        product_url=product_url,
        sold_out_text=sold_out_text,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturer=maker,
        manufacturing_country=manufacturing_country,
        price1=price1,
        price2=price2,
        price3=price3,
        delivery_fee=delivery_fee,
        quantity=quantity,
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


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#contents > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.buy-wrapper > div.buy-scroll-box > h2"
    if not (product_name := await document.inner_text(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "img.BigImage"
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


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "#contents > div.xans-element-.xans-product.xans-product-headcategory.path > ol > li > a"
    if not (category_texts := await document.query_selector_all(query)):
        raise error.QueryNotFound("Category text not found", query)

    return ">".join(
        [
            text
            for el in category_texts
            if await el.get_attribute("href") and (text := await el.text_content())
        ]
    )


class Table(NamedTuple):
    price2: int
    price3: int
    maker: str
    manufacturing_country: str
    delivery_fee: str
    quantity: str


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document):
    maker = manufacturing_country = delivery_fee = quantity = ""
    price2 = price3 = 0

    query = "#span_product_price_text"
    if not (price2_text := await document.text_content(query)):
        # ? Some products don't have price2
        # ? See: https://www.jujub2b.co.kr/product/%EC%BD%94%EB%9E%84%EB%A6%AC%EC%95%88-2021fw-cspm5077-bl-%EB%82%A8%EC%84%B1%EC%9A%A9-%EB%B0%98%EB%B0%94%EC%A7%80/8081/category/64/display/1/
        price2 = 0
    else:
        try:
            price2 = parse_int(price2_text)
        except ValueError as err:
            raise ValueError(
                f"Coudn't convert price2 text {price2_text} to number"
            ) from err

    query = "div.detailArea > div.buy-wrapper > div.buy-scroll-box > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody > tr"
    if not (table_items := await document.query_selector_all(query)):
        raise error.QueryNotFound("Table not found", query=query)

    for item in table_items:
        if not (el := await item.query_selector("th")) or not (
            heading := await el.text_content()
        ):
            continue

        if not (el := await item.query_selector("td")) or not (
            text := await el.text_content()
        ):
            continue

        if "소비자가" in heading:
            price3_text = text
            try:
                price3 = parse_int(price3_text)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price3 text {price3_text} to number"
                ) from err

        if "제조사" in heading:
            maker = text.strip()

        if "배송비" in heading:
            delivery_fee = text.strip()

        if "원산지" in heading:
            manufacturing_country = text.strip()

    query = "div.infoArea p[class^='info']"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query=query)

    return Table(
        price2,
        price3,
        maker,
        manufacturing_country,
        delivery_fee,
        quantity,
    )


@returns_future(error.QueryNotFound, ValueError, error.PlaywrightTimeoutError)
async def extract_price1(browser: PlaywrightBrowser, product_name: str):
    page = await browser.new_page()
    await visit_link(
        page,
        "https://www.jujusports.co.kr/product/search.html",
        wait_until="networkidle",
    )

    await page.click("input[id='keyword'][name='keyword'][fw-label='검색어']")
    await page.type(
        "input[id='keyword'][name='keyword'][fw-label='검색어']", product_name
    )

    async with page.expect_navigation():
        await page.press(
            "input[id='keyword'][name='keyword'][fw-label='검색어']",
            "Enter",
        )

    price1 = "not search"

    if product_name not in await page.content():
        return price1

    match await get_products(page):
        case Ok(products):
            for product in products:
                if (
                    el := await product.query_selector("div.description > p.name > a")
                ) and (text := await el.text_content()):
                    text = text.strip()

                    if product_name in text:
                        url = await el.get_attribute("href")
                        await visit_link(page, urljoin(page.url, url))

                        query = 'xpath=//tr[contains(th, "판매가")]/td/*/strong[@id="span_product_price_text"]'
                        if not (price1 := await page.text_content(query)):
                            raise error.QueryNotFound("Price1 not found", query=query)

                        try:
                            price1 = parse_int(price1)
                        except ValueError as err:
                            raise ValueError(
                                f"Coudn't convert price1 text {price1} to number"
                            ) from err

                        break
        case _:
            price1 = "not search"

    await page.close()

    return price1


async def extract_options(document: Document):
    option1_query = "select[id='product_option_id1']"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := (await option1.text_content()))
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    return list(option1.keys())


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

    if "(-" in option1:
        regex = compile_regex(r"\s?\(\-\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 -= additional_price

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return "#prdDetail > div.cont img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        if heading := await page.query_selector(
            "#prdDetail > div.xans-element-.xans-product.xans-product-detail.tabMenu > ul > li:nth-child(1) > a"
        ):
            await heading.click(timeout=2500)

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
            max_tries=10,
        ):
            case Ok(src):
                if ("juju_d.jpg" not in src) and ("juju_top.jpg" not in src):
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
