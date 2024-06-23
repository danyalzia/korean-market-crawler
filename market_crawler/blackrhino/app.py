# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache
from typing import NamedTuple, cast
from urllib.parse import urljoin

import backoff

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, load_page, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.blackrhino import config
from market_crawler.blackrhino.data import BlackrhinoCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.press(login_button_query, key="Enter")

    await page.wait_for_selector(
        'img[src="/shop/data/skin/everybag/img/main/topmenu_logout.gif"]'
    )


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name=m_id]",
        password_query="input[name=password]",
        login_button_query="#form > table > tbody > tr:nth-child(2) > td.noline > input[type=image]",
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
    log.action.visit_category(category.name, category_page_url)

    if not (total_products_text := await document.text_content("#b_white > font > b")):
        raise error.TotalProductsTextNotFound(
            "Total products text is not found on the page", url=category_page_url
        )

    log.detail.total_products_in_category(category.name, parse_int(total_products_text))

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
    regex = compile_regex(r"goodsno=(\d+\w+)&")
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
    query = "form[name='frmList'] > table:nth-child(8) > tbody > tr:nth-child(5) > td > table > tbody > tr td"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    query = "div > img"

    for icon in await product.query_selector_all(query):
        if (src := await icon.get_attribute("src")) and "soldout" in src:
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

    if not (products := (await get_products(document)).ok()):
        page = await browser.new_page()
        await visit_link(page, category_page_url, wait_until="networkidle")

        if not (document := await parse_document(content, engine="lxml")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        match await get_products(document):
            case Ok(products):
                product = products[idx]
            case Err(err):
                raise error.ProductsNotFound(err, url=category_page_url)

    else:
        product = products[idx]

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
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

    sold_out_text = await extract_soldout_text(product)

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
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        model_name,
        manufacturing_country,
        manufacturer,
        brand,
        release_date,
        price3,
        detailed_images_html_source,
        options,
    ) = await extract_data(browser, page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1 in options:
            match split_options_text(option1, price3):
                case Ok((_option1, _price3, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = BlackrhinoCrawlData(
                category=category_state.name,
                sold_out_text=sold_out_text,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                price3=_price3,
                brand=brand,
                release_date=release_date,
                detailed_images_html_source=detailed_images_html_source,
                option1=_option1,
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

    crawl_data = BlackrhinoCrawlData(
        category=category_state.name,
        sold_out_text=sold_out_text,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        price3=price3,
        brand=brand,
        release_date=release_date,
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
    browser: PlaywrightBrowser,
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    if not (product_name := (await extract_product_name(document)).ok()):
        page = await browser.new_page()
        await visit_link(page, product_url, wait_until="load")
        content = await page.content()
        await page.close()
        if not (document2 := await parse_document(content, engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        document = document2
        match await extract_product_name(document):
            case Ok(product_name):
                pass
            case Err(err):
                raise error.ProductNameNotFound(err, url=product_url)

    match await extract_thumbnail_images(document, product_url):
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

    match await extract_table(page):
        case Ok(
            Table(
                model_name,
                manufacturing_country,
                manufacturer,
                brand,
                release_date,
                price3,
            )
        ):
            pass
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match await extract_images(document, product_url, html_top, html_bottom):
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
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        model_name,
        manufacturing_country,
        manufacturer,
        brand,
        release_date,
        price3,
        detailed_images_html_source,
        options,
    )


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
            option3 = f"-{additional_price}"

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
async def extract_options(page: PlaywrightPage):
    await page.wait_for_load_state("networkidle")

    options: list[str] = []

    option1_query = (
        "#goods_spec > form > table:nth-child(9) > tbody > tr:nth-child(2) select"
    )
    option2_query = (
        "#goods_spec > form > table:nth-child(9) > tbody > tr:nth-child(3) select"
    )

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join((text).split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value")) not in ["", "*", "**"]
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
                "".join((text).split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                not in ["", "*", "**"]
            }

            options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)
        else:
            options.append(option1_text)

    return options


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "img[id=objImg]"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img.hand"
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
async def extract_product_name(document: Document):
    query = "#goods_spec > form > div:nth-child(4) > b, #goods_spec > form > div:nth-child(5) > b"
    if not (el := await document.query_selector(query)) or not (
        product_name := await el.text_content()
    ):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


class Table(NamedTuple):
    model_name: str
    origin: str
    manufacturer: str
    brand: str
    release_date: int
    price3: int


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(page: PlaywrightPage):
    model_name = origin = manufacturer = brand = release_date = ""

    query = "#goods_spec > form[name=frmView] > table:nth-child(7) > tbody"
    if not (
        table_tbody := await page.query_selector_all(
            query,
        )
    ):
        raise error.QueryNotFound("Table not present", query=query)

    tr_elements = await table_tbody[0].query_selector_all("tr")
    th_elements = await table_tbody[0].query_selector_all("tr > th")
    td_elements = await table_tbody[0].query_selector_all("tr > td")

    assert tr_elements

    assert th_elements

    assert td_elements

    assert len(th_elements) != len(td_elements)
    for th, td in zip(
        th_elements,
        td_elements[1:],
    ):  # ? the first <tr> is only for placeholder
        if not (th_str := await th.text_content()):
            continue

        if not (td_str := await td.text_content()):
            continue

        if "제품코드" in th_str:
            model_name = td_str
        if "원산지" in th_str:
            origin = td_str
        if "제조사" in th_str:
            manufacturer = td_str
        if "브랜드" in th_str:
            brand = td_str
        if "출시일" in th_str:
            release_date = td_str
            release_date = "".join(filter(lambda date: date.isdigit(), release_date))

    price3 = cast(str, (await page.text_content("span[id=price]")))
    price3 = parse_int(price3)

    return Table(model_name, origin, manufacturer, brand, int(release_date), price3)


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = "#contents > table center > img"

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
