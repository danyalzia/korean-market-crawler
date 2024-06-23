# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import re

from contextlib import suppress
from functools import cache
from urllib.parse import urljoin

import backoff

from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.extraction import load_content, load_page, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.aqus import config
from market_crawler.aqus.data import AQUSCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify import returns
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?pg" in current_url:
        return re.sub(r"\?pg=(\d*)", f"?pg={next_page_no}", current_url)
    if "&pg" in current_url:
        return re.sub(r"&pg=(\d*)", f"&pg={next_page_no}", current_url)

    return f"{current_url}&pg={next_page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    # ? Most websites redirect from login page when submitting the form, so we are putting it inside expect_navigation() block
    # ? If it doesn't work for some websites, copy this function to your code, remove expect_navigation() part and pass your function to LoginInfo object
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_load_state(
        state="load"
    )  # ? "load" is fine for most websites, but some websites don't show full page details until all the network requests are resolved, so for that "networkidle" can be used

    await page.pause()


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
        user_id_query="input#id",
        password_query="input#pass",
        login_button_query="input[name=formimage1]",
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
            raise error.HTMLParsingError(
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
                    browser,
                    settings,
                    category_page_url,
                    category_state,
                    category_html,
                    idx,
                    filename,
                    columns,
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
    regex = compile_regex(r"num=(\d*)")
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
    query = ".subItemList"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_product(
    browser: PlaywrightBrowser,
    settings: Settings,
    category_page_url: str,
    category_state: CategoryState,
    category_html: CategoryHTML,
    idx: int,
    filename: str,
    columns: list[str],
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
        raise error.HTMLParsingError(
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
            raise error.HTMLParsingError(
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
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise error.HTMLParsingError(
            "Document is not parsed correctly", url=product_url
        )

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        price3,
        brand,
        manufacturing_country,
        delivery_fee,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            try:
                match split_options_text(option, price2):
                    case Ok((option1, price2_, option2, option3)):
                        pass
                    case Err(err):
                        raise error.IncorrectData(
                            f"Could not split option text ({option}) into price2 due to an error -> {err}",
                            url=product_url,
                        )

            except IndexError as err:
                raise error.OptionsNotFound(err, url=product_url)

            crawl_data = AQUSCrawlData(
                category=category_state.name,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                price2=price2_,
                price3=price3,
                brand=brand,
                manufacturing_country=manufacturing_country,
                delivery_fee=delivery_fee,
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

    crawl_data = AQUSCrawlData(
        category=category_state.name,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        price2=price2,
        price3=price3,
        brand=brand,
        manufacturing_country=manufacturing_country,
        delivery_fee=delivery_fee,
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
        extract_thumbnail_images(document, product_url),
        extract_product_name(document),
        extract_price2(document),
        extract_price3(document),
        extract_table(document, product_url),
        extract_options(page),
    )

    (R1, R2, R3, R4, R5, R6) = await gather(*tasks)

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
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    match R5:
        case Ok(table):
            (brand, manufacturing_country, delivery_fee) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R6:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    detailed_images_html_source = await extract_html(
        page, product_url, html_top, html_bottom
    )

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        price3,
        brand,
        manufacturing_country,
        delivery_fee,
        options,
        detailed_images_html_source,
    )


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(error.TimeoutException)
async def extract_options(page: PlaywrightPage):
    # ? We have to wait for message_ajax.php related requests in order to show the options (if they are present)
    async with page.expect_request(lambda request: "message_ajax.php" in request.url):
        await page.wait_for_load_state("networkidle")

    options: list[str] = []

    # ? On some products, option1 query is present, but is hidden
    # ? See: http://aqusb2b.com/view.php?num=4161&tb=&count=&category=2r11&pg=3
    query = "tr[style='display:none;'] > td > table > tbody > tr > td > div[id='autostart_1'] > div[id='ColorView'] > select[id='search_category1']"
    if await page.query_selector(query):
        return options

    option1_query = "select[id='search_category1']"
    option2_query = "select[id='search_category2']"

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
            async with page.expect_request_finished(
                lambda request: "my_product_option_ajax" in request.url
            ):
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
            # ? We wait for message_ajax.php related requests again so that text in second option is updated correctly
            async with page.expect_request(
                lambda request: "message_ajax.php" in request.url
            ):
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


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3: str | int = 0

    if "(+-" in option1:
        regex = compile_regex(r"\s?\(\+-\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    elif "(+" in option1:
        regex = compile_regex(r"\s?\(\+\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    elif "(-" in option1:
        regex = compile_regex(r"\s?\(\-\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    option1 = option1.strip()

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "(품절)" in option1:
        return option1.replace("(품절)", ""), price2, "(품절)", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#image_large_0"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img[id^='image_thumb']"
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
    query = "form td td tr:nth-of-type(1) b"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = "td span:nth-of-type(2)"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query)

    try:
        price2 = parse_int(price2)
    except ValueError as err:
        raise ValueError(f"Unique Price2 <magenta>({price2})</> is present") from err

    return price2


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document):
    query = "td span:nth-of-type(1) strike"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query)

    try:
        price3 = parse_int(price3)
    except ValueError as err:
        raise ValueError(f"Unique Price3 <magenta>({price3})</> is present") from err

    return price3


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document | PlaywrightPage, product_url: str):
    brand = manufacturing_country = delivery_fee = ""

    query = (
        "#contents3 > form > div > table > tbody > tr > td > table:nth-child(4) > tbody"
    )
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    headings = await table_tbody.query_selector_all("tr > td:nth-child(1)")
    values = await table_tbody.query_selector_all("tr > td:nth-child(2)")

    for heading, value in zip(headings, values):
        if not (heading := await heading.text_content()):
            continue

        if not (text := await value.text_content()):
            continue

        heading = "".join(heading.split())

        text = text.strip()

        # brand
        if "브랜드" in heading:
            brand = text

        # manufacturing country
        if "원산지" in heading:
            manufacturing_country = text

        # delivery fee
        if "배송비" in heading:
            delivery_fee = text

    if not brand:
        log.warning(f"Brand is not present: <blue>{product_url}</>")

    if not manufacturing_country:
        log.warning(f"Manufacturing country is not present: <blue>{product_url}</>")

    if not delivery_fee:
        log.warning(f"Delivery fee is not present: <blue>{product_url}</>")

    return brand, manufacturing_country, delivery_fee


@cache
def image_quries():
    return "#contents3 > table #ct img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    try:
        await page.click("a > img[alt='상품상세설명']")
    except:
        log.warning(
            f"Button 상품상세설명 is not present at all <blue>| {product_url}</>"
        )

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
