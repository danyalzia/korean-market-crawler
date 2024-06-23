# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from urllib.parse import urljoin

import backoff

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
from market_crawler.franklinsports import config
from market_crawler.franklinsports.data import FranklinsportsCrawlData
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
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)

    await page.wait_for_selector(
        "text=로그아웃",
        state="visible",
    )


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#member_id",
        password_query="#member_passwd",
        login_button_query="img[alt='로그인']",
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
    regex = compile_regex(r"product_no=(\d*)")
    if not (match := regex.search(url)):
        return Err(f"Regex Not Matched: {url}")

    return Ok(str(match.group(1)))


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for img in await product.query_selector_all("div > p.name > img"):
        if (alt := await img.get_attribute("alt")) and "품절" in alt:
            return "품절"

    return ""


async def extract_message1(product: Element, category_url: str):
    if icon := await product.query_selector(
        "#contents_main > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody > tr > td > span[style='font-size:11px;color:#918c91;']"
    ):
        if src := await icon.get_attribute("src"):
            return urljoin(category_url, src)

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

    message1 = await extract_message1(product, category_page_url)

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        category_text,
        product_name,
        price1,
        price2,
        quantity,
        options,
        options4,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option, option4 in zip(options, options4):
            match split_options_text(option):
                case Ok((option1, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = FranklinsportsCrawlData(
                category=category_text,
                product_url=product_url,
                sold_out_text=sold_out_text,
                thumbnail_image_url=thumbnail_image_url,
                product_name=product_name,
                price1=price1,
                price2=price2,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                message1=message1,
                option1=option1,
                option2=option2,
                option3=str(option3),
                option4=option4,
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

    crawl_data = FranklinsportsCrawlData(
        category=category_text,
        product_url=product_url,
        sold_out_text=sold_out_text,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        price1=price1,
        price2=price2,
        quantity=quantity,
        detailed_images_html_source=detailed_images_html_source,
        message1=message1,
        option1="",
        option2="",
        option3="",
        option4="",
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
        extract_category_text(document),
        extract_product_name(document),
        extract_table(document, product_url),
        extract_options(page),
    )

    (R1, R2, R3, R4, R5) = await gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match R3:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R4:
        case Ok(table):
            (
                price1,
                price2,
                quantity,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R5:
        case Ok([options, options4]):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    detailed_images_html_source = await extract_html(
        page, product_url, html_top, html_bottom
    )

    return (
        thumbnail_image_url,
        category_text,
        product_name,
        price1,
        price2,
        quantity,
        options,
        options4,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#contents > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody > tr > td span[style='font-size:16px;color:#555555;font-weight:bold;']"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(AssertionError)
async def extract_options(page: PlaywrightPage):
    await page.wait_for_load_state("networkidle")

    options: list[str] = []
    options4: list[str] = []

    option1_radio_query = "div.detailArea > div.infoArea ul[class='ec-product-radio']"

    if option1 := await page.query_selector(option1_radio_query):
        for option1_radio in await option1.query_selector_all("li"):
            if option1_text := await option1_radio.text_content():
                options.append(option1_text)

                if not (
                    option4_before := await page.inner_text(
                        "#totalProducts > table > tfoot > tr > td > span.total em"
                    )
                ):
                    option4_before = ""

                (
                    await el.click()
                    if (el := await option1_radio.query_selector("input"))
                    else None
                )

                for i in range(1, 6):
                    if (
                        option4 := await page.inner_text(
                            "#totalProducts > table > tfoot > tr > td > span.total em"
                        )
                    ) and option4 == option4_before:
                        await page.wait_for_timeout(1000 * i)
                    else:
                        break
                else:
                    log.warning(
                        f"Option4 text has not changed after 5 attempts: {page.url}"
                    )

                if option4 := await page.inner_text(
                    "#totalProducts > table > tfoot > tr > td > span.total em"
                ):
                    options4.append(option4.strip())
                    with suppress(error.PlaywrightTimeoutError):
                        async with page.expect_navigation():
                            await page.click("#option_box1_del", timeout=2500)
                else:
                    options4.append("")
    else:
        option1_query = "select[id='product_option_id1']"
        option2_query = "select[id='product_option_id2']"

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
            if not (
                option4_before := await page.inner_text(
                    "#totalProducts > table > tfoot > tr > td > span.total em"
                )
            ):
                option4_before = ""

            # ? When there are a lot of requests at once, select_option() throws TimeoutError, so let's backoff here
            try:
                await page.select_option(
                    option1_query,
                    value=option1_value,
                )

                for i in range(1, 6):
                    if (
                        option4 := await page.inner_text(
                            "#totalProducts > table > tfoot > tr > td > span.total em"
                        )
                    ) and option4 == option4_before:
                        await page.wait_for_timeout(1000 * i)
                    else:
                        break
                else:
                    log.warning(
                        f"Option4 text has not changed after 5 attempts: {page.url}"
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
                    f"Timed out waiting for wait_for_load_state(): <blue>{page}</>"
                ) from err

            if option4 := await page.inner_text(
                "#totalProducts > table > tfoot > tr > td > span.total em"
            ):
                options4.append(option4.strip())
                with suppress(error.PlaywrightTimeoutError):
                    async with page.expect_navigation():
                        await page.click("#option_box1_del", timeout=2500)
            else:
                options4.append("")

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

                options.extend(
                    f"{option1_text}_{option2_text}" for option2_text in option2
                )
            else:
                options.append(option1_text)

        assert len(options) == len(
            options4
        ), f"Length of options and options4 must be equal: {len(options)} vs {len(options4)}"

    return options, options4


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document, product_url: str):
    query = "#span_product_price_text"
    if not (price1 := await document.text_content(query)):
        query = "th ~ td span[style='font-size:12px;color:#008BCC;font-weight:bold;']"
        if not (price1 := await document.text_content(query)):
            raise error.QueryNotFound("Price1 not found", query=query)
    else:
        try:
            price1 = parse_int(price1)
        except ValueError as err:
            raise ValueError(f"Coudn't convert price1 text {price1} to number") from err

    query = "#span_product_price_sale"
    if not (price2 := await document.inner_text(query)):
        log.warning(f"Price2 not found: <blue>{product_url}</>")
        price2 = ""
    else:
        try:
            price2 = parse_int(price2)
        except ValueError as err:
            raise ValueError(f"Coudn't convert price2 text {price2} to number") from err

    query = "#contents > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > p.info"
    if not (quantity := await document.inner_text(query)):
        raise error.QueryNotFound("Quantity not found", query=query)

    return (
        price1,
        price2,
        quantity.strip(),
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.keyImg > a > img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "#contents > div.xans-element-.xans-product.xans-product-headcategory.path > ol > li:not([class^='displaynone'])"
    if not (categories := await document.query_selector_all(query)):
        raise error.QueryNotFound("Categories text not found", query)

    return ">".join(
        [
            text.strip()
            for category in categories
            if (text := await category.text_content())
        ]
    )


@returns(IndexError, ValueError)
def split_options_text(option1: str):
    option3 = 0

    if "(+" in option1:
        regex = compile_regex(r"\(\+\w+[,]?\w*\)")
        option3 = regex.findall(option1)[0]
        option1 = regex.sub("", option1)

    if "(-" in option1:
        regex = compile_regex(r"\(\-\w+[,]?\w*\)")
        option3 = regex.findall(option1)[0]
        option1 = regex.sub("", option1)

    if "[품절]" in option1:
        return option1.replace("[품절]", "").strip(), "[품절]", option3 or ""
    if "(품절)" in option1:
        return option1.replace("(품절)", "").strip(), "(품절)", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", "").strip(), "품절", option3 or ""

    return option1.strip(), "", option3 or ""


@cache
def image_quries():
    return "#prdDetail > div.cont img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await page.click("#prdDetail > div.link > ul > li:nth-child(1) > a > img")

    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

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
        await element.click()
