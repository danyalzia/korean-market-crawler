# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from dataclasses import dataclass
from functools import cache, singledispatch
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
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.cutykids import config
from market_crawler.cutykids.data import CutyKidsCrawlData
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify import Err, MaxTriesReached, Ok, Result, do, isin, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    regex = compile_regex(r"&pg=\d+")
    if match := regex.search(current_url):
        return current_url.replace(match.group(), f"&pg={next_page_no}")

    regex = compile_regex(r"\?pg=\d+")
    if match := regex.search(current_url):
        return current_url.replace(match.group(), f"?pg={next_page_no}")

    return f"{current_url.removesuffix('/')}&pg={next_page_no}"


@dataclass(slots=True, frozen=True)
class Login:
    login_info: LoginInfo

    async def __call__(self, browser: PlaywrightBrowser) -> None:
        page = await browser.new_page()
        # ? Sometimes if there are network requests happening in the background, the "input" fields keep reverting to the original/previous state while typing
        await page.goto(self.login_info.login_url, wait_until="networkidle")

        if (
            text := await page.text_content("body > div > div > p")
        ) and "Please prove that you are human" in text:
            async with page.expect_navigation():
                await page.click("body > div > div > form > input[type=submit]")

        is_already_login = True
        try:
            await page.wait_for_selector(
                self.login_info.user_id_query,
                state="visible",
            )

            is_already_login = False
        except error.PlaywrightTimeoutError:
            is_already_login = True

        if not is_already_login:
            input_id = await page.query_selector(self.login_info.user_id_query)

            if input_id:
                await input_id.fill(self.login_info.user_id)
            else:
                raise error.LoginInputNotFound(
                    f"User ID ({self.login_info.user_id}) could not be entered"
                )

            if self.login_info.keep_logged_in_check_query:
                await page.check(self.login_info.keep_logged_in_check_query)

            input_password = await page.query_selector(
                self.login_info.password_query,
            )
            if input_password:
                await input_password.fill(self.login_info.password)
            else:
                raise error.PasswordInputNotFound(
                    f"Passowrd ({self.login_info.password}) could not be entered"
                )

            await self.login_info.login_button_strategy(
                page, self.login_info.login_button_query
            )

            log.success(
                f"Logged in <MAGENTA><w>(ID: {self.login_info.user_id}, PW: {self.login_info.password})</></>"
            )

        await page.close()


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='am_id']",
        password_query="input[name='am_pwd']",
        login_button_query='a:has-text("로그인")[onclick^="go_login()"]',
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
            browser_config=browser_config, playwright=playwright
        ).create()
        login = Login(login_info)
        await login(browser)

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
            category_name=category.name,
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

        if not (document := await parse_document(content, engine="lxml")):
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

    if not (document := await parse_document(content, engine="lxml")):
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

    sold_out_text = await extract_soldout_text(product)

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
    content = await page.content()

    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
    ) = await extract_data(document, page, product_url)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
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

    match R3:
        case Ok(table):
            brand, price3, percent, price2, message1, message2 = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R4:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.InvalidImageURL(err)):
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
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    await page.close()

    if options:
        for option1 in options:
            crawl_data = CutyKidsCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                price3=price3,
                price2=price2,
                brand=brand,
                percent=percent,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                message1=message1,
                message2=message2,
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

    crawl_data = CutyKidsCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        price3=price3,
        price2=price2,
        brand=brand,
        percent=percent,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        message1=message1,
        message2=message2,
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


async def extract_data(document: Document, page: PlaywrightPage, product_url: str):
    tasks = (
        extract_product_name(document),
        extract_thumbnail_images(document, product_url),
        extract_table(document),
        extract_images(document, product_url),
    )

    data = (*(await asyncio.gather(*tasks)), await extract_options(document, page))

    await page.close()

    return data


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"ai_id=(\d+\w+)&")
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
    query = 'xpath=//form[contains(table[name="간격조정"], "")]/table[5]/tbody/tr/td[@valign="top"]/table/tbody'
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element) -> str:
    if (
        (
            soldout := (
                await product.query_selector(
                    "tr > td > div.small > font[color='ff6100'][class='text8']"
                )
            )
        )
        and (soldout_text := await soldout.text_content())
        and "품절" in soldout_text
    ):
        return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = 'xpath=//tbody/tr/td[2]/font[@class="text13"]/b'
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#pic1 > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = 'table > tbody > tr > td > table.table > tbody > tr > td > a > img:not([style="display: none;"]'
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


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=3,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(document: Document, page: PlaywrightPage):
    options: list[str] = []

    option1_query = "xpath=//tr[contains(./td/text(), '색상')]/td/select[@name='color']"
    option2_query = (
        "xpath=//table/tbody/tr[contains(./td/text(), '치수')]/td[2]/table/tbody/tr/td"
    )

    option1_elements = await document.query_selector_all(f"{option1_query}/option")

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }
    if not (option2_elements := await document.query_selector_all(option2_query)):
        return list(option1.keys())

    for option1_text, option1_value in option1.items():
        # ? When there are a lot of requests at once, select_option() throws TimeoutError, so let's backoff here
        try:
            await page.select_option(
                option1_query,
                value=option1_value,
            )
        except error.PlaywrightTimeoutError as err:
            with suppress(error.PlaywrightTimeoutError):
                async with page.expect_navigation():
                    await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for select_option()"
            ) from err

        try:
            await page.wait_for_load_state("networkidle")
        except error.PlaywrightTimeoutError as err:
            with suppress(error.PlaywrightTimeoutError):
                async with page.expect_navigation():
                    await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for wait_for_load_state()"
            ) from err

        option2 = [
            "".join(text.split())
            for option2 in option2_elements
            if (text := await option2.text_content())
        ]

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


class Table(NamedTuple):
    brand: str
    price3: int
    percent: str
    price2: int
    percent: str
    message1: str
    message2: str


@backoff.on_exception(
    backoff.expo,
    Exception,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def extract_table(document: Document):
    brand = percent = message1 = ""
    price2 = price3 = 0

    try:
        brand, price2, price3, percent, message1, message2 = await asyncio.gather(
            extract_brand(document),
            extract_price2(document),
            extract_price3(document),
            extract_percent(document),
            extract_message1(document),
            extract_message2(document),
        )
    except (error.QueryNotFound, AttributeError) as err:
        return Err(err)

    try:
        price3 = parse_int(price3)
    except ValueError as err:
        raise ValueError(f"Couldn't convert price3 text {price3} to number") from err

    try:
        price2 = parse_int(price2)
    except ValueError as err:
        raise ValueError(f"Couldn't convert price2 text {price2} to number") from err

    return Ok(
        Table(
            brand=brand,
            price3=price3,
            percent=percent,
            price2=price2,
            message1=message1,
            message2=message2,
        )
    )


async def extract_brand(document: Document):
    query = 'xpath=//tbody/tr[contains(td/text(), "브랜드")]/td[2]/div[1]'
    if not (brand := await document.text_content(query)):
        raise error.QueryNotFound("Brand not found", query=query)

    brand = brand.strip()

    return brand


async def extract_percent(document: Document):
    query = 'xpath=//tbody/tr[contains(td[1]/text(), "할인율")]/td[2]/font'
    if not (percent := await document.text_content(query)):
        raise error.QueryNotFound("Percent not found", query=query)

    percent = percent.strip()

    return percent


async def extract_price2(document: Document):
    query = 'xpath=//tbody/tr[contains(td[1]/text(), "공급가")]/td[2]/font'
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    return price2


async def extract_price3(document: Document):
    query = 'xpath=//tbody/tr[contains(td[1]/text(), "시장가")]/td[2]'
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price3 not found", query=query)

    return price3


async def extract_message1(document: Document):
    fabric, size, registeration_date, memo = await asyncio.gather(
        document.text_content(
            'xpath=//tbody/tr[contains(td[1]/text(), "원") and contains(td[1]/text(), "단")]/td[2]'
        ),
        document.text_content(
            'xpath=//tbody/tr[contains(td[1]/text(), "사이즈")]/td[2]'
        ),
        document.text_content(
            'xpath=//tbody/tr[contains(td[1]/text(), "등록일")]/td[2]'
        ),
        document.text_content(
            'xpath=//tbody/tr[contains(td[1]/text(), "메") and contains(td[1]/text(), "모")]/td[2]'
        ),
    )

    fabric = (
        "원단: "
        + cast(
            str,
            fabric,
        ).strip()
    )

    size = (
        "사이즈: "
        + cast(
            str,
            size,
        ).strip()
    )

    registration_date = (
        "등록일: "
        + cast(
            str,
            registeration_date,
        ).strip()
    )

    memo = (
        "메모: "
        + cast(
            str,
            memo,
        ).strip()
    )

    return "\n".join([fabric, size, registration_date, memo])


async def extract_message2(document: Document):
    query = 'xpath=//table[@width="100%"]/tbody[contains(tr[1]/td[1][@height="5"], "")]/tr[2]/td[1]/font'
    if not (message2 := await document.text_content(query)):
        raise error.QueryNotFound("Message2 not found", query=query)

    message2 = message2.strip()

    return message2


@cache
def image_queries():
    return "body > div > form > div > img"


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
    query = image_queries()

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
    await document_or_page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    query = image_queries()
    elements = await document_or_page.query_selector_all(query)

    urls: list[str] = []
    for el in elements:
        action = lambda: get_src(el)
        focus = lambda: focus_element(document_or_page, el)
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


async def get_src(el: PlaywrightElementHandle):
    if attr := await el.get_attribute("src"):
        if (
            style := await el.get_attribute("style")
        ) and "visibility: hidden" not in style:
            return attr
        return attr
    return ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()
