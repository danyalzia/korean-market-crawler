# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import contextlib

from functools import cache
from typing import cast
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
from market_crawler.bonniepet import config
from market_crawler.bonniepet.data import BonniePetCrawlData
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
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)
    await page.wait_for_selector("text='로그아웃'")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#loginId",
        password_query="#loginPwd",
        login_button_query="#formLogin > div.login > button",
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


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsNo=(\d*\w*)")
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
    query = ".item-display li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    if not (product_name := (await extract_product_name(document)).ok()):
        await visit_link(page, product_url, wait_until="networkidle")
        if not (document := await parse_document(content, engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

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
            # ? See: http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000017131
            log.warning(f"Thumbnail not fouind: {product_url}")
            thumbnail_image_url = thumbnail_image_url2 = thumbnail_image_url3 = (
                thumbnail_image_url4
            ) = thumbnail_image_url5 = ""

    (
        manufacturer,
        manufacturing_country,
        quantity,
        sold_out_text,
        price2,
        price3,
        delivery_fee,
        model_name,
    ) = await extract_table(document, page, product_url)

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

    match await extract_options(page):
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    await page.close()

    if options:
        for option in options:
            match split_options_text(option):
                case Ok((option1, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = BonniePetCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                quantity=str(quantity),
                price3=price3,
                price2=price2,
                delivery_fee=delivery_fee,
                model_name=model_name,
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

    crawl_data = BonniePetCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        quantity=str(quantity),
        price3=price3,
        price2=price2,
        delivery_fee=delivery_fee,
        model_name=model_name,
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


async def extract_table(document: Document, page: PlaywrightPage, product_url: str):
    match await extract_manufacturer(page):
        case Ok(manufacturer):
            pass
        case Err(err):
            # ? Some products don't have manufacturer
            # ? See: http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000016960
            log.debug(f"Manufacturer not found: {product_url}")
            manufacturer = ""

    match await extract_manufacturing_country(page):
        case Ok(manufacturing_country):
            pass
        case Err(err):
            # ? Some products don't have manufacturing country
            # ? See: http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000016958
            log.debug(f"Manufacturing country not found: {product_url}")
            manufacturing_country = ""

    match await extract_quantity(page):
        case Ok(quantity):
            pass
        case Err(err):
            raise error.QuantityNotFound(err, url=product_url)

    sold_out_text = await extract_soldout_text(document)

    price2, price3, delivery_fee = await extract_prices(page)

    match await extract_model_name(page):
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    return (
        manufacturer,
        manufacturing_country,
        quantity,
        sold_out_text,
        price2,
        price3,
        delivery_fee,
        model_name,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = ".goods-header h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturer(page: PlaywrightPage):
    query = 'li:has(strong:has-text("제조사")) div'
    if not (manufacturer := await page.text_content(query)):
        raise error.QueryNotFound("Manufacturer not found", query)

    return manufacturer.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturing_country(page: PlaywrightPage):
    query = 'li:has(strong:has-text("원산지")) div'
    if not (manufacturing_country := await page.text_content(query)):
        raise error.QueryNotFound("Manufacturer country not found", query)

    return manufacturing_country.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_quantity(page: PlaywrightPage):
    query = 'li:has(strong:has-text("구매제한")) span'
    if not (quantity := await page.query_selector(query)):
        raise error.QueryNotFound("Quantity not found", query)

    return parse_int(await quantity.text_content())


async def extract_soldout_text(document: Document):
    if (text := await document.text_content("a.soldout > em")) and text == "구매 불가":
        return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div.thumbnail > a > img, a[id='mainImage'] > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = (
        "div.more-thumbnail > div.slide > div > div > div > span.swiper-slide > a > img"
    )
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


async def extract_prices(page: PlaywrightPage):
    price2, price3, delivery_fee = 0, 0, 0
    p_2 = await page.query_selector('li:has(strong:has-text("판매가")) div strong')
    p_3 = await page.query_selector('li:has(strong:has-text("정가")) div span')
    if p_2:
        with contextlib.suppress(Exception):
            price2 = parse_int(cast(str, await p_2.text_content()))
    if p_3:
        with contextlib.suppress(Exception):
            price3 = parse_int(cast(str, await p_3.text_content()))
    if delivery_fee_text := await page.text_content("li.delivery > span"):
        with contextlib.suppress(Exception):
            delivery_fee = parse_int(delivery_fee_text)
    return price2, price3, delivery_fee


@returns_future(error.QueryNotFound)
async def extract_model_name(page: PlaywrightPage):
    query = 'li:has(strong:has-text("상품코드")) div'
    if not (model_name := await page.text_content(query)):
        raise error.QueryNotFound("Model name not found", query)

    return model_name.strip()


@returns(IndexError, ValueError)
def split_options_text(option1: str):
    option2 = option3 = ""

    if ":" in option1 and "개" in option1:
        regex = compile_regex(r"[:]?\s*?\w+[,]?\w*개")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option2 = additional_price.replace(":", "").strip()

    if ":+" in option1 and "원" in option1:
        regex = compile_regex(r"[:]?\s*?\+\w+[,]?\w*원")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = additional_price.replace(":", "").strip()

    if ":-" in option1 and "원" in option1:
        regex = compile_regex(r"[:]?\s*?\-\w+[,]?\w*원")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = additional_price.replace(":", "").strip()

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), "[품절]", option3 or ""

    if "품절" in option1:
        return option1.replace("품절", ""), "품절", option3 or ""

    return option1, option2, option3


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

    # ? Only when first option is present, we need to click on it
    if await page.query_selector(
        '#frmView > div > div.choice > div > div select[name="optionSnoInput"]'
    ):
        await page.click("#frmView > div > div.choice > div > div > div > a")

    option1_query = 'select[name="optionNo_0"], #frmView > div > div.choice > div > div select[name="optionSnoInput"]'
    option2_query = 'select[name="option2"]'

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value")) not in ["", "*", "**"]
    }

    for option1_text, option1_value in option1.items():
        if option2_elements := await page.query_selector_all(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        ):
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

            option2 = {
                "".join(text.split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                not in ["", "*", "**"]
            }

            options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)
        else:
            options.append(option1_text)

    return options


@cache
def image_quries():
    return ".txt-manual img"


@returns_future(error.QueryNotFound)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    with contextlib.suppress(error.PlaywrightTimeoutError):
        await page.click("#detail > div.tab > a")

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

    if any("이용 기간이 만료" in url for url in urls):
        raise error.InvalidImageURL("Expiration period text in images")

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("ec-data-src, src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await page.click("#detail > div.tab > a")

    with contextlib.suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
