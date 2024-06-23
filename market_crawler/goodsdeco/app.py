# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

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
from market_crawler.excel import save_series_csv, to_series
from market_crawler.goodsdeco import config
from market_crawler.goodsdeco.data import GoodsdecoCrawlData
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
def page_url(*, category_page_url: str, next_page_no: int) -> str:
    if "?page" in category_page_url:
        return category_page_url.replace(
            f"?page={next_page_no-1}", f"?page={next_page_no}"
        )
    if "&page" in category_page_url:
        return category_page_url.replace(
            f"&page={next_page_no-1}", f"&page={next_page_no}"
        )

    return f"{category_page_url}&page={next_page_no}"


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#loginId",
        password_query="#loginPwd",
        login_button_query="#formLogin > div.member_login_box > div.login_input_sec > button",
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
            browser_config=browser_config,
            playwright=playwright,
            login_info=login_info,
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
        category_page_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    while True:
        category_page_url = page_url(
            category_page_url=category_page_url, next_page_no=category_state.pageno
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

        if isinstance(number_of_products, str):
            log.warning(
                f"Page # {category_state.pageno} doesn't contain any crawlable products, so we are skipping this page"
            )
            category_state.pageno += 1
            category_html.pageno = category_state.pageno

            if config.USE_CATEGORY_SAVE_STATES:
                await category_state.save()
            continue

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


async def has_products(document: Document) -> int | str | None:
    match await get_products(document):
        case Ok(products):
            if isinstance(products, list):
                return len(products)
            return products
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


@cache
def match_price(text: str):
    regex = compile_regex(r"\d+[,]?\d*원")
    return bool(regex.match(text.strip()))


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "#contents > div > div.content > div.goods_list_item > div.goods_list > div > div.item_basket_type > ul > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    query = "div > div.item_info_cont > div.item_money_box > strong"
    if filtered_products := [
        product
        for product in products
        if match_price(
            await (await product.query_selector_all(query))[0].text_content()
        )
    ]:
        return filtered_products

    return "not crawlable"


@returns_future(error.QueryNotFound)
async def extract_soldout_text(product: Element):
    query = "div > div.item_info_cont > div.item_icon_box > img"
    icons = await product.query_selector_all(query)

    if not icons:
        raise error.QueryNotFound(
            "Sold out icon not found",
            query,
        )

    for icon in icons:
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

    match await get_products(document):
        case Ok(products):
            if isinstance(products, str):
                return
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
                if isinstance(products, str):
                    return
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

    match await extract_soldout_text(product):
        case Ok(sold_out_text):
            pass
        case Err(error.QueryNotFound(err)):
            sold_out_text = ""

    if "품절" in sold_out_text:
        log.debug(f"Sold out text is present: Product no: {idx+1}")

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        price2,
        quantity,
        delivery_fee,
        model_name,
        country,
        message1,
        detailed_images_html_source,
        options,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            if isinstance(price2, int):
                option1, price2, option2, option3 = split_options_text(option, price2)
            else:
                option1 = option
                option2 = option3 = ""

            crawl_data = GoodsdecoCrawlData(
                category=category_state.name,
                sold_out_text=sold_out_text,
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                manufacturing_country=country,
                price2=price2,
                delivery_fee=delivery_fee,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
                message1=message1,
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

    crawl_data = GoodsdecoCrawlData(
        category=category_state.name,
        sold_out_text=sold_out_text,
        product_url=product_url,
        product_name=product_name,
        model_name=model_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        manufacturing_country=country,
        price2=price2,
        delivery_fee=delivery_fee,
        quantity=quantity,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
        message1=message1,
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
        extract_product_name(document),
        extract_thumbnail_images(document, product_url),
        extract_table(document),
        extract_options(document),
        extract_html(page, product_url, html_top, html_bottom),
    )

    R1, R2, R3, R4, R5 = await asyncio.gather(*tasks)

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
            price2, quantity, delivery_fee, model_name, country, message1 = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    options = R4
    detailed_images_html_source = R5

    return (
        product_name,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        price2,
        quantity,
        delivery_fee,
        model_name,
        country,
        message1,
        detailed_images_html_source,
        options,
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "div.item_photo_info_sec div.item_photo_slide > ul > div > div > li"
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
    query = "#frmView > div > div > div.item_detail_tit > h3"
    return (
        product_name.replace("(해외배송 가능상품)", "").strip()
        if (product_name := await document.text_content(query))
        else ""
    )


class Table(NamedTuple):
    price2: int | str
    quantity: str
    delivery_fee: int | str
    model_name: str
    country: str
    message1: str


@returns_future(IndexError)
async def extract_table(document: Document):
    quantity = delivery_fee = model_name = country = message1 = ""
    price2 = 0

    dts = [
        text.strip()
        for t in await document.query_selector_all(
            "#frmView > div > div > div.item_detail_list > dl > dt"
        )
        if (text := await t.text_content())
    ]
    dds = [
        text.strip()
        for t in await document.query_selector_all(
            "#frmView > div > div > div.item_detail_list > dl > dd"
        )
        if (text := await t.text_content())
    ]
    for i, (dt, dd) in enumerate(zip(dts, dds), start=1):
        if dt == "구매제한":
            quantity = dd

        elif dt == "배송비":
            # ? We want only the price part and not the full paragraph of detailed text
            if delivery_fee_text := (
                await (
                    await document.query_selector_all(
                        f"#frmView > div > div > div.item_detail_list > dl:nth-child({i}) > dd > strong"
                    )
                )[0].text_content()
            ):
                delivery_fee = delivery_fee_text.strip()

        elif dt == "상품코드":
            model_name = dd

        elif dt == "원산지":
            country = dd

        elif dt == "판매가":
            price2_text = dd
            try:
                price2 = parse_int(price2_text)
            except ValueError:
                price2 = price2_text

    query = "#detail > div.detail_cont > div > div.txt-manual > div:nth-child(1) > b[style='color: rgb(255, 0, 0); font-size: 18pt;']"
    if (message1_element := await document.query_selector(query)) and (
        message1_text := (await message1_element.text_content())
    ):
        message1 = message1_text.strip()

    return Table(price2, quantity, delivery_fee, model_name, country, message1)


def split_options_text(option1: str, price2: int):
    option3 = ""
    if ":+" in option1:
        regex = compile_regex(r"[:]?\s*?\+\w+[,]?\w*원")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = parse_int(additional_price)
        price2 += option3

    if ":-" in option1:
        regex = compile_regex(r"[:]?\s*?\-\w+[,]?\w*원")
        additional_price = regex.findall(option1)[0]
        option1 = regex.sub("", option1)
        option3 = parse_int(additional_price)
        price2 -= option3

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3

    return option1, price2, "", option3


async def extract_options(document: Document):
    option1_query = "#frmView > div > div > div.item_detail_list > div > dl > dd > select[name='optionSnoInput']"
    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    options = {
        await el.get_attribute("value"): "".join((text).split())
        for el in option1_elements
        if (text := await el.text_content())
    }
    return [text for value, text in options.items() if value not in ["", "*", "**"]]


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    query = "#detail > div.detail_cont > div > div.txt-manual > div > img, #detail > div.detail_cont > div > div.txt-manual > center > img, #detail > div.detail_cont > div > div.txt-manual img"

    if elements := await page.query_selector_all(query):
        await page.click(query)
    else:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

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
        await element.click()
        await page.wait_for_timeout(1000)
