# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import NamedTuple, cast
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import fetch_content, load_content, parse_document, visit_link
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
from market_crawler.rockwall import config
from market_crawler.rockwall.data import RockwallCrawlData
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


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )

    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright
        ).create()

        subcategories = await get_categories(sitename=config.SITENAME)
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

    log.action.visit_category(category.name, category_page_url)

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
        content = await fetch_content(
            browser=browser,
            url=category_page_url,
            rate_limit=config.DEFAULT_RATE_LIMIT,
        )
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
                raise error.ProductsNotFound(err, url=category_page_url)

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
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price3,
        price2,
        quantity,
        delivery_fee,
        model_name,
        message1,
        brand,
        manufacturer,
        manufacturing_country,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option in options:
            if isinstance(price2, int):
                match split_options_text(option, price2):
                    case Ok((option1, _price2, option2, option3)):
                        pass
                    case Err(err):
                        raise error.IncorrectData(
                            f"Could not split option text ({option}) into price2 due to an error -> {err}",
                            url=product_url,
                        )

                crawl_data = RockwallCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    model_name=model_name,
                    price3=price3,
                    price2=_price2,
                    quantity=quantity,
                    delivery_fee=delivery_fee,
                    message1=message1,
                    brand=brand,
                    manufacturer=manufacturer,
                    manufacturing_country=manufacturing_country,
                    detailed_images_html_source=detailed_images_html_source,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2=option2,
                    option3=str(option3),
                )
            else:
                crawl_data = RockwallCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    model_name=model_name,
                    price3=price3,
                    price2=price2,
                    quantity=quantity,
                    delivery_fee=delivery_fee,
                    message1=message1,
                    brand=brand,
                    manufacturer=manufacturer,
                    manufacturing_country=manufacturing_country,
                    detailed_images_html_source=detailed_images_html_source,
                    sold_out_text=sold_out_text,
                    option1=option,
                    option2="",
                    option3="",
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

    crawl_data = RockwallCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        price3=price3,
        price2=price2,
        quantity=quantity,
        delivery_fee=delivery_fee,
        message1=message1,
        brand=brand,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
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


class Data(NamedTuple):
    thumbnail_image_url: str
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    product_name: str
    price3: int | str
    price2: int | str
    quantity: str
    delivery_fee: int | str
    model_name: str
    message1: str
    brand: str
    manufacturer: str
    manufacturing_country: str
    options: list[str]
    detailed_images_html_source: str


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
        extract_table(document, product_url),
        extract_options(page),
    )

    (
        R1,
        R2,
        R3,
        R4,
    ) = await asyncio.gather(*tasks)

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

    (
        price3,
        price2,
        quantity,
        delivery_fee,
        model_name,
        message1,
        brand,
        manufacturer,
        manufacturing_country,
    ) = R3

    options = R4

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price3,
        price2,
        quantity,
        delivery_fee,
        model_name,
        message1,
        brand,
        manufacturer,
        manufacturing_country,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goodsNo=(\d*\w*)")
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


async def get_products(documeny: Document):
    query = "#contents > div > div > div.goods_list_item > div.goods_list > div > div > ul > li"
    return (
        Ok(products)
        if (products := await documeny.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all(
        "div > div.item_info_cont > div.item_icon_box > img"
    ):
        alt = str(await icon.get_attribute("alt"))
        if "품절" in alt:
            return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#frmView > div > div > div.item_detail_tit > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = (
        "div.item_photo_info_sec  div.item_photo_slide > ul > div > div > li > a > img"
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


class Table(NamedTuple):
    price3: int | str
    price2: int | str
    quantity: str
    delivery_fee: int | str
    model_name: str
    message1: str
    brand: str
    manufacturer: str
    manufacturing_country: str


async def extract_table(document: Document, product_url: str):
    quantity = delivery_fee = model_name = message1 = brand = manufacturer = (
        manufacturing_country
    ) = ""
    price3 = price2 = 0
    model_name_found = False

    for dl in await document.query_selector_all(
        "#frmView > div > div > div.item_detail_list > dl"
    ):
        dt = cast(
            str, await (await dl.query_selector_all("dt"))[0].text_content()
        ).strip()

        dd = cast(
            str, await (await dl.query_selector_all("dd"))[0].text_content()
        ).strip()

        if model_name_found:
            message1 += f"{dt}: {dd}\n"
        if dt == "배송비":
            delivery_fee_text = cast(
                str,
                await (await dl.query_selector_all("dd > strong"))[0].text_content(),
            ).strip()

            try:
                delivery_fee = parse_int(delivery_fee_text)
            except ValueError:
                log.warning(
                    f"Unusual delivery fee ({delivery_fee_text}): {product_url}"
                )
                delivery_fee = delivery_fee_text
        if dt == "구매제한":
            quantity = dd
        elif dt == "상품코드":
            model_name = dd
            model_name_found = True
        if dt == "정가":
            price3_text = dd
            try:
                price3 = parse_int(price3_text)
            except ValueError:
                price3 = price3_text
        if dt == "판매가":
            price2_text = dd
            try:
                price2 = parse_int(price2_text)
            except ValueError:
                price2 = price2_text

        if dt == "브랜드":
            brand = dd

        elif dt == "원산지":
            manufacturing_country = dd

        elif dt == "제조사":
            manufacturer = dd

    return Table(
        price3,
        price2,
        quantity,
        delivery_fee,
        model_name,
        message1,
        brand,
        manufacturer,
        manufacturing_country,
    )


async def extract_options(page: PlaywrightPage):
    await page.wait_for_load_state("networkidle", timeout=30000)

    options1_list: list[str] = []

    option1_query = "#frmView > div > div > div.item_detail_list > div > dl > dd > select[name='optionSnoInput']"
    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    if option1_elements:
        await page.click(
            "#frmView > div > div > div.item_detail_list > div > dl > dd > div > a"
        )

    for i in range(len(option1_elements)):
        option1_elements = await page.query_selector_all(
            f"{option1_query} > option, {option1_query} > optgroup > option"
        )
        option1_value: str = cast(str, await option1_elements[i].get_attribute("value"))
        options1_str: str = "".join(
            cast(str, await option1_elements[i].text_content()).split()
        )

        if option1_value not in ["", "*", "**"]:
            options1_list.append(options1_str)

    return options1_list


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if ":+" in option1:
        regex = compile_regex(r"[:]?\s*?\+\w+[,]?\w*원")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if ":-" in option1:
        regex = compile_regex(r"[:]?\s*?\-\w+[,]?\w*원")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    if ":" in option1 and "개" in option1:
        regex = compile_regex(r"[:]\s*?\w+[,]?\w*개")
        option2 = regex.findall(option1)[0].replace(":", "").strip()
        option1 = regex.sub("", option1)
        return option1.replace("[품절]", ""), price2, option2, option3 or ""

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return ", ".join(
        [
            "#detail > div.detail_cont > div > div.txt-manual > div > img",
            "#detail > div.detail_cont > div > div.txt-manual > center > img",
        ]
    )


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    await page.click('#detail > div.item_goods_tab > ul > li > a[href="#detail"]')

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
        await page.wait_for_timeout(1000)
