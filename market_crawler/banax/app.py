# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

import pandas as pd

from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser
from market_crawler import error, log
from market_crawler.banax import config
from market_crawler.banax.data import BanaxCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML, ProductHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?pg" in current_url:
        return current_url.replace(f"?pg={next_page_no-1}", f"?pg={next_page_no}")
    if "&pg" in current_url:
        return current_url.replace(f"&pg={next_page_no-1}", f"&pg={next_page_no}")

    return f"{current_url}&pg={next_page_no}"


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

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            categories = await get_categories(
                sitename=config.SITENAME,
                rate_limit=1,
            )
            log.detail.total_categories(len(categories))

            crawler = ConcurrentCrawler(
                categories=categories,
                start_category=config.START_CATEGORY,
                end_category=config.END_CATEGORY,
                chunk_size=config.CATEGORIES_CHUNK_SIZE,
                crawl=crawl,
            )
            await crawl_categories(crawler, browser, settings, columns)
            return None

        await crawl_urls(
            list(dict.fromkeys(settings.URLS)),
            browser,
            settings,
            columns,
        )


async def crawl_urls(
    urls: list[str], browser: PlaywrightBrowser, settings: Settings, columns: list[str]
):
    series: list[dict[str, str | int]] = []
    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(
                idx,
                browser,
                settings,
                urls[idx],
                series,
            )
            for idx in chunk
        )

        await asyncio.gather(*tasks)

    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )
    await asyncio.to_thread(
        pd.DataFrame(series, columns=columns).to_csv,  # type: ignore
        filename,  # type: ignore
        encoding="utf-8-sig",
        index=False,
    )


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    settings: Settings,
    product_url: str,
    series: list[dict[str, str | int]],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, product_url)

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        category_text,
        manufacturing_country,
        maker,
        model_names,
        options_price2_soldout_list,
        detailed_images_html_source,
    ) = await extract_data(
        document,
        product_url,
        html_top,
        html_bottom,
    )

    await page.close()

    if not options_price2_soldout_list:
        raise error.OptionsNotFound("Options not present", product_url)
    try:
        assert len(model_names) == len(options_price2_soldout_list)
    except AssertionError as err:
        # ? This page contains 16 soldout list items, but all of them are out of stock, and there is only one model name without any model name per option/soldout, so it's very unique in this respect
        if (
            product_url
            == "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12628&cate=0152_0156_&cate2=0152_"
        ) or len(model_names) == 1:
            log.warning(
                f"Skipping -> {product_url} <- because of unusual product listing (out of stock)"
            )
        else:
            raise AssertionError(
                f"Model names and options are not equal in length: {len(model_names)} vs {len(options_price2_soldout_list)} | {product_url}"
            ) from err

    tasks = (
        get_option_price_and_soldout(options_price2_soldout)
        for options_price2_soldout in options_price2_soldout_list
    )

    for model_name, options_price2_soldout in zip(model_names, await gather(*tasks)):
        match options_price2_soldout:
            case Ok(data):
                option1, price2, sold_out_text = data
            case Err(err):
                raise error.SupplyPriceNotFound(err, url=product_url)

        crawl_data = BanaxCrawlData(
            category=category_text,
            product_url=product_url,
            product_name=product_name,
            thumbnail_image_url=thumbnail_image_url,
            manufacturing_country=manufacturing_country,
            manufacturer=maker,
            detailed_images_html_source=detailed_images_html_source,
            model_name=model_name,
            price2=int(price2),
            option1=option1,
            sold_out_text=sold_out_text,
        )

        series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled_with_options(
        idx,
        product_url,
        len(model_names),
    )
    return None


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

    log.action.visit_category(category.name, category_page_url)

    if not (
        total_products_text := await document.text_content(
            "#contents > div > div > div.list_prod_wrap > div > div.list_prod_top > p.total > span.t2"
        )
    ):
        raise error.TotalProductsTextNotFound(
            "Total products text is not found on the page", url=category_page_url
        )

    log.detail.total_products_in_category(category.name, parse_int(total_products_text))

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

    productid = (await get_productid(product_url)).expect(
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

    product_html = ProductHTML(
        category_name=category_state.name,
        pageno=category_state.pageno,
        productid=productid,
        date=category_state.date,
        sitename=config.SITENAME,
    )

    content = await load_content(
        browser=browser,
        url=product_url,
        html=product_html,
        on_failure="fetch",
        wait_until="networkidle",
        async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
        rate_limit=config.DEFAULT_RATE_LIMIT,
    )
    if config.SAVE_HTML and not await product_html.exists():
        await product_html.save(content)

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        category_text,
        manufacturing_country,
        maker,
        model_names,
        options_price2_soldout_list,
        detailed_images_html_source,
    ) = await extract_data(
        document,
        product_url,
        html_top,
        html_bottom,
    )

    if not options_price2_soldout_list:
        raise error.OptionsNotFound("Options not present", product_url)
    try:
        assert len(model_names) == len(options_price2_soldout_list)
    except AssertionError as err:
        # ? This page contains 16 soldout list items, but all of them are out of stock, and there is only one model name without any model name per option/soldout, so it's very unique in this respect
        if (
            product_url
            == "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12628&cate=0152_0156_&cate2=0152_"
        ) or len(model_names) == 1:
            log.warning(
                f"Skipping -> {product_url} <- because of unusual product listing (out of stock)"
            )
        else:
            raise AssertionError(
                f"Model names and options are not equal in length: {len(model_names)} vs {len(options_price2_soldout_list)} | {product_url}"
            ) from err

    tasks = (
        get_option_price_and_soldout(options_price2_soldout)
        for options_price2_soldout in options_price2_soldout_list
    )

    for model_name, options_price2_soldout in zip(model_names, await gather(*tasks)):
        match options_price2_soldout:
            case Ok(data):
                option1, price2, sold_out_text = data
            case Err(err):
                raise error.SupplyPriceNotFound(err, url=product_url)

        crawl_data = BanaxCrawlData(
            category=category_text,
            product_url=product_url,
            product_name=product_name,
            thumbnail_image_url=thumbnail_image_url,
            manufacturing_country=manufacturing_country,
            manufacturer=maker,
            detailed_images_html_source=detailed_images_html_source,
            model_name=model_name,
            price2=int(price2),
            option1=option1,
            sold_out_text=sold_out_text,
        )

        await save_series_csv(
            to_series(crawl_data, settings.COLUMN_MAPPING),
            columns,
            filename,
        )

    log.action.product_crawled_with_options(
        idx,
        category_text,
        category_state.pageno,
        product_url,
        len(options_price2_soldout_list),
    )
    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    return None


class Data(NamedTuple):
    product_name: str
    thumbnail_image_url: str
    category_text: str
    manufacturing_country: str
    maker: str
    model_names: list[str]
    options_price2_soldout_list: list[str]
    detailed_images_html_source: str


async def extract_data(
    document: Document, product_url: str, html_top: str, html_bottom: str
):
    (R1, R2, R3, R4, R5) = await asyncio.gather(
        extract_product_name(document),
        extract_thumbnail_image(document, product_url),
        extract_category_text(document),
        extract_table(document),
        extract_images(document, product_url, html_top, html_bottom),
    )

    if (product_name := R1.ok()) is None:
        raise error.ProductNameNotFound(product_url)

    match R2:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R3:
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match R4:
        case Ok(
            Table(
                manufacturing_country,
                maker,
                model_names,
                options_price2_soldout_list,
            )
        ):
            pass
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R5:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return Data(
        product_name,
        thumbnail_image_url,
        category_text,
        manufacturing_country,
        maker,
        model_names,
        options_price2_soldout_list,
        detailed_images_html_source,
    )


async def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"p_idx=(\w+)&")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "p.prod_img"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#contents > div > div > div.view_top_wrap > div > div.info_wrap > form > div > div.info_top > div > p"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = (
        "#contents > div > div > div.view_top_wrap > div > div.img_wrap > div > p > img"
    )
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "div.info_wrap form div.info_top > p.nav"
    if not (category_text := await document.text_content(query)):
        raise error.QueryNotFound("Category text not found", query)

    return category_text.strip()


class Table(NamedTuple):
    manufacturing_country: str
    maker: str
    model_names: list[str]
    options_price2_soldout_list: list[str]


@returns_future(error.QueryNotFound, AssertionError)
async def extract_table(document: Document):
    manufacturing_country = maker = ""
    model_names: list[str] = []
    options_price2_soldout_list: list[str] = []

    query = "#contents > div > div > div.view_top_wrap > div > div.info_wrap > form > div > div.info_middle > table > tbody"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table is not present", query=query)

    tasks = (
        table_tbody.query_selector_all("tr"),
        table_tbody.query_selector_all("tr > th"),
        table_tbody.query_selector_all("tr > td"),
    )
    tr_elements, th_elements, td_elements = await asyncio.gather(*tasks)

    assert tr_elements, "<tr> elements not found"
    assert th_elements, "<th> elements not found"
    assert td_elements, "<td> elements not found"

    for th, td in zip(th_elements, td_elements, strict=True):
        if not (th_str := await th.text_content()):
            continue

        if not (td_str := await td.text_content()):
            continue

        if "제조국" in th_str:
            manufacturing_country = td_str
        elif "제조사" in th_str:
            maker = td_str
        elif "코드번호" in th_str:
            model_names = [x.strip() for x in td_str.split(",")]
        elif "사이즈" in th_str:
            options_price2_soldout_list.extend(
                [
                    text
                    for p in await td.query_selector_all("p")
                    if (text := await p.text_content())
                ]
            )

            options_price2_soldout_list.extend(
                [
                    text.strip()
                    for p in await td.query_selector_all("select option")
                    if (text := await p.text_content()) and "선택하세요" not in text
                ]
            )

    return Table(manufacturing_country, maker, model_names, options_price2_soldout_list)


class SplitOptionData(NamedTuple):
    option: str
    price: str
    soldout: str


@returns_future(ValueError)
async def get_option_price_and_soldout(options: str):
    log.debug(f"Option: {options}")

    splitted_options = options.split(" \xa0")
    if len(splitted_options) == 1:
        splitted_options = options.split("\n")

    log.debug(f"Option splitted: {splitted_options}")

    match splitted_options:
        case [option, price]:
            price = price.strip()
            option = option.strip()
            if len(price_split := price.split()) > 1:
                price = price_split[0].strip()
                soldout = price_split[1].strip()
            else:
                soldout = ""
        case [option, price, soldout]:
            option = option.strip()
            price = price.strip()
            soldout = ""
        case [option]:
            option = price = option.strip()
            soldout = ""
        case _:
            raise ValueError(f"Price is not present in option ({options})")

    price = "".join(filter(lambda x: x.isdigit(), price))

    log.debug(f"Option data: {(option, price, soldout)}")
    return SplitOptionData(option, price, soldout)


@cache
def image_quries():
    return ", ".join(
        [
            "#view_wrap1 > div.view_detail > center > img",
            "#view_wrap1 > div.view_detail > p > img",
            "#view_wrap1 > div.view_detail > center:nth-child(24) > center > center:nth-child(2) > img",
            "#view_wrap1 > div.view_detail > center > center > center > img",
            "#view_wrap1 > div.view_detail > img",
            "#view_wrap1 > div.view_detail > div > div > b > span > img",
            "#view_wrap1 > div.view_detail > center > strong > img",
            "#view_wrap1 > div.view_detail > div > img",
            "#view_wrap1 > div.view_detail > span > p > img",
            "#view_wrap1 > div.view_detail > div > center > div > b > span > img",
            "#view_wrap1 > div.view_detail > div > b > span > img",
            "#view_wrap1 > div.view_detail img",
        ]
    )


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = dict.fromkeys(
        [
            urljoin(product_url, src)
            for image in await document.query_selector_all(query)
            if (src := await image.get_attribute("src"))
        ]
    )

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    if any("base64" in url for url in urls):
        raise error.InvalidImageURL("Base64 is present in images")

    html_source = html_top

    query = "#view_wrap1 > div.view_detail > center > iframe, #view_wrap1 > div.view_detail > div > iframe"

    for frame in await document.query_selector_all(query):
        video_src = await frame.get_attribute("src")

        html_source = "".join(
            [
                html_source,
                f"""<iframe width="854" height="480" src="{video_src}" frameborder="0" allowfullscreen="" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"></iframe><br />""",
            ]
        )

    for image_url in urls:
        html_source = "".join(
            [
                html_source,
                f"<img src='{image_url}' /><br />",
            ]
        )

    html_source = "".join(
        [
            html_source,
            html_bottom,
        ],
    )
    return html_source.strip()
