# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from functools import cache
from typing import cast
from urllib.parse import urljoin

import pandas as pd

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import parse_document, visit_link
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.initialization import Category, get_categories
from market_crawler.nsrod import config
from market_crawler.nsrod.data import NSrodCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def visit_subcategory(page: PlaywrightPage):
    query = "#goodsSearchForm > form > table.gsf-category-table > tbody > tr > td > table > tbody > tr > td > a"
    subcategories = await page.query_selector_all(query)
    total_subcategories = len(subcategories)

    category_page_url = page.url
    log.info(
        f"Total sub-categories (for <blue>{category_page_url}</>): {total_subcategories}"
    )
    for idx1 in range(total_subcategories):
        await page.goto(category_page_url)
        if not (
            subcategory_text := await (await page.query_selector_all(query))[
                idx1
            ].text_content()
        ):
            continue

        subcategory_text = subcategory_text.strip()

        async with page.expect_navigation():
            await (await page.query_selector_all(query))[idx1].click()

        log.info(f"Sub-category: <blue>{subcategory_text}</>")

        sub_category_page_url = page.url

        sub_subcategories = await page.query_selector_all(query)
        total_sub_subcategories = len(sub_subcategories)
        log.info(
            f"Total sub sub-categories (for <blue>{subcategory_text}</>): {total_sub_subcategories}"
        )

        for idx2 in range(total_sub_subcategories):
            await page.goto(sub_category_page_url)
            # ? Sometimes we get timeouts here
            try:
                async with page.expect_navigation():
                    await (await page.query_selector_all(query))[idx2].click()
            except error.PlaywrightTimeoutError:
                await page.goto(sub_category_page_url)
                async with page.expect_navigation():
                    await (await page.query_selector_all(query))[idx2].click()

            sub_subcategory_text = cast(
                str,
                await (await page.query_selector_all(query))[idx2].text_content(),
            ).strip()

            log.info(f"Sub sub-category: <blue>{sub_subcategory_text}</>")

            yield subcategory_text, sub_subcategory_text, page.url


async def get_subcategories(browser: PlaywrightBrowser, categories: list[Category]):
    subcategories: list[Category] = []

    if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
        subcategories = await get_categories(
            sitename=config.SITENAME, filename="subcategories.txt"
        )
    else:
        page = await browser.new_page()
        for category in categories:
            await visit_link(page, category.url)
            async for subcategory, sub_subcategory, url in visit_subcategory(page):
                if subcategory:
                    if sub_subcategory:
                        subcategories.append(
                            Category(
                                category.name
                                + "_"
                                + subcategory
                                + "_"
                                + sub_subcategory,
                                url,
                            )
                        )
                    else:
                        subcategories.append(
                            Category(category.name + "_" + subcategory, url)
                        )
                else:
                    subcategories.append(Category(category.name, url))

        await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as f:
        await f.write(
            "{}".format("\n".join(cat.name + ", " + cat.url for cat in subcategories))
        )

    return subcategories


async def run(settings: Settings):
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright
        ).create()
        categories = await get_categories(sitename=config.SITENAME)

        # subcategories = await get_subcategories(browser, categories)

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

    category_page_url = page_url(
        current_url=category.url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category.name, category_page_url)

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        page = await browser.new_page()
        await visit_link(
            page,
            category_page_url,
            wait_until="networkidle",
        )

        # ? Sometimes website block access if there are a lot of requests at once
        while await page.query_selector("body:has-text('초동안 접속차단')"):
            await asyncio.sleep(1)
            await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()
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
                    filename,
                    settings,
                    columns,
                )
                for idx in chunk
            )

            await asyncio.gather(*tasks)

        log.action.category_page_crawled(category_state.name, category_state.pageno)

        category_state.pageno += 1

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
    filename: str,
    settings: Settings,
    columns: list[str],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="networkidle")

    while await page.query_selector("body:has-text('초동안 접속차단')"):
        await asyncio.sleep(1)
        await visit_link(page, category_page_url, wait_until="networkidle")

    content = await page.content()

    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    if not (products := (await get_products(document)).ok()):
        await asyncio.sleep(1)
        await visit_link(page, category_page_url, wait_until="networkidle")

        while await page.query_selector("body:has-text('초동안 접속차단')"):
            await asyncio.sleep(1)
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
        await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()

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

    await visit_link(page, product_url, wait_until="networkidle")

    while await page.query_selector("body:has-text('초동안 접속차단')"):
        await asyncio.sleep(1)
        await visit_link(page, product_url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        thumbnail_image_url,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    match await extract_table(page, content, product_url):
        case list(table_list):
            if len(table_list) > 1:
                for table in table_list:
                    crawl_data = NSrodCrawlData(
                        category=category_state.name,
                        product_url=product_url,
                        product_name=product_name,
                        thumbnail_image_url=thumbnail_image_url,
                        detailed_images_html_source=detailed_images_html_source,
                        model_name=table.model_name,
                        price2=table.price2,
                    )

                    await save_series_csv(
                        to_series(crawl_data, settings.COLUMN_MAPPING),
                        columns,
                        filename,
                    )

                log.action.product_crawled_with_options(
                    idx,
                    category_state.name,
                    category_state.pageno,
                    product_url,
                    len(table_list),
                )
                product_state.done = True
                if config.USE_PRODUCT_SAVE_STATES:
                    await product_state.save()

            else:
                try:
                    model_name = table_list[0].model_name
                    price2 = table_list[0].price2
                except IndexError:
                    model_name = ""
                    price2 = 0

                crawl_data = NSrodCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    product_name=product_name,
                    thumbnail_image_url=thumbnail_image_url,
                    detailed_images_html_source=detailed_images_html_source,
                    model_name=model_name,
                    price2=price2,
                )
                product_state.done = True
                if config.USE_PRODUCT_SAVE_STATES:
                    await product_state.save()

                await save_series_csv(
                    to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
                )

                log.action.product_crawled(
                    idx,
                    crawl_data.category,
                    category_state.pageno,
                    crawl_data.product_url,
                )

        case str(table_html):
            crawl_data = NSrodCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                detailed_images_html_source=detailed_images_html_source
                + "\n"
                + table_html,
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

    await page.close()

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
        extract_thumbnail_image(document, product_url),
        extract_images(document, product_url, html_top, html_bottom),
    )

    (
        R1,
        R2,
        R3,
    ) = await asyncio.gather(*tasks)

    if not (product_name := R1.ok()):
        log.warning(
            f"Product name is not found. Visiting the link ({product_url}) for second attempt"
        )

        await visit_link(page, product_url, wait_until="networkidle")
        if not (document2 := await parse_document(await page.content(), engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        document = document2

        tasks = (
            extract_product_name(document),
            extract_thumbnail_image(document, product_url),
            extract_images(document, product_url, html_top, html_bottom),
        )

        (
            R1,  # type: ignore
            R2,  # type: ignore
            R3,  # type: ignore
        ) = await asyncio.gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R3:
        case Ok(detailed_images_html_source):
            pass
        case Err(
            error.QueryNotFound(err)
        ) if "Product detail images are not present at all" in str(err):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return product_name, thumbnail_image_url, detailed_images_html_source


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?no=(\d*\w*)")
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
    query = "table.goodsDisplayItemWrap"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = (
        "#goods_thumbs > table > tbody > tr > td > div.slides_container.hide > a > img"
    )

    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#goods_view > table > tbody > tr > td > form > table > tbody > tr > td > span[class='goods_name']"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@dataclass(slots=True, frozen=True)
class Table:
    model_name: str
    price2: int


async def extract_table(page: PlaywrightPage, content: str, product_url: str):
    table_list: list[Table] = []

    try:
        dfs = await asyncio.to_thread(
            pd.read_html, content, keep_default_na=False, flavor="lxml"  # type: ignore
        )
    except ValueError:
        log.warning(
            f"Could not read the table with Pandas. Visiting the link ({product_url}) for second attempt"
        )
        dfs = await asyncio.to_thread(
            pd.read_html, await page.content(), keep_default_na=False, flavor="lxml"  # type: ignore
        )

    for i in range(len(dfs)):
        if any(
            "규격 Model" in str(z) for z in [x for y in zip(*dfs[i].values) for x in y]  # type: ignore
        ):
            df = dfs[i]
            df.columns = df.iloc[0]  # type: ignore
            df = df.drop([0])

            try:
                if df["규격 Model"].iloc[0] == "규격 Model":  # type: ignore
                    df = df.reset_index(drop=True).drop([0])
            except KeyError:
                # ? See: https://www.nsrod.co.kr/goods/view?no=30
                assert any(
                    "규격 Model" in str(z)  # type: ignore
                    for z in [x for y in zip(*dfs[i + 1].values) for x in y]  # type: ignore
                )
                df = dfs[i + 1]
                df.columns = df.iloc[0]  # type: ignore
                df = df.drop([0])

                if df["규격 Model"].iloc[0] == "규격 Model":  # type: ignore
                    df = df.reset_index(drop=True).drop([0])

            try:
                model_names: list[str] = list(df["규격 Model"])  # type: ignore
            except KeyError:
                raise error.ModelNameNotFound("Model names not found", product_url)

            try:
                prices2_list: list[str] = list(df["판매가(원)"])  # type: ignore
            except KeyError:
                try:
                    prices2_list: list[str] = list(df["판매가"])  # type: ignore
                except KeyError:
                    try:
                        prices2_list: list[str] = list(df["판매가(원)Price (₩)"])  # type: ignore
                    except KeyError:
                        try:
                            prices2_list: list[str] = list(df["판매가(원) Price(₩)"])  # type: ignore
                        except KeyError:
                            raise error.Price2NotFound("Prices2 not found", product_url)

            try:

                def fn(s: str):
                    try:
                        return parse_int(s)
                    except ValueError:
                        return 0

                prices2: list[int] = [fn(p) for p in prices2_list]
            except ValueError:
                raise error.Price2NotFound("Prices2 not found", product_url)
            break
    else:
        for i in range(len(dfs)):
            if any(
                "사이즈" in str(z) for z in [x for y in zip(*dfs[i].values) for x in y]  # type: ignore
            ):
                df = dfs[i]
                df.columns = df.iloc[0]  # type: ignore
                df = df.drop([0])

                try:
                    if df["사이즈  (size)"].iloc[0] == "사이즈  (size)":  # type: ignore
                        df = df.reset_index(drop=True).drop([0])
                except KeyError:
                    log.debug(
                        f"Model name column '사이즈  (size)' is not found: <blue>{product_url}</>"
                    )
                    match await extract_html_table(page):
                        case Ok(table):
                            pass
                        case Err(
                            error.QueryNotFound(err)
                        ) if "HTML Table not found" in str(err):
                            log.debug(f"{err}: <yellow>{product_url}</>")
                            table = ""
                        case Err(err):
                            raise err

                    return table

                try:
                    model_names = list(df["사이즈  (size)"])  # type: ignore
                except KeyError:
                    raise error.ModelNameNotFound("Model names not found", product_url)

                try:
                    prices2_list: list[str] = list(df["소비자가(￦)  (Price)"])  # type: ignore
                    prices2 = [parse_int(p) for p in prices2_list]
                except KeyError:
                    raise error.Price2NotFound("Prices2 not found", product_url)
                break
        else:
            match await extract_html_table(page):
                case Ok(table):
                    pass
                case Err(error.QueryNotFound(err)) if "HTML Table not found" in str(
                    err
                ):
                    log.debug(f"{err}: <yellow>{product_url}</>")
                    table = ""
                case Err(err):
                    raise err

            return table

    for model_name, price2 in zip(model_names, prices2, strict=True):
        table_list.append(Table(model_name, price2))

    return table_list


@returns_future(error.QueryNotFound, AssertionError)
async def extract_html_table(page: PlaywrightPage):
    full_html = ""

    query = "#goods_view > div.goods_description > table[bgcolor='#96b9d5']"
    if not (tbody_elements := await page.query_selector_all(query)):
        raise error.QueryNotFound("HTML Table not found", query=query)

    query = "xpath=.."
    for tbody in tbody_elements:
        if not (table := await tbody.query_selector(query)):
            raise error.QueryNotFound("<tbody> not found in HTML Table", query=query)

        html = (await table.inner_html()).strip()
        assert "<table" in html, r"HTML doesn't start with <table>: {html}"
        full_html += html + "\n"

    return full_html


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = "#goods_view > div.goods_description > p > img, #goods_view > div.goods_description > img, #goods_view > div.goods_description > div > p > img, #goods_view > div.goods_description > div img, #goods_view > div.goods_description img"

    images = await document.query_selector_all(query)

    urls = [await get_srcs(image) for image in images]

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


async def get_srcs(el: Element):
    return src if (src := await el.get_attribute("src")) else ""
