# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import re

from contextlib import suppress
from functools import cache
from urllib.parse import urljoin

import backoff
import pandas as pd

from aiofile import AIOFile
from playwright.async_api import async_playwright, expect

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
from market_crawler.deviyoga import config
from market_crawler.deviyoga.data import DeviyogaCrawlData
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
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


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
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

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            if os.path.exists("bad_options.txt"):
                os.remove("bad_options.txt")

            categories = await get_categories(sitename=config.SITENAME)
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
    await visit_link(page, product_url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        quantity,
        message2,
        delivery_fee,
        message1,
        model_name,
        model_name2,
        brand,
        manufacturer,
        manufacturing_country,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    # ? Reinitialize document after page DOM changes
    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    await page.close()

    if options:
        for option, price3 in options:
            match split_options_text(option):
                case Ok((option1, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = DeviyogaCrawlData(
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                price2=price2,
                price3=price3,
                delivery_fee=str(delivery_fee),
                message1=message1,
                message2=message2,
                quantity=quantity,
                model_name=model_name,
                model_name2=model_name2,
                brand=brand,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=str(option2),
                option3=str(option3),
            )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )

        return None

    price3 = await extract_price3_standalone(document)

    crawl_data = DeviyogaCrawlData(
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        price2=price2,
        price3=price3,
        delivery_fee=str(delivery_fee),
        message1=message1,
        message2=message2,
        quantity=quantity,
        model_name=model_name,
        model_name2=model_name2,
        brand=brand,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )


async def extract_price3_standalone(document: Document):
    if price3 := await document.query_selector("strong.total_price"):
        price3_text = await price3.text_content()

        try:
            price3 = parse_int(price3_text)
        except ValueError:
            price3 = ""
    else:
        price3 = ""

    return price3


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
    regex = compile_regex(r"goodsNo=(\w+)")
    return (
        Ok(str(match[0]))
        if (match := regex.findall(url))
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
    query = "#content > div > div > div.cg-main > div.goods-list > div > div > ul > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    query = "div > div > a"
    if not (product_link := await product.query_selector(query)):
        raise error.QueryNotFound("Product link not found", query=query)

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all(
        "div > div.proinfo_wrap > div.txt > div > img"
    ):
        src = str(await icon.get_attribute("src"))
        if "soldout" in src:
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
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        quantity,
        message2,
        delivery_fee,
        message1,
        model_name,
        model_name2,
        brand,
        manufacturer,
        manufacturing_country,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option, price3 in options:
            match split_options_text(option):
                case Ok((option1, option2, option3)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = DeviyogaCrawlData(
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
                delivery_fee=str(delivery_fee),
                message1=message1,
                message2=message2,
                quantity=quantity,
                model_name=model_name,
                model_name2=model_name2,
                brand=brand,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1,
                option2=str(option2),
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

    price3 = await extract_price3_standalone(document)

    crawl_data = DeviyogaCrawlData(
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
        delivery_fee=str(delivery_fee),
        message1=message1,
        message2=message2,
        quantity=quantity,
        model_name=model_name,
        model_name2=model_name2,
        brand=brand,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1="",
        option2="",
        option3="",
    )

    # ? Don't save product state if options are empty
    if not await document.query_selector(
        "#frmView > div > div.choice > div > div > div"
    ) or await document.query_selector("strong.total_price"):
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        await save_series_csv(
            to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
        )
    else:
        async with AIOFile("bad_options.txt", "a") as f:
            await f.write(f"{product_url}\n")

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
        extract_table(document, product_url),
        extract_options(page, product_url),
    )

    (R1, R2, R3, R4) = await gather(*tasks)

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
            raise error.ModelNameNotFound(err, url=product_url)

    match R3:
        case Ok(table):
            (
                price2,
                quantity,
                message2,
                delivery_fee,
                message1,
                model_name,
                model_name2,
                brand,
                manufacturer,
                manufacturing_country,
            ) = table
        case Err(err):
            await visit_link(page, product_url, wait_until="networkidle")
            R3 = await extract_table(document, product_url)  # type: ignore

            match R3:
                case Ok(table):
                    (
                        price2,
                        quantity,
                        message2,
                        delivery_fee,
                        message1,
                        model_name,
                        model_name2,
                        brand,
                        manufacturer,
                        manufacturing_country,
                    ) = table
                case Err(err):
                    raise error.TableNotFound(err, url=product_url)

    match R4:
        case Ok(options):
            pass
        case Err(err):
            match await extract_options(page, product_url):
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
        quantity,
        message2,
        delivery_fee,
        message1,
        model_name,
        model_name2,
        brand,
        manufacturer,
        manufacturing_country,
        options,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#frmView > div > div.goods-header > div.top > div > h2"

    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@backoff.on_exception(
    backoff.expo,
    (error.TimeoutException, ValueError),
    max_tries=10,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError, error.QueryNotFound)
async def extract_options(page: PlaywrightPage, product_url: str):
    await page.wait_for_load_state("networkidle")

    options: list[tuple[str, int]] = []

    option1_query = "#frmView > div > div.choice > div > div > div"

    if not await page.query_selector(option1_query):
        log.warning(f"Options not present: <blue>{product_url}</blue>")

        return options

    await open_options(page)

    options_locator = page.locator(
        "#frmView > div > div.choice > div > div > div > div > ul > li"
    )

    handles = await options_locator.element_handles()
    for idx in range(1, len(handles)):
        option1_element = handles[idx]

        if not (option1_text := await option1_element.text_content()):
            continue

        option1_text = option1_text.strip()

        if (cls := await option1_element.get_attribute("class")) and (
            "disabled-result" in cls
        ):
            log.warning(f"Option # {idx} is disabled: <blue>{product_url}</blue>")
            options.append((option1_text, 0))
            continue

        try:
            async with page.expect_request_finished():
                await option1_element.click(timeout=2000)
        except error.PlaywrightError:
            log.warning(
                f"Couldn't click on option # {idx} within 2000 milliseconds: <blue>{product_url}</blue>"
            )

            await page.wait_for_timeout(2000)
            if not await page.query_selector(
                "div.option_total_display_area > div.end-price > ul > li.total > strong"
            ):
                options.append((option1_text, 0))
                continue

            if (
                price_element := await page.query_selector(
                    "div.option_total_display_area > div.end-price > ul > li.total > strong"
                )
            ) and not await price_element.is_visible():
                options.append((option1_text, 0))
                continue

            for _ in range(5):
                await open_options(page)

                handles = await options_locator.element_handles()
                option1_element = handles[idx]
                if not (option1_text := await option1_element.text_content()):
                    raise error.OptionsNotFound(
                        f"Option is empty: <blue>{product_url}</blue>"
                    )

                try:
                    async with page.expect_request_finished():
                        await option1_element.click()
                except error.PlaywrightError:
                    log.warning(
                        f"Couldn't click on option # {idx} within 2000 milliseconds: <blue>{product_url}</blue>"
                    )
                else:
                    break
            else:
                # ? See: http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000330
                log.warning(
                    f"After 5th attempt, couldn't click on option # {idx}: <blue>{product_url}</blue>"
                )
                async with AIOFile("bad_options.txt", "a") as f:
                    await f.write(f"{product_url}\n")

                option1_elements = await page.query_selector_all(
                    f"{option1_query} > div > ul > li"
                )
                options = [
                    (text.strip(), 0)
                    for option in option1_elements
                    if (text := await option.text_content())
                ]
                return options

        else:
            await page.wait_for_timeout(1000)

            try:
                await page.wait_for_load_state("networkidle")
            except error.PlaywrightTimeoutError as err:
                await page.reload()
                raise error.TimeoutException(
                    f"Timed out waiting for select_option(): {product_url}"
                ) from err

            price3 = await extract_price3(page)

            if price3 == False:
                options.append((option1_text, 0))
                continue

            while (price3 := await extract_price3(page)) == 0:
                log.warning(f"Price3 is 0: <blue>{product_url}</blue>")
                await page.wait_for_timeout(2000)

                with suppress(error.PlaywrightTimeoutError):
                    async with page.expect_request_finished():
                        await page.click("div > div.del > button")
                    await page.wait_for_timeout(2000)

                await open_options(page)

                option1_elements = await page.query_selector_all(
                    f"{option1_query} > div > ul > li"
                )
                option1_element = option1_elements[idx]
                if not (option1_text := await option1_element.text_content()):
                    raise error.OptionsNotFound(
                        f"Option is empty: <blue>{product_url}</blue>"
                    )

                async with page.expect_request_finished():
                    await option1_element.click()
                await page.wait_for_timeout(2000)

                await open_options(page)

            # ? Delete the selected option so that total doesn't contain accumulated price
            with suppress(error.PlaywrightTimeoutError):
                async with page.expect_request_finished():
                    await page.click("div > div.del > button")
                await page.wait_for_timeout(2000)

            await open_options(page)

            if price3 == 0:
                log.warning(f"Price3 is 0: <blue>{product_url}</blue>")
                await page.reload()
                raise ValueError(f"Price3 is 0: <blue>{product_url}</blue>")

            options.append((option1_text, price3))

    return options


async def open_options(page: PlaywrightPage):
    option1_query = "#frmView > div > div.choice > div > div > div"

    locator = page.locator(option1_query)
    try:
        await locator.first.locator("a > div").click()
    except Exception:
        await page.reload()
        await page.wait_for_load_state("networkidle")
        await open_options(page)
    await page.wait_for_timeout(1000)
    await page.wait_for_load_state("networkidle")

    await expect(locator.first).to_have_class(re.compile("chosen-with-drop"))


async def extract_price3(page: PlaywrightPage):
    query = "div.option_total_display_area > div.end-price > ul > li.total > strong"
    if not (price3 := await page.text_content(query)):
        return False

    price3_locator = page.locator(query)
    try:
        await expect(price3_locator).not_to_be_hidden()
    except AssertionError:
        return False

    try:
        price3 = parse_int(price3)
    except ValueError as err:
        raise ValueError(f"Coudn't convert price3 text {price3} to number") from err

    return price3


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document, product_url: str):
    query = "li.price > div > strong"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query=query)

    try:
        price2 = parse_int(price2)
    except ValueError as err:
        raise ValueError(f"Coudn't convert price2 text {price2} to number") from err

    query = "xpath=//li[contains(./strong, '구매제한')]/div/span"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query=query)

    query = "li.benefits > div > p.sale"
    if not (message2 := await document.text_content(query)):
        raise error.QueryNotFound("Message2 not found", query=query)

    query = "li.delivery > div > span"
    if not (delivery_fee := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    try:
        delivery_fee = parse_int(delivery_fee)
    except ValueError:
        log.warning(
            f"Coudn't convert delivery fee text {delivery_fee} to number: <blue>{product_url}</blue>"
        )
        delivery_fee = delivery_fee.strip()

    query = "li.delivery > div > div.detail"
    if not (message1 := await document.text_content(query)):
        # ? See: http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000330
        log.warning(f"Message1 not found: <blue>{product_url}</blue>")
        message1 = ""

    query = "xpath=//li[contains(./strong, '상품코드')]/div"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query=query)

    query = "xpath=//li[contains(./strong, '모델명')]/div"
    if not (model_name2 := await document.text_content(query)):
        model_name2 = ""

    query = "xpath=//li[contains(./strong, '브랜드')]/div"
    if not (brand := await document.text_content(query)):
        # ? See: http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000276
        log.warning(f"Brand not found: <blue>{product_url}</blue>")
        brand = ""

    query = "xpath=//li[contains(./strong, '제조사')]/div"
    if not (manufacturer := await document.text_content(query)):
        # ? See: http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000276
        log.warning(f"Manufacturer not found: <blue>{product_url}</blue>")
        manufacturer = ""

    query = "xpath=//li[contains(./strong, '원산지')]/div"
    if not (manufacturing_country := await document.text_content(query)):
        # ? See: http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000276
        log.warning(f"Manufacturing country not found: <blue>{product_url}</blue>")
        manufacturing_country = ""

    return (
        price2,
        quantity.strip(),
        message2.strip(),
        delivery_fee,
        message1.strip(),
        model_name.strip(),
        model_name2.strip(),
        brand.strip(),
        manufacturer.strip(),
        manufacturing_country.strip(),
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "#mainImage > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        log.warning(f"Thumbnail image not found: {product_url}")
        thumbnail_image_url = ""
    else:
        thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "div.more-thumbnail span.swiper-slide > a > img"
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


@returns(IndexError, ValueError)
def split_options_text(option1: str):
    option3 = 0

    if "+" in option1 and "원" in option1:
        regex = compile_regex(r"\s?:\s*?\+\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            option3 = parse_int(additional_price)

    if "-" in option1 and "원" in option1:
        regex = compile_regex(r"\s?:\s*?\-\w+[,]?\w*원?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            option3 = parse_int(additional_price)

    if ":" in option1 and "개" in option1:
        regex = compile_regex(r"\s?:\s*?\w+[,]?\w*개?")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            option3 = parse_int(additional_price)

    if "[품절]" in option1:
        return (
            option1.replace("[품절]", "").strip(),
            "[품절]",
            option3 or "",
        )
    if "품절" in option1:
        return option1.replace("품절", "").strip(), "품절", option3 or ""

    return option1.strip(), "", option3 or ""


@cache
def image_quries():
    return "#detail > div.txt-manual > center img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await page.click("#detail > div.tab > a")
        await page.wait_for_timeout(1000)

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
            max_tries=50,
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
        await page.click("#detail > div.tab > a")
        await page.wait_for_timeout(1000)

    with suppress(error.PlaywrightTimeoutError):
        await element.click()
        await page.wait_for_timeout(1000)
