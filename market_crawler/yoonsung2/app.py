# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from collections.abc import Iterable
from contextlib import suppress
from functools import cache
from urllib.parse import urljoin

from playwright.async_api import Route, async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import parse_document, visit_link
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
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.initialization import Category, get_categories
from market_crawler.memory import MemoryOptimizer
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.yoonsung2 import config
from market_crawler.yoonsung2.data import YoonSung2CrawlData
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


# ? Abort specific type of requests for page speedup
async def block_requests(route: Route):
    if route.request.resource_type in [
        "preflight",
        "ping",
        "image",
        "font",
        "script",
        "stylesheet",
        "other",
        "xhr",
    ]:
        await route.abort()
    else:
        await route.continue_()


@cache
def page_url(current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def login(login_info: LoginInfo, browser: PlaywrightBrowser) -> None:
    page = await browser.new_page()
    await page.goto(login_info.login_url, wait_until="networkidle")

    # ? Need to close the banner showing in front of login form
    with suppress(error.PlaywrightTimeoutError):
        await page.click("div.popup-close > img", timeout=20000)

    is_already_login = True
    try:
        await page.wait_for_selector(
            login_info.user_id_query,
            state="visible",
        )

        is_already_login = False
    except error.PlaywrightTimeoutError:
        is_already_login = True

    if not is_already_login:
        input_id = await page.query_selector(login_info.user_id_query)

        if input_id:
            await input_id.fill(login_info.user_id)
        else:
            raise error.LoginInputNotFound(
                f"User ID ({login_info.user_id}) could not be entered"
            )

        if login_info.keep_logged_in_check_query:
            await page.check(login_info.keep_logged_in_check_query)

        input_password = await page.query_selector(
            login_info.password_query,
        )
        if input_password:
            await input_password.fill(login_info.password)
        else:
            raise error.PasswordInputNotFound(
                f"Passowrd ({login_info.password}) could not be entered"
            )

        await login_info.login_button_strategy(page, login_info.login_button_query)

        log.success(
            f"Logged in <MAGENTA><w>(ID: {login_info.user_id}, PW: {login_info.password})</></>"
        )

    await page.close()


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_load_state(state="networkidle")

    await page.wait_for_selector('a[onclick="logout()"]')


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='user_id']",
        password_query="input[name='password']",
        login_button_query="div.login_btn > input[type=submit]",
        login_button_strategy=login_button_strategy,
    )


async def run(settings: Settings) -> None:
    if config.HEADLESS:
        log.warning(
            "HEADLESS must be False for number of products to work correctly in YOONSUNG2, therefore changing it to 'False'"
        )
        config.HEADLESS = False

    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    async with async_playwright() as playwright:
        login_info = get_login_info()

        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()

        await login(login_info, browser)

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
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
    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )

    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(idx, browser, urls[idx], filename, settings, columns)
            for idx in chunk
        )

        await asyncio.gather(*tasks)


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    product_url: str,
    filename: str,
    settings: Settings,
    columns: list[str],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    productid = get_productid(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name="CUSTOM",
            date=settings.DATE,
        )
    ):
        return None

    page = await browser.new_page()
    await page.route("**/*", block_requests)

    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    for retry in range(1, 11):
        try:
            (
                product_name,
                model_name,
                price2,
                price3,
                sold_out_text,
                quantity,
                manufacturing_country,
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
                detailed_images_html_source,
            ) = await extract_data(page, document, product_url, html_top, html_bottom)
        except Exception as err:
            text = str(err)
            # ? Fix the loguru's mismatch of <> tag for ANSI color directive
            if source := compile_regex(r"\<\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
            if source := compile_regex(r"\<\/\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("</", "<"))
            if source := compile_regex(r"\<.*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
                text = text.replace(source[0], source[0].replace("</", "<"))
            log.error(text)
            log.warning(f"Retrying for # {retry} times ({product_url}) ...")

            await asyncio.sleep(retry)

            await visit_link(page, product_url, wait_until="load")

            if not (
                document := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                ) from err
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data ({product_url}) even after 10 retries"
        )

    await page.close()

    crawl_data = YoonSung2CrawlData(
        product_url=product_url,
        sold_out_text=sold_out_text,
        product_name=product_name,
        model_name=model_name,
        price2=price2,
        price3=price3,
        quantity=quantity,
        manufacturing_country=manufacturing_country,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        detailed_images_html_source=detailed_images_html_source,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_custom_url_crawled(
        idx,
        product_url,
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

    category_page_url = category.url

    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="networkidle")

    try:
        await page.fill(
            "input[name='user_id']",
            config.ID,
            timeout=3000,
        )
        await page.fill(
            "input[name='password']",
            config.PW,
        )
        await page.click("div.login_btn > input[type=submit]")

        await page.wait_for_selector("a[onclick='logout()']", state="visible")
        await visit_link(page, category_page_url, wait_until="networkidle")
    except Exception:
        await visit_link(page, category_page_url, wait_until="networkidle")

    for retry in range(1, 11):
        try:
            await page.wait_for_selector(
                "#innerPage div > div > div > ul > li.choice",
                state="visible",
                timeout=300000,
            )
        except error.PlaywrightError as err:
            text = str(err)
            # ? Fix the loguru's mismatch of <> tag for ANSI color directive
            if source := compile_regex(r"\<\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
            if source := compile_regex(r"\<\/\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("</", "<"))
            if source := compile_regex(r"\<.*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
                text = text.replace(source[0], source[0].replace("</", "<"))
            log.error(text)
            log.warning(
                f"Retrying for # {retry} times ({category_page_url}) (Page # {category_state.pageno}) ..."
            )

            await asyncio.sleep(retry)

            await page.close()

            page = await browser.new_page()
            try:
                await page.fill(
                    "input[name='user_id']",
                    config.ID,
                    timeout=3000,
                )
                await page.fill(
                    "input[name='password']",
                    config.PW,
                )
                await page.click("div.login_btn > input[type=submit]")

                await page.wait_for_selector("a[onclick='logout()']", state="visible")
                await visit_link(page, category_page_url, wait_until="networkidle")
            except Exception:
                await visit_link(page, category_page_url, wait_until="networkidle")
        else:
            break
    else:
        # ? Sometimes category don't contain any products
        # ? See: http://shop.yoonsunginc.com/new/shoppinglist.aspx?itemcode=&brandcode=6400$$$$$$&search=&check=0
        log.warning(
            "Couldn't find the navigation choice selector even after 10 retries"
        )
        # ? We won't raise the error in such cases
        return

    log.action.visit_category(category.name, category_page_url)

    memory_optimizer = MemoryOptimizer(
        max_products_chunk_size=config.MAX_PRODUCTS_CHUNK_SIZE,
    )
    category_chunk_size = config.CATEGORIES_CHUNK_SIZE
    products_chunk_size = config.MIN_PRODUCTS_CHUNK_SIZE

    while True:
        if category_state.pageno > 1:
            last_page_reached: bool = False
            while True:
                for retry in range(1, 11):
                    try:
                        if not (
                            current_page_no_choice := await page.text_content(
                                "#innerPage div > div > div > ul > li.choice"
                            )
                        ):
                            raise ValueError("Current page no choice not found")

                        next_page_link = await page.query_selector_all(
                            "div.paginate > ul > li"
                        )
                        try:
                            async with page.expect_navigation():
                                await next_page_link[-1].click()
                        except error.PlaywrightTimeoutError as err:
                            if not (
                                current_page_no_choice_now := await page.text_content(
                                    "#innerPage div > div > div > ul > li.choice"
                                )
                            ):
                                raise ValueError(
                                    "Current page no choice not found"
                                ) from err

                            if current_page_no_choice_now != str(category_state.pageno):
                                raise AssertionError(
                                    f"Paginagtion navigation was not successful to {category_state.pageno} from {current_page_no_choice_now}"
                                ) from err
                    except AssertionError as err:
                        text = str(err)
                        # ? Fix the loguru's mismatch of <> tag for ANSI color directive
                        if source := compile_regex(r"\<\w*\>").findall(text):
                            text = text.replace(
                                source[0], source[0].replace("<", r"\<")
                            )
                        if source := compile_regex(r"\<\/\w*\>").findall(text):
                            text = text.replace(source[0], source[0].replace("</", "<"))
                        if source := compile_regex(r"\<.*\>").findall(text):
                            text = text.replace(
                                source[0], source[0].replace("<", r"\<")
                            )
                            text = text.replace(source[0], source[0].replace("</", "<"))
                        log.error(text)
                        log.warning(
                            f"Retrying for # {retry} times ({category_page_url}) (Page # {category_state.pageno}) ..."
                        )

                        await asyncio.sleep(retry)
                        await visit_link(
                            page, category_page_url, wait_until="networkidle"
                        )
                        await page.wait_for_selector(
                            "#innerPage div > div > div > ul > li.choice"
                        )
                    else:
                        break
                else:
                    raise AssertionError(
                        f"Couldn't navigate the page to ({category_state.pageno}) even after 10 retries"
                    )

                if not (
                    current_page_no_choice_now := await page.text_content(
                        "#innerPage div > div > div > ul > li.choice"
                    )
                ):
                    log.warning("Current page no choice not found")
                    await page.pause()
                    raise ValueError("Current page no choice not found")

                if current_page_no_choice_now == str(category_state.pageno):
                    break

                # ? If the selected page is same even after clicking on next page, it means it's the last page
                if current_page_no_choice_now == current_page_no_choice:
                    last_page_reached = True
                    break

            if last_page_reached:
                break

            current_page_no_choice = current_page_no_choice_now

            assert current_page_no_choice == str(
                category_state.pageno
            ), f"Current page no choice is not the same as page no: {current_page_no_choice} vs {str(category_state.pageno)}"

        if not (
            products := await get_products(
                page, category_page_url, category_state.pageno
            )
        ):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        number_of_products = len(products)
        log.detail.total_products_on_page(number_of_products, category_state.pageno)

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        products_chunk_size = await memory_optimizer.optimize_products_chunk_sizes(
            browser, category_chunk_size, products_chunk_size
        )

        for chunk in chunks(range(number_of_products), products_chunk_size):
            tasks = (
                extract_product(
                    idx,
                    browser,
                    page,
                    category_page_url,
                    category_state,
                    filename,
                    columns,
                    settings,
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

    await page.close()


async def get_products(
    page_or_document: PlaywrightPage | Document, category_url: str, pageno: int
):
    if not (
        products := await page_or_document.query_selector_all(
            "#innerPage > div.item-table.checkboxStyle > table > tbody > tr[id^='tr_']"
        )
    ):
        log.warning(
            f"There are no products on the current url ({category_url}) of Page # {pageno}"
        )

    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element) -> str:
    if not (
        product_selector := await product.query_selector("td.item-name > span.underTXT")
    ):
        raise error.QueryNotFound(
            "Product link not found", "td.item-name > span.underTXT"
        )

    if not (onclick := await product_selector.get_attribute("onclick")):
        raise error.QueryNotFound("Product link not found", "onclick")

    # ? For YOONSUNG, product url is not the actual url but a JavaScript function (i.e., "javascript:callViewPage('REBA03982');", etc.) which contains page code that we can use to build the product url
    if not (match := compile_regex(r"Page\('(.*)'\)").search(onclick)):
        if not (match := compile_regex(r"ProductSeqNo=(\d*\w*)").search(onclick)):
            raise ValueError(f'Product ID not found in "{onclick}"')

    product_id = match.group(1)

    return f"http://shop.yoonsunginc.com/new/itemdetails.aspx?spec={product_id}"


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document, product: Element) -> str:
    first_heading = "#innerPage > div.item-table.checkboxStyle > table > tbody > tr > th:nth-child(1)"
    if main_category := await document.text_content(first_heading):
        try:
            assert "대분류" in main_category.strip()
        except AssertionError as err:
            raise AssertionError("'대분류' is not present in first heading") from err

    second_heading = "#innerPage > div.item-table.checkboxStyle > table > tbody > tr > th:nth-child(2)"
    if midde_category := await document.text_content(second_heading):
        try:
            assert "중분류" in midde_category.strip()
        except AssertionError as err:
            raise AssertionError("'중분류' is not present in second heading") from err

    if not (category_texts := await product.query_selector_all("td")):
        raise error.QueryNotFound("Category text not found", "td")

    return ">".join(
        [text.strip() for el in category_texts[:2] if (text := await el.text_content())]
    )


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    page: PlaywrightPage,
    category_page_url: str,
    category_state: CategoryState,
    filename: str,
    columns: list[str],
    settings: Settings,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    content = await page.content()

    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    try:
        products = await get_products(
            document, category_page_url, category_state.pageno
        )
    except Exception as err:
        raise error.ProductsNotFound(err, url=category_page_url) from err
    else:
        product = products[idx]

    match await get_product_link(product):
        case Ok(product_url):
            pass
        case Err(err):
            raise error.ProductLinkNotFound(err, url=category_page_url)

    match await extract_category_text(document, product):
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    productid = get_productid(product_url).expect(
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

    page = await browser.new_page()
    await page.route("**/*", block_requests)

    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    for retry in range(1, 11):
        try:
            (
                product_name,
                model_name,
                price2,
                price3,
                sold_out_text,
                quantity,
                manufacturing_country,
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
                detailed_images_html_source,
            ) = await extract_data(page, document, product_url, html_top, html_bottom)
        except Exception as err:
            text = str(err)
            # ? Fix the loguru's mismatch of <> tag for ANSI color directive
            if source := compile_regex(r"\<\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
            if source := compile_regex(r"\<\/\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("</", "<"))
            if source := compile_regex(r"\<.*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
                text = text.replace(source[0], source[0].replace("</", "<"))
            log.error(text)
            log.warning(f"Retrying for # {retry} times ({product_url}) ...")

            await asyncio.sleep(retry)

            await visit_link(page, product_url, wait_until="load")

            if not (
                document := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                ) from err
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data ({product_url}) even after 10 retries"
        )

    await page.close()

    crawl_data = YoonSung2CrawlData(
        category=category_text,
        product_url=product_url,
        sold_out_text=sold_out_text,
        product_name=product_name,
        model_name=model_name,
        price2=price2,
        price3=price3,
        quantity=quantity,
        manufacturing_country=manufacturing_country,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        detailed_images_html_source=detailed_images_html_source,
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
        extract_model_name(document),
        extract_prices(document, product_url),
        extract_soldout_text(document),
        extract_quantity(document),
        extract_manufacturing_country(document),
        extract_thumbnail_images(document, product_url),
        extract_images(page, product_url, html_top, html_bottom),
    )

    (R1, R2, R3, R4, R5, R6, R7, R8) = await gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R3:
        case Ok((price2, price3)):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R4:
        case Ok(sold_out_text):
            pass
        case Err(err):
            raise error.SoldOutNotFound(err, url=product_url)

    match R5:
        case Ok(quantity):
            pass
        case Err(err):
            raise error.QuantityNotFound(err, url=product_url)

    match R6:
        case Ok(manufacturing_country):
            pass
        case Err(err):
            log.warning(f"Manufacturing country not found: {product_url}")
            manufacturing_country = ""

    match R7:
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

    match R8:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return (
        product_name,
        model_name,
        price2,
        price3,
        sold_out_text,
        quantity,
        manufacturing_country,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        detailed_images_html_source,
    )


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"spec=(\d*\w*)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "ul.item-info > li > div.table > div.td > div.Name"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), 'SPEC')]/div[@class='td']"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query)

    return model_name.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_prices(document: Document, product_url: str):
    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), '판매가격')]/div[@class='td']/div"
    if not (price2 := await document.inner_text(query)):
        raise error.QueryNotFound("Price2 not found", query)

    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), '판매가격')]/div[@class='td']/div/span[2]"
    if not (price3 := await document.text_content(query)):
        log.warning(f"Price3 not found: {product_url}")
        price3 = ""
    else:
        price3 = parse_int(price3.strip())

    price2 = parse_int(price2.strip())

    return price2, price3


@returns_future(error.QueryNotFound)
async def extract_soldout_text(document: Document):
    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), '재고')]/div[@class='td']/div[@class='item-count']"
    if not (soldout_text := await document.text_content(query)):
        raise error.QueryNotFound("Soldout text not found", query)

    return soldout_text.strip()


@returns_future(error.QueryNotFound)
async def extract_quantity(document: Document):
    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), '포장단위')]/div[@class='td']/div[@class='Order_drainage']"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query)

    return quantity.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_manufacturing_country(document: Document):
    query = "xpath=//li/div[@class='table'][contains(div[@class='th']/text(), '규격/원산지')]/div[@class='td']"
    if not (manufacturing_country := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturing country not found", query)

    manufacturing_country = manufacturing_country.strip()

    regex = compile_regex(r"\s+(\w*)")
    if not (match := regex.findall(manufacturing_country)):
        raise ValueError(
            f"Regex for manufacturer country is not matched: {manufacturing_country}"
        )

    return match[0].strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div[class='main-image'][id='mainImageFrameMain']"
    if not (thumbnail_image := await document.query_selector(query)):
        raise error.QueryNotFound("Thumbnail image not found", query)

    regex = compile_regex(r"background-image:\s?url(((.*)));")

    if (
        not (style := await thumbnail_image.get_attribute("style"))
        or "background-image: url" not in style
    ):
        raise ValueError("Thumbnail url is not present inside 'style' attribute")

    if regex_match := regex.search(
        style,
    ):
        thumbnail_image_url = regex_match.group(1).replace("('", "").replace("')", "")
    else:
        raise ValueError(f"Regex not found in {style}")

    query = "div[class='sub-image'] li[id^='mainImageList']"
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := (await document.query_selector_all(query))[1:]:
        new_thumbnail_images: list[str] = []
        for thumbnail_image in thumbnail_images:
            if (
                not (style := await thumbnail_image.get_attribute("style"))
                or "background-image:url" not in style
            ):
                raise ValueError(
                    "Thumbnail url is not present inside 'style' attribute"
                )

            if regex_match := regex.search(
                style,
            ):
                thumbnail_image_url_extra = (
                    regex_match.group(1).replace("('", "").replace("')", "")
                )
            else:
                raise ValueError(f"Regex not found in {style}")

            new_thumbnail_images.append(thumbnail_image_url_extra)

        thumbnail_images = new_thumbnail_images
        N = 4
        thumbnail_images += [""] * (N - len(thumbnail_images))
        (
            thumbnail_image_url2,
            thumbnail_image_url3,
            thumbnail_image_url4,
            thumbnail_image_url5,
            *_,
        ) = thumbnail_images

    # ? Remove unnecessary query after the image
    # ? e.g., http://ip57.dahaeinc.co.kr/YSFNB/Image1/1561424865896l0.jpg?v=033653785
    regex = compile_regex(r"\?v=\w*")
    thumbnail_image_url = urljoin(product_url, regex.sub("", thumbnail_image_url))

    if thumbnail_image_url2:
        thumbnail_image_url2 = urljoin(product_url, regex.sub("", thumbnail_image_url2))

    if thumbnail_image_url3:
        thumbnail_image_url3 = urljoin(product_url, regex.sub("", thumbnail_image_url3))

    if thumbnail_image_url4:
        thumbnail_image_url4 = urljoin(product_url, regex.sub("", thumbnail_image_url4))

    if thumbnail_image_url5:
        thumbnail_image_url5 = urljoin(product_url, regex.sub("", thumbnail_image_url5))

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    )


@returns_future(error.QueryNotFound)
async def extract_html_table(page: PlaywrightPage, product_url: str):
    full_html = ""

    query = "#divDetailImg table tbody"

    tbody_elements = await page.query_selector_all(query)

    if not tbody_elements:
        raise error.QueryNotFound("HTML Table not found", query=query)

    for tbody in tbody_elements:
        if not (table := await tbody.query_selector("xpath=..")):
            raise error.QueryNotFound(
                f"HTML Table not found: {product_url}", query=query
            )

        html = (await table.inner_html()).strip()
        if "<table" not in html:
            html = f"<table>{html}</table>"

        assert "<table" in html, f"HTML: {html}"
        assert "<tbody" in html, f"HTML: {html}"
        full_html += html + "\n"

    return full_html


@cache
def image_quries():
    return "#divDetailImg img"


@returns_future(error.QueryNotFound, MaxTriesReached)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_html_table(page, product_url):
        case Ok(table_html):
            pass
        case Err(_):
            table_html = ""

    query = image_quries()

    if elements := await page.query_selector_all(query):
        await page.click(query)

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
                if src and all(
                    i not in src
                    for i in [
                        "item-details-bar01.png",
                        "item-details-bar02.png",
                        "item-details-bar03.png",
                    ]
                ):
                    urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        if table_html:
            log.warning(
                f"Detail images are not present, but table HTML is found: {product_url}"
            )
        else:
            raise error.QueryNotFound(
                "Neither detail images nor table HTML is present",
                query=query,
            )

    return build_html(
        table_html,
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)
        await page.wait_for_timeout(1000)


def build_html(
    table_html: str, images_construction: Iterable[str], html_top: str, html_bottom: str
):
    html_source = html_top

    for image_url in images_construction:
        html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

    html_source = "".join(
        [
            html_source,
            table_html,
            html_bottom,
        ],
    )

    return html_source.strip()
