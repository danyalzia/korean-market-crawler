# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import Any
from urllib.parse import urljoin

import lxml.html as lxml

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.element import Element
from dunia.extraction import visit_link
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
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.sinsunhi import config
from market_crawler.sinsunhi.data import SinsunhiCrawlData
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)

    # ? Sometimes a dialog is shown that needs to be closed for products to show correctly
    with suppress(error.PlaywrightTimeoutError):
        await page.click("article > button > svg", timeout=900)
        log.debug("LOGIN: First Popup is clicked")

    # ? Sometimes a second dialog is shown after first one is closed
    with suppress(error.PlaywrightTimeoutError):
        await page.click("article > button > svg", timeout=900)
        log.debug("LOGIN: Second Popup is clicked")

    with suppress(error.PlaywrightTimeoutError):
        await page.click("div > div > div > button > div > svg", timeout=900)
        log.debug("LOGIN: Third Popup is clicked")

    await page.wait_for_selector(
        "xpath=//button[contains(./text(), '로그아웃')]",
        state="visible",
    )


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='email']",
        password_query="input[name='password']",
        login_button_query="div.container.mx-auto.max-w-lg.min-h-buyer.relative.flex.flex-col.justify-center.pb-20 button[type='submit']",
        login_button_strategy=login_button_strategy,
    )


async def run(settings: Settings):
    if config.HEADLESS:
        log.warning(
            "HEADLESS must be False for number of products to work correctly in SINSUNHI, therefore changing it to 'False'"
        )
        config.HEADLESS = False

    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
        user_data_dir="",
    )
    login_info = get_login_info()
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()

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

        # ? Don't exceed 10 if custom url is being crawled otherwise the website becomes very flaky
        config.MAX_PRODUCTS_CHUNK_SIZE = min(config.MAX_PRODUCTS_CHUNK_SIZE, 10)
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
    await visit_link(page, product_url, wait_until="networkidle")

    data = await extract_data(page, product_url, html_top, html_bottom)

    if not data:
        await page.close()
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    (
        thumbnail_image_url,
        product_name,
        message2,
        manufacturing_country,
        options,
        option4,
        detailed_images_html_source,
    ) = data

    await page.close()

    if options:
        for option, price3, delivery_fee, message1 in options:
            match split_options_text(option):
                case Ok((option1, option2, price2)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = SinsunhiCrawlData(
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                product_name=product_name,
                message1=message1,
                message2=message2,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option4=option4,
                price2=price2,
                price3=str(price3).removesuffix(".0"),
                delivery_fee=str(delivery_fee).removesuffix(".0"),
            )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )

        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    crawl_data = SinsunhiCrawlData(
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        message1="",
        message2=message2,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option4=option4,
        price2="",
        price3="",
        delivery_fee="",
    )

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    return None


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

    category_page_url = category_url

    log.action.visit_category(category_name, category_page_url)

    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="networkidle")

    if not (products := (await get_products(page)).ok()):
        log.action.products_not_present_on_page(
            category_page_url, category_state.pageno
        )
        return

    number_of_products = len(products)

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
                products[idx],
                browser,
                category_state,
                filename,
                columns,
                settings,
            )
            for idx in chunk
        )

        await asyncio.gather(*tasks)

    log.action.category_page_crawled(category_state.name, category_state.pageno)

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\/products\/(\d*\w*)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(page: PlaywrightPage) -> int | None:
    match await get_products(page):
        case Ok(products):
            return len(products)
        case _:
            return None


async def close_popups(page: PlaywrightPage):
    # ? Sometimes a dialog is shown that needs to be closed for products to show correctly
    with suppress(error.PlaywrightTimeoutError):
        await page.click("article > button > svg", timeout=900)
        log.debug("GET_PRODUCTS(): First Popup is clicked")

    # ? Sometimes a second dialog is shown after first one is closed
    with suppress(error.PlaywrightTimeoutError):
        await page.click("article > button > svg", timeout=900)
        log.debug("GET_PRODUCTS(): Second Popup is clicked")

    with suppress(error.PlaywrightTimeoutError):
        await page.click("div > div > div > button > div > svg", timeout=900)
        log.debug("GET_PRODUCTS(): Third Popup is clicked")


@returns_future(error.QueryNotFound)
async def get_products(page: PlaywrightPage):
    # ? Check notice
    with suppress(error.PlaywrightTimeoutError):
        await page.click("xpath=//a[contains(./div, '공지사항 확인')]", timeout=2000)

    await close_popups(page)

    # ? Products query
    query = "div[class^='mt-['] > ol > div > div:nth-child(1)"

    # ? Javascript's scrollIntoView
    # ? Scroll to the bottom smoothly
    # ? See: https://stackoverflow.com/questions/4884839/how-do-i-get-an-element-to-scroll-into-view-using-jquery
    for _ in range(50):
        try:
            await page.evaluate(
                """
                () => {
                    document.querySelector('footer').scrollIntoView({behavior: "smooth", block: "start"});
                }
                """
            )
        except error.PlaywrightError:
            pass
        else:
            await page.wait_for_timeout(500)

        await page.mouse.wheel(delta_x=0, delta_y=-1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=-1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=1600)

        try:
            await page.evaluate(
                """
                () => {
                    document.querySelector('body').scrollIntoView({behavior: "smooth", block: "start"});
                }
                """
            )
        except error.PlaywrightError:
            pass
        else:
            await page.wait_for_timeout(500)

    if not (products := await page.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


async def is_prohibited_product(product: Element):
    query1 = "span.mt-2.font-bold.text-lg.text-green-500"
    query2 = "span.text-sm.text-text-L2"

    return bool(
        await product.query_selector(query1) or await product.query_selector(query2)
    )


async def extract_soldout_text(product: Element):
    for span in await product.query_selector_all(
        "div.absolute.bottom-0.w-full.h-10.bg-gray-600.flex.items-center.justify-center.opacity-90 > span"
    ):
        if (text := await span.text_content()) and "품절" in text:
            return "품절"

    return ""


async def extract_product(
    idx: int,
    product: PlaywrightElementHandle,
    browser: PlaywrightBrowser,
    category_state: CategoryState,
    filename: str,
    columns: list[str],
    settings: Settings,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    if await is_prohibited_product(product):
        log.warning(f"Skipping the prohibited product: # {idx}")
        return None

    sold_out_text = await extract_soldout_text(product)

    if not (el := await product.query_selector("img")) or not (
        alt_text := await el.get_attribute("alt")
    ):
        raise ValueError("'alt' attribute is not present in product image")

    alt_text = alt_text.strip()

    page = await browser.new_page()

    for _ in range(5):
        await visit_link(
            page, "https://www.sinsunhi.com/?tab=quick", wait_until="networkidle"
        )

        log.info(f"Searching for {alt_text} ...")

        await page.click("input[placeholder='찾고있는 작물을 검색해보세요']")
        await page.fill("input[placeholder='찾고있는 작물을 검색해보세요']", alt_text)

        async with page.expect_navigation():
            await page.press(
                "input[placeholder='찾고있는 작물을 검색해보세요']", "Enter"
            )

        try:
            await page.wait_for_load_state("networkidle")
        except error.PlaywrightError:
            await page.wait_for_selector("div > div[class^='max-w-[']")

        await page.mouse.wheel(delta_x=0, delta_y=-1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=-1600)
        await page.wait_for_timeout(500)
        await page.mouse.wheel(delta_x=0, delta_y=1600)

        for product in await page.query_selector_all("div > div[class^='max-w-[']"):
            if not (el := await product.query_selector("img")) or not (
                new_alt_text := await el.get_attribute("alt")
            ):
                raise ValueError("'alt' attribute is not present in product image")

            new_alt_text = new_alt_text.strip()

            if alt_text == new_alt_text:
                previous_url = page.url

                # ? Sometimes clicking only once does not lead to new page
                while page.url == previous_url:
                    with suppress(error.PlaywrightError):
                        await product.click()

                break
        else:
            log.warning(f"Couldn't find 'alt' text: {alt_text}")
            await page.close()
            return None

        break
    else:
        raise AssertionError(f"Couldn't find 'alt' text: {alt_text}")

    product_url = page.url

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
        await page.close()
        return None

    data = await extract_data(page, product_url, html_top, html_bottom)

    if not data:
        await page.close()
        return None

    (
        thumbnail_image_url,
        product_name,
        message2,
        manufacturing_country,
        options,
        option4,
        detailed_images_html_source,
    ) = data

    await page.close()

    if options:
        for option, price3, delivery_fee, message1 in options:
            match split_options_text(option):
                case Ok((option1, option2, price2)):
                    pass
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = SinsunhiCrawlData(
                category=category_state.name,
                product_url=product_url,
                sold_out_text=sold_out_text,
                thumbnail_image_url=thumbnail_image_url,
                product_name=product_name,
                message1=message1,
                message2=message2,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option4=option4,
                price2=price2,
                price3=str(price3).removesuffix(".0"),
                delivery_fee=str(delivery_fee).removesuffix(".0"),
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

    crawl_data = SinsunhiCrawlData(
        category=category_state.name,
        product_url=product_url,
        sold_out_text=sold_out_text,
        thumbnail_image_url=thumbnail_image_url,
        product_name=product_name,
        message1="",
        message2=message2,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option4=option4,
        price2="",
        price3="",
        delivery_fee="",
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
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
):
    (R1, R2, R3, R4, R5, R6) = (
        await extract_thumbnail_image(page, product_url),
        await extract_product_name(page),
        await extract_table(page),
        await extract_options(page, product_url),
        await extract_option4(page),
        await extract_html(page, product_url, html_top, html_bottom),
    )

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            log.warning(
                f"Thumbnail not found: {product_url}. Skipping irregular product ..."
            )
            return None

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R3:
        case Ok(table):
            (message2, manufacturing_country) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R4:
        case Ok(options):
            pass
        case Err(err):
            log.error(err)
            options = []

    match R5:
        case Ok(option4):
            pass
        case Err(err):
            log.warning(f"Option4 not found: {product_url}")
            option4 = ""

    detailed_images_html_source = R6

    return (
        thumbnail_image_url,
        product_name,
        message2,
        manufacturing_country,
        options,
        option4,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(page: PlaywrightPage):
    query = "section.pb-4 > section > h1, div.w-full > section:nth-child(1) > h1[class='text-[32px] text-gray-800']"
    if not (product_name := await page.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound, error.PlaywrightTimeoutError)
async def extract_options(page: PlaywrightPage, product_url: str):
    options: list[tuple[str, int | str, int | str, str]] = []

    await open_options(page)

    query = "[id^='radix-:r'] div[role='menuitem'], [id^='radix-:r'] > div > div > div > div"

    message1 = ""
    delivery_fee = 0

    # ? Step 1: Check for the presence of delivery fee and message1 in option text
    for idx in range(len(await page.query_selector_all(query))):
        await open_options(page)

        if not (
            text := await (await page.query_selector_all(query))[idx].text_content()
        ):
            raise error.QueryNotFound(f"Option # {idx} text not found", query=query)

        text = text.strip()

        # ? 배송비: delivery fee
        # ? 택배비: courier fee
        if "배송비" in text or "택배비" in text:
            regex = compile_regex(r"\/\s*(\w*[,]?\w+[,]?\w*\s*원)\s*")

            if match := regex.findall(text):
                delivery_fee = parse_int(match[0])
                message1 = regex.sub("", text).strip()

    # ? Step 2: Click on options that don't contain delivery fee and message1 to get price3
    for idx in range(len(await page.query_selector_all(query))):
        await open_options(page)

        if not (
            text := await (await page.query_selector_all(query))[idx].text_content()
        ):
            raise error.QueryNotFound(f"Option # {idx} text not found", query=query)

        text = text.strip()

        # ? 배송비: delivery fee
        # ? 택배비: courier fee
        if "배송비" in text or "택배비" in text:
            # ? Since delivery fee and message1 was already extracted from this particular option, we will skip this option
            continue

        await (await page.query_selector_all(query))[idx].focus()

        # ? Click on option item
        try:
            await (await page.query_selector_all(query))[idx].click()
        except error.PlaywrightError as err:
            log.error(f"{product_url}\n{err}")
            # ? Option could not be clicked, which means the option was disabled (not clickable)
            # ? e.g., https://www.sinsunhi.com/products/10694 (last two options)
            options.append((text, "", "", ""))
            continue

        price3_query = 'xpath=*//div[button/img[@src="/icons/reset-input-gray-circle@3x.png"]]/span'
        await page.wait_for_selector(price3_query)

        if not (price3 := await page.text_content(price3_query)):
            raise error.QueryNotFound("Price3 not found", query=price3_query)

        price3 = parse_int(price3)

        if delivery_fee:
            price3 = delivery_fee + price3

        # ? We want delivery fee by old rules ONLY if the message1 isn't present
        if not message1:
            delivery_fee_query = "div.py-7.px-6.flex.items-center.justify-between > div > span.text-gray-600"
            if not (
                delivery_fee := await page.text_content(
                    delivery_fee_query, timeout=2000
                )
            ):
                # ? See: https://www.sinsunhi.com/products/13338
                delivery_fee = 0
            else:
                try:
                    delivery_fee = parse_int(delivery_fee)
                except ValueError:
                    log.debug(
                        f"Delivery fee cannot be converted into number: {delivery_fee}"
                    )
                    delivery_fee = 0

        # ? Close the options menu
        await page.click("html")

        # ? Clear the selected option so that price3 won't accumulate when it selects next options
        await page.click('img[src="/icons/reset-input-gray-circle@3x.png"]')
        await page.wait_for_selector(price3_query, state="hidden")

        options.append((text, price3, delivery_fee, message1))

    return options


async def open_options(page: PlaywrightPage):
    options_query = "[id^='radix-:r'] div[role='menuitem'], [id^='radix-:r'] > div > div > div > div"
    if await page.query_selector_all(options_query):
        return None

    # ? Only check for closed option button state
    option1_button_query = (
        "div[class='w-full'] button[id^='radix-:r'][data-state='closed'] svg"
    )

    if await page.query_selector(option1_button_query):
        with suppress(error.PlaywrightTimeoutError):
            await page.click(
                option1_button_query,
                timeout=1500,
            )

        for _ in range(10):
            if await page.query_selector_all(options_query):
                break
            try:
                with suppress(error.PlaywrightTimeoutError):
                    await page.click(option1_button_query, timeout=2500)
            except error.PlaywrightTimeoutError as err:
                raise error.TimeoutException(
                    "Could not click on Options button"
                ) from err
        else:
            # ? Some products have option selection, but without any option, so let's not raise the error
            # ? See: https://www.sinsunhi.com/products/10456
            raise AssertionError(
                f"Options are still not visible after 10 attempts: {page.url}"
            )


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(page: PlaywrightPage):
    query = "xpath=//*/div/div[contains(./h1, '작물/품종')]/span"
    if not (message2 := await page.text_content(query, timeout=2000)):
        query = "xpath=//*/div/div[contains(./span, '작물/품종')]/span[2]"
        if not (message2 := await page.text_content(query)):
            raise error.QueryNotFound("Message2 not found", query=query)

    query = "xpath=//*/div/div[contains(./h1, '산지')]/span"
    if not (manufacturing_country := await page.text_content(query, timeout=2000)):
        query = "xpath=//*/div/div[contains(./span, '산지')]/span[2]"
        if not (manufacturing_country := await page.text_content(query)):
            raise error.QueryNotFound("Manufacturing country not found", query=query)

    return message2, manufacturing_country


@returns_future(error.QueryNotFound)
async def extract_option4(page: PlaywrightPage):
    query = r"section.pt-16 > div.pt-16 > div.mt-14.flex.min-h-\[204px\]"
    if not (option4 := await page.text_content(query)):
        raise error.QueryNotFound("Option4 not found", query)

    return option4.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(page: PlaywrightPage, product_url: str):
    query = "div[class^='relative w-[664px]'] > img"
    if not (thumbnail_image := await page.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns(IndexError, ValueError)
def split_options_text(option1: str):
    price2 = ""

    regex = compile_regex(r"\/\s*(\w*[,]?\w+[,]?\w*\s*원)\s*")
    if match := regex.findall(option1):
        price2 = match[0]
        price2 = parse_int(price2)
        option1 = regex.sub("", option1)

    if "- 품절" in option1:
        return option1.replace("- 품절", "").strip(), "품절", price2 or ""
    if "-품절" in option1:
        return option1.replace("-품절", "").strip(), "품절", price2 or ""
    if "[품절]" in option1:
        return option1.replace("[품절]", "").strip(), "[품절]", price2 or ""
    if "(품절)" in option1:
        return option1.replace("(품절)", "").strip(), "(품절)", price2 or ""
    if "품절" in option1:
        return option1.replace("품절", "").strip(), "품절", price2 or ""

    return option1.strip(), "", price2 or ""


@cache
def image_quries():
    return "div[id='editor-inline'] img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    for _ in range(2):
        await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")
        await page.evaluate("() => { window.scrollBy(0, -window.innerHeight); }")

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
            return await build_html2(page, images, html_top, html_bottom)
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


async def build_html2(
    page: PlaywrightPage, urls: list[str], html_top: str, html_bottom: str
) -> str:
    """
    Build HTML from the URLs of the images based on our template
    """

    html_source = html_top
    html_source = "".join(
        [
            html_source,
            "".join(f"<img src='{url}' /><br />" for url in dict.fromkeys(urls)),
        ]
    )

    if tables := await page.query_selector_all("#editor-inline table tbody"):
        for table in tables:
            if div := await table.query_selector("xpath=.."):
                # ? Parent of <table>
                table_html_source = (await div.inner_html()).strip()

                html_source = "".join(
                    [
                        html_source,
                        "<table>",
                        await asyncio.to_thread(
                            remove_styles_in_table, table_html_source
                        ),
                        "</table>",
                    ]
                )
    else:
        log.warning(f"Specifications Table HTML not found <blue>| {page.url}</>")

    html_source = "".join(
        [
            html_source,
            detailed_images_html_source_bottom2(),
        ],
    )

    return html_source.strip()


@cache
def detailed_images_html_source_bottom2():
    return "<img src='http://ai.esmplus.com/baydam2/common/bottom/02.jpg' /></div>"


def remove_styles_in_table(html_content: str) -> str:
    document: lxml.HtmlElement = lxml.fromstring(html_content)

    td = document.xpath("//*/td")
    for p in td:
        _remove_attrs(p)

    td = document.xpath("//*/tr")
    for p in td:
        _remove_attrs(p)

    return lxml.tostring(document, pretty_print=True, encoding="utf-8").decode("utf-8").replace("\n", "").replace("  ", "")  # type: ignore


def _remove_attrs(p: Any):
    if "style" in p.attrib:
        del p.attrib["style"]
    if "align" in p.attrib:
        del p.attrib["align"]
    if "data-mce-style" in p.attrib:
        del p.attrib["data-mce-style"]
    if "width" in p.attrib:
        del p.attrib["width"]
