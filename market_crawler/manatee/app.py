# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from contextlib import suppress
from functools import cache
from urllib.parse import urljoin

from aiofile import AIOFile
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
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.manatee import config
from market_crawler.manatee.data import ManateeCrawlData
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


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "http://rain119.co.kr/goods/goods_list.php?cateCd=001")

    categories = await page.query_selector_all(
        "#top > div.top-service > div > div.lnb > ul > li"
    )

    for category in categories:
        if not (el := await category.query_selector("a")):
            continue

        if not (category_text := await el.text_content()):
            continue

        if not (category_page_url := await el.get_attribute("href")):
            continue

        print(category_text)
        category_page_url = urljoin(page.url, category_page_url)

        subcategories = await category.query_selector_all(".sub-category > ul > li")
        print(len(subcategories))

        # ? Not all main categories may have sub categories
        if not subcategories:
            full_subcategories.append(Category(category_text, category_page_url))

        for subcategory in subcategories:
            if not (eel := await subcategory.query_selector("a")):
                continue

            if not (subcategory_page_url := await eel.get_attribute("href")):
                continue

            if not (subcategory_text := await eel.text_content()):
                continue

            url = urljoin(
                page.url,
                subcategory_page_url,
            )

            haschilds = await subcategory.query_selector_all("ul > li")
            # not_incl = await subcategory.query_selector_all("ul > li > ul > li")

            # ? Not all main sub-categories may have childs
            if not haschilds:
                full_text = f"{category_text}>{subcategory_text}"
                full_subcategories.append(Category(full_text, url))

            for haschild in haschilds:
                if not (eeel := await haschild.query_selector("a")):
                    continue

                if not (haschild_page_url := await eeel.get_attribute("href")):
                    continue

                if not (haschild_text := await eeel.text_content()):
                    continue

                haschild_url = urljoin(
                    page.url,
                    haschild_page_url,
                )

                has_nested_childs = await haschild.query_selector_all("ul > li")

                if not has_nested_childs:
                    full_text = f"{category_text}>{subcategory_text}>{haschild_text}"
                    full_subcategories.append(Category(full_text, haschild_url))

                for has_nested_child in has_nested_childs:
                    if not (eeeel := await has_nested_child.query_selector("a")):
                        continue

                    if not (
                        has_nested_child_page_url := await eeeel.get_attribute("href")
                    ):
                        continue

                    if not (has_nested_child_text := await eeeel.text_content()):
                        continue

                    has_nested_child_url = urljoin(
                        page.url,
                        has_nested_child_page_url,
                    )

                    full_text = f"{category_text}>{subcategory_text}>{haschild_text}>{has_nested_child_text}"
                    full_subcategories.append(Category(full_text, has_nested_child_url))

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    await page.click(login_button_query)

    await page.wait_for_selector("text='LOGOUT'")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input#loginId",
        password_query="input#loginPwd",
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

        if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
            subcategories = await get_categories(
                sitename=config.SITENAME, filename="subcategories.txt"
            )
        else:
            subcategories = await find_subcategories(browser)
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
        Ok(str(match.group(0)))
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
    query = "#content > div.contents > div > div.cg-main > div.goods-list > div.item-display > div > ul > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all(".soldout-img"):
        alt = str(await icon.text_content())
        if "품절" in alt:
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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        quantity,
        delivery_fee,
        model_name,
        message2,
        brand,
        price3,
        price2,
        options,
        detailed_images_html_source,
        message1,
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

                crawl_data = ManateeCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    product_name=product_name,
                    quantity=quantity,
                    delivery_fee=delivery_fee,
                    model_name=model_name,
                    message2=message2,
                    brand=brand,
                    price3=price3,
                    price2=_price2,
                    detailed_images_html_source=detailed_images_html_source,
                    message1=message1,
                    sold_out_text=sold_out_text,
                    option1=option1,
                    option2=option2,
                    option3=str(option3),
                )
            else:
                crawl_data = ManateeCrawlData(
                    category=category_state.name,
                    product_url=product_url,
                    thumbnail_image_url=thumbnail_image_url,
                    thumbnail_image_url2=thumbnail_image_url2,
                    thumbnail_image_url3=thumbnail_image_url3,
                    thumbnail_image_url4=thumbnail_image_url4,
                    thumbnail_image_url5=thumbnail_image_url5,
                    product_name=product_name,
                    quantity=quantity,
                    delivery_fee=delivery_fee,
                    model_name=model_name,
                    message2=message2,
                    brand=brand,
                    price3=price3,
                    price2=price2,
                    detailed_images_html_source=detailed_images_html_source,
                    message1=message1,
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

    crawl_data = ManateeCrawlData(
        category=category_state.name,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        quantity=quantity,
        delivery_fee=delivery_fee,
        model_name=model_name,
        message2=message2,
        brand=brand,
        price3=price3,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        message1=message1,
        sold_out_text=sold_out_text,
        option1="",
        option2="",
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
        extract_multiple_thumbnail_image(document, product_url),
        extract_product_name(document),
        extract_table(document),
        extract_options(page),
        extract_message1(document),
    )

    (R1, R2, R3, R4, R5, R6) = await gather(*tasks)

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    (
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    ) = R2

    match R3:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R4:
        case Ok(table):
            quantity, delivery_fee, model_name, message2, brand, price3, price2 = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R5:
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    match R6:
        case Ok(message1):
            pass
        case Err(err):
            raise error.Message1NotFound(err, url=product_url)

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
        quantity,
        delivery_fee,
        model_name,
        message2,
        brand,
        price3,
        price2,
        options,
        detailed_images_html_source,
        message1,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "div.top > div.tit > h2"
    return (
        product_name.strip()
        if (product_name := await document.text_content(query))
        else ""
    )


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document):
    query = "div#detail span"
    return message1.strip() if (message1 := await document.text_content(query)) else ""


@returns_future(IndexError, error.PlaywrightTimeoutError)
async def extract_options(page: PlaywrightPage):
    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_load_state("networkidle")

    options: list[str] = []

    if await page.query_selector("a.gv-notorderpossible"):
        return options

    total_option_selectors = len(await page.query_selector_all(".list span"))

    if total_option_selectors == 1:
        option1_query = "select[name='optionSnoInput']"
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

        options.extend(iter(option1))
    elif total_option_selectors == 2:
        option1_query = "div:nth-of-type(3) div .chosen-container a[class='chosen-single chosen-sch']"
        if locator := await page.query_selector(option1_query):
            await locator.click()
            await asyncio.sleep(1)

        option1_elements_query = "#frmView > div > div:nth-child(5) > div > div > div > div > ul.chosen-results > li"

        total_option1_elements = len(
            await page.query_selector_all(option1_elements_query)
        )

        for idx in range(1, total_option1_elements):
            while True:
                try:
                    option1_trick = (
                        await page.query_selector_all(option1_elements_query)
                    )[idx]
                except IndexError:
                    # print(f"IndexError: {idx}")
                    if locator := await page.query_selector(option1_query):
                        await locator.click()
                        await asyncio.sleep(1)
                else:
                    try:
                        await option1_trick.click()
                    except error.PlaywrightError:
                        # print(f"PlaywrightError: {idx}")
                        if locator := await page.query_selector(option1_query):
                            await locator.click()
                            await asyncio.sleep(1)
                    else:
                        await asyncio.sleep(1)
                        break

            option1_text = await option1_trick.text_content()

            option2_query = "select[name='optionNo_1']"
            option2_elements = await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )

            option2 = {
                "".join(text.split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                and value not in ["", "*", "**"]
            }

            options.extend(f"{option1_text},{option2_text}" for option2_text in option2)

    return options


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document | PlaywrightPage):
    quantity = delivery_fee = model_name = message2 = brand = ""
    price3 = price2 = 0

    query = "#frmView > div.info > div.item > ul"
    if not (table_tbody := await document.query_selector(query)):
        raise error.QueryNotFound("Table not found", query=query)

    ths = await table_tbody.query_selector_all("li > strong")
    tds = await table_tbody.query_selector_all("li > div")

    for th, td in zip(ths, tds):
        if not (heading := await th.text_content()):
            continue

        if not (text := await td.text_content()):
            continue

        # price3
        if "정가" in heading:
            price3_str = text
            try:
                price3 = parse_int(price3_str)
            except ValueError as err:
                raise ValueError(
                    f"Coudn't convert price3 text {price3_str} to number"
                ) from err

        # price2
        if "판매가" in heading:
            price2_str = text
            try:
                price2 = parse_int(price2_str)
            except ValueError as err:
                # raise ValueError(
                #     f"Coudn't convert price2 text {price2_str} to number"
                # ) from err
                price2 = price2_str

        # quantity
        if "구매제한" in heading:
            quantity = text

        # delivery_fee
        if "배송비" in heading:
            if (el := await td.query_selector("span")) and (
                delivery_fee_text := await el.text_content()
            ):
                delivery_fee = delivery_fee_text

        # model_name
        if heading == "상품코드":
            model_name = text

        # message2
        if "자체상품코드" in heading:
            message2 = text

        # brand
        if "브랜드" in heading:
            brand = text.strip()

    return quantity, delivery_fee, model_name, message2, brand, price3, price2


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "div.thumbnail img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


async def extract_multiple_thumbnail_image(document: Document, product_url: str):
    query = ".more-thumbnail span:nth-child(n+2) img"
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := await document.query_selector_all(query):
        thumbnail_images = [
            urljoin(product_url, await ti.get_attribute("src"))
            for ti in thumbnail_images
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
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    )


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if "+" in option1:
        regex = compile_regex(r"\s?\+\w+[,]?\w*원")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if "-" in option1:
        regex = compile_regex(r"\s?\-\w+[,]?\w*원")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""

    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return "#detail img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    query = image_quries()
    unwanted_url = "http://ai.esmplus.com/manatee4155/rain119/notice/%EC%83%81%EB%8B%A8%EA%B3%B5%EC%A7%80%EC%82%AC%ED%95%AD.jpg"

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

    if unwanted_url in urls:
        urls.remove(unwanted_url)

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