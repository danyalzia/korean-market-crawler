# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache, singledispatch
from typing import NamedTuple, overload
from urllib.parse import urljoin

import backoff

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
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
from market_crawler.luxgolf import config
from market_crawler.luxgolf.data import LuxgolfCrawlData
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify import MaxTriesReached
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

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            subcategories = await get_categories(sitename=config.SITENAME)
            log.detail.total_categories(len(subcategories))

            crawler = ConcurrentCrawler(
                categories=subcategories,
                start_category=config.START_CATEGORY,
                end_category=config.END_CATEGORY,
                chunk_size=config.CATEGORIES_CHUNK_SIZE,
                crawl=crawl,
            )
            await crawl_categories(crawler, browser, settings, columns)
            return None

        # ? Don't exceed 2 if custom url is being crawled otherwise the website becomes very flaky
        config.MAX_PRODUCTS_CHUNK_SIZE = min(config.MAX_PRODUCTS_CHUNK_SIZE, 2)
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

    for retry in range(1, 11):
        try:
            (
                thumbnail_image_url,
                product_name,
                model_name,
                price3,
                options,
                detailed_images_html_source,
            ) = await extract_data(browser, product_url, html_top, html_bottom)
        except error.ThumbnailNotFound as err:
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
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data from product url ({product_url}) even after 10 retries"
        )

    if options:
        for option in options:
            match split_options_text(option, price3):
                case Ok(result):
                    option1, _price3, option2, option3 = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = LuxgolfCrawlData(
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                price3=_price3,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
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

    crawl_data = LuxgolfCrawlData(
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        price3=price3,
        detailed_images_html_source=detailed_images_html_source,
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
        name=category.name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
    )

    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    page = await browser.new_page()

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        await visit_link(page, category_page_url, wait_until="networkidle")

        if not (number_of_products := await has_products(page)):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        log.detail.total_products_on_page(number_of_products, category_state.pageno)

        await category_html.save(await page.content())

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
                    number_of_products,
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
    number_of_products: int,
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

    assert (
        len(products) == number_of_products
    ), "Total number of products on the page seems to have been changed"

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

    for retry in range(1, 11):
        try:
            (
                thumbnail_image_url,
                product_name,
                model_name,
                price3,
                options,
                detailed_images_html_source,
            ) = await extract_data(browser, product_url, html_top, html_bottom)
        except error.ThumbnailNotFound as err:
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
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data from product url ({product_url}) even after 10 retries"
        )

    if options:
        for option in options:
            match split_options_text(option, price3):
                case Ok(result):
                    option1, _price3, option2, option3 = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = LuxgolfCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                price3=_price3,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
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

    crawl_data = LuxgolfCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        price3=price3,
        detailed_images_html_source=detailed_images_html_source,
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
    product_name: str
    model_name: str
    price3: int
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    browser: PlaywrightBrowser, product_url: str, html_top: str, html_bottom: str
):
    page = await browser.new_page()
    await visit_product_link(page, product_url)

    content = await page.content()

    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (R1, R2, R3, R4, R5) = await asyncio.gather(
        extract_thumbnail_image(document, product_url),
        extract_product_title(document),
        extract_table(document),
        extract_options(page),
        extract_images(document, product_url, html_top, html_bottom),
    )

    match R1:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            await page.close()
            page = await browser.new_page()
            await visit_product_link(page, product_url)

            content = await page.content()
            if not (document2 := await parse_document(content, engine="lxml")):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document2
            (R1, R2, R3, R4, R5) = await asyncio.gather(  # type: ignore
                extract_thumbnail_image(document, product_url),
                extract_product_title(document),
                extract_table(document),
                extract_options(page),
                extract_images(document, product_url, html_top, html_bottom),
            )
            match R1:
                case Ok(thumbnail_image_url):
                    pass
                case Err(err):
                    raise error.ThumbnailNotFound(err, url=product_url)

    match R2:
        case Ok(product_title):
            model_name = product_title.split()[-1]
            product_name = " ".join(product_title.split()[:-1])
        case Err(err):
            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )
            match await extract_product_title(document2):
                case Ok(product_title):
                    model_name = product_title.split()[-1]
                    product_name = " ".join(product_title.split()[:-1])
                case Err(err):
                    raise error.ProductTitleNotFound(err, url=product_url)

    match R3:
        case Ok(table):
            price3 = table
        case Err(err):
            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )
            match await extract_table(document2):
                case Ok(table):
                    price3 = table
                case Err(err):
                    raise error.SellingPriceNotFound(err, url=product_url)

    match R4:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R5:
        case Ok(detailed_images_html_source):
            pass

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"

        case Err(error.InvalidImageURL(err)):
            # ? Use Playwright's Page for parsing in case of Base64
            match await extract_images(page, product_url, html_top, html_bottom):
                case Ok(detailed_images_html_source):
                    pass
                case Err(error.QueryNotFound(err)):
                    log.debug(f"{err}: <yellow>{product_url}</>")
                    detailed_images_html_source = "NOT PRESENT"
                case Err(err):
                    raise error.ProductDetailImageNotFound(err, product_url)

        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    await page.close()

    return Data(
        thumbnail_image_url,
        product_name,
        model_name,
        price3,
        options,
        detailed_images_html_source,
    )


@backoff.on_exception(
    backoff.expo,
    error.InvalidURL,
    max_tries=30,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def visit_product_link(page: PlaywrightPage, product_url: str):
    await visit_link(page, product_url, wait_until="networkidle")

    # ? When there are a lot of requests at once, LUXGOLF gives the error
    if forbidden_img := await page.query_selector(
        "body > center > table > tbody > tr:nth-child(2) > td > img"
    ):
        if (src := await forbidden_img.get_attribute("src")) and "403page" in src:
            msg = "Forbidden page error occurred. Retrying ..."
            log.warning(msg)
            raise error.InvalidURL(msg, url=product_url)


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\?branduid=(\w+)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(tree: PlaywrightPage) -> int | None:
    match await get_products(tree):
        case Ok(products):
            return len(products)
        case _:
            return None


@overload
async def get_products(
    tree: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    tree: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(tree: Document | PlaywrightPage):
    query = "#prdBrand > div.item-wrap > div.product_wrap div.prod_thumb"
    return (
        Ok(products)
        if (products := await tree.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, category_page_url: str):
    query = "#productDetail > div > div.thumb-info-wrap > div > div > div > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(category_page_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_product_title(document: Document):
    query = "h3.tit-prd"
    if not (product_title := await document.text_content(query)):
        raise error.QueryNotFound("Product title not found", query=query)

    return product_title


@returns_future(error.QueryNotFound)
async def extract_table(
    document: Document,
):
    query = "#form1 > div > div > div.table-opt > table > tbody > tr:nth-child(3) > th > div"
    if not (discount_price_heading := await document.text_content(query)):
        raise error.QueryNotFound("Discount price heading is not present", query=query)

    if "할인 가격" in discount_price_heading:
        if price3 := await document.text_content(
            "//*[@id='form1']//tr[contains(./th/div/text(), '할인 가격')]/td/div"
        ):
            return parse_int(price3)

    if sell_price_heading := await document.text_content(
        "#form1 > div > div > div.table-opt > table > tbody > tr:nth-child(2) > th > div"
    ):
        if "판매가격" in sell_price_heading:
            if price3 := await document.text_content(
                "//*[@id='form1']//tr[contains(./th/div/text(), '판매가격')]/td/div"
            ):
                return parse_int(price3)

    if price3 := await document.text_content(
        "//*[@id='form1']//tr[contains(./th/div/text(), '적립금')]/td/div"
    ):
        return parse_int(price3)

    raise error.QueryNotFound("Sell price is not present", query=query)


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    option1_query = (
        "xpath=/*//dl[contains(./dt/text(), '색상')]//select[@name='optionlist[]']"
    )
    option2_query = (
        "xpath=/*//dl[contains(./dt/text(), '사이즈')]//select[@name='optionlist[]']"
    )

    # ? Sometimes options order is swaped
    # ? See: http://www.luxgolf.net/shop/shopdetail.html?branduid=111453&xcode=055&mcode=002&scode=004&type=X&sort=regdate&cur_code=055&GfDT=bm90W14%3D

    # ? First option is default text that is why we need to take it into consideration in our conditional logic
    if (
        (option1_elements := await page.query_selector_all(f"{option1_query}//option"))
        and len(option1_elements) < 2
    ) and (
        (option2_elements := await page.query_selector_all(f"{option2_query}//option"))
        and len(option2_elements) > 1
    ):
        option1_query, option2_query = option2_query, option1_query

    option1_elements = await page.query_selector_all(f"{option1_query}//option")

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := (await option1.text_content()))
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    # ? If option2 is not present, then we don't need to use Page methods
    if not (await page.query_selector(f"{option2_query}//option")):
        return list(option1.keys())

    options: list[str] = []

    for option1_text, option1_value in option1.items():
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
            "".join(text.split()): option2_value
            for option2 in await page.query_selector_all(f"{option2_query}//option")
            if (text := (await option2.text_content()))
            and (option2_value := await option2.get_attribute("value"))
            not in ["", "*", "**"]
        }

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if "(+" in option1:
        regex = compile_regex(r"\s?\(\+\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if "(-" in option1:
        regex = compile_regex(r"\s?\(\-\w+[,]?\w*원?\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 -= additional_price

    if "- [품절]" in option1:
        return (
            option1.replace("- [품절]", "").strip(),
            price2,
            "- [품절]",
            option3 or "",
        )
    elif "-[품절]" in option1:
        return option1.replace("-[품절]", "").strip(), price2, "-[품절]", option3 or ""
    elif "[품절]" in option1:
        return option1.replace("[품절]", "").strip(), price2, "[품절]", option3 or ""
    elif "- 품절" in option1:
        return option1.replace("- 품절", "").strip(), price2, "- 품절", option3 or ""
    elif "-품절" in option1:
        return option1.replace("-품절", "").strip(), price2, "-품절", option3 or ""
    elif "품절" in option1:
        return option1.replace("품절", "").strip(), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return "#productDetail > div > div.prd-detail img"


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    parent = (
        frame
        if (frame := await document.query_selector("iframe[name='contents_frame']"))
        else document
    )

    query = image_quries()

    urls = [
        src
        for image in await parent.query_selector_all(query)
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
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    if frame := page.frame(name="contents_frame"):
        elements = await frame.query_selector_all(query)
    else:
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

    await element.scroll_into_view_if_needed()
    await element.focus()
