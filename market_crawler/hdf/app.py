# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache, singledispatch
from typing import NamedTuple, overload
from urllib.parse import urljoin

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
from market_crawler.hdf import config
from market_crawler.hdf.data import HDFCrawlData
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
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def run(settings: Settings) -> None:
    print(f"{config.HEADLESS = }")
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
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    match await extract_product_name(document):
        case Ok(name):
            product_name = name
        case Err(err):
            raise error.ProductNameNotFound(err, url=category_page_url)

    match await extract_thumbnail_image(document, product_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match await extract_model_name(document):
        case Ok(model_name):
            pass
        case Err(err):
            # ? Some products don't have model name: https://shop.ihdf.co.kr/shop_goods/goods_view.htm?category=03080300&goods_idx=8484&goods_bu_id=
            log.warning(f"Model name is not found: {product_url}")
            model_name = ""

    if not (delivery_fee := (await extract_delivery_fee(document)).ok()):
        raise error.DeliveryFeeNotFound(product_url)

    match await extract_images(document, product_url):
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

    if await is_options_changing_prices_present(page):
        all_price3_option1 = await extract_options2(page)

        for price3, option1 in all_price3_option1:
            crawl_data = HDFCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                model_name=model_name,
                delivery_fee=delivery_fee,
                detailed_images_html_source=detailed_images_html_source,
                price3=price3,
                option1=option1,
            )
            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_crawled_with_options(
            idx,
            category_state.name,
            category_state.pageno,
            product_url,
            len(all_price3_option1),
        )
        await page.close()
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    match await extract_price3(document):
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    crawl_data = HDFCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        model_name=model_name,
        delivery_fee=delivery_fee,
        detailed_images_html_source=detailed_images_html_source,
        price3=price3,
        option1="",
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )
    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await page.close()


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"goods_idx=(\w+)")
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


@overload
async def get_products(
    tree: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    tree: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(tree: Document | PlaywrightPage):
    query = "#container > div.contents.goods_list > div.glores-A-goods-list.item_box.item_list > ul > li > div > div[class='item_figure']"
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
async def extract_product_name(document: Document):
    query = "#container > div.contents > div.goods_detail > div > div > div.goods_info > div > h3"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, category_page_url: str):
    query = "#goods_view_img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(category_page_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "div.info > form dd[class='model']"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query=query)

    return model_name


@returns_future(error.QueryNotFound)
async def extract_delivery_fee(document: Document):
    query = "div.info > form select[name='gv_move_sel'] > option"
    if not (delivery_fee := await document.text_content(query)):
        raise error.QueryNotFound("Delivery fee not found", query=query)

    return delivery_fee


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document | PlaywrightPage):
    query = "div.info > form dl.col_price > dd > strong"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price3 not found", query=query)

    return parse_int(price3.strip())


class TextChangingPrice(NamedTuple):
    text: str
    price3: int


async def is_options_changing_prices_present(page: PlaywrightPage):
    if options_dropdown := (
        await page.query_selector_all(
            "div.info > form select[name='multi_price_no'] > option"
        )
    ):
        if len(options_dropdown) > 1:
            for option in options_dropdown:
                if (text := await option.text_content()) and ":" in text:
                    return True

    return False


class OptionsData(NamedTuple):
    price3: int
    option1: str


async def extract_options2(page: PlaywrightPage):
    all_crawl_data: list[OptionsData] = []

    option1_selectors = await page.query_selector_all(
        "div.info > form select[name='multi_price_no'] > option"
    )

    options: dict[str, str] = {
        value: text
        for selector in option1_selectors
        if (text := await selector.text_content())
        and (value := await selector.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    texts_changing_price: list[TextChangingPrice] = []
    texts_not_changing_price: list[TextChangingPrice] = []

    initial_price3: int = (await extract_price3(page)).unwrap()
    # print(f"{initial_price3 = }")

    for value, text in options.items():
        if not (
            option1_selector := await page.query_selector(
                "div.info > form select[name='multi_price_no']"
            )
        ):
            raise ValueError("Cannot select the option1 dropdown")

        await option1_selector.scroll_into_view_if_needed()
        await option1_selector.focus()
        with suppress(error.PlaywrightTimeoutError):
            await option1_selector.click()
            await option1_selector.select_option(value)
        try:
            price3 = parse_int(text.split(":")[-1])
            option1 = text.split(":")[0].strip()

        except Exception as err:
            raise Exception(f"{text} | {page.url}") from err

        if price3 != initial_price3:
            # print(f"'{text}' changes the price from {initial_price3} to {price3}")
            texts_changing_price.append(TextChangingPrice(option1, price3))
        else:
            # print(
            #     f"'{text}' does not change the price from {initial_price3} to {price3}"
            # )
            texts_not_changing_price.append(TextChangingPrice(option1, initial_price3))

    if texts_changing_price:
        # ? It doesn't matter which index we take, as the prices are same for common prices
        common_price3 = texts_changing_price[0].price3
        same_group = False
        for text_changing in texts_changing_price:
            same_group = text_changing.price3 == common_price3
        if same_group:
            for item in texts_changing_price:
                same_group_list: list[TextChangingPrice] = []
                common_price3 = item.price3
                for text_changing in texts_changing_price:
                    if text_changing.price3 == common_price3:
                        common_price3 = text_changing.price3
                        same_group_list.append(
                            TextChangingPrice(
                                text=text_changing.text,
                                price3=text_changing.price3,
                            )
                        )

                combined_option = ",".join([text.text for text in same_group_list])

                # ? We don't want duplicates in all_crawl_data (which itself is not hashable)
                # TODO: This is a bit hacky and might be slow, so needs testing for other alternatives
                if all(
                    combined_option not in crawl.option1 for crawl in all_crawl_data
                ):
                    price3 = int(same_group_list[0].price3)
                    option1 = "".join(combined_option.split())
                    all_crawl_data.append(OptionsData(price3, option1))
        else:
            for text_changing in texts_changing_price:
                price3 = int(text_changing.price3)
                option1 = text_changing.text
                all_crawl_data.append(OptionsData(price3, option1))

    if texts_not_changing_price:
        # ? It doesn't matter which index we take, as the prices are same for common prices
        common_price3 = texts_not_changing_price[0].price3
        same_group = False
        for text_not_changing in texts_not_changing_price:
            same_group = text_not_changing.price3 == common_price3
        if same_group:
            combined_option = ",".join([text.text for text in texts_not_changing_price])
            price3 = int(texts_not_changing_price[0].price3)
            option1 = "".join(combined_option.split())
            all_crawl_data.append(OptionsData(price3, option1))
        else:
            for text_not_changing in texts_not_changing_price:
                price3 = int(text_not_changing.price3)
                option1 = text_not_changing.text
                all_crawl_data.append(OptionsData(price3, option1))

    return all_crawl_data


@cache
def image_quries():
    return "#container > div.contents > div.goods_dscr > div.tab_con img"


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
    query = image_quries()

    urls = [
        src
        for image in await document.query_selector_all(query)
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
    await page.mouse.wheel(delta_x=0, delta_y=500)
    await page.mouse.wheel(delta_x=0, delta_y=-500)

    await element.scroll_into_view_if_needed()
    await element.focus()
