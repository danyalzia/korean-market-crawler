# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache
from typing import Any, cast
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML, ProductHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.mscoop import config
from market_crawler.mscoop.data import MscoopCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}&page={next_page_no}"


async def run(settings: Settings):
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#label_U_ID",
        password_query="#label_U_Pass",
        login_button_query="#agreeBtn",
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()

        # ? MSCOOP has blocked the remote computer when sending a lot of requests at once, so just send one request at a time
        categories: Any = await get_categories(sitename=config.SITENAME, rate_limit=1)
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

    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    category_html = CategoryHTML(
        name=category.name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
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

    product_html = ProductHTML(
        category_name=category_state.name,
        pageno=category_state.pageno,
        productid=productid,
        date=category_state.date,
        sitename=config.SITENAME,
    )

    page = await browser.new_page()
    content, _ = await asyncio.gather(
        load_content(
            browser=browser,
            url=product_url,
            html=product_html,
            on_failure="fetch",
            wait_until="networkidle",
            async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
            rate_limit=config.DEFAULT_RATE_LIMIT,
        ),
        visit_link(page, product_url),
    )
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    try:
        (R1, R2, R3, R4, R5) = await asyncio.gather(
            extract_product_name_model_name_and_soldout(document, product_url),
            extract_thumbnail_image(document, product_url),
            extract_manufacturing_country(document),
            extract_table(document),
            extract_images(page, html_top, html_bottom),
        )
    except AssertionError:
        log.debug(
            f"Extraction failed using Document (first time): <blue>{product_url}</>"
        )
        if not (document := await parse_document(await page.content(), engine="lxml")):
            raise HTMLParsingError("Document is not parsed correctly", url=product_url)

        try:
            (R1, R2, R3, R4) = await asyncio.gather(  # type: ignore
                extract_product_name_model_name_and_soldout(document, product_url),
                extract_thumbnail_image(document, product_url),
                extract_manufacturing_country(document),
                extract_table(document),
            )
            R5 = await extract_images(page, html_top, html_bottom)  # type: ignore
        except AssertionError:
            log.debug(
                f"Extraction failed using Document (second time): <blue>{product_url}</>"
            )
            # ? Let's try it after fully loading the page
            await visit_link(page, product_url, wait_until="networkidle")
            await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

            (R1, R2, R3, R4) = await asyncio.gather(  # type: ignore
                extract_product_name_model_name_and_soldout(document, product_url),
                extract_thumbnail_image(document, product_url),
                extract_manufacturing_country(document),
                extract_table(document),
            )
            R5 = await extract_images(page, html_top, html_bottom)  # type: ignore

    (
        product_name,
        model_name,
        sold_out_status_text,
    ) = R1

    match R2:
        case Ok(thumbnail_image):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R3:
        case Ok(manufacturing_country):
            pass
        case Err(err):
            raise error.ManufacturingCountryNotFound(err, url=product_url)

    match R4:
        case Ok(table):
            (
                price3,
                price2,
                delivery_fee,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    detailed_images_html_source = R5

    if "[품절]" in sold_out_status_text:
        sold_out_status_text = "[품절]"
        text_other_than_sold_out = sold_out_status_text.split("[품절]")[-1]
    elif sold_out_status_text:
        text_other_than_sold_out = sold_out_status_text
        sold_out_status_text = ""
    else:
        sold_out_status_text = ""
        text_other_than_sold_out = ""

    crawl_data = MscoopCrawlData(
        product_name=product_name,
        model_name=model_name,
        category=category_state.name,
        sold_out_text=sold_out_status_text,
        text_other_than_sold_out=text_other_than_sold_out,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image,
        detailed_images_html_source=detailed_images_html_source,
        price3=price3,
        price2=price2,
        delivery_fee=delivery_fee,
        manufacturing_country=manufacturing_country,
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )
    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    await page.close()

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"CODE=(\d*\w*)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_selector := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_selector.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "div[class=container] div[class=thumbnailG_70BMA]"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)

    return products


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(
    document: Document | PlaywrightPage, product_url: str
):
    query = "div[class=flexslider] img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


async def extract_product_name_model_name_and_soldout(
    document: Document | PlaywrightPage, product_url: str
):
    product_title = ""
    titles = await document.query_selector_all("p.jgoodsName")
    for title in titles:
        if text := await title.text_content():
            product_title = text

    try:
        assert product_title, product_url
    except AssertionError as err:
        query = ", ".join(
            [
                "div.viewGA > div > p:nth-child(3)",
                "div.viewGA > div > p:nth-child(2)",
                "div.col-xs-12.col-sm-12.col-md-12.pull-left.jgoodsNamewrap",
                "body > div:nth-child(20) > div > div:nth-child(2) > div > div.viewGA > div",
                "body > div:nth-child(20) > div > div:nth-child(2) > div > div.viewGA > div > p:nth-child(3)",
            ]
        )

        if not (product_name_selector := await document.query_selector(query)):
            raise error.QueryNotFound("Product name not found", query=query) from err

        product_title = cast(str, await product_name_selector.text_content())

    assert product_title, product_url

    try:
        product_title_split_list = product_title.strip().split("\n")
        # ? split("\n") works fine even when there is no "\n" in text for splitting, but it gives the same text as single element in list, so in order to check whether sold out text is present or not, we check it's length
        assert len(product_title_split_list) >= 2, product_url

        # ? Sometimes even though sold out text section of product heading is not present, <br> tag (for newline) is in place of it, that is why split("\n") works, but first element is just emptry string, so we need to check for both empty string and IndexError for making sure sold out text is handled properly
        # ? See: http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&E=62461COD000
        sold_out_status_text = product_title_split_list[0]

        # ? assert "" evaluates to False, so we don't need to write assert sold_out_status_text != ""
        assert sold_out_status_text, product_url
    except (IndexError, AssertionError):
        log.warning(f"Sold out text is empty <blue>| {product_url}</>")
        sold_out_status_text = ""
        try:
            product_title_split_list = product_title.strip().split("\n")

            product_name = " ".join(product_title.strip().split(" ")[1:])
            assert product_name, product_url
            assert product_name != " ", product_url
        except IndexError as e:
            raise error.ProductTitleNotFound(
                "Not able to split Product title/heading text into product name, model name and sold out text",
                product_url,
            ) from e

        except AssertionError as e:
            raise error.ProductNameNotFound(product_url) from e

        try:
            model_name = product_title.strip().split(" ")[0].strip()
            assert model_name, product_url
            assert model_name != " ", product_url
        except Exception as e:
            raise error.ModelNameNotFound(product_url) from e
    else:
        try:
            model_name = product_title.strip().split("\n")[1].strip().split(" ")[0]
            product_name = " ".join(
                product_title.strip().split("\n")[1].strip().split(" ")[1:]
            )
            assert product_name, product_url

        except IndexError as e:
            raise error.ProductTitleNotFound(
                "Not able to split Product title/heading text into product name, model name and sold out text",
                product_url,
            ) from e

    return product_name, model_name, sold_out_status_text


@returns_future(error.QueryNotFound)
async def extract_manufacturing_country(document: Document | PlaywrightPage):
    query = "ul[class=Goptc] > li > span:nth-child(2)"  # ? Because the first "span" has 원산지 (origin) as its text
    if not (manufacturing_country := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturing country not found", query)

    return manufacturing_country.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document | PlaywrightPage):
    query = "li[class=goodinfo_xsB]"
    try:
        price3 = cast(
            str,
            (
                await (
                    await (await document.query_selector_all(query))[
                        0
                    ].query_selector_all("span > p")
                )[0].text_content()
            ),
        )
    except (IndexError, AttributeError) as err:
        raise error.QueryNotFound("Price3 not found", query=query) from err

    try:
        price3 = parse_int(price3)
    except ValueError as err:
        raise ValueError(f"Couldn't convert price3 text {price3} to number") from err

    query = "li[class=goodinfo_xsB]"
    try:
        price2 = cast(
            str,
            (
                await (
                    await (await document.query_selector_all(query))[
                        1
                    ].query_selector_all("span > p")
                )[0].text_content()
            ),
        )
    except (IndexError, AttributeError) as err:
        raise error.QueryNotFound("Price2 not found", query=query) from err

    try:
        price2 = parse_int(price2)
    except ValueError as err:
        raise ValueError(f"Couldn't convert price2 text {price2} to number") from err

    query = "li[class=goodinfo_xsB]"
    try:
        delivery_fee = cast(
            str,
            (
                await (
                    await (await document.query_selector_all(query))[
                        3
                    ].query_selector_all("span")
                )[1].text_content()
            ),
        )
    except (IndexError, AttributeError) as err:
        raise error.QueryNotFound("Delivery fee not found", query=query) from err

    try:
        delivery_fee_digits = parse_int(delivery_fee)
    except ValueError:
        delivery_fee = "".join(delivery_fee.split())
    else:
        delivery_fee = delivery_fee_digits

    return price3, price2, delivery_fee


@cache
def get_image_quries():
    return "#horizontalTab > div.resp-tabs-container > div.tab-1.resp-tab-content.resp-tab-content-active > div > ul > li img"


async def extract_images(page: PlaywrightPage, html_top: str, html_bottom: str):
    images_construction: list[str] = []

    html_source = html_top

    # ? Some products have HTML Source button that will give us the raw HTML
    try:
        html_source_button = await page.wait_for_selector(
            "#tip-twitter", state="visible"
        )

        if html_source_button:
            await html_source_button.click()

            if content_source := await page.query_selector("#element_tagviewPopup xmp"):
                content_source = cast(str, await content_source.text_content())
            else:
                raise error.ProductDetailImageNotFound(
                    "Could not copy HTML source from HTML source button. Probably because the user is not logged in",
                    page.url,
                )

            html_source = "".join([html_source, content_source])
            log.debug(f"HTML source button is located <blue>| {page.url}</>")

    except (error.PlaywrightTimeoutError, error.ProductDetailImageNotFound) as e:
        log.warning(f"HTML source button is not located <blue>| {page.url}</>")

        images = await page.query_selector_all(get_image_quries())

        for image_element in images:
            url = ""

            for _ in range(5):
                if url := await image_element.get_attribute("src"):
                    if "base64" in url:
                        await image_element.scroll_into_view_if_needed()
                        await image_element.focus()
                    elif not url.startswith("http"):
                        images_construction.append(f"http:{url}")
                        break
                    else:
                        images_construction.append(url)
                        break

        if not images_construction:
            raise error.ProductDetailImageNotFound(
                "Product detail image not found on the page", page.url
            ) from e

        for image_url in images_construction:
            html_source = "".join([html_source, f"<img src='{image_url}' /><br />"])

    html_source = "".join(
        [
            html_source,
            html_bottom,
        ]
    )

    return html_source.strip()
