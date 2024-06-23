# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from functools import cache, singledispatch
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.artinus import config
from market_crawler.artinus.data import ArtinusCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
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
from robustify.result import Err, Ok, Result, returns_future


@cache
def page_url(*, url: str, page_no: int) -> str:
    if "?page" in url:
        return url.replace(f"?page={page_no-1}", f"?page={page_no}")
    if "&page" in url:
        return url.replace(f"&page={page_no-1}", f"&page={page_no}")

    return f"{url}&page={page_no}"


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#memid",
        password_query="#password",
        login_button_query="#signup > div > div > fieldset > div:nth-child(1) > button",
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
        category_text,
        product_name,
        price2,
        price3,
        model_name,
        manufacturer,
        manufacturing_country,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        options,
        detailed_images_html_source,
    ) = await extract_data(browser, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1_text, option1_value in options.items():
            option1, option2, price2_ = split_options_text(
                option1_text, option1_value, price2
            )
            crawl_data = ArtinusCrawlData(
                category=category_text,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                price2=price2_,
                price3=price3,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
            )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )

        return None

    crawl_data = ArtinusCrawlData(
        category=category_text,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        price2=price2,
        price3=price3,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )


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

    category_page_url = page_url(url=category_url, page_no=category_state.pageno)

    log.action.visit_category(category_name, category_page_url)

    while True:
        category_page_url = page_url(
            url=category_page_url, page_no=category_state.pageno
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


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"&pcode=(.*)")
    return (
        Ok(str(match.group(1)).replace("/", "_"))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("img.icon_img"):
        src = str(await icon.get_attribute("src"))
        if "icon_07" in src:
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

    sold_out_text = await extract_soldout_text(product)

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        category_text,
        product_name,
        price2,
        price3,
        model_name,
        manufacturer,
        manufacturing_country,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        options,
        detailed_images_html_source,
    ) = await extract_data(browser, document, product_url, html_top, html_bottom)

    await page.close()

    if options:
        for option1_text, option1_value in options.items():
            option1, option2, price2_ = split_options_text(
                option1_text, option1_value, price2
            )
            crawl_data = ArtinusCrawlData(
                category=category_text,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                price2=price2_,
                price3=price3,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1,
                option2=option2,
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

    crawl_data = ArtinusCrawlData(
        category=category_text,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        price2=price2,
        price3=price3,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
        option1="",
        option2="",
    )

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    return None


async def extract_data(
    browser: PlaywrightBrowser,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_category_text(document),
        extract_product_name(document),
        extract_prices(document),
        extract_table(document),
        extract_thumbnail_images(document, product_url),
        extract_options(document),
    )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
        R6,
    ) = await gather(*tasks)

    category_text = R1

    match R2:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    price2, price3 = R3

    model_name, manufacturer, manufacturing_country = R4

    if not model_name:
        raise error.ModelNameNotFound(product_url)

    match R5:
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

    match R6:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match await extract_images(document, product_url):
        case Ok(detailed_images_html_source):
            pass
        case Err(error.InvalidImageURL(err)):
            # ? Use Playwright's Page for parsing in case of Base64
            log.debug("Using Playwright's Page for extract_images()")

            page = await browser.new_page()
            await visit_link(page, product_url)

            match await extract_images(page, product_url, html_top, html_bottom):
                case Ok(detailed_images_html_source):
                    pass
                case Err(error.QueryNotFound(err)):
                    log.debug(f"{err}: <yellow>{product_url}</>")
                    detailed_images_html_source = "NOT PRESENT"
                case Err(err):
                    raise error.ProductDetailImageNotFound(err, product_url)

            await page.close()

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err}: <yellow>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    return (
        category_text,
        product_name,
        price2,
        price3,
        model_name,
        manufacturer,
        manufacturing_country,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        options,
        detailed_images_html_source,
    )


async def extract_category_text(document: Document):
    return ">".join(
        [
            text.strip()
            for el in await document.query_selector_all(
                "#contents > div.bg_gray > div > div.xans-element-.xans-product.xans-product-headcategory.path > ol > li > a"
            )
            if (text := await el.text_content())
        ]
    )


@returns_future(IndexError)
async def extract_options(document: Document):
    option1_query = "select[id='opidx']"

    option1_elements = await document.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    return {
        "".join(text.split()): option1_value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (option1_value := await option1.get_attribute("value"))
        and option1_value not in ["", "*", "**"]
    }


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#contents > div.bg_gray > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.headingArea > h2"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


async def extract_prices(document: Document):
    price2_query = "#contents > div.bg_gray > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.disno > div.pricewarp > div"
    price3_query = "#contents > div.bg_gray > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.disno > div.pricewarp > div > p"

    tasks = (
        document.text_content(price2_query),
        document.text_content(price3_query),
    )
    price2, price3 = await asyncio.gather(*tasks)

    if price2 is None:
        raise error.QueryNotFound("Supply price not found", query=price2_query)

    price2 = price2.split("소비자가")  # 소비자가 -> Consumers

    assert len(price2) > 1, f"Split for price2 is not done properly: {price2}"

    price2 = price2[0]

    assert "원" in price2, f"Price2 is invalid: {price2}"

    try:
        price2 = parse_int(price2)
    except ValueError as err:
        raise ValueError(
            f"Price2 text ({price2}) cannot be converted to number"
        ) from err

    if price3 is None:
        raise error.QueryNotFound("Price3 not found", query=price3_query)

    assert "원" in price3, f"Price3 is invalid: {price3}"

    try:
        price3 = parse_int(price3)
    except ValueError as err:
        raise ValueError(
            f"Price3 text ({price3}) cannot be converted to number"
        ) from err

    return price2, price3


async def extract_table(document: Document):
    model_name = manufacturer = manufacturing_country = ""

    query = "#contents > div.bg_gray > div > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.disno > div.xans-element-.xans-product.xans-product-detaildesign > ul"
    if not (
        table_body := await document.query_selector(
            query,
        )
    ):
        raise error.QueryNotFound("Table not found", query=query)

    query = "li > span:nth-child(1)"
    if not (item_headings := await table_body.query_selector_all(query)):
        raise error.QueryNotFound("Table item headings are not found", query=query)

    query = "li > span:nth-child(2)"
    if not (item_values := await table_body.query_selector_all(query)):
        raise error.QueryNotFound("Table item values are not found", query=query)

    assert len(item_headings) == len(
        item_values
    ), f"Item headings and item values are not equal in length ({len(item_headings)} vs {len(item_values)})"

    for key, val in zip(
        item_headings,
        item_values,
    ):
        if not (heading := await key.text_content()):
            continue

        if not (text := await val.text_content()):
            continue

        heading = heading.strip()
        text = text.strip()

        if "자체 상품코드" in heading:
            model_name = text

        if "제조사" in heading:
            manufacturer = text

        if "원산지" in heading:
            manufacturing_country = text

    return model_name, manufacturer, manufacturing_country


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = (
        "div.xans-element-.xans-product.xans-product-addimage.listImg > ul > li > img"
    )
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := await document.query_selector_all(query):
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


def split_options_text(option1_text: str, option1_value: str, price2: int):
    price2 = (
        parse_int(split[-1])
        if ((split := option1_value.split("|")) and len(split) > 1)
        else price2
    )

    if "원" in option1_text:
        regex = compile_regex(r"\s?\([\+|\-]?\w+[,]?\w*원\)")
        option1_text = regex.sub("", option1_text)

    if "[품절]" in option1_text:
        return option1_text.replace("[품절]", ""), "[품절]", price2
    elif "(품절)" in option1_text:
        return option1_text.replace("(품절)", ""), "품절", price2
    elif "품절" in option1_text:
        return option1_text.replace("품절", ""), "품절", price2

    return option1_text, "", price2


@cache
def image_quries():
    return ", ".join(
        [
            "#icontab_one > div > div > div > img",
            "#icontab_one > div > div > img",
            "#icontab_one > div img",
        ]
    )


@singledispatch
@returns_future(error.QueryNotFound, error.InvalidImageURL, error.Base64Present)
async def extract_images(
    document_or_page: Document | PlaywrightPage, product_url: str, settings: Settings
) -> str: ...


@extract_images.register
@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def _(
    document_or_page: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = [
        src
        for image in await document_or_page.query_selector_all(query)
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
    document_or_page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()
    if not (elements := await document_or_page.query_selector_all(query)):
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    urls: list[str] = []
    for el in elements:
        action = lambda: get_srcs(el)
        focus = lambda: focus_element(document_or_page, el)
        is_base64 = isin("base64")
        match await do(action).retry_if(
            predicate=is_base64,
            on_retry=focus,
            max_tries=5,
        ):
            case Ok(src):
                urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

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
