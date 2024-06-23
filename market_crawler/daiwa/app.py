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

import backoff
import pandas as pd
import playwright.async_api as playwright

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.extraction import load_page, visit_link
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.daiwa import config
from market_crawler.daiwa.data import DaiwaCrawlData
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import ProductHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state
from robustify.result import Err, Ok, Result, returns_future


@cache
async def read_page_data(page_no: int) -> pd.DataFrame:
    path = os.path.join("urls", f"{page_no}.xlsx")
    df: pd.DataFrame = await asyncio.to_thread(pd.read_excel, path, dtype=str)  # type: ignore
    df = df[df["itemUrl"].notnull()]
    return df


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
        user_id_query="#username",
        password_query="#password",
        login_button_query="form button:has-text('로그인')",
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()
        categories = await get_categories(sitename=config.SITENAME)

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


@backoff.on_exception(
    backoff.expo,
    error.IncorrectData,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def get_total_pages(page: playwright.Page, category_url: str):
    query = "#sencha_view"
    if not (frame := page.frame_locator(query)):
        raise error.QueryNotFound("Frame not found", query=query)

    await page.wait_for_load_state("networkidle", timeout=30000)

    # ? This will only give us not sold out products
    with suppress(error.PlaywrightTimeoutError):
        async with page.expect_request_finished():
            await frame.locator("#checkbox-1046-displayEl").click()
        await page.wait_for_load_state("networkidle", timeout=30000)

    # ? We need to click on '조회' button to load the products table
    query = 'a[role="button"]:has-text("조회")'

    await frame.locator(query).click()

    with suppress(error.PlaywrightTimeoutError):
        async with page.expect_request_finished():
            await frame.locator(query).click()
        await page.wait_for_load_state("networkidle", timeout=30000)

    query = "#tbtext-1056"
    if not (total_pages := await frame.locator(query).text_content()):
        raise error.QueryNotFound("Total pages text not found", query=query)

    total_pages = parse_int(total_pages)
    if total_pages == 0:
        await page.goto(category_url, wait_until="networkidle")
        error.IncorrectData("Total pages are 0")

    return total_pages


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    # category_url = category.url
    category_name = category.name

    if not (
        category_state := await get_category_state(
            config=config,
            category_name=category_name,
            date=settings.DATE,
        )
    ):
        return None

    # ? Let's just hardcode total pages for now
    # page = await browser.context.new_page()
    # await page.goto(category_url, wait_until="networkidle")

    # total_pages = await get_total_pages(page, category_url)

    total_pages = 15
    log.detail.total_pages(total_pages)

    while category_state.pageno <= total_pages:
        page_data = await read_page_data(category_state.pageno)
        number_of_products = len(page_data)

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
                    page_data,
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


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(
        r"\/j[p|a]\/(.*)\/\w*",
    )
    return (
        Ok(str(match.group(1)).replace("/", "_"))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def is_page_not_found(page: PlaywrightPage):
    return bool(
        (heading := await page.query_selector("body > h1"))
        and (await heading.text_content()) == "Not Found"
    )


async def is_page_empty(page: PlaywrightPage):
    return bool(
        (body := await page.query_selector("body"))
        and (await body.text_content())
        in {
            "\n",
            "",
            " ",
        }
    )


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    page_data: pd.DataFrame,
    category_state: CategoryState,
    filename: str,
    columns: list[str],
    settings: Settings,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    model_name: str = page_data["stndDetailItem"].iloc[idx]  # type: ignore
    product_name: str = page_data["nmItem"].iloc[idx]  # type: ignore
    product_url: str = page_data["itemUrl"].iloc[idx]  # type: ignore

    productid = get_productid(product_url).expect(
        f"Product ID not found: {product_url}"
    )
    product_html = ProductHTML(
        category_name=category_state.name,
        pageno=category_state.pageno,
        productid=productid,
        date=category_state.date,
        sitename=config.SITENAME,
    )

    page = await load_page(
        browser=browser,
        url=product_url,
        html=product_html,
        on_failure="visit",
        rate_limit=config.DEFAULT_RATE_LIMIT,
        async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
        wait_until="networkidle",
    )
    content = await page.content()
    if config.SAVE_HTML and not await product_html.exists():
        await product_html.save(content)

    try:
        await page.set_content(content)
    except error.PlaywrightTimeoutError:
        await visit_link(page, product_url, wait_until="networkidle")

    if await is_page_not_found(page):
        log.warning("Page contains 'Not Found' text, so skipping it.")
        await page.close()
        return

    if await is_page_empty(page):
        log.warning("Page is empty, so skipping it.")
        await page.close()
        return

    # await translate(page, product_url)

    match await extract_thumbnail_images(page, product_url):
        case Ok(thumbnail_images):
            thumbnail_images = thumbnail_images[:5]
        case Err(err):
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url="not present",
                detailed_images_html_source="not present",
            )
            await page.close()

            log.action.product_crawled(
                idx, category_state.name, category_state.pageno, crawl_data.product_url
            )
            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

            return None

    match await extract_images(
        page, thumbnail_images, product_url, html_top, html_bottom
    ):
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    # ? Let's save the translated page
    await product_html.save(await page.content())

    match thumbnail_images:
        case [url1, url2, url3, url4, url5]:
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=url1,
                thumbnail_image_url2=url2,
                thumbnail_image_url3=url3,
                thumbnail_image_url4=url4,
                thumbnail_image_url5=url5,
                detailed_images_html_source=detailed_images_html_source,
            )

        case [url1, url2, url3, url4]:
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=url1,
                thumbnail_image_url2=url2,
                thumbnail_image_url3=url3,
                thumbnail_image_url4=url4,
                detailed_images_html_source=detailed_images_html_source,
            )

        case [url1, url2, url3]:
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=url1,
                thumbnail_image_url2=url2,
                thumbnail_image_url3=url3,
                detailed_images_html_source=detailed_images_html_source,
            )

        case [url1, url2]:
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=url1,
                thumbnail_image_url2=url2,
                detailed_images_html_source=detailed_images_html_source,
            )

        case [url1]:
            crawl_data = DaiwaCrawlData(
                product_url=product_url,
                product_name=product_name,
                model_name=model_name,
                thumbnail_image_url=url1,
                detailed_images_html_source=detailed_images_html_source,
            )
        case _:
            raise error.ThumbnailNotFound(
                "No thumbnail image URL is present", url=product_url
            )

    await page.close()

    log.action.product_crawled(
        idx, category_state.name, category_state.pageno, crawl_data.product_url
    )
    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )


@returns_future(error.QueryNotFound)
@backoff.on_exception(
    backoff.expo,
    error.QueryNotFound,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def extract_thumbnail_images(page: PlaywrightPage, product_url: str):
    # ? Translating it here screws the loaded the thumbnail images
    # await translate(page, product_url)

    query = "li[id^='slick-slide'] button, #i_a_mv_img > img, #co_contents > div.blockTemplateArea span[style^='background-image'], #wrapper > div.other.line_up > div.main > div > p.im > img"

    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    thumbnail_images: list[str] = []

    for thumbnail_image in await page.query_selector_all(query):
        if style := await thumbnail_image.get_attribute("style"):
            thumbnail_images.append(
                urljoin(
                    product_url,
                    compile_regex(r"background-image: url\(\"(.*)\"").findall(style)[0],
                )
            )
        if src := await thumbnail_image.get_attribute("src"):
            thumbnail_images.append(urljoin(product_url, src))

    if not thumbnail_images:
        await visit_link(page, product_url, wait_until="networkidle")
        # await translate(page, product_url)
        raise error.QueryNotFound("Thumbnail image not found", query)

    return thumbnail_images


@cache
def image_quries():
    return "#co_contents > table:nth-child(4), #co_contents > table:nth-child(5), #co_contents > table:nth-child(6), #co_contents > table:nth-child(7), #co_contents > table:nth-child(8), #co_contents > table:nth-child(9), #co_contents > table:nth-child(10), #co_contents > table:nth-child(11), #co_contents > table:nth-child(12), #co_contents > table:nth-child(13), #wrapper > div.megathis > div.lineup > div > div.lineupArea > div > div.specBox > div > table"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage,
    thumbnail_images: list[str],
    product_url: str,
    html_top: str,
    html_bottom: str,
) -> str:
    # await translate(page, product_url)

    query = image_quries()

    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    full_html = ""
    tbody_elements = await page.query_selector_all(query)

    for tbody in tbody_elements:
        html = f"<table>{(await tbody.inner_html()).strip()}</table>"
        html = html.replace("\n", "").replace("\t", "")
        full_html += html + "\n"

    src_regex = compile_regex(r'src="(.*?)"')
    images = src_regex.findall(full_html)

    for image in set(images):
        if not image.startswith("http"):
            full_html = full_html.replace(image, urljoin(product_url, image))

    return build_html(thumbnail_images, full_html, html_top, html_bottom)


@backoff.on_exception(
    backoff.expo,
    AssertionError,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
async def translate(page: PlaywrightPage, product_url: str):
    content = await page.content()

    # ? Skip if already translated
    if "google_translate_element" in content:
        return None

    original_body = await page.text_content("body")

    body_regex = compile_regex(r"""(<body.*>)""")
    content = body_regex.sub(
        """<body id="BodyID" class="white">""" + google_translate_element(), content
    )
    await page.set_content(content)

    with suppress(error.PlaywrightTimeoutError):
        query = '[aria-label="언어\\ 번역\\ 위젯"]'
        await page.wait_for_selector(query)
        await page.select_option(query, "ko", timeout=2500)
        await page.wait_for_selector("html[lang='ko']")
        await page.wait_for_timeout(2000)

    translated_body = await page.text_content("body")
    try:
        assert original_body != translated_body, f"{product_url}"
    except AssertionError as err:
        await visit_link(page, product_url, wait_until="networkidle")
        raise err from err


def build_html(
    thumbnail_images: list[str], table_html: str, html_top: str, html_bottom: str
):
    html_source = "".join(
        f"<img src='{url}' /><br />" for url in dict.fromkeys(thumbnail_images)
    )

    html_source = "".join([html_top, html_source, table_html])

    html_source = "".join(
        [
            html_source,
            html_bottom,
        ],
    )

    return html_source.strip()


@cache
def google_translate_element():
    return """
        <div id="google_translate_element"></div>
        <script type="text/javascript">
            function googleTranslateElementInit() {
                new google.translate.TranslateElement(
                { pageLanguage: "jp" },
                "google_translate_element"
                );
            }
        </script>

        <script
        type="text/javascript"
        src="https://translate.google.com/translate_a/element.js?cb=googleTranslateElementInit"
        ></script>
    """
