# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from functools import cache, partial
from typing import Any, Literal, cast
from urllib.parse import urljoin

import aiohttp

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser, PlaywrightPage
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.interocean import config
from market_crawler.interocean.data import InteroceanCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify.result import Err, Ok, Result, returns_future


@cache
def get_images_download_dir(dirname: Literal["Thumbnails", "Detailed Images"]) -> str:
    images_download_dir = os.path.join(os.path.dirname(__file__), dirname)
    if not os.path.exists(images_download_dir):
        os.makedirs(images_download_dir, exist_ok=True)

    return images_download_dir


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
    await visit_link(
        page,
        "http://interocean.co.kr/category/%EB%B8%8C%EB%9E%9C%EB%93%9C/25/",
        wait_until="networkidle",
    )

    categories = await page.query_selector_all(
        "#contents > div.xans-element-.xans-product.xans-product-menupackage > ul > li > a"
    )

    for category in categories:
        if not (text := await category.text_content()):
            continue

        text = text.strip()

        if not (category_text := await category.text_content()):
            continue

        category_text = category_text.strip().removesuffix("()").strip()

        if not (category_page_url := await category.get_attribute("href")):
            continue

        category_page_url = urljoin(page.url, category_page_url)

        # ? Visit each category to extract sub categories
        page2 = await browser.new_page()
        await visit_link(page2, category_page_url)

        subcategories = await page2.query_selector_all(
            "#contents > div.xans-element-.xans-product.xans-product-menupackage > ul > li > a"
        )

        # ? Not all main categories may have sub categories
        if not subcategories:
            full_subcategories.append(Category(category_text, category_page_url))

        for subcategory in subcategories:
            if not (subcategory_page_url := await subcategory.get_attribute("href")):
                continue

            if not (subcategory_text := await subcategory.text_content()):
                continue

            subcategory_text = subcategory_text.strip().removesuffix("()").strip()

            url = urljoin(
                page.url,
                subcategory_page_url,
            )
            full_text = f"{category_text}>{subcategory_text}"
            full_subcategories.append(Category(full_text, url))

        await page2.close()

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def run(settings: Settings):
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    async with async_playwright() as playwright, aiohttp.ClientSession() as session:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
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
            crawl=partial(crawl, session=session),
        )
        await crawl_categories(crawler, browser, settings, columns)

    # if not settings.TEST_MODE:
    #     thumbnails_dir = get_images_download_dir("Thumbnails")
    #     detailed_images_dir = get_images_download_dir("Detailed Images")

    #     thumbnail_images = os.listdir(thumbnails_dir)
    #     detailed_images = os.listdir(detailed_images_dir)

    #     results1: list[Future[Any]] = []
    #     results2: list[Future[Any]] = []
    #     with ThreadPoolExecutor(max_workers=2 * cpu_count() + 1) as executor:
    #         log.info("Copying Thumbnails to Google Drive ...")
    #         results1 = [
    #             executor.submit(
    #                 shutil.copy,  # type: ignore
    #                 os.path.join(get_images_download_dir("Thumbnails"), img),  # type: ignore
    #                 os.path.join(
    #                     GOOGLE_DRIVE_DIR,
    #                     config.SITENAME.upper(),
    #                     "Thumbnails",
    #                     img,
    #                 ),
    #             )
    #             for img in cast(list[str], tqdm(thumbnail_images))
    #         ]

    #         log.info("Copying Detailed Images to Google Drive ...")
    #         results2 = [
    #             executor.submit(
    #                 shutil.copy,  # type: ignore
    #                 os.path.join(get_images_download_dir("Detailed Images"), img),  # type: ignore
    #                 os.path.join(
    #                     GOOGLE_DRIVE_DIR,
    #                     config.SITENAME.upper(),
    #                     "Detailed Images",
    #                     img,
    #                 ),
    #             )
    #             for img in cast(list[str], tqdm(detailed_images))
    #         ]

    #     for r in results1:
    #         r.result()

    #     for r in results2:
    #         r.result()


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
    session: Any,
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

    if not (
        total_products_text := await document.text_content(
            "#contents > div.xans-element-.xans-product.xans-product-normalpackage > div.xans-element-.xans-product.xans-product-normalmenu > div > p > strong"
        )
    ):
        raise error.TotalProductsTextNotFound(
            "Total products text is not found on the page", category_page_url
        )

    number_of_products = parse_int(total_products_text)

    log.detail.total_products_in_category(category_name, number_of_products)

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
                    category_name,
                    category_page_url,
                    category_state,
                    category_html,
                    filename,
                    settings,
                    columns,
                    session,
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
    regex = compile_regex(r"product_no=(\w+)")
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


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "li[id^='anchorBoxId_']"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    category_name: str,
    category_page_url: str,
    category_state: CategoryState,
    category_html: CategoryHTML,
    filename: str,
    settings: Settings,
    columns: list[str],
    session: Any,
):
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
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    product_name, price2 = await extract_table(page)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    ) = await download_thumbnail_images(document, product_name, product_url, session)

    detailed_images_html_source = "\n".join(
        await download_detailed_image(page, product_name, session)
    )

    await page.close()

    split = category_name.split(">")
    brand = split[0]
    category = split[1]

    crawl_data = InteroceanCrawlData(
        category=category,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        brand=brand,
        price2=price2,
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


async def download_thumbnail_images(
    document: Document,
    product_name: str,
    product_url: str,
    session: aiohttp.ClientSession,
):
    semaphore = asyncio.Semaphore(5)

    dirname = get_images_download_dir("Thumbnails")

    if "(" in product_name:
        product_name = product_name.split("(")[0].strip()

    product_name = product_name.replace("/", "").replace('"', "")

    query = "div.keyImg > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    image_filepath = os.path.join(dirname, f"{product_name}_1.jpg")
    await download_images(session, semaphore, thumbnail_image_url, image_filepath)

    query = "img.ThumbImage"
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

    if thumbnail_image_url2:
        image_filepath = os.path.join(dirname, f"{product_name}_2.jpg")
        await download_images(session, semaphore, thumbnail_image_url2, image_filepath)

    if thumbnail_image_url3:
        image_filepath = os.path.join(dirname, f"{product_name}_3.jpg")
        await download_images(session, semaphore, thumbnail_image_url3, image_filepath)

    if thumbnail_image_url4:
        image_filepath = os.path.join(dirname, f"{product_name}_4.jpg")
        await download_images(session, semaphore, thumbnail_image_url4, image_filepath)

    if thumbnail_image_url5:
        image_filepath = os.path.join(dirname, f"{product_name}_5.jpg")
        await download_images(session, semaphore, thumbnail_image_url5, image_filepath)

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    )


async def download_images(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    image_url: str,
    image_filepath: str,
):
    if os.path.exists(image_filepath) or os.path.isfile(image_filepath):
        return
    async with semaphore:
        try:
            response = await session.get(image_url)
        except (
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientConnectorError,
            TimeoutError,
        ) as err:
            log.warning("Timeout. Retrying ...")
            raise error.TimeoutException(image_url) from err

        if response.status != 200:
            raise error.InvalidURL(
                f"Image URL is not valid: {image_url}",
            )

        async with await AIOFile(
            image_filepath,
            mode="wb",
        ) as afp:
            try:
                payload = await response.read()
            except TimeoutError as err:
                log.warning("Timeout. Retrying ...")
                raise error.TimeoutException(image_url) from err
            await afp.write(payload)


async def download_detailed_image(
    page: PlaywrightPage, product_name: str, session: aiohttp.ClientSession
):
    semaphore = asyncio.Semaphore(5)
    all_downloaded_images: list[str] | set[str] = []
    image_selectors = await page.query_selector_all(image_quries())

    for image in image_selectors:
        image_url = ""
        for _ in range(
            4
        ):  # ? How many times we try focusing on the image element to get rid of base64 string
            if image_url := await image.get_attribute("src"):
                if "base64" in image_url:
                    await image.scroll_into_view_if_needed()
                    await image.focus()
                else:
                    if image_url.startswith("/web"):
                        image_url = image_url.replace(
                            "/web", "http://interocean.co.kr/web"
                        )
                    all_downloaded_images.append(image_url)
                    break
            else:
                log.warning(f"src attribute not found in images | {page.url}")
                raise error.InvalidImageURL(page.url)

        # ? for ... else construct (if the break didn't happen)
        # ? https://stackoverflow.com/questions/9979970/why-does-python-use-else-after-for-and-while-loops
        # nobreak
        else:
            log.warning(f"base64 is present in the image | {page.url}")
            raise ValueError(page.url)

    all_downloaded_images = set(all_downloaded_images)

    for img_idx, image_url in enumerate(all_downloaded_images, start=1):
        dirname = get_images_download_dir("Detailed Images")

        if "(" in product_name:
            product_name = product_name.split("(")[0].strip()

        product_name = product_name.replace("/", "").replace('"', "")
        image_filepath = os.path.join(dirname, f"{product_name}_0{img_idx}.jpg")

        await download_images(session, semaphore, image_url, image_filepath)

    return all_downloaded_images


async def extract_table(page: PlaywrightPage):
    price2 = 0
    product_name = ""

    table_tbody_elements = await page.query_selector_all(
        "#contents > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody",
    )

    for table_tbody in table_tbody_elements:
        th_elements = await table_tbody.query_selector_all("tr > th")
        td_elements = await table_tbody.query_selector_all("tr > td")

        assert len(th_elements) == len(
            td_elements
        ), f"Not equal {len(th_elements)} vs {len(td_elements)}"

        for th, td in zip(
            th_elements,
            td_elements,
        ):
            th_str = cast(str, await th.text_content()).strip()
            td_str = cast(str, await td.text_content()).strip()

            if "상품명" in th_str:
                product_name = td_str

            if "판매가" in th_str:
                try:
                    price2 = parse_int(td_str)
                except ValueError:
                    log.warning(
                        f"Unique Price <magenta>({td_str})</> is present <blue>| {page.url}</>"
                    )

    if not price2 and price2 != 0:
        raise error.Price2NotFound(page.url)

    if not product_name:
        raise error.ProductTitleNotFound(page.url)

    return product_name, price2


@cache
def image_quries():
    return ", ".join(
        ["#prdDetail > div > img", "#prdDetail > div > p > img", "#prdDetail > div img"]
    )
