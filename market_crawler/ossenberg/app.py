# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import base64
import os

from functools import cache
from typing import cast, overload
from urllib.parse import urljoin

import aiohttp
import backoff

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.aio import gather
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
from market_crawler.ossenberg import config
from market_crawler.ossenberg.data import OssenbergCrawlData
from market_crawler.path import temporary_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from robustify import returns
from robustify.result import Err, Ok, Result, returns_future


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
            browser_config=browser_config,
            playwright=playwright,
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

    # if not settings.TEST_MODE:
    #     detailed_images_dir = get_images_download_dir("Base64 Detailed Images")

    #     detailed_images = os.listdir(detailed_images_dir)

    #     if not os.path.exists(
    #         google_drive_base64_dir := os.path.join(
    #             GOOGLE_DRIVE_DIR, config.SITENAME.upper(), "Base64 Detailed Images"
    #         )
    #     ):
    #         os.makedirs(google_drive_base64_dir)

    #     results: list[Any[Any]] = []
    #     with ThreadPoolExecutor(max_workers=2 * cpu_count() + 1) as executor:
    #         log.info("Copying Base64 Detailed Images to Google Drive ...")
    #         results = [
    #             executor.submit(
    #                 shutil.copy,  # type: ignore
    #                 os.path.join(
    #                     get_images_download_dir("Base64 Detailed Images"), img
    #                 ),  # type: ignore
    #                 os.path.join(
    #                     GOOGLE_DRIVE_DIR,
    #                     config.SITENAME.upper(),
    #                     "Base64 Detailed Images",
    #                     "".join(img.split()),
    #                 ),
    #             )
    #             for img in cast(list[str], tqdm(detailed_images))
    #         ]

    #     for r in results:
    #         r.result()


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
            category_name=category_name,
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

        log.action.category_page_crawled(category_name, category_state.pageno)

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

        if not (document := await parse_document(content, engine="lexbor")):
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

    sold_out_text = await extract_soldout_text(product)

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

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    tasks = (
        extract_thumbnail_images(document, product_url),
        extract_table(page),
    )

    (
        R1,
        R2,
    ) = await gather(*tasks)

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
        case Ok(table):
            (
                product_name,
                model_name,
                manufacturer,
                manufacturing_country,
                price3,
                price2,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    tasks = (
        extract_options(document, page),
        extract_images(page, product_name, html_top, html_bottom),
    )

    (
        R1,  # type: ignore
        R2,  # type: ignore
    ) = await gather(*tasks)

    match R1:
        case Ok(options):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    detailed_images_html_source = R2

    await page.close()

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok(result):
                    (option1_, price2_, option2, option3) = result
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option1}) into price2 due to an error -> {err}",
                        url=product_url,
                    )

            crawl_data = OssenbergCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                model_name=model_name,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                price3=price3,
                price2=price2_,
                detailed_images_html_source=detailed_images_html_source,
                sold_out_text=sold_out_text,
                option1=option1_,
                option2=option2,
                option3=str(option3),
            )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

            # ? Copied from previous iteration of changes
            # ? It's here in case default save_series_csv() implementation doesn't work
            # df = pd.DataFrame(
            #     to_series(crawl_data, settings.COLUMN_MAPPING), columns=columns
            # )

            # try:
            #     await asyncio.to_thread(
            #         df.to_csv,  # type: ignore
            #         filename,  # type: ignore
            #         encoding="utf-8-sig",
            #         index=False,
            #     )
            # except Exception:
            #     # ? https://stackoverflow.com/questions/42306755/how-to-remove-illegal-characters-so-a-dataframe-can-write-to-excel
            #     ILLEGAL_CHARACTERS_RE = re.compile(
            #         r"[\000-\010]|[\013-\014]|[\016-\037]"
            #     )

            #     df = df.applymap(lambda t: str(t).strip())  # type: ignore

            #     df = df.applymap(
            #         lambda x: ILLEGAL_CHARACTERS_RE.sub(r"", x)
            #         if isinstance(x, str)
            #         else x
            #     )
            #     await asyncio.to_thread(
            #         df.to_csv,  # type: ignore
            #         filename,  # type: ignore
            #         encoding="utf-8-sig",
            #         index=False,
            #     )

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

    crawl_data = OssenbergCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        model_name=model_name,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        price3=price3,
        price2=price2,
        detailed_images_html_source=detailed_images_html_source,
        sold_out_text=sold_out_text,
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

    # ? Copied from previous iteration of changes
    # ? It's here in case default save_series_csv() implementation doesn't work
    # df = pd.DataFrame(to_series(crawl_data, settings.COLUMN_MAPPING), columns=columns)

    # try:
    #     await asyncio.to_thread(
    #         df.to_csv,  # type: ignore
    #         filename,  # type: ignore
    #         encoding="utf-8-sig",
    #         index=False,
    #     )
    # except Exception:
    #     # ? https://stackoverflow.com/questions/42306755/how-to-remove-illegal-characters-so-a-dataframe-can-write-to-excel
    #     ILLEGAL_CHARACTERS_RE = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")  # type: ignore

    #     df = df.applymap(lambda t: str(t).strip())  # type: ignore

    #     df = df.applymap(
    #         lambda x: ILLEGAL_CHARACTERS_RE.sub(r"", x) if isinstance(x, str) else x
    #     )
    #     await asyncio.to_thread(
    #         df.to_csv,  # type: ignore
    #         filename,  # type: ignore
    #         encoding="utf-8-sig",
    #         index=False,
    #     )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )

    return None


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"product_no=(\d+)")
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


@overload
async def get_products(
    document_or_page: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    document_or_page: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(document_or_page: Document | PlaywrightPage):
    query = "li[id^='anchorBoxId_']"
    return (
        Ok(products)
        if (products := await document_or_page.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


async def extract_soldout_text(product: Element):
    for icon in await product.query_selector_all("img.icon_img"):
        if (alt := await icon.get_attribute("alt")) and "품절" in alt:
            return "품절"

    return ""


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "div.xans-element-.xans-product.xans-product-image.imgArea > div.keyImg > a > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "div.xans-element-.xans-product.xans-product-image.imgArea img.ThumbImage"
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


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(page: PlaywrightPage):
    product_name = ""
    model_name = ""
    manufacturer = ""
    manufacturing_country = ""
    price3 = 0
    price2 = 0

    query = "#contents > div.xans-element-.xans-product.xans-product-detail > div.detailArea > div.infoArea > div.xans-element-.xans-product.xans-product-detaildesign > table > tbody"
    if not (table_body := await page.query_selector(query)):
        raise error.QueryNotFound("Table body not found", query=query)

    item_headings = await table_body.query_selector_all("tr > th")
    item_values = await table_body.query_selector_all("tr > td")

    assert item_headings
    assert item_values

    assert len(item_headings) == len(
        item_values
    ), f"Not equal {len(item_headings)} vs {len(item_values)}"

    for key, val in zip(
        item_headings,
        item_values,
    ):
        key_str = cast(str, await key.text_content()).strip()
        val_str = cast(str, await val.text_content()).strip()

        if "상품명" in key_str:
            product_title_split = val_str.split("(")

            if len(product_title_split) == 2:
                product_name = product_title_split[0].strip()
                model_name = (
                    product_title_split[-1]
                    .strip()
                    .removesuffix(".")
                    .removesuffix(")")
                    .strip()
                )

            # ? If there are more than one parenthesized names (or no parenthesis) in the title, then we will just copy the text into product name and empty the model name
            elif len(product_title_split) >= 3 or len(product_title_split) == 1:
                product_name = val_str

        if "제조사" in key_str:
            manufacturer = val_str

        if "원산지" in key_str:
            manufacturing_country = val_str

        if "소비자가" in key_str:
            price3_str = val_str

            try:
                assert (
                    "원" in price3_str
                ), f"Won (원) not present in price3: ({price3_str}) | {page.url}"
            except AssertionError:
                raise

            try:
                price3 = parse_int(price3_str)
            except ValueError:
                log.warning(
                    f"Unique price3 <magenta>({price3_str})</> is present <blue>| {page.url}</>"
                )

        if "판매가" in key_str:
            price2_str = val_str

            try:
                assert (
                    "원" in price2_str
                ), f"Won (원) not present in price2: ({price2_str}) | {page.url}"
            except AssertionError:
                raise

            try:
                price2 = parse_int(price2_str)
            except ValueError:
                log.warning(
                    f"Unique price2 <magenta>({price2_str})</> is present <blue>| {page.url}</>"
                )

    try:
        assert product_name
    except AssertionError as err:
        raise error.ProductNameNotFound(page.url)

    # ? See: http://ossenberg.co.kr/product/detail.html?product_no=152&cate_no=50&display_group=1
    if not model_name:
        log.warning(f"Model name is not present <blue>| {page.url}</>")

    try:
        assert manufacturer
    except AssertionError as err:
        raise error.ManufacturerNotFound(page.url) from err

    try:
        assert manufacturing_country
    except AssertionError as err:
        raise error.ManufacturingCountryNotFound(page.url) from err

    # ? See: http://ossenberg.co.kr/product/detail.html?product_no=246&cate_no=224&display_group=1
    if not price3:
        log.warning(f"Price3 is not present <blue>| {page.url}</>")

    if not price2:
        log.warning(f"Price2 is not present <blue>| {page.url}</>")
        raise error.Price2NotFound(page.url)

    return (
        product_name,
        model_name,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
    )


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

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "(품절)" in option1:
        return option1.replace("(품절)", ""), price2, "(품절)", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
)
@returns_future(IndexError)
async def extract_options(
    document_or_page: Document | PlaywrightPage, page: PlaywrightPage
):
    option1_query = "select[id='product_option_id1']"
    option2_query = "select[id='product_option_id2']"

    option1_elements = await document_or_page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): option1_value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (option1_value := await option1.get_attribute("value"))
        and option1_value not in ["", "*", "**"]
    }

    # ? If option2 is not present, then we don't need to use Page methods
    if not (
        await document_or_page.query_selector(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        )
    ):
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
            for option2 in await page.query_selector_all(
                f"{option2_query} > option, {option2_query} > optgroup > option"
            )
            if (text := await option2.text_content())
            and (option2_value := await option2.get_attribute("value"))
            and option2_value not in ["", "*", "**"]
        }

        options.extend(f"{option1_text}_{option2_text}" for option2_text in option2)

    return options


@cache
def get_images_download_dir(dirname: str) -> str:
    images_download_dir = os.path.join(os.path.dirname(__file__), dirname)
    if not os.path.exists(images_download_dir):
        os.makedirs(images_download_dir, exist_ok=True)

    return images_download_dir


async def extract_images(
    page: PlaywrightPage, product_name: str, html_top: str, html_bottom: str
):
    # ? For the problematic images that can't be changed from base64, we will download it (along with rest of the images as well, base64 or not, so that it doesn't cause confusion when reading product detail images column in Excel file)
    # ? It's the similar logic that was applied in JSTICK market
    semaphore = asyncio.Semaphore(5)
    session = aiohttp.ClientSession()
    n_times_check_base64 = 2

    # ? We want to save both kinds of images in different list (for ease of debugging)
    all_non_base64_downloaded_images: list[str] | set[str] = []
    all_base64_downloaded_images: list[str] | set[str] = []
    inner_html = ""

    for _ in range(n_times_check_base64):
        if not (inner_html_element := await page.query_selector("#prdDetail > div")):
            continue

        inner_html = await inner_html_element.inner_html()
        if "base64" in inner_html:
            log.warning(f"base64 found: {page.url}")

            await inner_html_element.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)

            images = await page.query_selector_all("#prdDetail img")
            for image in images:
                await image.scroll_into_view_if_needed()
                await page.wait_for_timeout(1000)

        else:
            urls: list[str] = []

            images = await page.query_selector_all("#prdDetail img")

            for image in images:
                if src := await image.get_attribute("src"):
                    urls.append(src)

            html_source = "".join(
                f"<img src='{url}' /><br />"
                for url in dict.fromkeys(map(lambda url: urljoin(page.url, url), urls))
            )

            html_source = "".join(
                [
                    html_top,
                    html_source,
                    html_bottom,
                ],
            )

            assert "base64" not in html_source

            assert len(html_source) < 32767

            break
    else:
        html_source = "PROBLEM"
        images = await page.query_selector_all("#prdDetail img")
        for image in images:
            url = cast(str, await image.get_attribute("src"))
            if "base64" in url:
                all_base64_downloaded_images.append(
                    url.removeprefix("data:image/png;base64,")
                )
            else:
                append_url_strategy(all_non_base64_downloaded_images, url)

        all_base64_downloaded_images = set(all_base64_downloaded_images)

        for img_idx, image_url in enumerate(all_base64_downloaded_images, start=1):
            dirname = get_images_download_dir("Base64 Detailed Images")

            product_name = product_name.replace("/", "").replace('"', "")
            image_filepath = os.path.join(dirname, f"{product_name}_0{img_idx}.jpg")

            imgdata = base64.b64decode(image_url)
            async with await AIOFile(
                image_filepath,
                mode="wb",
            ) as afp:
                await afp.write(imgdata)

        all_non_base64_downloaded_images = set(all_non_base64_downloaded_images)

        for img_idx, image_url in enumerate(all_non_base64_downloaded_images, start=1):
            dirname = get_images_download_dir("Base64 Detailed Images")

            product_name = product_name.replace("/", "").replace('"', "")
            image_filepath = os.path.join(dirname, f"{product_name}_0{img_idx}.jpg")

            await download_images(session, semaphore, image_url, image_filepath)

    await session.close()

    return html_source


def append_url_strategy(images_construction: list[str], url: str):
    if (
        "/web/upload/NNEditor/20150630/ossenberg_shop1_205242.jpg" in url
        or "web/upload/NNEditor/20150703/ossenberg_shop1_201440.jpg" in url
        or "web/upload/NNEditor/20150726/ossenberg_shop1_150120.jpg" in url
        or "web/upload/NNEditor/20170329/copy(1490791745)-kc20-20EBB3B5EC82ACEBB3B8.jpg"
        in url
        or "web/upload/NNEditor/20150728/ossenberg_shop1_210907.jpg" in url
        or "/web/upload/NNEditor/20150715/ossenberg_shop1_121835.jpg" in url
    ):
        return
    if not url.startswith("http"):
        images_construction.append(f"https://ossenberg.co.kr{url}")
    else:
        images_construction.append(url)


@backoff.on_exception(
    backoff.expo,
    error.TimeoutException,
    max_tries=5,
    on_backoff=error.backoff_hdlr,  # type: ignore
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
            log.warning(
                f"Image URL is not valid: {image_url}",
            )
        else:
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
