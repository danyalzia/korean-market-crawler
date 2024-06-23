from __future__ import annotations

import os

from argparse import ArgumentParser
from datetime import datetime
from importlib import import_module
from multiprocessing import freeze_support
from pathlib import Path
from typing import TYPE_CHECKING

from colorama import Fore, init

from market_crawler.bot import run_bot
from market_crawler.excel import get_column_mapping
from market_crawler.helpers import compile_regex
from market_crawler.log import error, info, success, warning
from market_crawler.settings import Settings
from market_crawler.template import dump_template_column_mapping_to_json


if TYPE_CHECKING:
    from typing import Any

if __name__ == "__main__":
    freeze_support()

    parser = ArgumentParser()

    parser.add_argument(
        "--market",
        help="Market that needs crawling",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--date",
        help="Date for output files",
        type=str,
    )
    parser.add_argument(
        "--headless",
        help="Headless mode",
        action="store_true",
    )
    parser.add_argument(
        "--headful",
        help="Headful mode",
        action="store_true",
    )
    parser.add_argument(
        "--id",
        help="ID",
        type=str,
    )
    parser.add_argument(
        "--pw",
        help="Password",
        type=str,
    )
    parser.add_argument(
        "--categories_chunk_size",
        help="Chunk size for categories",
        type=int,
    )
    parser.add_argument(
        "--products_chunk_size",
        help="Chunk size for products",
        type=int,
    )
    parser.add_argument(
        "--start_category",
        help="End category",
        type=str,
    )
    parser.add_argument(
        "--end_category",
        help="End category",
        type=str,
    )
    parser.add_argument(
        "--test_mode",
        help="Test mode",
        action="store_true",
    )
    parser.add_argument(
        "--reset",
        help="Remove all the state files, temporary crawled data files, HTML and cache files of today's date",
        action="store_true",
    )
    parser.add_argument(
        "--resume",
        help="Resume crawling from the last date",
        action="store_true",
    )
    parser.add_argument(
        "--urls",
        help="Crawl only the specific URLs (.txt file)",
        type=str,
    )
    parser.add_argument(
        "--column_mapping_file",
        help="Column Mapping information file (.json file)",
        type=str,
    )
    parser.add_argument(
        "--template_file",
        help="Excel template for the format of crawled data",
        type=str,
    )
    parser.add_argument(
        "--output_file",
        help="Final output Excel filename of crawled data",
        type=str,
    )
    parser.add_argument(
        "--remove_duplicated_data",
        help='Remove duplicate data from the output file (data columns are separated by ",")',
        type=str,
    )
    parser.add_argument(
        "--detailed_images_html_source_top",
        help="Start of HTML source template (.html file)",
        type=str,
    )
    parser.add_argument(
        "--detailed_images_html_source_bottom",
        help="End of HTML source template (.html file)",
        type=str,
    )
    args = parser.parse_args()

    urls = []
    if args.urls:
        urls = Path(args.urls).read_text()
        urls = [
            url.strip().replace('"', "").replace("'", "")
            for url in urls.split()
            if url and "http" in url
        ]
        urls = list(set(urls))

    if args.detailed_images_html_source_top:
        if args.detailed_images_html_source_top.endswith(".html"):
            info(
                f"Reading detailed images html source top from the file ({args.detailed_images_html_source_top}) ..."
            )
            detailed_images_html_source_top = Path(
                args.detailed_images_html_source_top
            ).read_text()
        else:
            detailed_images_html_source_top = args.detailed_images_html_source_top
    else:
        detailed_images_html_source_top = ""

    if args.detailed_images_html_source_bottom:
        if args.detailed_images_html_source_bottom.endswith(".html"):
            info(
                f"Reading detailed images html source bottom from the file ({args.detailed_images_html_source_bottom}) ..."
            )
            detailed_images_html_source_bottom = Path(
                args.detailed_images_html_source_bottom
            ).read_text()
        else:
            detailed_images_html_source_bottom = args.detailed_images_html_source_bottom
    else:
        detailed_images_html_source_bottom = ""

    init(autoreset=True)

    # ? We are going to dynamically import modules to make the framework scale better with new market additions
    try:
        app: Any = import_module(f"market_crawler.{args.market}.app")
        config: Any = import_module(f"market_crawler.{args.market}.config")
    except ModuleNotFoundError as e:
        from market_crawler.error import MarketNotFound

        folders = (
            f
            for f in os.listdir("market_crawler")
            if os.path.isdir(os.path.join("market_crawler", f))
            and not f.startswith(".")
            and not f.startswith("__")
        )
        markets = [
            folder
            for folder in folders
            if os.path.exists(os.path.join("market_crawler", folder, "app.py"))
            and os.path.exists(os.path.join("market_crawler", folder, "config.py"))
        ]

        raise MarketNotFound(
            f"""{"".join(['"', str(args.market), '"'])} has not been implemented\n\n"""
            f"{Fore.BLUE}Supported Markets\n=================\n"
            f"""{f"{Fore.WHITE}, ".join(f'{Fore.LIGHTYELLOW_EX}{str(x).lower()}' for x in markets)}"""
        ) from e

    bot = getattr(app, "run")

    if args.headless and args.headful:
        raise ValueError(
            f"{Fore.YELLOW}--headless {Fore.RED}and {Fore.YELLOW}--headful {Fore.RED}can't exist at the same time. Please choose either of these."
        )

    # * Default values in config.py will be overwritten by these command line arguments
    if args.headless:
        config.HEADLESS = True

    if args.headful:
        config.HEADLESS = False

    if args.id:
        config.ID = args.id

    if args.pw:
        config.PW = args.pw

    if args.categories_chunk_size:
        config.CATEGORIES_CHUNK_SIZE = args.categories_chunk_size

    if args.products_chunk_size:
        config.MIN_PRODUCTS_CHUNK_SIZE = args.products_chunk_size
        config.MAX_PRODUCTS_CHUNK_SIZE = args.products_chunk_size

    if args.start_category:
        config.START_CATEGORY = args.start_category

    if args.end_category:
        config.END_CATEGORY = args.end_category

    date = args.date or datetime.now().strftime("%Y%m%d")

    if not args.column_mapping_file:
        if not args.template_file:
            text = "Please provide either --column_mapping_file or --template_file"
            error(text)
            raise ValueError(text)

        warning(
            "Column mapping file is not provided, creating default file column_mapping.json in market directory ..."
        )
        dump_template_column_mapping_to_json(config.SITENAME)
        column_mapping_file = "column_mapping.json"
    else:
        column_mapping_file = args.column_mapping_file
        column_mapping_file = os.path.join(
            os.path.dirname(__file__),
            "market_crawler",
            config.SITENAME,
            column_mapping_file,
        )

        if not os.path.exists(column_mapping_file):
            raise FileNotFoundError(column_mapping_file)

    if args.remove_duplicated_data:
        remove_duplicated_data: list[str] = args.remove_duplicated_data.split(",")
    else:
        remove_duplicated_data = []

    try:
        run_bot(
            bot,
            config,
            Settings(
                date,
                args.test_mode or False,
                args.reset or False,
                args.resume or False,
                urls,
                get_column_mapping(column_mapping_file),
                args.template_file or "",
                args.output_file
                or (
                    f"{config.SITENAME.upper()}_CUSTOM_URLS_{date}.xlsx"
                    if urls
                    else f"{config.SITENAME.upper()}_{date}.xlsx"
                ),
                remove_duplicated_data,
                detailed_images_html_source_top,
                detailed_images_html_source_bottom,
            ),
        )

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
        error(text)
        raise err from err

    success("Program has been run successfully")
