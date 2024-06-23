from __future__ import annotations

import locale
import os
import platform
import sys

from argparse import ArgumentParser
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Any

from market_crawler.bot import finalize
from market_crawler.excel import get_column_mapping
from market_crawler.log import LOGGER_FORMAT_STR, error, logger, warning
from market_crawler.settings import Settings
from market_crawler.template import dump_template_column_mapping_to_json


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "--market",
        help="Market name",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--date",
        help="Date for output files",
        type=str,
    )
    parser.add_argument(
        "--test_mode",
        help="Test mode",
        action="store_true",
    )
    parser.add_argument(
        "--column_mapping_file",
        help="Column Mapping information file (.json file)",
        type=str,
    )
    parser.add_argument(
        "--template_file",
        help="Filename of Excel template in which crawled data will be saved",
        type=str,
    )
    parser.add_argument(
        "--output_file",
        help="Final output Excel filename of crawled data",
    )
    parser.add_argument(
        "--remove_duplicated_data",
        help='Remove duplicate data from the output file (data columns are separated by ",")',
        type=str,
    )
    args = parser.parse_args()

    sitename = args.market
    date: str = args.date or datetime.now().strftime("%Y%m%d")
    market_dir = os.path.join(os.path.dirname(__file__), "market_crawler", sitename)
    temp_dir = os.path.join(market_dir, "temp")

    if args.test_mode:
        logger.enable("dunia")
        logger.enable("robustify")
        logger.enable("excelsheet")
        logger.configure(
            handlers=[
                dict(
                    sink=sys.stderr,
                    level="DEBUG",
                    format=LOGGER_FORMAT_STR,
                    colorize=True,
                    enqueue=True,
                ),
                dict(
                    sink=os.path.join(
                        market_dir,
                        "logs",
                        f"{date}.log",
                    ),
                    level="DEBUG",
                    format=LOGGER_FORMAT_STR,
                    enqueue=True,
                    encoding="utf-8-sig",
                ),
            ]
        )
    else:
        logger.disable("dunia")
        logger.disable("robustify")
        logger.disable("excelsheet")
        logger.configure(
            handlers=[
                dict(
                    sink=sys.stderr,
                    level="INFO",
                    format=LOGGER_FORMAT_STR,
                    colorize=True,
                    enqueue=True,
                ),
                dict(
                    sink=os.path.join(
                        market_dir,
                        "logs",
                        f"{date}.log",
                    ),
                    level="INFO",
                    format=LOGGER_FORMAT_STR,
                    enqueue=True,
                    encoding="utf-8-sig",
                ),
            ]
        )

    local_lang = locale.getdefaultlocale()[0]
    if local_lang == "en_US":
        logger.info(f"Locale: <MAGENTA><w>{local_lang} (English) </w></MAGENTA>")
    else:
        logger.info(f"Locale: <MAGENTA><w>{local_lang} (Korean) </w></MAGENTA>")

    current_os = platform.system()
    logger.info(f"Current OS: <CYAN><w>{current_os}</w></CYAN>")

    products_excel_file: Path = Path(f"{sitename.upper()}_{date}.xlsx")

    # ? We are going to dynamically import modules to make the framework scale better with new Market additions
    try:
        config: Any = import_module(f"market_crawler.{args.market}.config")
    except ModuleNotFoundError as e:
        from colorama import Fore, init

        init(autoreset=True)

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

    output_file = args.output_file or f"{config.SITENAME.upper()}_{date}.xlsx"
    output_file = os.path.join(market_dir, output_file)

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

    column_mapping = get_column_mapping(column_mapping_file)

    settings = Settings(
        date,
        args.test_mode or False,
        False,
        False,
        [],
        column_mapping,
        args.template_file or "",
        output_file,
        remove_duplicated_data,
        "",
        "",
    )

    if production_run := not args.test_mode:
        finalize(
            config=config,
            settings=settings,
            market_dir=market_dir,
            temp_dir=temp_dir,
            output_file=output_file,
            column_mapping=column_mapping,
        )
    else:
        try:
            from pyinstrument.profiler import Profiler
        except ModuleNotFoundError:
            finalize(
                config=config,
                settings=settings,
                market_dir=market_dir,
                temp_dir=temp_dir,
                output_file=output_file,
                column_mapping=column_mapping,
            )
        else:
            p = Profiler(async_mode="enabled")
            p.start()

            finalize(
                config=config,
                settings=settings,
                market_dir=market_dir,
                temp_dir=temp_dir,
                output_file=output_file,
                column_mapping=column_mapping,
            )

            p.stop()
            p.print(color=True)
            profile_html = p.output_html()

            with open(
                os.path.join(market_dir, "profiles" f"{date}.html"),
                "w",
                encoding="utf-8",
            ) as f:
                f.write(profile_html)
