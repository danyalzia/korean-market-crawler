# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import locale
import os
import platform
import re
import shutil
import sys

from contextlib import suppress
from datetime import datetime, timedelta
from functools import wraps
from glob import glob
from pathlib import Path
from time import time
from typing import TYPE_CHECKING

import pandas as pd

from openpyxl.utils.exceptions import IllegalCharacterError

from market_crawler.config import get_market_data
from market_crawler.excel import (
    concat_df_from_dir,
    copy_dataframe_cells_to_excel_template,
)
from market_crawler.log import LOGGER_FORMAT_STR, info, logger, success, warning


if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from market_crawler.config import Config
    from market_crawler.settings import Settings


def timeit_save[
    ReturnType, **ParamsType
](reports_dir: str, products_file: str, date: str):
    """
    Decorator that will tell how much time a function has took and then save the information to a .txt file

    It doesn't work with async functions
    """

    def decorator(
        fn: Callable[ParamsType, ReturnType]
    ) -> Callable[ParamsType, ReturnType]:
        @wraps(fn)
        def wrapper(*args: ParamsType.args, **kwargs: ParamsType.kwargs) -> ReturnType:
            start_time, now = time(), datetime.now()

            full_start_time = now.strftime("%H:%M:%S %p")

            result = fn(*args, **kwargs)
            end_time, now = time(), datetime.now()

            full_end_time = now.strftime("%H:%M:%S %p")

            time_took = end_time - start_time

            save_path = os.path.join(
                reports_dir,
                f"{date}.txt",
            )
            # ? "r+" operation fails if the file doesn't exists
            if not os.path.exists(save_path):
                Path(save_path).touch()
            with open(
                save_path,
                "r+",
                encoding="utf-8",
            ) as f:
                content = f.read()
                run_count = content.count("Run #") + 1
                if run_count > 1:
                    f.write("\n\n")
                f.write(f"Run #{run_count}\n")
                f.write(f"Output file: {products_file}\n")
                f.write(f"Run Date: {date}\n")
                f.write(
                    f"Start Time: {datetime.now().strftime('%Y%m%d')} {full_start_time}\n"
                )
                f.write(
                    f"End Time: {datetime.now().strftime('%Y%m%d')} {full_end_time}\n"
                )
                f.write(f"Time took: {timedelta(seconds=time_took)}")

            success(f"Report file saved to <light-cyan>{save_path}</>")

            return result

        return wrapper

    return decorator


# ? This is the entry point for the market crawler
def run_bot(bot: Callable[..., Any], config: Config, settings: Settings):
    market_dir = os.path.join(os.path.dirname(__file__), config.SITENAME)

    # ? Directory to save the crawled data files while the program is running
    # ? The final output will be generated by combining the files from this directory
    temp_dir = os.path.join(market_dir, "temp")

    # ? Directory to save screenshots of the webpage while the program is running
    screenshot_dir = os.path.join(market_dir, "screenshots")

    # ? Directory to save all crawled HTML files (if desired) for a particular market
    html_dir = os.path.join(market_dir, "html")

    # ? Directory to save all the pickled states
    states_dir = os.path.join(market_dir, "states")

    # ? Directory to save the log files
    logs_dir = os.path.join(market_dir, "logs")

    # ? Directory to save report files after every successful program run
    reports_dir = os.path.join(market_dir, "reports")

    # ? Directory to save the files generated by profilers (i.e., .prof, etc.)
    profiles_dir = os.path.join(market_dir, "profiles")

    output_file: str = os.path.join(
        market_dir,
        settings.OUTPUT_FILE,
    )

    if settings.TEST_MODE:
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
                    sink=(
                        os.path.join(
                            logs_dir,
                            f"{settings.DATE}.log",
                        )
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
                        logs_dir,
                        f"{settings.DATE}.log",
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
        info(f"Locale: <MAGENTA><w>{local_lang} (English) </w></MAGENTA>")
    else:
        info(f"Locale: <MAGENTA><w>{local_lang} (Korean) </w></MAGENTA>")

    current_os = platform.system()
    info(f"OS: <CYAN><w>{current_os}</w></CYAN>")

    initialize(
        settings=settings,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
        states_dir=states_dir,
        html_dir=html_dir,
        screenshot_dir=screenshot_dir,
        reports_dir=reports_dir,
        profiles_dir=profiles_dir,
        output_file=output_file,
    )

    @timeit_save(reports_dir, output_file, settings.DATE)
    def run():
        asyncio.get_event_loop().run_until_complete(bot(settings))

    if not settings.TEST_MODE:
        run()
    else:
        try:
            from pyinstrument.profiler import Profiler
        except ModuleNotFoundError:
            run()
        else:
            p = Profiler(async_mode="enabled")
            p.start()

            run()

            p.stop()
            p.print(color=True)
            profile_html = p.output_html()

            with open(
                os.path.join(
                    profiles_dir,
                    f"{settings.DATE}.html",
                ),
                "w",
                encoding="utf-8",
            ) as f:
                f.write(profile_html)

    finalize(
        config=config,
        settings=settings,
        market_dir=market_dir,
        temp_dir=temp_dir,
        output_file=output_file,
        column_mapping=settings.COLUMN_MAPPING,
    )


def initialize(
    *,
    settings: Settings,
    logs_dir: str,
    temp_dir: str,
    states_dir: str,
    html_dir: str,
    screenshot_dir: str,
    reports_dir: str,
    profiles_dir: str,
    output_file: str,
):
    os.system("chcp 65001")

    if not settings.TEMPLATE_FILE:
        warning("Template file is not provided")
    else:
        info(f"Template file: <RED>{Path(settings.TEMPLATE_FILE).name}</RED>")
    info(f"Today's date: <BLUE><white>{settings.DATE}</white></BLUE>")

    if os.path.exists(output_file):
        warning(
            f"File <light-green>({Path(output_file).name})</light-green> already exists in the current directory <light-cyan>({Path(output_file).parent.absolute()})</>"
        )
    else:
        info(
            f"File <light-green>({Path(output_file).name})</light-green> does not exists in the current directory <light-cyan>({Path(output_file).parent.absolute()})</>"
        )

    if settings.RESET:
        logger.log(
            "ACTION",
            f"Deleting files in <light-cyan>{settings.DATE}</> directories ...",
        )

        if os.path.exists(os.path.join(temp_dir, settings.DATE)):
            shutil.rmtree(os.path.join(temp_dir, settings.DATE))
        if os.path.exists(os.path.join(screenshot_dir, settings.DATE)):
            shutil.rmtree(os.path.join(screenshot_dir, settings.DATE))
        if os.path.exists(os.path.join(html_dir, settings.DATE)):
            shutil.rmtree(os.path.join(html_dir, settings.DATE))
        if os.path.exists(os.path.join(states_dir, settings.DATE)):
            shutil.rmtree(os.path.join(states_dir, settings.DATE))

    os.makedirs(logs_dir, exist_ok=True)
    os.makedirs(
        os.path.join(temp_dir, settings.DATE),
        exist_ok=True,
    )
    os.makedirs(
        os.path.join(
            screenshot_dir,
            settings.DATE,
        ),
        exist_ok=True,
    )
    os.makedirs(
        os.path.join(html_dir, settings.DATE),
        exist_ok=True,
    )
    os.makedirs(
        os.path.join(states_dir, settings.DATE),
        exist_ok=True,
    )
    os.makedirs(
        reports_dir,
        exist_ok=True,
    )
    os.makedirs(
        profiles_dir,
        exist_ok=True,
    )

    if settings.URLS:
        return None

    if settings.RESUME:
        existing_temp_directories = [
            folder
            for folder in os.listdir(temp_dir)
            if os.path.isdir(os.path.join(temp_dir, folder))
        ]

        if not (found_dates := find_dates(existing_temp_directories, settings.DATE)):
            return None

        last_date = find_last(datetime.now(), found_dates).strftime("%Y%m%d")

        copy_last_date_directories(
            settings.DATE, last_date, temp_dir, states_dir, html_dir
        )


def copy_last_date_directories(
    date: str, last_date: str, temp_dir: str, states_dir: str, html_dir: str
):
    logger.log(
        "ACTION",
        f"Copying files from <light-cyan>{last_date}</> directories ...",
    )

    for file in os.listdir(os.path.join(temp_dir, last_date)):
        src = os.path.join(temp_dir, last_date, file)
        dst = os.path.join(temp_dir, date)

        with suppress(OSError):
            shutil.copy(
                src,
                dst,
            )

    states_last_date_dir = os.path.join(states_dir, last_date)
    if os.path.exists(states_last_date_dir):
        for file in os.listdir(states_last_date_dir):
            src = os.path.join(states_last_date_dir, file)
            dst = os.path.join(states_dir, date)

            with suppress(OSError):
                shutil.copy(
                    src,
                    dst,
                )

    html_last_date_dir = os.path.join(html_dir, last_date)
    if os.path.exists(states_last_date_dir):
        for file in os.listdir(html_last_date_dir):
            src = os.path.join(html_last_date_dir, file)
            dst = os.path.join(html_dir, date)

            if os.path.isdir(src):
                with suppress(OSError):
                    shutil.copytree(
                        src,
                        dst,
                        dirs_exist_ok=True,
                    )
            else:
                with suppress(OSError):
                    shutil.copy(
                        src,
                        dst,
                    )


def finalize(
    *,
    config: Config,
    settings: Settings,
    market_dir: str,
    temp_dir: str,
    output_file: str,
    column_mapping: dict[str, str],
):
    save_dir = os.path.join(temp_dir, settings.DATE)

    logger.log(
        "ACTION",
        f"Creating <light-cyan>{Path(output_file).name}</> in the current directory ...",
    )

    if settings.URLS:
        dfs = [
            pd.read_csv(
                os.path.join(save_dir, filename), encoding="utf-8-sig", dtype="str"
            )
            for filename in sorted(
                glob(os.path.join(save_dir, "*_CUSTOM_URLS_temporary.csv"))
            )
        ]
        dfs.extend(
            [
                pd.read_excel(
                    os.path.join(save_dir, filename),
                    engine="openpyxl",
                    dtype="str",
                )
                for filename in sorted(
                    glob(os.path.join(save_dir, "*_CUSTOM_URLS_temporary.xlsx"))
                )
            ]
        )
    else:
        logger.log(
            "ACTION",
            f" |__ Reading files from <light-cyan>{Path(save_dir).relative_to(market_dir)}</> folder ...",
        )

        dfs = concat_df_from_dir(save_dir)

        try:
            assert len(dfs) != 0
        except AssertionError as err:
            from colorama import Back, Fore

            raise AssertionError(
                "".join(
                    [
                        Fore.RED,
                        f"There are no .XLSX or .CSV files in {Back.RED}{Fore.WHITE}{settings.DATE}{Fore.RED}{Back.RESET} folder",
                        Fore.RESET,
                    ]
                )
            ) from err

        logger.log(
            "ACTION",
            f" |__ Concatenating all the <light-magenta>*_temporary.csv</> and <light-magenta>*_temporary.xlsx</> files from <light-cyan>{Path(save_dir).relative_to(market_dir)}</> folder ...",
        )

    df: pd.Series[Any] | pd.DataFrame = pd.concat(dfs)

    # ? We need to remove the already existing file if present, otherwise shutil.copy fails
    if os.path.exists(output_file):
        os.remove(output_file)

    if settings.REMOVE_DUPLICATED_DATA:
        compare_cols = [
            column_mapping[data_column]
            for data_column in settings.REMOVE_DUPLICATED_DATA
        ]

        logger.log(
            "ACTION",
            f" |__ Removing duplicated rows by columns <light-cyan>{', '.join([''.join(col.split()) + ' (' + name + ')' for col, name in zip(compare_cols, ['Product Name', 'Option1', 'Price2', 'Price3', 'Category', 'Product URL', 'Model Name'])])}</> ...",
        )

        df = df.drop_duplicates(subset=compare_cols)

    logger.log("ACTION", f" |__ Saving <light-cyan>{Path(output_file).name}</> ...")

    # ? In case of illegal character in dataframe, we need to remove it first before saving dataframe into .xlsx format
    # ? See: https://stackoverflow.com/questions/42306755/how-to-remove-illegal-characters-so-a-dataframe-can-write-to-excel
    try:
        df.to_excel(  # type: ignore
            output_file,
            index=False,
            engine="openpyxl",
        )
    except IllegalCharacterError:
        ILLEGAL_CHARACTERS_RE = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")
        df = df.applymap(
            lambda x: ILLEGAL_CHARACTERS_RE.sub(r"", x) if isinstance(x, str) else x
        )
        df.to_excel(  # type: ignore
            output_file,
            index=False,
            engine="openpyxl",
        )

    if settings.TEMPLATE_FILE:
        logger.log(
            "ACTION", f" |__ Formatting <light-cyan>{Path(output_file).name}</> ..."
        )

        crawl_data = get_market_data(config.SITENAME)

        copy_dataframe_cells_to_excel_template(
            output_file=output_file,
            template_file=settings.TEMPLATE_FILE,
            column_mapping=column_mapping,
            crawl_data=crawl_data,
        )

    success(
        f"File <light-cyan>{Path(output_file).relative_to(market_dir)}</> has been created",
    )


def find_last(mydate: datetime, dates: list[datetime]):
    return min(dates, key=lambda x: abs(x - mydate))  # type: ignore


def find_dates(existing_product_files: list[str], date: str):
    found_dates: list[datetime] = []
    for f in existing_product_files:
        if m := re.search(r"\d{4}\d{2}\d{2}", f):  # Has Date in name
            found_dates.append(datetime.strptime(m.group(), "%Y%m%d"))

    # ? We don't want to include the current date (in case we have already run the program and saved the current date file)
    return [x for x in found_dates if date not in x.strftime("%Y%m%d")]
