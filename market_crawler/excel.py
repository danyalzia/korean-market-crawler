# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import json
import os

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from glob import glob
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
import pandas as pd

from openpyxl import load_workbook  # type: ignore

from excelsheet import col_to_excel, write_to_excel_template_cell_openpyxl
from market_crawler.log import logger
from market_crawler.path import temporary_csv_file


if TYPE_CHECKING:
    from market_crawler.data import CrawlData


def get_column_mapping(filename: str):
    with open(filename, encoding="utf-8") as f:
        column_mapping: dict[str, str] = json.loads(f.read())

    return column_mapping


def data_column_mapping(
    column_mapping: dict[str, str], crawl_data: CrawlData
) -> list[tuple[str | int, str]]:
    crawl_data_attrs = [
        attr for attr in set(dir(crawl_data)) if not attr.startswith("__")
    ]

    # ? Dictionary doesn't work here because "None" is not a unique key
    mappings: list[tuple[str | int, str]] = [
        (data, column_mapping[f"{attr}"])
        for attr in crawl_data_attrs
        if (data := crawl_data.__getattribute__(attr)) is not None
    ]
    return mappings


def to_series(crawl_data: CrawlData, column_mapping: dict[str, str]):
    return {
        column: data for data, column in data_column_mapping(column_mapping, crawl_data)
    }


async def save_series_csv(
    series: dict[str, str | int], columns: list[str], filename: str
):
    exists = await asyncio.to_thread(os.path.exists, filename)

    # ? Append mode writing can't be done concurrently
    # ? asyncio.to_thread() requires waiting for other threads to finish, so it doesn't make sense to use asyncio.to_thread()
    pd.DataFrame([series], columns=columns).to_csv(
        filename,  # type: ignore
        mode="a",  # type: ignore
        header=not exists,
        encoding="utf-8-sig",
        index=False,
    )


async def save_temporary_csv(
    series: list[dict[str, str | int]],
    columns: list[str],
    sitename: str,
    date: str,
    category_name: str,
    pageno: int,
):
    temporary_excel: str = temporary_csv_file(
        sitename=sitename,
        date=date,
        category_name=category_name,
        page_no=pageno,
    )
    await asyncio.to_thread(
        pd.DataFrame(series, columns=columns).to_csv,  # type: ignore
        temporary_excel,  # type: ignore
        encoding="utf-8-sig",
        index=False,
    )


def copy_dataframe_cells_to_excel_template(
    *,
    output_file: str,
    template_file: str,
    column_mapping: dict[str, str],
    crawl_data: CrawlData,
):
    """
    Copies the cells in columns from DataFrame to Openpyxl format
    """
    if output_file.endswith(".xlsx"):
        df = pd.read_excel(
            output_file,
            dtype="str",
            engine="openpyxl",
        )
    else:
        df = pd.read_csv(
            output_file,
            dtype="str",
            encoding="utf-8-sig",
        )

    df = df.dropna(subset=[column_mapping["product_name"]], how="all")
    df = (
        df.replace(np.nan, "", regex=True)
        .replace("None", "", regex=True)
        .replace("nan", "", regex=True)
        .replace("NaN", "", regex=True)
    )

    copy_to_openpyxl_template(
        df=df,
        output_file=output_file,
        template_file=template_file,
        column_mapping=column_mapping,
        crawl_data=crawl_data,
    )


def copy_to_openpyxl_template(
    *,
    df: pd.DataFrame,
    output_file: str,
    template_file: str,
    column_mapping: dict[str, str],
    crawl_data: CrawlData,
):
    wb: Any = load_workbook(template_file)
    ws = wb.active

    columns: tuple[str] = tuple(df.columns)

    col_dict: dict[str, str] = {
        columns[x - 1]: col_to_excel(x) for x in range(1, len(columns) + 1)
    }

    write_df_column_to_excel_template_cell_openpyxl = partial(
        write_to_excel_template_cell_openpyxl, worksheet=ws, df=df
    )

    for _, column in data_column_mapping(column_mapping, crawl_data):
        write_df_column_to_excel_template_cell_openpyxl(
            name=column,
            alphabet=col_dict[column],
        )

    wb.save(output_file.replace(".csv", ".xlsx"))


class DataFrameFromDir(NamedTuple):
    filename: str
    dataframe: pd.DataFrame


def concat_df_from_dir(directory: str):
    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        results = [
            DataFrameFromDir(
                filename,
                executor.submit(
                    pd.read_csv,  # type: ignore
                    os.path.join(directory, filename),  # type: ignore
                    encoding="utf-8-sig",
                    dtype="str",
                ),
            )
            for filename in sorted(glob(os.path.join(directory, "*_temporary.csv")))
            if "CUSTOM_URLS" not in filename
        ]

        # ? Let's also read all the .xlsx files in case we choose to save the temporary files in .xlsx format
        results.extend(
            [
                DataFrameFromDir(
                    filename,
                    executor.submit(
                        pd.read_excel,  # type: ignore
                        os.path.join(directory, filename),  # type: ignore
                        engine="openpyxl",
                        dtype="str",
                    ),
                )
                for filename in sorted(
                    glob(os.path.join(directory, "*_temporary.xlsx"))
                )
                if "CUSTOM_URLS" not in filename
            ]
        )

        result: list[pd.DataFrame] = []
        for r in results:
            try:
                df: pd.DataFrame = r.dataframe.result()
            except pd.errors.EmptyDataError:
                logger.error(
                    f"Data is empty in <light-cyan>{Path(r.filename).name}</> without any columns"
                )
                raise
            else:
                result.append(df)

    return result


def change_multiindex_column_names(
    df: pd.DataFrame, *, new_columns: list[str], level: int = 1
) -> pd.DataFrame:
    """
    This function is useful when we get DataFrame from pandas compare() function that have mutliindex columns (self vs other, etc.)
    """
    try:
        columns: Any = df.columns[level]
        assert len(columns) == len(
            new_columns
        ), f"Length of columns must be same for both dataframes (received {len(df.columns)} vs {len(new_columns)})"
    except IndexError as err:
        raise ValueError("DataFrame is empty") from err

    # ? Let's create the copy manually as set_levels() mutates the original DataFrame
    df2 = df.copy()
    df2.columns = df2.columns.set_levels(new_columns, level=level)  # type: ignore
    return df2


def compare_dataframes(
    old_df: pd.DataFrame, new_df: pd.DataFrame
) -> pd.DataFrame | None:
    """
    This only works if the rows have been sorted already and that they are identically labeled (which means same column names and index)
    """

    # ? They should have same columns
    try:
        assert all(old_df.columns.isin(new_df.columns).tolist())  # type: ignore
    except (AssertionError, IndexError):
        # ? If not same columns, then extract the common ones
        new_df = new_df[old_df.columns[old_df.columns.isin(new_df.columns)]]  # type: ignore
        old_df = old_df[new_df.columns[new_df.columns.isin(old_df.columns)]]  # type: ignore

        try:
            assert all(old_df.columns.isin(new_df.columns).tolist())  # type: ignore
        except AssertionError as err:
            raise AssertionError(
                "One of compared dataframes doesn't have identical column names, therefore they can't be compared"
            ) from err

    # ? We will use minimum length of rows so that extra rows don't get compared
    # ? This is required for pandas compare() function
    return new_df[:min_len].compare(old_df[:min_len]) if (min_len := min(len(old_df), len(new_df))) else None  # type: ignore
