# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import re

from functools import cache, wraps
from time import time
from typing import TYPE_CHECKING, overload

from market_crawler.log import info


if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from typing import Any


@cache
def parse_int(text: str) -> int:
    """
    Strips the non-numeric characters in a text and convert to int

    Will throw error if string has no digit
    """
    try:
        return int("".join(compile_regex(r"(\d)").findall(text)))
    except ValueError as e:
        raise ValueError(
            f"Text don't have any digit: '{text}', so it cannot be converted to int"
        ) from e


def timeit[
    ReturnType, **ParamsType
](fn: Callable[ParamsType, ReturnType]) -> Callable[ParamsType, ReturnType]:
    """
    Decorator that will tell how much time a function has took

    It doesn't work with async functions
    """

    @wraps(fn)
    def wrapper(*args: ParamsType.args, **kwargs: ParamsType.kwargs) -> ReturnType:
        start_time = time()
        result = fn(*args, **kwargs)
        end_time = time()
        info(f"{str(fn.__name__).upper()} took {(end_time - start_time)} seconds")
        return result

    return wrapper


@overload
def chunks(lst: Sequence[int], n: int) -> Generator[Sequence[int], None, None]: ...


@overload
def chunks(lst: Sequence[str], n: int) -> Generator[Sequence[str], None, None]: ...


# ? Divide the list/sequence into evenly chunks
# ? If the last chunk is not the same length as previous chunks, then it simply returns the remaining elements in the sequence
# ? https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(lst: Sequence[Any], n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


@cache
def compile_regex(r: str):
    return re.compile(r)


# ? Case-insensitive getattr
# ? See: https://stackoverflow.com/questions/51875460/case-insensitive-getattr
def igetattr(obj: Any, attr: str) -> Any:
    for a in dir(obj):
        if a.lower() == attr.lower():
            return getattr(obj, a)
