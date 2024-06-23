# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

from collections.abc import AsyncGenerator, Generator

import pytest

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser
from market_crawler.blackrhino import config
from market_crawler.blackrhino.app import get_login_info
from market_crawler.log import info


def pytest_collection_modifyitems(items: list[pytest.Function]) -> None:
    for item in items:
        item.add_marker("asyncio")


@pytest.fixture(scope="module", autouse=True)
def setup() -> None:
    sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="package")
async def browser() -> AsyncGenerator[PlaywrightBrowser, None]:
    browser_config = BrowserConfig(
        headless=True,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = get_login_info()
    info("Running browser login fixture (headless) ...")

    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()


@pytest.fixture(scope="package")
async def browser_headed() -> AsyncGenerator[PlaywrightBrowser, None]:
    browser_config = BrowserConfig(
        headless=False,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = get_login_info()
    info("Running browser login fixture (headed) ...")
    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()
