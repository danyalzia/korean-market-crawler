import asyncio
import os
import sys

import pytest

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.playwright import AsyncPlaywrightBrowser
from market_crawler.log import logger
from market_crawler.vinyltap import config


def pytest_collection_modifyitems(items: list[pytest.Function]):
    for item in items:
        item.add_marker("asyncio")


@pytest.fixture(scope="module", autouse=True)
def setup():
    sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="package")
async def browser():
    browser_config = BrowserConfig(
        headless=True,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
        viewport={"width": 1280, "height": 720},
    )
    logger.info("Running browser fixture (headless) ...")

    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()


@pytest.fixture(scope="package")
async def browser_headed():
    browser_config = BrowserConfig(
        headless=False,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
        viewport={"width": 1280, "height": 720},
    )
    logger.info("Running browser fixture (headed) ...")

    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()
