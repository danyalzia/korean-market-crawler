# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

import pytest

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.playwright import AsyncPlaywrightBrowser
from market_crawler import log
from market_crawler.bagissue import config
from market_crawler.bagissue.app import get_login_info


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
    )
    login_info = get_login_info()
    log.info("Running browser login fixture (headless) ...")

    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()


@pytest.fixture(scope="package")
async def browser_headed():
    browser_config = BrowserConfig(
        headless=False,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = get_login_info()
    log.info("Running browser login fixture (headed) ...")
    async with async_playwright() as playwright:
        yield await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()


@pytest.fixture(scope="package")
async def page_headed():
    browser_config = BrowserConfig(
        headless=False,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    log.info("Running page login fixture (headed) ...")
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch_persistent_context(
            browser_config.user_data_dir, headless=False
        )
        page = await browser.new_page()
        await page.goto(config.LOGIN_URL, wait_until="networkidle")

        user_id_query = "#member_id"
        password_query = "#member_passwd"
        login_button_query = "div.login > fieldset > span.login_btn"

        await page.fill(user_id_query, config.ID)
        await page.fill(password_query, config.PW)

        async with page.expect_navigation():
            await page.click(login_button_query)

        await page.wait_for_timeout(2000)

        yield page
