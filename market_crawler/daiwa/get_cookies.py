# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import sys

from multiprocessing import freeze_support

from playwright.async_api import async_playwright

from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser
from market_crawler.daiwa import config
from market_crawler.daiwa.app import BrowserConfig


sys.path.insert(0, "..")
sys.path.insert(0, "../..")


async def main():
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#username",
        password_query="#password",
        login_button_query="form button:has-text('로그인')",
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()
        context = browser
        await context.storage_state(path="cookies.json")


if __name__ == "__main__":
    freeze_support()
    asyncio.run(main())
