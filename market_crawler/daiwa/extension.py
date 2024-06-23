import asyncio

from playwright.async_api import async_playwright


async def main():
    pathToExtension = r"C:\Users\Test\AppData\Local\Google\Chrome\User Data\Default\Extensions\aapbdbdomjkkjkaonfhkkikfgjllcleb\2.0.11_0"
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch_persistent_context(
            "",
            headless=False,
            args=[
                f"--disable-extensions-except={pathToExtension}",
                f"--load-extension={pathToExtension}",
            ],
            devtools=True,
        )

        page = await browser.new_page()
        await page.goto(
            "https://www.daiwa.com/jp/fishing/item/lure/salt_le/kasago_mimiika_zukin/index.html",
            timeout=300000,
        )

        content = await page.content()
        content = content.replace(
            """<body id="BodyID" class="white">""",
            """<body id="BodyID" class="white">""" + translate_frame(),
        )

        await page.set_content(content)
        await page.locator(
            '[aria-label="Language\\ Translate\\ Widget"]'
        ).select_option("ko")

        await page.wait_for_load_state("networkidle")
        await page.pause()


def translate_frame():
    return """
        <div id="google_translate_element"></div>
        <script type="text/javascript">
            function googleTranslateElementInit() {
                new google.translate.TranslateElement(
                { pageLanguage: "jp" },
                "google_translate_element"
                );
            }
        </script>

        <script
        type="text/javascript"
        src="https://translate.google.com/translate_a/element.js?cb=googleTranslateElementInit"
        ></script>
    """


if __name__ == "__main__":
    asyncio.run(main())
