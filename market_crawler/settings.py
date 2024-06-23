# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from dataclasses import dataclass


# ? Program settings
@dataclass(slots=True, frozen=True)
class Settings:
    DATE: str
    TEST_MODE: bool
    RESET: bool
    RESUME: bool
    URLS: list[str]
    COLUMN_MAPPING: dict[str, str]
    TEMPLATE_FILE: str
    OUTPUT_FILE: str
    REMOVE_DUPLICATED_DATA: list[str]
    DETAILED_IMAGES_HTML_SOURCE_TOP: str
    DETAILED_IMAGES_HTML_SOURCE_BOTTOM: str
