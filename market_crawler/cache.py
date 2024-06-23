# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import os

from pathlib import Path
from typing import Final

import cashews


# ? Cache directory for saving browser cookies and cashews' cache files
CACHE_DIR: Final[str] = os.path.join(Path().absolute(), "cache")

async_diskcache = cashews.Cache(Path().absolute().name)
Gb = 1073741824  # ? 1 GB in bytes
async_diskcache.setup(
    f"disk://?directory={CACHE_DIR}",  # ? It uses SQLite as the disk cache
    size_limit=3 * Gb,  # ? Let's make it 3 GB in size for now
    shards=12,  # ? SQLite shards; number is arbitrary (copied directly from cashews' examples)
)
