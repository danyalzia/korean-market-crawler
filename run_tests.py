from __future__ import annotations

import os
import subprocess


if __name__ == "__main__":
    for folder in os.listdir("market_crawler"):
        if os.path.isfile(folder) or folder.startswith("__") or folder.startswith("."):
            continue

        market_folder = os.path.join("market_crawler", folder)
        test_directory = os.path.join(market_folder, "tests")
        if not os.path.exists(test_directory):
            continue

        subprocess.run(
            [
                "pytest",
                "tests",
            ],
            cwd=market_folder,
        )
