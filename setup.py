"""used to package ionbus_parquet_cache"""

from __future__ import annotations

import os
import re
import subprocess
from glob import glob

from setuptools import setup


def get_latest_tag() -> str | None:
    """Gets latest tag from git; returns None if not found."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


avoid_regexes = [
    re.compile(f"^{x}$")
    for x in [
        r"\d+",
        "build",
        "egg-info",
        "__pycache__",
        "log",
        ".+egg-info",
        "dist",
        "tests",
        "integration_tests",
        "conda-recipe",
    ]
]

def ok_dir(name: str) -> bool:
    """Returns ok if we should keep."""
    for regex in avoid_regexes:
        if regex.search(name):
            return False
    return True


if latest_tag := get_latest_tag():
    with open("_version.py", "w", encoding="utf-8") as ver_file:
        ver_file.write(f'__version__ = "{latest_tag}"\n')

all_dirs = [x for x in glob("*") if os.path.isdir(x) and ok_dir(x)]
packages = ["ionbus_parquet_cache"] + [f"ionbus_parquet_cache/{x}" for x in all_dirs]
package_dirs = {
    "ionbus_parquet_cache": ".",
}
package_dirs.update({f"ionbus_parquet_cache/{x}": f"./{x}" for x in all_dirs})

with open("readme_pip.md", "r", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

setup(
    name="ionbus-parquet-cache",
    url="https://github.com/ionbus/ionbus_parquet_cache",
    packages=packages,
    package_dir=package_dirs,
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_data={
        "ionbus_parquet_cache": [
            "*.md",
            "*/*.md",
        ]
    },
)
