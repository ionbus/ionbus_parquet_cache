"""
Snapshot suffix utilities for ionbus_parquet_cache.

Snapshot suffixes are base-36 encoded timestamps (digits + A–Z) that:
- Are exactly 7 characters (valid through April 5, 4453)
- Use only digits 0–9 and A–Z (36 characters)
- Have lexicographic ordering matching chronological ordering
- Are generated from seconds since Unix epoch

Base-36 is used because macOS and Windows both have case-insensitive
filesystems. A mixed-case alphabet (e.g. base-62) would produce pairs of
suffixes that differ only in case and resolve to the same file. Base-36
(digits 0–9 and A–Z only) avoids this entirely.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

from ionbus_utils.base_utils import base_to_int, int_to_base

# 7-character base-36 (covers through April 5, 4453)
SUFFIX_LENGTH = 7

# Matches 7-char base-36 suffixes (digits 0–9 and A–Z)
SUFFIX_PATTERN = re.compile(r"^[0-9A-Z]{7}$")

# Extracts suffix from filenames
FILENAME_SUFFIX_PATTERN = re.compile(r"_([0-9A-Z]{7})(?:\.[^_/\\]+)*$")


def generate_snapshot_suffix(timestamp: float | None = None) -> str:
    """
    Generate a snapshot suffix from a timestamp.

    The suffix is a base-36 encoded count of seconds since Unix epoch,
    exactly 7 characters long. This ensures lexicographic ordering
    matches chronological ordering.

    Args:
        timestamp: Unix timestamp in seconds. If None, uses current time.

    Returns:
        7-character base-36 suffix (e.g. "1H4DW00").

    Example:
        >>> generate_snapshot_suffix(1704067200.0)  # 2024-01-01 00:00:00 UTC
        '1H4DW00'
    """
    seconds = int(timestamp) if timestamp is not None else int(time.time())
    suffix = int_to_base(seconds, base=36, minimum_width=SUFFIX_LENGTH)
    if len(suffix) > SUFFIX_LENGTH:
        raise ValueError(
            f"Timestamp {seconds} produces {len(suffix)}-char base-36 suffix, "
            f"exceeds max {SUFFIX_LENGTH}"
        )
    return suffix


def parse_snapshot_suffix(suffix: str) -> int:
    """
    Parse a snapshot suffix back to Unix timestamp (seconds).

    Args:
        suffix: Snapshot suffix string.

    Returns:
        Unix timestamp in seconds.

    Raises:
        ValueError: If suffix is invalid.

    Example:
        >>> parse_snapshot_suffix("1H4DW00")
        1704067200
    """
    if not SUFFIX_PATTERN.match(suffix):
        raise ValueError(
            f"Invalid snapshot suffix '{suffix}': must be 7 base-36 characters (0-9, A-Z)"
        )
    return base_to_int(suffix, base=36)


def extract_suffix_from_filename(filename: str) -> str | None:
    """
    Extract the snapshot suffix from a filename.

    Handles patterns like:
    - "dataset_1H4DW00.parquet" -> "1H4DW00"
    - "dataset_1H4DW00.pkl.gz" -> "1H4DW00"
    - "dataset_1H4DW00/" -> "1H4DW00" (directory)
    - "dataset_1H4DW00" -> "1H4DW00"

    Args:
        filename: Filename or path to extract suffix from.

    Returns:
        The suffix string, or None if no valid suffix found.
    """
    if isinstance(filename, Path):
        filename = filename.name

    filename = filename.rstrip("/\\")

    match = FILENAME_SUFFIX_PATTERN.search(filename)
    if match:
        return match.group(1)

    if "_" in filename:
        potential_suffix = filename.rsplit("_", 1)[-1]
        if SUFFIX_PATTERN.match(potential_suffix):
            return potential_suffix

    return None


def get_current_suffix(suffixes: list[str]) -> str | None:
    """
    Get the current (most recent) snapshot suffix from a list.

    The current snapshot is the one with the lexicographically largest
    suffix, which corresponds to the most recent timestamp.

    Args:
        suffixes: List of snapshot suffixes.

    Returns:
        The lexicographically largest suffix, or None if list is empty.

    Example:
        >>> get_current_suffix(["1H4DW00", "1H4DW01", "1H4DW02"])
        '1H4DW02'
    """
    if not suffixes:
        return None
    valid = [s for s in suffixes if SUFFIX_PATTERN.match(s)]
    if not valid:
        return None
    return max(valid)


def is_valid_suffix(suffix: str) -> bool:
    """
    Check if a string is a valid snapshot suffix.

    Args:
        suffix: String to check.

    Returns:
        True if valid, False otherwise.
    """
    return bool(SUFFIX_PATTERN.match(suffix))
