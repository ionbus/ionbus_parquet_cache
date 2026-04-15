"""
Snapshot suffix utilities for ionbus_parquet_cache.

Snapshot suffixes are base-62 encoded timestamps (e.g., "1Gz5hK") that:
- Are exactly 6 characters (valid through ~year 3770)
- Use digits 0-9, letters A-Z and a-z (62 characters total)
- Have lexicographic ordering matching chronological ordering
- Are generated from seconds since Unix epoch

Uses ionbus_utils.general.timestamped_id for base-62 encoding.
"""

from __future__ import annotations

import re
from pathlib import Path

from ionbus_utils.base_utils import base_to_int, int_to_base
from ionbus_utils.general import timestamped_id

# Suffix is exactly 6 characters
SUFFIX_LENGTH = 6

# Pattern to match a valid suffix (6 base-62 characters)
SUFFIX_PATTERN = re.compile(r"^[0-9A-Za-z]{6}$")

# Pattern to extract suffix from filename: name_SUFFIX.ext or name_SUFFIX/
# (handles .pkl.gz etc.)
FILENAME_SUFFIX_PATTERN = re.compile(r"_([0-9A-Za-z]{6})(?:\.[^_/\\]+)*$")


def generate_snapshot_suffix(timestamp: float | None = None) -> str:
    """
    Generate a snapshot suffix from a timestamp.

    The suffix is a base-62 encoded count of seconds since Unix epoch,
    exactly 6 characters long. This ensures lexicographic ordering
    matches chronological ordering.

    Args:
        timestamp: Unix timestamp in seconds. If None, uses current time.

    Returns:
        6-character base-62 encoded suffix (e.g., "1Gz5hK").

    Example:
        >>> generate_snapshot_suffix(1704067200.0)  # 2024-01-01 00:00:00 UTC
        '1H4Dw0'
    """
    if timestamp is None:
        # Use ionbus_utils timestamped_id with min_length for padding
        return timestamped_id(use_seconds=True, min_length=SUFFIX_LENGTH)

    # For explicit timestamps, encode directly with padding
    seconds = int(timestamp)
    suffix = int_to_base(seconds, base=62)

    # Ensure exactly 6 characters by padding with leading '0's
    if len(suffix) < SUFFIX_LENGTH:
        suffix = "0" * (SUFFIX_LENGTH - len(suffix)) + suffix
    elif len(suffix) > SUFFIX_LENGTH:
        raise ValueError(
            f"Timestamp {timestamp} produces {len(suffix)}-char suffix, "
            f"exceeds max {SUFFIX_LENGTH}"
        )

    return suffix


def parse_snapshot_suffix(suffix: str) -> int:
    """
    Parse a snapshot suffix back to Unix timestamp (seconds).

    Args:
        suffix: 6-character base-62 encoded suffix.

    Returns:
        Unix timestamp in seconds.

    Raises:
        ValueError: If suffix is invalid (wrong length or characters).

    Example:
        >>> parse_snapshot_suffix("1H4Dw0")
        1704067200
    """
    if not SUFFIX_PATTERN.match(suffix):
        raise ValueError(
            f"Invalid snapshot suffix '{suffix}': must be exactly 6 "
            f"base-62 characters"
        )

    return base_to_int(suffix, base=62)


def extract_suffix_from_filename(filename: str) -> str | None:
    """
    Extract the snapshot suffix from a filename.

    Handles patterns like:
    - "dataset_1Gz5hK.parquet" -> "1Gz5hK"
    - "dataset_1Gz5hK.pkl.gz" -> "1Gz5hK"
    - "dataset_1Gz5hK/" -> "1Gz5hK" (directory)
    - "dataset_1Gz5hK" -> "1Gz5hK"

    Args:
        filename: Filename or path to extract suffix from.

    Returns:
        The 6-character suffix, or None if no valid suffix found.
    """
    # Handle Path objects
    if isinstance(filename, Path):
        filename = filename.name

    # Remove trailing slash for directories
    filename = filename.rstrip("/\\")

    # Try to match the suffix pattern
    match = FILENAME_SUFFIX_PATTERN.search(filename)
    if match:
        return match.group(1)

    # Also try without extension (for bare names like "dataset_1Gz5hK")
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
        suffixes: List of 6-character snapshot suffixes.

    Returns:
        The lexicographically largest suffix, or None if list is empty.

    Example:
        >>> get_current_suffix(["1Gz4Ab", "1Gz5hK", "1Gz3zZ"])
        '1Gz5hK'
    """
    if not suffixes:
        return None

    # Filter to valid suffixes and find max
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
        True if suffix is exactly 6 base-62 characters, False otherwise.
    """
    return bool(SUFFIX_PATTERN.match(suffix))
