"""
Instrument hash-bucketing utilities for ionbus_parquet_cache.

These functions are the single source of truth for the bucket hash function.
BucketedDataSource subclasses should import from here rather than
reimplementing the hash.

Hash function: zlib.crc32(instrument.encode()) % num_buckets
Bucket strings: 2-character base-62 (e.g. "0g") for consistent
lexicographic ordering in directory names. Bucket strings are 2-char base-62
(e.g. "0g"), which covers up to 3844 buckets before names exceed 2 characters.
Beyond that the strings grow longer and lose fixed-width lexicographic ordering,
but since bucket order is irrelevant to correctness, this is harmless in
practice. Typical values are 20–256; no sane dataset needs anywhere near 3844.
"""

from __future__ import annotations

import zlib
from collections import defaultdict

from ionbus_utils.base_utils import int_to_base

# Reserved partition column name injected by the library for bucketed datasets.
INSTRUMENT_BUCKET_COL = "__instrument_bucket__"


def instrument_bucket(instrument: str, num_buckets: int) -> str:
    """
    Return the bucket string for a single instrument.

    Args:
        instrument: Instrument identifier (e.g. "AAPL").
        num_buckets: Total number of buckets.

    Returns:
        Base-62 bucket string, zero-padded to at least 2 characters (e.g. "0g").

    Example:
        >>> instrument_bucket("AAPL", 256)
        '0g'
    """
    bucket_num = zlib.crc32(str(instrument).encode()) % num_buckets
    return int_to_base(bucket_num, 62).zfill(2)


def all_bucket_strings(num_buckets: int) -> list[str]:
    """
    Return the sorted list of all bucket strings for a given bucket count.

    Args:
        num_buckets: Total number of buckets.

    Returns:
        List of 2-character base-62 bucket strings in sorted order.

    Example:
        >>> all_bucket_strings(3)
        ['00', '01', '02']
    """
    return [int_to_base(i, 62).zfill(2) for i in range(num_buckets)]


def bucket_instruments(
    instruments: list[str] | set[str],
    num_buckets: int,
) -> dict[str, list[str]]:
    """
    Group instruments into hash buckets.

    Args:
        instruments: Instrument identifiers to group.
        num_buckets: Total number of buckets.

    Returns:
        Dict mapping bucket string → sorted list of instruments in that bucket.
        Only buckets that contain at least one instrument are included.

    Example:
        >>> buckets = bucket_instruments(["AAPL", "MSFT", "GOOG"], num_buckets=256)
        >>> buckets["0g"]
        ['AAPL']
    """
    groups: dict[str, list[str]] = defaultdict(list)
    for instrument in instruments:
        bucket = instrument_bucket(instrument, num_buckets)
        groups[bucket].append(instrument)
    return {k: sorted(v) for k, v in sorted(groups.items())}
