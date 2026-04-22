"""
Instrument hash-bucketing utilities for ionbus_parquet_cache.

These functions are the single source of truth for the bucket hash function.
BucketedDataSource subclasses should import from here rather than
reimplementing the hash.

Hash function: zlib.crc32(instrument.encode()) % num_buckets
Bucket strings: 3-character base-36 (digits 0–9 and A–Z), e.g. "02K".

Base-36 is used because macOS and Windows both have
case-insensitive filesystems. A mixed-case alphabet like base-62 produces pairs
such as "1U" and "1u" that resolve to the same directory, silently merging
buckets and corrupting the dataset. All-base-36 avoids this entirely.
Three digits support up to 46,656 buckets (36³), far more than any real dataset
will need.
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
        3-character base-36 bucket string (e.g. "02K").

    Example:
        >>> instrument_bucket("AAPL", 256)
        '02K'
    """
    bucket_num = zlib.crc32(str(instrument).encode()) % num_buckets
    return int_to_base(bucket_num, 36, minimum_width=3)


def all_bucket_strings(num_buckets: int) -> list[str]:
    """
    Return the sorted list of all bucket strings for a given bucket count.

    Args:
        num_buckets: Total number of buckets.

    Returns:
        List of 3-character base-36 bucket strings in sorted order.

    Example:
        >>> all_bucket_strings(3)
        ['000', '001', '002']
    """
    return [int_to_base(i, 36, minimum_width=3) for i in range(num_buckets)]


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
