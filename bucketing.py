"""
Instrument hash-bucketing utilities for ionbus_parquet_cache.

These functions are the single source of truth for the bucket hash function.
Both the internal _BucketedDataSourceWrapper and user-written DataSources
should import from here rather than reimplementing the hash.

Hash function: zlib.crc32(instrument.encode()) % num_buckets
Bucket strings: 2-character base-62 (e.g. "0g") for consistent
lexicographic ordering in directory names. Supports up to 3844 buckets.
"""

from __future__ import annotations

import zlib
from collections import defaultdict

from ionbus_utils.base_utils import int_to_base


def instrument_bucket(instrument: str, num_buckets: int) -> str:
    """
    Return the bucket string for a single instrument.

    Args:
        instrument: Instrument identifier (e.g. "AAPL").
        num_buckets: Total number of buckets (max 3844 for 2-char base-62).

    Returns:
        2-character base-62 bucket string (e.g. "0g").

    Example:
        >>> instrument_bucket("AAPL", 256)
        '0g'
    """
    bucket_num = zlib.crc32(str(instrument).encode()) % num_buckets
    return int_to_base(bucket_num, 62).zfill(2)


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
