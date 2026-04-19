"""
Instrument hash-bucketing utilities for ionbus_parquet_cache.

These functions are the single source of truth for the bucket hash function.
Both the internal _BucketedDataSourceWrapper and user-written DataSources
should import from here rather than reimplementing the hash.

Hash function: zlib.crc32(instrument.encode()) % num_buckets
Bucket strings: zero-padded to 4 digits (e.g. "0042") for consistent
lexicographic ordering in directory names.
"""

from __future__ import annotations

import zlib
from collections import defaultdict


def instrument_bucket(instrument: str, num_buckets: int) -> str:
    """
    Return the bucket string for a single instrument.

    Args:
        instrument: Instrument identifier (e.g. "AAPL").
        num_buckets: Total number of buckets.

    Returns:
        Zero-padded 4-digit bucket string (e.g. "0042").

    Example:
        >>> instrument_bucket("AAPL", 256)
        '0042'
    """
    return f"{zlib.crc32(instrument.encode()) % num_buckets:04d}"


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
        >>> buckets["0042"]
        ['AAPL']
    """
    groups: dict[str, list[str]] = defaultdict(list)
    for instrument in instruments:
        bucket = instrument_bucket(instrument, num_buckets)
        groups[bucket].append(instrument)
    return {k: sorted(v) for k, v in sorted(groups.items())}
