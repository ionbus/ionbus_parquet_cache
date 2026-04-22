"""Tests for snapshot.py - base-36 timestamp utilities."""

from __future__ import annotations

import time

import pytest

from ionbus_parquet_cache.snapshot import (
    generate_snapshot_suffix,
    parse_snapshot_suffix,
    extract_suffix_from_filename,
    get_current_suffix,
    is_valid_suffix,
    SUFFIX_LENGTH,
)


class TestGenerateSnapshotSuffix:
    """Tests for generate_snapshot_suffix()."""

    def test_generates_7_char_string(self) -> None:
        """Suffix should always be exactly 7 characters."""
        suffix = generate_snapshot_suffix()
        assert len(suffix) == SUFFIX_LENGTH

    def test_all_chars_are_base36_uppercase(self) -> None:
        """All characters should be digits or A-Z."""
        suffix = generate_snapshot_suffix()
        assert all(c in "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" for c in suffix)

    def test_is_valid_suffix(self) -> None:
        suffix = generate_snapshot_suffix()
        assert is_valid_suffix(suffix)

    def test_known_timestamp(self) -> None:
        """Test encoding of a known timestamp."""
        # 2024-01-01 00:00:00 UTC = 1704067200
        suffix = generate_snapshot_suffix(1704067200.0)
        assert len(suffix) == SUFFIX_LENGTH
        assert parse_snapshot_suffix(suffix) == 1704067200

    def test_monotonic_ordering(self) -> None:
        """Later timestamps should produce lexicographically larger suffixes."""
        t1 = 1704067200.0  # 2024-01-01
        t2 = 1704153600.0  # 2024-01-02

        s1 = generate_snapshot_suffix(t1)
        s2 = generate_snapshot_suffix(t2)

        assert s2 > s1, "Later timestamp should produce larger suffix"

    def test_current_time_produces_valid_suffix(self) -> None:
        """Current time should produce a valid suffix."""
        suffix = generate_snapshot_suffix()
        assert is_valid_suffix(suffix)
        parsed = parse_snapshot_suffix(suffix)
        assert abs(parsed - int(time.time())) < 2


class TestParseSnapshotSuffix:
    """Tests for parse_snapshot_suffix()."""

    def test_roundtrip(self) -> None:
        """Encoding then decoding should give back the original value."""
        for timestamp in [0, 1, 1000, 1704067200, int(time.time())]:
            suffix = generate_snapshot_suffix(float(timestamp))
            parsed = parse_snapshot_suffix(suffix)
            assert parsed == timestamp

    def test_invalid_raises(self) -> None:
        """Invalid suffixes should raise ValueError."""
        with pytest.raises(ValueError):
            parse_snapshot_suffix("12345")    # 5 chars
        with pytest.raises(ValueError):
            parse_snapshot_suffix("12345678") # 8 chars
        with pytest.raises(ValueError):
            parse_snapshot_suffix("12345!!")  # invalid char
        with pytest.raises(ValueError):
            parse_snapshot_suffix("1Gz5hK")   # 6-char — not valid
        with pytest.raises(ValueError):
            parse_snapshot_suffix("aaaaaaa")  # lowercase — not valid


class TestExtractSuffixFromFilename:
    """Tests for extract_suffix_from_filename()."""

    def test_parquet_file(self) -> None:
        assert extract_suffix_from_filename("dataset_1H4DW00.parquet") == "1H4DW00"

    def test_pickle_file(self) -> None:
        assert extract_suffix_from_filename("dataset_1H4DW00.pkl.gz") == "1H4DW00"

    def test_directory(self) -> None:
        assert extract_suffix_from_filename("dataset_1H4DW00/") == "1H4DW00"

    def test_bare_name(self) -> None:
        assert extract_suffix_from_filename("dataset_1H4DW00") == "1H4DW00"

    def test_no_suffix(self) -> None:
        assert extract_suffix_from_filename("dataset.parquet") is None
        assert extract_suffix_from_filename("dataset") is None

    def test_invalid_suffix(self) -> None:
        assert extract_suffix_from_filename("dataset_short.parquet") is None
        assert extract_suffix_from_filename("dataset_toolongx1.parquet") is None
        assert extract_suffix_from_filename("dataset_1Gz5hK.parquet") is None  # 6-char


class TestGetCurrentSuffix:
    """Tests for get_current_suffix()."""

    def test_empty_list(self) -> None:
        assert get_current_suffix([]) is None

    def test_single_suffix(self) -> None:
        assert get_current_suffix(["1H4DW00"]) == "1H4DW00"

    def test_multiple_suffixes(self) -> None:
        suffixes = ["1H4DW00", "1H4DW02", "1H4DW01"]
        assert get_current_suffix(suffixes) == "1H4DW02"

    def test_filters_invalid(self) -> None:
        suffixes = ["1H4DW00", "short", "1H4DW01"]
        assert get_current_suffix(suffixes) == "1H4DW01"

    def test_all_invalid(self) -> None:
        assert get_current_suffix(["short", "toolongx1"]) is None


class TestIsValidSuffix:
    """Tests for is_valid_suffix()."""

    def test_valid(self) -> None:
        assert is_valid_suffix("1H4DW00")
        assert is_valid_suffix("0000000")
        assert is_valid_suffix("ZZZZZZZ")
        assert is_valid_suffix("ABC1234")

    def test_invalid_suffixes(self) -> None:
        assert not is_valid_suffix("12345")    # 5 chars
        assert not is_valid_suffix("12345678") # 8 chars
        assert not is_valid_suffix("12345!!")  # invalid char
        assert not is_valid_suffix("")
        assert not is_valid_suffix("aaaaaaa")  # lowercase
        assert not is_valid_suffix("1Gz5hK")   # 6-char base-62 — no longer valid
