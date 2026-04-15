"""Tests for snapshot.py - base-62 timestamp utilities."""

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

    def test_generates_6_char_string(self) -> None:
        """Suffix should always be exactly 6 characters."""
        suffix = generate_snapshot_suffix()
        assert len(suffix) == SUFFIX_LENGTH

    def test_all_chars_are_base62(self) -> None:
        """All characters should be valid base-62."""
        suffix = generate_snapshot_suffix()
        assert is_valid_suffix(suffix)

    def test_known_timestamp(self) -> None:
        """Test encoding of a known timestamp."""
        # 2024-01-01 00:00:00 UTC = 1704067200
        suffix = generate_snapshot_suffix(1704067200.0)
        assert len(suffix) == SUFFIX_LENGTH
        # Should decode back to same value
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
        # Should be parseable
        parsed = parse_snapshot_suffix(suffix)
        # Should be close to current time
        assert abs(parsed - int(time.time())) < 2


class TestParseSnapshotSuffix:
    """Tests for parse_snapshot_suffix()."""

    def test_roundtrip(self) -> None:
        """Encoding then decoding should give back the original value."""
        for timestamp in [0, 1, 1000, 1704067200, int(time.time())]:
            suffix = generate_snapshot_suffix(float(timestamp))
            parsed = parse_snapshot_suffix(suffix)
            assert parsed == timestamp

    def test_invalid_length_raises(self) -> None:
        """Non-6-character strings should raise ValueError."""
        with pytest.raises(ValueError, match="must be exactly 6"):
            parse_snapshot_suffix("12345")  # Too short

        with pytest.raises(ValueError, match="must be exactly 6"):
            parse_snapshot_suffix("1234567")  # Too long

    def test_invalid_chars_raises(self) -> None:
        """Invalid characters should raise ValueError."""
        with pytest.raises(ValueError, match="must be exactly 6"):
            parse_snapshot_suffix("12345!")  # Invalid char


class TestExtractSuffixFromFilename:
    """Tests for extract_suffix_from_filename()."""

    def test_parquet_file(self) -> None:
        """Extract from .parquet filename."""
        assert extract_suffix_from_filename("dataset_1Gz5hK.parquet") == "1Gz5hK"

    def test_pickle_file(self) -> None:
        """Extract from .pkl.gz filename."""
        assert extract_suffix_from_filename("dataset_1Gz5hK.pkl.gz") == "1Gz5hK"

    def test_directory(self) -> None:
        """Extract from directory name."""
        assert extract_suffix_from_filename("dataset_1Gz5hK/") == "1Gz5hK"

    def test_bare_name(self) -> None:
        """Extract from name without extension."""
        assert extract_suffix_from_filename("dataset_1Gz5hK") == "1Gz5hK"

    def test_no_suffix(self) -> None:
        """Return None when no valid suffix found."""
        assert extract_suffix_from_filename("dataset.parquet") is None
        assert extract_suffix_from_filename("dataset") is None

    def test_invalid_suffix(self) -> None:
        """Return None for invalid suffix patterns."""
        assert extract_suffix_from_filename("dataset_short.parquet") is None
        assert extract_suffix_from_filename("dataset_toolong1.parquet") is None


class TestGetCurrentSuffix:
    """Tests for get_current_suffix()."""

    def test_empty_list(self) -> None:
        """Empty list should return None."""
        assert get_current_suffix([]) is None

    def test_single_suffix(self) -> None:
        """Single suffix should be returned."""
        assert get_current_suffix(["1Gz5hK"]) == "1Gz5hK"

    def test_multiple_suffixes(self) -> None:
        """Should return lexicographically largest suffix."""
        suffixes = ["1Gz3zZ", "1Gz5hK", "1Gz4Ab"]
        assert get_current_suffix(suffixes) == "1Gz5hK"

    def test_filters_invalid(self) -> None:
        """Should filter out invalid suffixes."""
        suffixes = ["1Gz5hK", "short", "1Gz3zZ"]
        assert get_current_suffix(suffixes) == "1Gz5hK"

    def test_all_invalid(self) -> None:
        """Should return None if all suffixes are invalid."""
        assert get_current_suffix(["short", "toolong1"]) is None


class TestIsValidSuffix:
    """Tests for is_valid_suffix()."""

    def test_valid_suffixes(self) -> None:
        """Valid suffixes should return True."""
        assert is_valid_suffix("1Gz5hK")
        assert is_valid_suffix("000000")
        assert is_valid_suffix("zzzzzz")
        assert is_valid_suffix("ZZZZZZ")
        assert is_valid_suffix("abc123")

    def test_invalid_suffixes(self) -> None:
        """Invalid suffixes should return False."""
        assert not is_valid_suffix("12345")  # Too short
        assert not is_valid_suffix("1234567")  # Too long
        assert not is_valid_suffix("12345!")  # Invalid char
        assert not is_valid_suffix("")  # Empty
        assert not is_valid_suffix("12_456")  # Invalid char
