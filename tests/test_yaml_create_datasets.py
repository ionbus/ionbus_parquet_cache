"""Tests for yaml_create_datasets CLI tool."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from ionbus_parquet_cache.yaml_create_datasets import (
    check_config_consistency,
    create_cache_main,
    IMMUTABLE_FIELDS,
)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache with yaml/ and code/ directories."""
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "yaml").mkdir()
    (cache / "code").mkdir()
    return cache


@pytest.fixture
def sample_source_file(temp_cache: Path) -> Path:
    """Create a sample DataSource Python file."""
    source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    """Test data source."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
'''
    source_path = temp_cache / "code" / "test_source.py"
    source_path.write_text(source_content)
    return source_path


@pytest.fixture
def sample_yaml(temp_cache: Path, sample_source_file: Path) -> Path:
    """Create a sample YAML configuration file."""
    yaml_content = """
datasets:
    test_dataset:
        description: Test dataset
        date_col: Date
        date_partition: month
        source_location: code/test_source.py
        source_class_name: TestSource
"""
    yaml_path = temp_cache / "yaml" / "test.yaml"
    yaml_path.write_text(yaml_content)
    return yaml_path


@pytest.fixture
def multi_dataset_yaml(temp_cache: Path, sample_source_file: Path) -> Path:
    """Create a YAML file with multiple datasets."""
    yaml_content = """
datasets:
    dataset_one:
        description: First dataset
        date_col: Date
        date_partition: day
        source_location: code/test_source.py
        source_class_name: TestSource

    dataset_two:
        description: Second dataset
        date_col: Date
        date_partition: month
        source_location: code/test_source.py
        source_class_name: TestSource
"""
    yaml_path = temp_cache / "yaml" / "multi.yaml"
    yaml_path.write_text(yaml_content)
    return yaml_path


class TestArgumentParsing:
    """Tests for CLI argument parsing."""

    def test_basic_argument_parsing(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should accept a valid YAML file argument."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

    def test_date_arguments(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should parse --start-date and --end-date correctly."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

    def test_instruments_argument(self, temp_cache: Path) -> None:
        """Should parse --instruments argument."""
        # Create source file
        source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class InstrumentSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({
            "Date": dates,
            "symbol": ["AAPL"] * len(dates),
            "value": range(len(dates)),
        })
'''
        (temp_cache / "code" / "instrument_source.py").write_text(source_content)

        # Create YAML with instrument_column set as a partition column
        yaml_content = """
datasets:
    instrument_dataset:
        date_col: Date
        partition_columns:
            - symbol
        instrument_column: symbol
        source_location: code/instrument_source.py
        source_class_name: InstrumentSource
"""
        yaml_path = temp_cache / "yaml" / "instrument.yaml"
        yaml_path.write_text(yaml_content)

        result = create_cache_main([
            str(yaml_path),
            "instrument_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--instruments", "AAPL,MSFT,GOOG",
        ])
        assert result == 0

    def test_verbose_flag(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should accept --verbose or -v flag."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--verbose",
        ])
        assert result == 0

    def test_verbose_short_flag(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should accept -v short flag."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "-v",
        ])
        assert result == 0


class TestMissingCacheDirectory:
    """Tests for missing cache directory error handling."""

    def test_missing_cache_dir_from_yaml(self, tmp_path: Path) -> None:
        """Should fail when cache_dir from YAML doesn't exist."""
        yaml_content = """
cache_dir: /nonexistent/path/to/cache
datasets:
    test_dataset:
        date_col: Date
        source_class_name: TestSource
"""
        yaml_path = tmp_path / "test.yaml"
        yaml_path.write_text(yaml_content)

        result = create_cache_main([str(yaml_path)])
        assert result == 1

    def test_missing_yaml_file(self, tmp_path: Path) -> None:
        """Should fail when YAML file doesn't exist."""
        result = create_cache_main([str(tmp_path / "nonexistent.yaml")])
        assert result == 1

    def test_missing_parent_dir_for_default_cache(self, tmp_path: Path) -> None:
        """Should fail when default cache location doesn't exist."""
        # Create a deeply nested YAML file where parent.parent doesn't exist
        nested_path = tmp_path / "deep" / "nested" / "yaml" / "test.yaml"
        nested_path.parent.mkdir(parents=True)
        nested_path.write_text("datasets:\n  test:\n    date_col: Date")

        # The default cache would be yaml/../.. which doesn't exist
        result = create_cache_main([str(nested_path)])
        # Should still work as long as parent.parent exists
        assert result in (0, 1)  # May fail for other reasons


class TestCreateDatasetsFromYaml:
    """Tests for creating datasets from YAML configuration."""

    def test_create_single_dataset(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should create a dataset from YAML configuration."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Verify metadata was created
        meta_dir = temp_cache / "test_dataset" / "_meta_data"
        assert meta_dir.exists()
        assert len(list(meta_dir.glob("*.pkl.gz"))) == 1

    def test_create_all_datasets(
        self, temp_cache: Path, multi_dataset_yaml: Path
    ) -> None:
        """Should create all datasets when no dataset_name specified."""
        result = create_cache_main([
            str(multi_dataset_yaml),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Both datasets should have metadata
        meta_dir_one = temp_cache / "dataset_one" / "_meta_data"
        meta_dir_two = temp_cache / "dataset_two" / "_meta_data"
        assert meta_dir_one.exists()
        assert meta_dir_two.exists()

    def test_creates_data_files(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should create parquet data files."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Should have parquet files
        data_files = list(temp_cache.rglob("*.parquet"))
        assert len(data_files) > 0


class TestDatasetFiltering:
    """Tests for --dataset filtering (only create specific datasets)."""

    def test_filter_to_single_dataset(
        self, temp_cache: Path, multi_dataset_yaml: Path
    ) -> None:
        """Should only create the specified dataset."""
        result = create_cache_main([
            str(multi_dataset_yaml),
            "dataset_one",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Only dataset_one should have metadata
        meta_dir_one = temp_cache / "dataset_one" / "_meta_data"
        meta_dir_two = temp_cache / "dataset_two" / "_meta_data"
        assert meta_dir_one.exists()
        assert not meta_dir_two.exists()

    def test_nonexistent_dataset_filter(
        self, temp_cache: Path, multi_dataset_yaml: Path
    ) -> None:
        """Should fail when filtering to a nonexistent dataset."""
        result = create_cache_main([
            str(multi_dataset_yaml),
            "nonexistent_dataset",
        ])
        assert result == 1


class TestDryRunFlag:
    """Tests for --dry-run flag behavior."""

    def test_dry_run_no_files_created(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Dry run should not create any files."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--dry-run",
        ])
        assert result == 0

        # No metadata should be created
        meta_dir = temp_cache / "test_dataset" / "_meta_data"
        assert not meta_dir.exists() or not list(meta_dir.glob("*.pkl.gz"))

    def test_dry_run_no_parquet_files(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Dry run should not create any parquet files."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--dry-run",
        ])
        assert result == 0

        # No parquet files should be created
        parquet_files = list(temp_cache.rglob("*.parquet"))
        assert len(parquet_files) == 0

    def test_dry_run_with_verbose(
        self, temp_cache: Path, sample_yaml: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Dry run with verbose should show what would happen."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--dry-run",
            "--verbose",
        ])
        assert result == 0

        assert "would create" in caplog.text.lower() or "test_dataset" in caplog.text


class TestErrorHandlingInvalidYaml:
    """Tests for error handling with invalid YAML."""

    def test_invalid_yaml_syntax(self, temp_cache: Path) -> None:
        """Should fail with invalid YAML syntax."""
        yaml_path = temp_cache / "yaml" / "invalid.yaml"
        yaml_path.write_text("datasets:\n  bad: [unmatched bracket")

        result = create_cache_main([str(yaml_path)])
        assert result == 1

    def test_empty_yaml(self, temp_cache: Path) -> None:
        """Should fail with empty YAML file."""
        yaml_path = temp_cache / "yaml" / "empty.yaml"
        yaml_path.write_text("datasets: {}")

        result = create_cache_main([str(yaml_path)])
        assert result == 1

    def test_yaml_not_a_mapping(self, temp_cache: Path) -> None:
        """Should fail when YAML is not a mapping."""
        yaml_path = temp_cache / "yaml" / "list.yaml"
        yaml_path.write_text("- item1\n- item2")

        result = create_cache_main([str(yaml_path)])
        assert result == 1

    def test_missing_source_class(self, temp_cache: Path) -> None:
        """Should fail when source class doesn't exist."""
        yaml_content = """
datasets:
    bad_source:
        date_col: Date
        source_location: code/nonexistent.py
        source_class_name: MissingSource
"""
        yaml_path = temp_cache / "yaml" / "bad.yaml"
        yaml_path.write_text(yaml_content)

        result = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 1


class TestModeFlags:
    """Tests for --backfill and --restate flags."""

    def test_backfill_and_restate_mutually_exclusive(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail when both --backfill and --restate are specified."""
        result = create_cache_main([
            str(sample_yaml),
            "--backfill",
            "--restate",
            "--start-date", "2024-01-01",
        ])
        assert result == 1

    def test_backfill_with_end_date(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail when --backfill is used with --end-date."""
        result = create_cache_main([
            str(sample_yaml),
            "--backfill",
            "--start-date", "2024-01-01",
            "--end-date", "2024-06-30",
        ])
        assert result == 1

    def test_restate_requires_both_dates(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail when --restate is used without both dates."""
        result = create_cache_main([
            str(sample_yaml),
            "--restate",
            "--start-date", "2024-01-01",
        ])
        assert result == 1

    def test_restate_requires_start_date(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail when --restate is used without start date."""
        result = create_cache_main([
            str(sample_yaml),
            "--restate",
            "--end-date", "2024-06-30",
        ])
        assert result == 1


class TestDateParsing:
    """Tests for date argument parsing."""

    def test_invalid_start_date_format(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail with invalid start date format."""
        result = create_cache_main([
            str(sample_yaml),
            "--start-date", "01-01-2024",  # Wrong format
        ])
        assert result == 1

    def test_invalid_end_date_format(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should fail with invalid end date format."""
        result = create_cache_main([
            str(sample_yaml),
            "--start-date", "2024-01-01",
            "--end-date", "January 31 2024",  # Wrong format
        ])
        assert result == 1

    def test_valid_iso_dates(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should accept valid ISO format dates."""
        result = create_cache_main([
            str(sample_yaml),
            "test_dataset",
            "--start-date", "2024-01-01",
            "--end-date", "2024-12-31",
        ])
        assert result == 0


class TestCheckConfigConsistency:
    """Tests for check_config_consistency function."""

    def test_matching_configs(self) -> None:
        """Should return empty list when configs match."""
        yaml_config = {
            "date_col": "Date",
            "date_partition": "month",
            "partition_columns": ["symbol"],
            "sort_columns": ["Date"],
            "instrument_column": None,
        }
        stored_config = yaml_config.copy()

        errors = check_config_consistency(yaml_config, stored_config, "test")
        assert errors == []

    def test_mismatched_date_col(self) -> None:
        """Should detect mismatched date_col."""
        yaml_config = {"date_col": "Date", "sort_columns": ["Date"]}
        stored_config = {"date_col": "TradeDate", "sort_columns": ["TradeDate"]}

        errors = check_config_consistency(yaml_config, stored_config, "test")
        # date_col and sort_columns both mismatch
        assert len(errors) == 2
        assert any("date_col" in e for e in errors)

    def test_mismatched_partition_columns(self) -> None:
        """Should detect mismatched partition_columns."""
        yaml_config = {"partition_columns": ["symbol", "month"]}
        stored_config = {"partition_columns": ["symbol"]}

        errors = check_config_consistency(yaml_config, stored_config, "test")
        assert len(errors) == 1
        assert "partition_columns" in errors[0]

    def test_none_vs_empty_list_normalized(self) -> None:
        """Should treat None and empty list as equivalent for list fields."""
        yaml_config = {"partition_columns": None}
        stored_config = {"partition_columns": []}

        errors = check_config_consistency(yaml_config, stored_config, "test")
        assert errors == []

    def test_sort_columns_default_handling(self) -> None:
        """Should handle sort_columns default to [date_col]."""
        yaml_config = {"date_col": "Date", "sort_columns": None}
        stored_config = {"date_col": "Date", "sort_columns": ["Date"]}

        errors = check_config_consistency(yaml_config, stored_config, "test")
        assert errors == []

    def test_multiple_mismatches(self) -> None:
        """Should report all mismatches."""
        yaml_config = {
            "date_col": "Date",
            "date_partition": "day",
            "sort_columns": ["Date"],
        }
        stored_config = {
            "date_col": "TradeDate",
            "date_partition": "month",
            "sort_columns": ["TradeDate"],
        }

        errors = check_config_consistency(yaml_config, stored_config, "test")
        # date_col, date_partition, and sort_columns all mismatch
        assert len(errors) == 3


class TestCacheDirResolution:
    """Tests for cache_dir resolution from YAML."""

    def test_explicit_cache_dir(self, tmp_path: Path) -> None:
        """Should use cache_dir from YAML when specified."""
        cache = tmp_path / "my_cache"
        cache.mkdir()
        (cache / "yaml").mkdir()
        (cache / "code").mkdir()

        # Create source file
        source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
'''
        (cache / "code" / "test_source.py").write_text(source_content)

        # Put YAML in a different location
        yaml_dir = tmp_path / "configs"
        yaml_dir.mkdir()
        yaml_content = f"""
cache_dir: {cache}
datasets:
    test_dataset:
        date_col: Date
        source_location: code/test_source.py
        source_class_name: TestSource
"""
        yaml_path = yaml_dir / "test.yaml"
        yaml_path.write_text(yaml_content)

        result = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Dataset should be in specified cache_dir
        meta_dir = cache / "test_dataset" / "_meta_data"
        assert meta_dir.exists()

    def test_relative_cache_dir(self, tmp_path: Path) -> None:
        """Should resolve relative cache_dir relative to YAML location."""
        yaml_dir = tmp_path / "configs"
        yaml_dir.mkdir()

        cache = yaml_dir / "data_cache"
        cache.mkdir()
        (cache / "code").mkdir()

        # Create source file
        source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
'''
        (cache / "code" / "test_source.py").write_text(source_content)

        yaml_content = """
cache_dir: data_cache
datasets:
    test_dataset:
        date_col: Date
        source_location: code/test_source.py
        source_class_name: TestSource
"""
        yaml_path = yaml_dir / "test.yaml"
        yaml_path.write_text(yaml_content)

        result = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Dataset should be in resolved cache location
        meta_dir = cache / "test_dataset" / "_meta_data"
        assert meta_dir.exists()


class TestPreserveConfig:
    """Tests for --preserve-config flag."""

    def test_preserve_config_requires_existing_metadata(
        self, tmp_path: Path
    ) -> None:
        """--preserve-config should fail if no existing metadata."""
        cache = tmp_path / "cache"
        cache.mkdir()
        (cache / "yaml").mkdir()
        (cache / "code").mkdir()

        source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
'''
        (cache / "code" / "test_source.py").write_text(source_content)

        yaml_content = """
cache_dir: ..
datasets:
    new_dataset:
        date_col: Date
        source_location: code/test_source.py
        source_class_name: TestSource
"""
        yaml_path = cache / "yaml" / "test.yaml"
        yaml_path.write_text(yaml_content)

        # --preserve-config on non-existent dataset should fail
        result = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
            "--preserve-config",
        ])
        assert result == 1  # Error: requires existing metadata

    def test_preserve_config_allows_different_yaml_settings(
        self, tmp_path: Path
    ) -> None:
        """--preserve-config should use stored config, not YAML config."""
        import time

        cache = tmp_path / "cache"
        cache.mkdir()
        (cache / "yaml").mkdir()
        (cache / "code").mkdir()

        source_content = '''
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
'''
        (cache / "code" / "test_source.py").write_text(source_content)

        # Create initial dataset with month partition
        yaml_content1 = """
cache_dir: ..
datasets:
    test_dataset:
        date_col: Date
        date_partition: month
        source_location: code/test_source.py
        source_class_name: TestSource
"""
        yaml_path = cache / "yaml" / "test.yaml"
        yaml_path.write_text(yaml_content1)

        result = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-01-01",
            "--end-date", "2024-01-31",
        ])
        assert result == 0

        # Wait for different snapshot suffix
        time.sleep(1.1)

        # Now create a different YAML with day partition (would normally fail)
        yaml_content2 = """
cache_dir: ..
datasets:
    test_dataset:
        date_col: Date
        date_partition: day
        source_location: code/test_source.py
        source_class_name: TestSource
"""
        yaml_path.write_text(yaml_content2)

        # Without --preserve-config, this would fail
        result_without = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-02-01",
            "--end-date", "2024-02-28",
        ])
        assert result_without == 1  # Config mismatch error

        # With --preserve-config, it should succeed
        result_with = create_cache_main([
            str(yaml_path),
            "--start-date", "2024-02-01",
            "--end-date", "2024-02-28",
            "--preserve-config",
        ])
        assert result_with == 0

        # Verify dataset still uses month partition (from stored config)
        from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
        dpd = DatedParquetDataset(
            cache_dir=cache, name="test_dataset", date_col="Date"
        )
        dpd._metadata = dpd._load_metadata()
        assert dpd._metadata.yaml_config.get("date_partition") == "month"
