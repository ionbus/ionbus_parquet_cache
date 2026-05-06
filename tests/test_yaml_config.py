"""Tests for YAML configuration loading."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ionbus_parquet_cache.data_cleaner import DataCleaner
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import ConfigurationError
from ionbus_parquet_cache.yaml_config import (
    DatasetConfig,
    apply_yaml_transforms,
    get_dataset_config,
    load_all_configs,
    load_yaml_file,
)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory with yaml/ subdirectory."""
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "yaml").mkdir()
    (cache / "code").mkdir()
    return cache


@pytest.fixture
def sample_yaml(temp_cache: Path) -> Path:
    """Create a sample YAML file."""
    yaml_content = """
datasets:
    md.test_dataset:
        description: Test dataset
        date_col: Date
        date_partition: month
        partition_columns:
            - symbol
            - month
        sort_columns:
            - symbol
            - Date
        repull_n_days: 5
        source_location: code/test_source.py
        source_class_name: TestSource
        source_init_args:
            api_key: test123
            timeout: 30
"""
    yaml_path = temp_cache / "yaml" / "test.yaml"
    yaml_path.write_text(yaml_content)
    return yaml_path


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
        return pd.DataFrame({"Date": [], "value": []})
'''
    source_path = temp_cache / "code" / "test_source.py"
    source_path.write_text(source_content)
    return source_path


@pytest.fixture
def sample_cleaner_file(temp_cache: Path) -> Path:
    """Create a sample DataCleaner Python file."""
    cleaner_content = '''
from __future__ import annotations
from ionbus_parquet_cache.data_cleaner import DataCleaner


class TestCleaner(DataCleaner):
    """Test data cleaner."""

    def __call__(self, rel):
        return rel
'''
    cleaner_path = temp_cache / "code" / "test_cleaner.py"
    cleaner_path.write_text(cleaner_content)
    return cleaner_path


class TestLoadYamlFile:
    """Tests for load_yaml_file()."""

    def test_load_basic_yaml(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should load a basic YAML file."""
        configs = load_yaml_file(sample_yaml, temp_cache)

        assert "md.test_dataset" in configs
        config = configs["md.test_dataset"]
        assert config.name == "md.test_dataset"
        assert config.description == "Test dataset"
        assert config.date_col == "Date"
        assert config.date_partition == "month"

    def test_load_partition_columns(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should load partition and sort columns."""
        configs = load_yaml_file(sample_yaml, temp_cache)

        config = configs["md.test_dataset"]
        assert config.partition_columns == ["symbol", "month"]
        assert config.sort_columns == ["symbol", "Date"]

    def test_load_source_settings(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should load data source settings."""
        configs = load_yaml_file(sample_yaml, temp_cache)

        config = configs["md.test_dataset"]
        assert config.source_location == "code/test_source.py"
        assert config.source_class_name == "TestSource"
        assert config.source_init_args == {
            "api_key": "test123",
            "timeout": 30,
        }

    def test_load_annotations(self, temp_cache: Path) -> None:
        """Annotations should be preserved when explicitly supplied."""
        yaml_path = temp_cache / "yaml" / "annotations.yaml"
        yaml_path.write_text("""
datasets:
    annotated:
        annotations:
            bitmask:
                1: active
                2: stale
        source_class_name: TestSource
""")

        configs = load_yaml_file(yaml_path, temp_cache)
        config = configs["annotated"]

        assert config.annotations == {"bitmask": {1: "active", 2: "stale"}}
        assert config.to_yaml_config()["annotations"] == config.annotations

    def test_omitted_annotations_stay_omitted(
        self,
        temp_cache: Path,
        sample_yaml: Path,
    ) -> None:
        """Omitted annotations should not be added by to_yaml_config()."""
        config = load_yaml_file(sample_yaml, temp_cache)["md.test_dataset"]

        assert config.annotations is None
        assert "annotations" not in config.to_yaml_config()

    def test_annotations_must_be_mapping(self, temp_cache: Path) -> None:
        """Annotations must be a YAML mapping when present."""
        yaml_path = temp_cache / "yaml" / "bad_annotations.yaml"
        yaml_path.write_text("""
datasets:
    bad:
        annotations:
            - nope
        source_class_name: TestSource
""")

        with pytest.raises(ConfigurationError, match="annotations"):
            load_yaml_file(yaml_path, temp_cache)

    def test_load_multiple_datasets(self, temp_cache: Path) -> None:
        """Should load multiple datasets from one file."""
        yaml_content = """
datasets:
    dataset_one:
        date_col: Date
        source_class_name: SourceOne

    dataset_two:
        date_col: TradeDate
        source_class_name: SourceTwo
"""
        yaml_path = temp_cache / "yaml" / "multi.yaml"
        yaml_path.write_text(yaml_content)

        configs = load_yaml_file(yaml_path, temp_cache)

        assert len(configs) == 2
        assert "dataset_one" in configs
        assert "dataset_two" in configs
        assert configs["dataset_one"].date_col == "Date"
        assert configs["dataset_two"].date_col == "TradeDate"

    def test_invalid_yaml_raises(self, temp_cache: Path) -> None:
        """Invalid YAML syntax should raise ConfigurationError."""
        yaml_path = temp_cache / "yaml" / "invalid.yaml"
        yaml_path.write_text("datasets:\n  bad: [unmatched")

        with pytest.raises(ConfigurationError, match="Invalid YAML"):
            load_yaml_file(yaml_path, temp_cache)

    def test_missing_file_raises(self, temp_cache: Path) -> None:
        """Missing YAML file should raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="not found"):
            load_yaml_file(temp_cache / "yaml" / "missing.yaml", temp_cache)


class TestLoadAllConfigs:
    """Tests for load_all_configs()."""

    def test_load_all_from_yaml_dir(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should load all configs from yaml/ directory."""
        configs = load_all_configs(temp_cache)

        assert "md.test_dataset" in configs

    def test_empty_yaml_dir(self, temp_cache: Path) -> None:
        """Empty yaml/ directory should return empty dict."""
        configs = load_all_configs(temp_cache)

        assert configs == {}

    def test_no_yaml_dir(self, tmp_path: Path) -> None:
        """Missing yaml/ directory should return empty dict."""
        cache = tmp_path / "cache"
        cache.mkdir()

        configs = load_all_configs(cache)

        assert configs == {}

    def test_duplicate_name_raises(self, temp_cache: Path) -> None:
        """Duplicate dataset names should raise ConfigurationError."""
        yaml1 = temp_cache / "yaml" / "file1.yaml"
        yaml1.write_text("datasets:\n  same_name:\n    date_col: Date")

        yaml2 = temp_cache / "yaml" / "file2.yaml"
        yaml2.write_text("datasets:\n  same_name:\n    date_col: Date")

        with pytest.raises(ConfigurationError, match="Duplicate"):
            load_all_configs(temp_cache)


class TestGetDatasetConfig:
    """Tests for get_dataset_config()."""

    def test_get_existing_dataset(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should return config for existing dataset."""
        config = get_dataset_config(temp_cache, "md.test_dataset")

        assert config.name == "md.test_dataset"

    def test_missing_dataset_raises(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Missing dataset should raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="not found"):
            get_dataset_config(temp_cache, "nonexistent")


class TestDatasetConfig:
    """Tests for DatasetConfig class."""

    def test_to_dpd(self, temp_cache: Path, sample_yaml: Path) -> None:
        """Should create a DPD from config."""
        config = get_dataset_config(temp_cache, "md.test_dataset")
        dpd = config.to_dpd()

        assert isinstance(dpd, DatedParquetDataset)
        assert dpd.name == "md.test_dataset"
        assert dpd.date_col == "Date"
        assert dpd.date_partition == "month"

    def test_load_source_class(
        self, temp_cache: Path, sample_yaml: Path, sample_source_file: Path
    ) -> None:
        """Should load DataSource class dynamically."""
        config = get_dataset_config(temp_cache, "md.test_dataset")
        source_class = config.load_source_class()

        assert issubclass(source_class, DataSource)
        assert source_class.__name__ == "TestSource"

    def test_create_source(
        self, temp_cache: Path, sample_yaml: Path, sample_source_file: Path
    ) -> None:
        """Should create a DataSource instance."""
        config = get_dataset_config(temp_cache, "md.test_dataset")
        dpd = config.to_dpd()
        source = config.create_source(dpd)

        assert isinstance(source, DataSource)
        assert source.api_key == "test123"
        assert source.timeout == 30

    def test_load_cleaner_class(
        self, temp_cache: Path, sample_cleaner_file: Path
    ) -> None:
        """Should load DataCleaner class dynamically."""
        yaml_content = """
datasets:
    cleaned_data:
        date_col: Date
        source_class_name: SomeSource
        cleaning_class_location: code/test_cleaner.py
        cleaning_class_name: TestCleaner
"""
        (temp_cache / "yaml" / "cleaned.yaml").write_text(yaml_content)

        config = get_dataset_config(temp_cache, "cleaned_data")
        cleaner_class = config.load_cleaner_class()

        assert cleaner_class is not None
        assert issubclass(cleaner_class, DataCleaner)

    def test_create_cleaner(
        self, temp_cache: Path, sample_cleaner_file: Path
    ) -> None:
        """Should create a DataCleaner instance."""
        yaml_content = """
datasets:
    cleaned_data:
        date_col: Date
        source_class_name: SomeSource
        cleaning_class_location: code/test_cleaner.py
        cleaning_class_name: TestCleaner
        cleaning_init_args:
            min_price: 1.5
"""
        (temp_cache / "yaml" / "cleaned.yaml").write_text(yaml_content)

        config = get_dataset_config(temp_cache, "cleaned_data")
        dpd = config.to_dpd()
        cleaner = config.create_cleaner(dpd)

        assert cleaner is not None
        assert isinstance(cleaner, DataCleaner)
        assert cleaner.min_price == 1.5

    def test_no_cleaner_returns_none(
        self, temp_cache: Path, sample_yaml: Path
    ) -> None:
        """Should return None when no cleaner configured."""
        config = get_dataset_config(temp_cache, "md.test_dataset")

        assert config.load_cleaner_class() is None
        assert config.create_cleaner(config.to_dpd()) is None

    def test_missing_source_file_raises(self, temp_cache: Path) -> None:
        """Missing source file should raise ConfigurationError."""
        yaml_content = """
datasets:
    bad_source:
        source_location: code/nonexistent.py
        source_class_name: MissingSource
"""
        (temp_cache / "yaml" / "bad.yaml").write_text(yaml_content)

        config = get_dataset_config(temp_cache, "bad_source")

        with pytest.raises(ConfigurationError, match="not found"):
            config.load_source_class()

    def test_has_yaml_transforms(self, temp_cache: Path) -> None:
        """Should detect if YAML transforms are configured."""
        yaml_content = """
datasets:
    with_transforms:
        columns_to_drop:
            - unwanted_col

    without_transforms:
        date_col: Date
"""
        (temp_cache / "yaml" / "transforms.yaml").write_text(yaml_content)

        configs = load_all_configs(temp_cache)

        assert configs["with_transforms"].has_yaml_transforms() is True
        assert configs["without_transforms"].has_yaml_transforms() is False


class TestApplyYamlTransforms:
    """Tests for apply_yaml_transforms()."""

    def test_rename_columns(self, temp_cache: Path) -> None:
        """Should rename columns."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            columns_to_rename={"old_name": "new_name"},
        )

        df = pd.DataFrame({"old_name": [1, 2, 3], "other": ["a", "b", "c"]})
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert "new_name" in result_df.columns
        assert "old_name" not in result_df.columns
        assert "other" in result_df.columns

    def test_drop_columns(self, temp_cache: Path) -> None:
        """Should drop specified columns."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            columns_to_drop=["drop_me", "also_drop"],
        )

        df = pd.DataFrame(
            {
                "keep": [1, 2, 3],
                "drop_me": [4, 5, 6],
                "also_drop": [7, 8, 9],
            }
        )
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert "keep" in result_df.columns
        assert "drop_me" not in result_df.columns
        assert "also_drop" not in result_df.columns

    def test_dropna_columns(self, temp_cache: Path) -> None:
        """Should drop rows with nulls in specified columns."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            dropna_columns=["required_col"],
        )

        df = pd.DataFrame(
            {
                "required_col": [1, None, 3, None],
                "other": ["a", "b", "c", "d"],
            }
        )
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert len(result_df) == 2
        assert result_df["required_col"].tolist() == [1.0, 3.0]

    def test_dedup_columns_keep_last(self, temp_cache: Path) -> None:
        """Should deduplicate keeping last occurrence."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            dedup_columns=["key"],
            dedup_keep="last",
        )

        df = pd.DataFrame(
            {
                "key": ["a", "b", "a", "b"],
                "value": [1, 2, 3, 4],
            }
        )
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert len(result_df) == 2

    def test_dedup_columns_keep_first(self, temp_cache: Path) -> None:
        """Should deduplicate keeping first occurrence."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            dedup_columns=["key"],
            dedup_keep="first",
        )

        df = pd.DataFrame(
            {
                "key": ["a", "b", "a", "b"],
                "value": [1, 2, 3, 4],
            }
        )
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert len(result_df) == 2

    def test_no_transforms(self, temp_cache: Path) -> None:
        """Should pass through unchanged when no transforms."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
        )

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert list(result_df.columns) == ["a", "b"]
        assert len(result_df) == 3

    def test_combined_transforms(self, temp_cache: Path) -> None:
        """Should apply multiple transforms in order."""

        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            columns_to_rename={"old_col": "new_col"},
            columns_to_drop=["unwanted"],
            dropna_columns=["new_col"],
            dedup_columns=["key"],
        )

        df = pd.DataFrame(
            {
                "key": ["a", "a", "b"],
                "old_col": [1, None, 3],
                "unwanted": [4, 5, 6],
            }
        )
        rel = duckdb.from_df(df)

        result = apply_yaml_transforms(rel, config)
        result_df = result.df()

        assert "new_col" in result_df.columns
        assert "old_col" not in result_df.columns
        assert "unwanted" not in result_df.columns
        assert result_df["new_col"].isna().sum() == 0


class TestTransformDefaults:
    """Tests for YAML transform defaults."""

    def test_default_values(self, temp_cache: Path) -> None:
        """DatasetConfig should have correct defaults."""
        config = DatasetConfig(name="test", cache_dir=temp_cache)

        assert config.date_col == "Date"
        assert config.date_partition == "day"
        assert config.partition_columns == []
        assert config.sort_columns is None
        assert config.repull_n_days == 0
        assert config.columns_to_drop == []
        assert config.columns_to_rename == {}
        assert config.dropna_columns == []
        assert config.dedup_columns == []
        assert config.dedup_keep == "last"


class TestInstalledModuleDataSource:
    """Tests for loading DataSource from installed packages via module://."""

    def test_load_from_module_builtin(self, temp_cache: Path) -> None:
        """Should load a DataSource from ionbus_parquet_cache builtin."""
        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            source_location="",
            source_class_name="HiveParquetSource",
        )
        cls = config.load_source_class()
        assert cls.__name__ == "HiveParquetSource"

    def test_load_from_module_installed(self, temp_cache: Path) -> None:
        """Should load a DataSource from an installed module path."""
        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            source_location="module://ionbus_parquet_cache.builtin_sources",
            source_class_name="HiveParquetSource",
        )
        cls = config.load_source_class()
        assert cls.__name__ == "HiveParquetSource"

    def test_module_source_not_found(self, temp_cache: Path) -> None:
        """Should raise ConfigurationError if module cannot be imported."""
        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            source_location="module://nonexistent_package.data_sources",
            source_class_name="SomeSource",
        )
        with pytest.raises(ConfigurationError) as exc_info:
            config.load_source_class()
        assert "Could not import module" in str(exc_info.value)

    def test_module_class_not_found(self, temp_cache: Path) -> None:
        """Should raise ConfigurationError if class not in module."""
        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            source_location="module://ionbus_parquet_cache.builtin_sources",
            source_class_name="NonExistentClass",
        )
        with pytest.raises(ConfigurationError) as exc_info:
            config.load_source_class()
        assert "not found in module" in str(exc_info.value)

    def test_module_class_wrong_base(self, temp_cache: Path) -> None:
        """Should raise ConfigurationError if class doesn't inherit DataSource."""
        config = DatasetConfig(
            name="test",
            cache_dir=temp_cache,
            source_location="module://pathlib",
            source_class_name="Path",
        )
        with pytest.raises(ConfigurationError) as exc_info:
            config.load_source_class()
        assert "must inherit from DataSource" in str(exc_info.value)

    def test_create_source_from_metadata_with_module(
        self, temp_cache: Path
    ) -> None:
        """Should load DataSource from metadata with module:// location."""
        import datetime as dt

        import pandas as pd
        import pyarrow as pa

        from ionbus_parquet_cache.dated_dataset import (
            DatedParquetDataset,
            FileMetadata,
        )

        # Create a DPD and publish a snapshot with module:// metadata
        dpd = DatedParquetDataset(
            name="test.dataset",
            cache_dir=temp_cache,
            date_col="date",
            partition_columns=[],
        )

        # Create minimal test data and write to parquet
        test_data = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "value": range(5),
            }
        )
        table = pa.Table.from_pandas(test_data)

        # Create parquet file
        parquet_dir = temp_cache / "data"
        parquet_dir.mkdir()
        parquet_path = parquet_dir / "test.parquet"
        pa.parquet.write_table(table, parquet_path)

        # Create metadata with module:// source_location
        file_metadata = FileMetadata.from_path(
            parquet_path, parquet_dir, partition_values={}
        )

        yaml_config_with_module = {
            "source_location": "module://ionbus_parquet_cache.builtin_sources",
            "source_class_name": "HiveParquetSource",
            "source_init_args": {"path": str(parquet_dir)},
        }

        # Publish snapshot with module:// metadata
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=table.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 5),
            partition_values={},
            yaml_config=yaml_config_with_module,
        )

        # Now test create_source_from_metadata - this exercises the actual
        # code path in dated_dataset.py, not just the helper
        source = dpd.create_source_from_metadata()

        # Verify the source was instantiated correctly from module://
        assert source.__class__.__name__ == "HiveParquetSource"
        # Verify it's from the right module
        assert (
            source.__class__.__module__
            == "ionbus_parquet_cache.builtin_sources"
        )
