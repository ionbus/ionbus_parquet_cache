"""Tests for the canonical DPD configuration contract."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ionbus_parquet_cache.builtin_sources import DPDSource
from ionbus_parquet_cache.cache_registry import CacheRegistry, DatasetType
from ionbus_parquet_cache.cleanup_cache import cleanup_cache_main
from ionbus_parquet_cache.dated_dataset import (
    DPD_CONFIG_EXCLUDED_FIELDS,
    DPD_CONFIG_FIELDS,
    DatedParquetDataset,
    FileMetadata,
    dpd_config_from_dataset,
    dpd_from_config,
    dpd_from_metadata_config,
)
from ionbus_parquet_cache.exceptions import (
    ConfigurationError,
    ValidationError,
)
from ionbus_parquet_cache.local_subset import _new_destination_dpd
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.update_datasets import _discover_datasets_from_disk
from ionbus_parquet_cache.yaml_config import DatasetConfig, load_yaml_file


def _rich_dpd(cache_dir: Path, *, name: str = "rich") -> DatedParquetDataset:
    """Return a DPD with non-default values for every DPD config field."""
    return DatedParquetDataset(
        cache_dir=cache_dir,
        name=name,
        date_col="trade_date",
        date_partition="month",
        partition_columns=["venue"],
        sort_columns=["symbol", "trade_date"],
        description="Rich DPD config.",
        start_date_str="2020-01-01",
        end_date_str="2024-12-31",
        repull_n_days=7,
        instrument_column="symbol",
        instruments=["AAPL", "MSFT"],
        num_instrument_buckets=8,
        lock_dir=cache_dir / "mutable-locks",
        use_update_lock=False,
        row_group_size=12345,
    )


def _rich_dataset_config(cache_dir: Path) -> DatasetConfig:
    """Return a DatasetConfig with the same DPD values plus orchestration."""
    return DatasetConfig(
        name="rich",
        cache_dir=cache_dir,
        date_col="trade_date",
        date_partition="month",
        partition_columns=["venue"],
        sort_columns=["symbol", "trade_date"],
        description="Rich DPD config.",
        start_date_str="2020-01-01",
        end_date_str="2024-12-31",
        repull_n_days=7,
        instrument_column="symbol",
        instruments=["AAPL", "MSFT"],
        num_instrument_buckets=8,
        lock_dir=cache_dir / "mutable-locks",
        use_update_lock=False,
        row_group_size=12345,
        source_location="code/source.py",
        source_class_name="Source",
        source_init_args={"token": "redacted"},
        columns_to_drop=["drop_me"],
        cleaning_class_location="code/cleaner.py",
        cleaning_class_name="Cleaner",
        cleaning_init_args={"floor": 1},
    )


def _publish_config_snapshot(
    dpd: DatedParquetDataset,
    *,
    suffix: str = "1AAAAAA",
) -> None:
    """Publish metadata-only snapshot enough for reconstruction tests."""
    dpd._publish_snapshot(
        files=[],
        schema=pa.schema(
            [
                (dpd.date_col, pa.date32()),
                ("symbol", pa.string()),
            ]
        ),
        cache_start_date=dt.date(2024, 1, 1),
        cache_end_date=dt.date(2024, 1, 31),
        partition_values={},
        yaml_config=dpd._default_yaml_config(),
        suffix=suffix,
    )


def _assert_dpd_config_equal(
    actual: DatedParquetDataset,
    expected: DatedParquetDataset,
) -> None:
    """Assert all canonical DPD config fields match."""
    assert dpd_config_from_dataset(actual) == dpd_config_from_dataset(
        expected
    )


def test_dpd_model_fields_are_classified() -> None:
    """Every DPD model field must be classified exactly once."""
    model_fields = set(DatedParquetDataset.model_fields)

    assert DPD_CONFIG_FIELDS
    assert DPD_CONFIG_FIELDS.isdisjoint(DPD_CONFIG_EXCLUDED_FIELDS)
    assert DPD_CONFIG_FIELDS | DPD_CONFIG_EXCLUDED_FIELDS == model_fields


def test_dataset_config_round_trips_all_dpd_fields(tmp_path: Path) -> None:
    """DatasetConfig should use the canonical DPD field contract."""
    cache_dir = tmp_path / "cache"
    config = _rich_dataset_config(cache_dir)

    dpd = config.to_dpd()
    yaml_config = config.to_yaml_config()

    assert {
        field: yaml_config[field] for field in DPD_CONFIG_FIELDS
    } == dpd_config_from_dataset(dpd)
    assert yaml_config["source_class_name"] == "Source"
    assert yaml_config["source_init_args"] == {"token": "redacted"}
    assert yaml_config["columns_to_drop"] == ["drop_me"]
    assert yaml_config["cleaning_class_name"] == "Cleaner"


def test_default_yaml_config_uses_canonical_dpd_fields(
    tmp_path: Path,
) -> None:
    """Direct DPD metadata capture should use the same DPD config helper."""
    dpd = _rich_dpd(tmp_path / "cache")

    assert dpd._default_yaml_config() == dpd_config_from_dataset(dpd)


def test_dpd_from_config_reconstructs_runtime_metadata_config(
    tmp_path: Path,
) -> None:
    """Runtime metadata config with injected bucket columns should rebuild."""
    original = _rich_dpd(tmp_path / "cache")
    config = dpd_config_from_dataset(original)

    with pytest.raises(ValidationError, match="reserved"):
        dpd_from_config(tmp_path / "cache", "rich", config)

    rebuilt = dpd_from_metadata_config(tmp_path / "cache", "rich", config)

    _assert_dpd_config_equal(rebuilt, original)


def test_yaml_config_rejects_reserved_runtime_bucket_column(
    tmp_path: Path,
) -> None:
    """User-authored YAML must not smuggle in runtime bucket columns."""
    cache_dir = tmp_path / "cache"
    yaml_path = cache_dir / "reserved_bucket.yaml"
    yaml_path.parent.mkdir(parents=True)
    yaml_path.write_text(
        """
datasets:
    bad:
        source_class_name: Source
        instrument_column: symbol
        num_instrument_buckets: 4
        partition_columns: [__instrument_bucket__]
""",
        encoding="utf-8",
    )

    config = load_yaml_file(yaml_path, cache_dir)["bad"]
    with pytest.raises(ValidationError, match="reserved"):
        config.to_dpd()


def test_relative_lock_dir_round_trips_as_portable_config(
    tmp_path: Path,
) -> None:
    """Relative lock dirs should remain relative in captured config."""
    cache_dir = tmp_path / "cache"
    yaml_path = cache_dir / "locks.yaml"
    yaml_path.parent.mkdir(parents=True)
    yaml_path.write_text(
        """
datasets:
    locky:
        source_class_name: Source
        lock_dir: mutable-locks
""",
        encoding="utf-8",
    )

    config = load_yaml_file(yaml_path, cache_dir)["locky"]
    dpd = config.to_dpd()
    yaml_config = config.to_yaml_config()

    assert config.lock_dir == "mutable-locks"
    assert dpd.lock_dir == cache_dir / "mutable-locks"
    assert yaml_config["lock_dir"] == "mutable-locks"

    rebuilt = dpd_from_metadata_config(
        tmp_path / "other-cache",
        "locky",
        yaml_config,
    )
    assert rebuilt.lock_dir == tmp_path / "other-cache" / "mutable-locks"

    dpd._publish_snapshot(
        files=[],
        schema=pa.schema([(dpd.date_col, pa.date32())]),
        yaml_config=yaml_config,
        suffix="1AAAAAA",
    )
    assert dpd._metadata is not None
    assert dpd._metadata.yaml_config["lock_dir"] == "mutable-locks"


def test_absolute_lock_dir_round_trips_as_absolute_config(
    tmp_path: Path,
) -> None:
    """Explicit absolute lock dirs should remain absolute."""
    lock_dir = tmp_path / "absolute-locks"
    config = DatasetConfig(
        name="absolute",
        cache_dir=tmp_path / "cache",
        source_class_name="Source",
        lock_dir=lock_dir,
    )

    yaml_config = config.to_yaml_config()
    rebuilt = dpd_from_metadata_config(
        tmp_path / "other-cache",
        "absolute",
        yaml_config,
    )

    assert yaml_config["lock_dir"] == lock_dir
    assert rebuilt.lock_dir == lock_dir


def test_gcs_lock_dir_is_rejected_until_gcs_locks_are_supported(
    tmp_path: Path,
) -> None:
    """GCS lock dirs must not silently become local pathlib paths."""
    config = DatasetConfig(
        name="gcs_lock",
        cache_dir=tmp_path / "cache",
        source_class_name="Source",
        lock_dir="gs://bucket/locks",
    )

    with pytest.raises(ValidationError, match="GCS lock_dir"):
        config.to_dpd()


def test_update_cache_reconstructs_all_dpd_config_fields(
    tmp_path: Path,
) -> None:
    """update-cache discovery should preserve the canonical DPD config."""
    cache_dir = tmp_path / "cache"
    original = _rich_dpd(cache_dir, name="from_disk")
    _publish_config_snapshot(original)

    datasets = _discover_datasets_from_disk(cache_dir)

    assert "from_disk" in datasets
    _assert_dpd_config_equal(datasets["from_disk"], original)


def test_cache_registry_reconstructs_all_dpd_config_fields(
    tmp_path: Path,
) -> None:
    """CacheRegistry should preserve the canonical DPD config."""
    cache_dir = tmp_path / "cache"
    original = _rich_dpd(cache_dir, name="registry")
    _publish_config_snapshot(original)

    CacheRegistry.reset()
    try:
        registry = CacheRegistry.instance(test=cache_dir)
        loaded = registry.get_dataset(
            "registry",
            cache_name="test",
            dataset_type=DatasetType.DATED,
        )
    finally:
        CacheRegistry.reset()

    assert isinstance(loaded, DatedParquetDataset)
    _assert_dpd_config_equal(loaded, original)


def test_dpd_source_metadata_fallback_reconstructs_all_dpd_config_fields(
    tmp_path: Path,
) -> None:
    """DPDSource metadata fallback should use the canonical DPD config."""
    cache_dir = tmp_path / "cache"
    original = _rich_dpd(cache_dir, name="source")
    _publish_config_snapshot(original)
    target = DatedParquetDataset(
        cache_dir=tmp_path / "target",
        name="target",
        date_col="other_date",
    )

    source = DPDSource(
        target,
        dpd_name="source",
        dpd_cache_path=str(cache_dir),
    )
    loaded = source._get_source_dpd()

    _assert_dpd_config_equal(loaded, original)


def test_local_subset_destination_uses_canonical_layout_fields(
    tmp_path: Path,
) -> None:
    """Local subset destinations should not hand-copy layout fields."""
    source = _rich_dpd(tmp_path / "source", name="source")

    dest = _new_destination_dpd(tmp_path / "dest", "dest", source)

    assert dest.date_col == source.date_col
    assert dest.date_partition == source.date_partition
    assert dest.partition_columns == source.partition_columns
    assert dest.sort_columns == source.sort_columns
    assert dest.description == source.description
    assert dest.start_date_str is None
    assert dest.end_date_str is None
    assert dest.repull_n_days == 0
    assert dest.instrument_column == source.instrument_column
    assert dest.instruments is None
    assert dest.num_instrument_buckets == source.num_instrument_buckets
    assert dest.row_group_size == source.row_group_size
    assert dest.use_update_lock is False
    assert dest.lock_dir is None


def test_cleanup_trim_reconstructs_bucketed_dpd_from_metadata(
    tmp_path: Path,
) -> None:
    """cleanup-cache trim should not reopen DPDs with default config."""
    cache_dir = tmp_path / "cache"
    dpd = DatedParquetDataset(
        cache_dir=cache_dir,
        name="bucketed",
        date_col="trade_date",
        date_partition="month",
        partition_columns=[],
        instrument_column="symbol",
        num_instrument_buckets=4,
    )
    table = pa.table(
        {
            "trade_date": pa.array([dt.date(2024, 1, 2)]),
            "symbol": pa.array(["AAPL"]),
        }
    )
    parquet_path = (
        Path(dpd.dataset_dir)
        / "__instrument_bucket__=0"
        / "month=M2024-01"
        / "data_1AAAAAA.parquet"
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, parquet_path)
    file_metadata = FileMetadata.from_path(
        parquet_path,
        Path(dpd.dataset_dir),
        {
            "__instrument_bucket__": "0",
            "month": "M2024-01",
        },
    )
    dpd._publish_snapshot(
        files=[file_metadata],
        schema=table.schema,
        cache_start_date=dt.date(2024, 1, 2),
        cache_end_date=dt.date(2024, 1, 2),
        partition_values={
            "__instrument_bucket__": ["0"],
            "month": ["M2024-01"],
        },
        suffix="1AAAAAA",
    )

    result = cleanup_cache_main(
        [str(cache_dir), "--before-date", "2024-02-01"]
    )

    assert result == 0
    assert not parquet_path.exists()
    assert (parquet_path.parent / "data_1AAAAAA_trimmed.parquet").exists()


def test_unknown_yaml_dataset_keys_raise(tmp_path: Path) -> None:
    """Unknown YAML dataset keys should not be silently ignored."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    yaml_path = cache_dir / "bad.yaml"
    yaml_path.write_text(
        """
datasets:
    bad:
        source_class_name: Source
        date_col_typo: Date
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="date_col_typo"):
        load_yaml_file(yaml_path, cache_dir)


def test_npd_snapshot_info_rejects_dpd_config_fields(tmp_path: Path) -> None:
    """NPD snapshot info remains separate from DPD config."""
    npd = NonDatedParquetDataset(cache_dir=tmp_path / "cache", name="npd")

    with pytest.raises(ValidationError, match="date_col"):
        npd._validate_snapshot_info_keys(
            {"date_col": "trade_date"},
            "NPD info",
        )
