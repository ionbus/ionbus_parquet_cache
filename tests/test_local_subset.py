"""Tests for local DPD subsetting."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa

from ionbus_parquet_cache.cache_registry import CacheRegistry, DatasetType
from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
)
from ionbus_parquet_cache.local_subset import local_subset, local_subset_main
from ionbus_parquet_cache.partition import PartitionSpec
from ionbus_parquet_cache.update_pipeline import build_update_plan


def _sample_rows() -> pd.DataFrame:
    """Return source rows spanning instruments, dates, and currencies."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-02",
                    "2024-01-03",
                    "2024-02-01",
                    "2024-02-02",
                ]
            ),
            "instrument_id": ["ETF1", "ETF2", "ETF1", "ETF3"],
            "close": [10.0, 20.0, 11.0, 30.0],
            "currency": ["USD", "USD", "USD", "EUR"],
        }
    )


def _publish_source_snapshot(
    cache_dir: Path,
    *,
    name: str = "md.equities_daily",
    suffix: str = "1AAAAAA",
    bucketed: bool = False,
    notes: str | None = "Source notes.",
) -> DatedParquetDataset:
    """Publish a small DPD source snapshot with real parquet files."""
    dpd = DatedParquetDataset(
        cache_dir=cache_dir,
        name=name,
        date_col="date",
        date_partition="month",
        partition_columns=[],
        sort_columns=["instrument_id", "date"],
        instrument_column="instrument_id",
        num_instrument_buckets=4 if bucketed else None,
    )
    df = _sample_rows()
    working = df.copy()
    working["month"] = working["date"].dt.strftime("M%Y-%m")
    if bucketed:
        from ionbus_parquet_cache.bucketing import instrument_bucket

        working["__instrument_bucket__"] = [
            instrument_bucket(value, dpd.num_instrument_buckets)
            for value in working["instrument_id"]
        ]

    specs = []
    frames_by_key = {}
    group_cols = list(dpd.partition_columns)
    grouped = working.groupby(group_cols, sort=True, dropna=False)
    for key_values, group in grouped:
        if not isinstance(key_values, tuple):
            key_values = (key_values,)
        partition_values = dict(zip(group_cols, key_values))
        spec = PartitionSpec(
            partition_values=partition_values,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 2, 29),
        )
        specs.append(spec)
        frames_by_key[spec.partition_key] = df.loc[group.index]

    temp_dir = Path(dpd.dataset_dir) / "_tmp_source"
    plan = build_update_plan(dpd, specs, temp_dir, suffix=suffix)
    files = []
    for spec in specs:
        group = plan.groups[spec.partition_key]
        assert group.final_path is not None
        group.final_path.parent.mkdir(parents=True, exist_ok=True)
        frames_by_key[spec.partition_key].to_parquet(
            group.final_path,
            index=False,
        )
        files.append(
            FileMetadata.from_path(
                group.final_path,
                Path(dpd.dataset_dir),
                dict(spec.partition_values),
            )
        )

    yaml_config = dpd._default_yaml_config()
    if notes is not None:
        yaml_config["notes"] = notes
    yaml_config["annotations"] = {"source": "unit-test"}
    yaml_config["column_descriptions"] = {
        "instrument_id": "Quiet symbology_v2 listing-level UUID.",
    }
    dpd._publish_snapshot(
        files=files,
        schema=pa.Table.from_pandas(df, preserve_index=False).schema,
        cache_start_date=dt.date(2024, 1, 2),
        cache_end_date=dt.date(2024, 2, 2),
        partition_values={
            col: sorted({file.partition_values[col] for file in files})
            for col in group_cols
        },
        yaml_config=yaml_config,
        suffix=suffix,
    )
    return dpd


def _write_spec(
    path: Path,
    source_cache: Path,
    dest_cache: Path,
    body: str,
) -> Path:
    """Write a local subset spec."""
    path.write_text(
        f"""
source_cache: {source_cache}
dest_cache: {dest_cache}

datasets:
{body}
""",
        encoding="utf-8",
    )
    return path


def _get_dest_dpd(cache_dir: Path, name: str) -> DatedParquetDataset:
    """Load a destination DPD through CacheRegistry."""
    registry = CacheRegistry()
    registry.add_cache("dest", cache_dir)
    dpd = registry.get_dataset(name, "dest", DatasetType.DATED)
    assert isinstance(dpd, DatedParquetDataset)
    return dpd


def test_local_subset_publishes_filtered_dpd_snapshot(tmp_path: Path) -> None:
    """local_subset should publish a normal filtered local DPD snapshot."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    instruments = tmp_path / "etfs.txt"
    instruments.write_text("ETF1\nETF2\n", encoding="utf-8")
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    start_date: 2024-01-01
    end_date: 2024-01-31
    instruments_file: etfs.txt
    filters:
      - [currency, "==", USD]
    columns: [date, month, instrument_id, close]
    notes: Local ETF subset.
""",
    )

    results = local_subset(spec)

    assert len(results) == 1
    assert results[0].status == "published"
    assert results[0].rows == 2
    dest = _get_dest_dpd(dest_cache, "md.etfs_daily")
    actual = dest.read_data().sort_values("instrument_id").reset_index(drop=True)
    assert actual["instrument_id"].tolist() == ["ETF1", "ETF2"]
    assert actual["close"].tolist() == [10.0, 20.0]
    assert dest.get_notes() == "Local ETF subset."
    assert dest.get_annotations() == {"source": "unit-test"}
    assert dest.get_column_descriptions() == {
        "instrument_id": "Quiet symbology_v2 listing-level UUID."
    }
    provenance = dest.read_provenance()
    assert provenance["kind"] == "local_subset"
    assert provenance["dest_cache"] == str(dest_cache)
    assert provenance["source_snapshot"] == "1AAAAAA"
    assert provenance["subset_spec_hash"] == results[0].spec_hash
    assert dest.cache_history()[0].operation == "local_subset"


def test_local_subset_dry_run_writes_nothing_and_counts(
    tmp_path: Path,
) -> None:
    """Dry-run should report planned work without creating destination files."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    instruments: [ETF1]
""",
    )

    results = local_subset(spec, dry_run=True, count=True)

    assert results[0].status == "publish"
    assert results[0].rows == 2
    assert not (dest_cache / "md.etfs_daily").exists()


def test_instruments_file_contents_affect_spec_hash(tmp_path: Path) -> None:
    """Changing an instruments file in place should change the spec hash."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    instruments = tmp_path / "etfs.txt"
    instruments.write_text("ETF1\n", encoding="utf-8")
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    instruments_file: etfs.txt
""",
    )

    first = local_subset(spec, dry_run=True)[0].spec_hash
    instruments.write_text("ETF2\n", encoding="utf-8")
    second = local_subset(spec, dry_run=True)[0].spec_hash

    assert first != second


def test_projection_missing_required_column_errors(tmp_path: Path) -> None:
    """Projection must include inherited layout columns."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    columns: [date, close]
""",
    )

    result = local_subset(spec)[0]

    assert result.status == "error"
    assert "missing required layout column" in str(result.message)
    assert not (dest_cache / "md.etfs_daily").exists()


def test_local_subset_rejects_zero_row_output(tmp_path: Path) -> None:
    """A subset that matches no rows should not publish a snapshot."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.empty_daily
    instruments: [NOPE]
""",
    )

    result = local_subset(spec)[0]

    assert result.status == "error"
    assert "produced no rows" in str(result.message)
    assert not (dest_cache / "md.empty_daily").exists()


def test_second_run_skips_when_source_and_spec_match(tmp_path: Path) -> None:
    """Re-running the same subset should skip when provenance matches."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    instruments: [ETF1]
""",
    )

    first = local_subset(spec)[0]
    second = local_subset(spec)[0]

    assert first.status == "published"
    assert second.status == "skipped"
    assert second.dest_snapshot == first.dest_snapshot


def test_local_subset_cli_returns_nonzero_on_partial_failure(
    tmp_path: Path,
) -> None:
    """CLI should leave successful datasets published but return nonzero."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache)
    _publish_source_snapshot(
        source_cache,
        name="md.other_daily",
        suffix="1AAAAB0",
    )
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.etfs_daily
    instruments: [ETF1]
  md.other_daily:
    dest_name: md.bad_daily
    columns: [date, close]
""",
    )

    result = local_subset_main([str(spec)])

    assert result == 1
    assert (dest_cache / "md.etfs_daily" / "_meta_data").exists()
    assert not (dest_cache / "md.bad_daily").exists()


def test_bucketed_source_subset_writes_bucketed_destination(
    tmp_path: Path,
) -> None:
    """Bucketed source layout should be preserved at the destination."""
    source_cache = tmp_path / "source"
    dest_cache = tmp_path / "dest"
    source_cache.mkdir()
    _publish_source_snapshot(source_cache, bucketed=True)
    spec = _write_spec(
        tmp_path / "subset.yaml",
        source_cache,
        dest_cache,
        """
  md.equities_daily:
    dest_name: md.bucketed_etfs_daily
    instruments: [ETF1]
""",
    )

    result = local_subset(spec)[0]

    assert result.status == "published"
    dest = _get_dest_dpd(dest_cache, "md.bucketed_etfs_daily")
    assert dest.num_instrument_buckets == 4
    assert "__instrument_bucket__" in dest.partition_columns
    actual = dest.read_data()
    assert sorted(actual["instrument_id"].unique()) == ["ETF1"]
