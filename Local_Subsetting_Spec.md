# Local Subsetting Of Remote Datasets

Status: implemented for the initial DPD-only version.

This document describes the DPD-only command/API for creating a local,
repeatedly usable subset of a remote cache dataset without syncing the source
parquet files verbatim.

This spec extends the main parquet cache contract in
[Parquet_Cache_Spec.md](Parquet_Cache_Spec.md). The main spec remains
authoritative for DPD snapshot metadata, partition value behavior, provenance
sidecars, cache discovery, and sync semantics; this document specifies the
local-subset workflow layered on top of those rules.

The initial scope is DatedParquetDataset (DPD). NPD support can follow the same
high-level idea later, but DPD semantics are the hard part and should drive the
first design.

## Problem

A remote cache may contain a broad universe, such as all US equities, while a
local workflow may repeatedly need only a narrow subset, such as a fixed list of
ETFs. Reading the remote cache directly can work, especially when instrument
bucketing and predicate pushdown prune most files, but repeated reads still pay
remote metadata and object access costs.

The goal is to let a user specify the subset they want, read only the relevant
remote snapshot data, and publish the result into a normal local cache dataset.

## Non-Goals

- Do not create a special "non-updatable" cache type.
- Do not reuse remote parquet files or remote snapshot suffixes as local files.
- Do not make `sync-cache` responsible for row filtering.
- Do not support direct writes to remote caches.
- Do not implement incremental row-level merge semantics in the first version.

## Terminology

**Subset spec**: A YAML file describing one or more source datasets, destination
datasets, snapshot selection, and filters.

**Local subset**: A local cache dataset created from a subset spec. It is a
normal DPD snapshot with local files and local metadata.

**Source snapshot**: The remote DPD snapshot suffix that was read.

**Local snapshot**: The new local DPD snapshot suffix created by materializing
the subset. This must be generated locally and must not be the same identity as
the source snapshot.

## User Model

The user writes a subset spec:

```yaml
source_cache: gs://quiet-cache/prod
dest_cache: ~/cache/quiet-subsets

datasets:
  md.equities_daily:
    dest_name: md.etfs_daily
    source_snapshot: latest
    start_date: 2020-01-01
    instruments_file: etfs.txt
```

Then creates or updates the local subset:

```bash
python -m ionbus_parquet_cache.local_subset etfs.subset.yaml
```

Running the same command again later should read the latest matching remote
snapshot and publish a new local snapshot if the source snapshot or subset spec
changed.

## Output Dataset Semantics

For a DPD source, the destination should be a normal local DPD.

The destination DPD should preserve the source dataset's core layout unless the
spec explicitly supports an override in a later version:

- `date_col`
- `date_partition`
- `partition_columns`
- `instrument_column`
- `num_instrument_buckets`
- `sort_columns`
- `row_group_size`

Keeping the source layout makes read behavior, pruning, and future updates
predictable. Even if the subset is smaller than the source universe, changing
partitioning or bucketing should be a deliberate future feature, not an implicit
side effect of subsetting.

The subset spec should not expose a sort override in the first version. The
destination inherits `sort_columns` from the source snapshot metadata, and the
materializer is responsible for writing the local files in a way that preserves
the destination DPD's expected sort behavior.

The destination dataset may use a different dataset name. This should be the
standard operating pattern for most local subsets: keep the remote dataset's
"real" name for the complete source dataset and give the local subset an
explicit subset name, such as `md.etfs_daily` or
`md.equities_daily.subset.etfs`.

Using the source dataset name for the destination is allowed when the source and
destination caches are different. That can be useful when a local cache should
shadow the remote cache in registry lookup order and callers can still target
the remote cache explicitly by cache name. If the destination cache is the same
as the source cache, the destination name must be different from the source
name.

## Snapshot Identity

The local snapshot is not the remote snapshot. It is derived from the remote
snapshot.

Therefore:

- The local DPD must publish a new local snapshot suffix.
- Local metadata files and local parquet data files must use the local suffix.
- The source snapshot suffix must be stored in lineage/provenance, not encoded
  as the local snapshot identity.

Example:

```text
source:
  gs://quiet-cache/prod/md.equities_daily/_meta_data/md.equities_daily_1H8Q3AZ.pkl.gz

destination:
  ~/cache/quiet-subsets/md.etfs_daily/_meta_data/md.etfs_daily_1H8R02K.pkl.gz
  ~/cache/quiet-subsets/md.etfs_daily/date=2026-05-11/part_1H8R02K.parquet
```

`1H8R02K` is the local subset snapshot. `1H8Q3AZ` is recorded as the source
snapshot.

## Update Behavior

Re-running materialization should be allowed. It means:

1. Resolve the source snapshot (`latest` by default, or an explicit suffix).
2. Load the previous local subset snapshot, if any.
3. Compare the previous source snapshot and subset spec hash.
4. If neither changed, exit successfully without publishing a new snapshot
   unless `--force` is provided.
5. If either changed, read the selected source rows and publish a new local DPD
   snapshot.

The first implementation should use full rematerialization. It should not try to
append only newly available dates or merge individual partitions. Full
rematerialization is simpler, safer, and easier to explain. Incremental
materialization can be added later if the use case requires it.

By default, a spec processes all dataset entries. Use `--dataset NAME` to run
only one entry.

## Effective Spec Hash

The spec hash must be content-based. It must not rely on file modification times
or on the literal `instruments_file` path alone.

For each dataset, the materializer should build an effective spec before hashing:

1. Merge top-level defaults into the dataset entry.
2. Resolve relative paths against the subset spec file directory.
3. Resolve `source_snapshot: latest` to the concrete source snapshot suffix.
4. Resolve `instruments_file` to a normalized instrument list.
5. Canonicalize the result as JSON with sorted keys.
6. Hash that canonical representation, for example with SHA-256.

Instrument file normalization should:

- read the file contents;
- strip whitespace;
- ignore empty lines;
- ignore full-line `#` comments;
- reject an empty list;
- de-duplicate values;
- sort values so harmless file ordering changes do not force a new local
  snapshot.

The provenance sidecar should store the resulting spec hash. It may also store a
separate hash of the normalized instrument list for easier debugging.

## Filtering

The MVP should support filters that already map cleanly to the current read API:

- date range: `start_date`, `end_date`
- instrument list: `instruments`, `instruments_file`
- additional PyArrow-compatible filter tuples
- optional column projection

Example:

```yaml
source_cache: gs://quiet-cache/prod
dest_cache: ~/cache/quiet-subsets

datasets:
  md.equities_daily:
    dest_name: md.etfs_daily
    start_date: 2020-01-01
    end_date: 2026-05-11
    instruments_file: etfs.txt
    filters:
      - [currency, "==", USD]
    columns:
      - date
      - instrument_id
      - close
      - volume
```

If `instruments` or `instruments_file` is provided, the source DPD must have
`instrument_column` configured. For non-bucketed source datasets, the
materializer should still apply a row-level filter on `instrument_column`. For
bucketed source datasets, it should use the existing DPD read filter behavior so
the source read can prune `__instrument_bucket__` partitions before row-level
filtering.

### Column Projection

If `columns` is omitted, materialization preserves the full source schema after
filtering.

If `columns` is supplied, it must include every column required by the inherited
destination layout:

- `date_col`
- `partition_columns`
- `instrument_column`, if configured
- `sort_columns`

If any required layout column is missing, the command should fail with a clear
validation error. The first version should not silently add omitted layout
columns, because that makes the output schema surprising.

Required partition columns are structural. They must be available while
materializing the subset so the destination DPD can compute its partition
directories, but partition columns may be omitted from the physical parquet
files according to the normal DPD layout rules.

## Subset Spec Shape

Proposed top-level fields:

```yaml
source_cache: gs://quiet-cache/prod          # required
dest_cache: ~/cache/quiet-subsets           # required, local only
defaults:                                   # optional
  source_snapshot: latest

datasets:
  source.dataset.name:
    dest_name: dest.dataset.name            # required
    source_snapshot: latest                 # optional; latest or explicit suffix
    start_date: 2020-01-01                  # optional
    end_date: 2026-05-11                    # optional
    instruments: [ETF1, ETF2]               # optional
    instruments_file: etfs.txt              # optional, one instrument per line
    filters: []                             # optional list of filter tuples
    columns: []                             # optional projection
    notes: "Subset of US ETFs."             # optional snapshot info override
    annotations: {}                         # optional snapshot info override
    column_descriptions: {}                 # optional snapshot info override
```

Rules:

- `dest_cache` must be local.
- `source_cache` may be local or remote.
- `source_snapshot` defaults to `latest`.
- `latest` is resolved once per dataset at materialization time.
- `instruments` and `instruments_file` are mutually exclusive for a dataset.
- Empty lines and `#` comments in `instruments_file` are ignored.
- Unknown keys are errors.

## Publishing Algorithm

For each dataset entry:

1. Register/open the source cache.
2. Resolve the source DPD and source snapshot suffix.
3. Resolve and validate the destination DPD configuration from the source
   snapshot metadata.
4. Read the source snapshot with date, instrument, filter, and column
   projection.
5. Write the filtered table to destination temporary files using the normal DPD
   partitioning rules and a newly generated local suffix.
6. Publish local DPD metadata with the local suffix.
7. Attach lineage/provenance that records the source and subset spec.
8. Clean up temporary files on failure.

The command should never publish a destination snapshot until all destination
data files for that snapshot have been written successfully.

If a dataset entry matches zero rows, the command should fail that entry without
publishing a destination snapshot.

Publishing is atomic per dataset, not across the whole spec. If dataset A
publishes successfully and dataset B fails, dataset A remains published, dataset
B publishes nothing, and the command exits nonzero with per-dataset status.
There is no cross-dataset rollback.

## Lineage And Provenance

The local DPD snapshot should record derivation from the remote source snapshot.

Lineage should use the existing DPD lineage model where possible:

```python
SnapshotLineage(
    base_snapshot="<source_snapshot_suffix>",
    first_snapshot_id="<first_local_subset_snapshot_or_current>",
    operation="local_subset",
    requested_date_range=...,
    added_date_ranges=[...],
    rewritten_date_ranges=[...],
    instrument_column="instrument_id",
    instrument_scope="subset",
    instruments=[...],  # only if reasonably small
)
```

`local_subset` is a valid DPD lineage `operation` string. Any code that
reads lineage should treat operation as an open string and handle unknown values
without failing.

Because the existing lineage model only stores a snapshot suffix for
`base_snapshot`, richer source identity should be stored in a provenance sidecar:

```yaml
kind: local_subset
source_cache: gs://quiet-cache/prod
source_dataset: md.equities_daily
source_snapshot: 1H8Q3AZ
dest_cache: ~/cache/quiet-subsets
dest_dataset: md.etfs_daily
subset_spec_hash: sha256:...
subset_spec_path: etfs.subset.yaml
materialized_at: 2026-05-11T14:03:00Z
filters:
  start_date: 2020-01-01
  instruments_file: etfs.txt
```

The subset spec hash should be computed from a canonicalized version of the
effective dataset spec, including resolved file contents for instrument lists.

## Snapshot Info

The destination snapshot should resolve snapshot info in this order:

1. Start from the previous local destination snapshot info, if one exists.
2. Fill missing fields from the source snapshot info.
3. Apply dataset-level fields from the subset spec.

This keeps local destination history stable while still picking up source
descriptions on first materialization.

The snapshot info fields are:

- notes
- annotations
- column descriptions

Dataset-level fields in the subset spec should use the existing snapshot-info
validation rules:

- `notes` may replace the previous string, including with `""`.
- `annotations` may add keys, but may not remove keys or change existing values.
- `column_descriptions` may add keys or change text, but may not remove keys.

Provenance should not be carried forward implicitly; materialization should
always write explicit provenance for the new local snapshot.

## CLI

Proposed command:

```bash
python -m ionbus_parquet_cache.local_subset SPEC.yaml [options]
```

Options:

- `--dataset NAME`: process only one dataset entry from the spec.
- `--source-snapshot SUFFIX`: override all `source_snapshot` entries.
- `--force`: publish even if source snapshot and effective spec hash match the
  latest local subset.
- `--dry-run`: validate the spec, resolve source snapshots, and report planned
  actions without writing files.
- `--count`: compute row counts during dry-run. This may scan remote data.
- `--verbose`: print resolved source snapshots, row counts after materialization,
  and output paths.

Dry-run should be cheap by default. It should not read all matching rows just to
produce counts. It should report:

- source dataset;
- destination dataset;
- destination cache/path;
- resolved source snapshot;
- latest local subset source snapshot, if any;
- effective spec hash;
- action: `publish`, `skip`, or `error`;
- validation errors, if any.

Dry-run should not report a planned destination snapshot suffix unless the
output clearly labels it as illustrative. The real local suffix is generated
only when publishing.

## Python API

Proposed API:

```python
from ionbus_parquet_cache import local_subset

result = local_subset("etfs.subset.yaml")
```

The returned result should include one entry per dataset:

```python
{
    "source_dataset": "md.equities_daily",
    "dest_dataset": "md.etfs_daily",
    "source_snapshot": "1H8Q3AZ",
    "dest_snapshot": "1H8R02K",
    "rows": 123456,
    "status": "published",
    "spec_hash": "sha256:...",
}
```

## Performance Notes

This feature avoids syncing entire source files, but it is still a read and
rewrite operation. It is fast when the source layout supports pruning:

- date partitions prune the requested date range;
- instrument bucketing prunes the relevant `__instrument_bucket__` directories;
- row group statistics and column projection reduce bytes read.

If the source cache is not partitioned or bucketed by the subset criteria,
materialization may still scan a large portion of the remote dataset. That is
expected and should be documented in command output.

## Failure And Safety

- Destination writes must be local-only.
- The command must not mutate the source cache.
- The command must use temporary destination files and publish metadata last.
- If publishing fails, temporary data files and sidecars should be removed best
  effort.
- A successfully published dataset is not rolled back if a later dataset in the
  same spec fails.
- If a local snapshot suffix collision occurs, fail with the same guidance used
  by normal DPD publishing.
- Partial output for one dataset should not imply success for another; multi
  dataset materialization should report per-dataset status.

## Open Questions

1. Should destination partitioning overrides be rejected initially, or allowed
   for advanced users?
2. Should large instrument lists be stored in provenance as full lists, hashes,
   or both?
3. Should a subset spec be stored inside the destination cache for easy reruns,
   or should the command only record the spec hash and path?
4. Should `--count` be implemented in the first version, or deferred until the
   base materialization path is working?
