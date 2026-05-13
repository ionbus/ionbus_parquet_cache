# DPD Config Round-Trip Refactor Plan

## Goal

Make DPD configuration flow through the system from one canonical contract,
instead of copying field lists by hand in multiple places. This prevents bugs
where a setting is accepted in YAML but later lost when reconstructing from
snapshot metadata.

## Current Status

This plan is for the core DPD config round-trip refactor. The immediate branch
may also contain adjacent orchestration cleanup, such as shared
DataSource/DataCleaner class-loading helpers. That cleanup is useful, but it is
not the same thing as the canonical DPD config contract and should not be
treated as completion of this plan.

## Non-Goals

- Do not redesign NPDs. They do not use the DPD YAML config path.
- Do not implement native GCS locking in this refactor.
- Do not pass arbitrary YAML keys into `DatedParquetDataset`.
- Do not silently accept unknown config keys.

## Phase 0: Immediate Fixes and Adjacent Cleanup

Before the broader refactor, it is acceptable to make small fixes for known
field wiring gaps, especially `use_update_lock` and `lock_dir`, so current
testing is not blocked. Phase 0 work should stay narrow:

- wire known missing YAML fields through the existing manual path
- add focused regression tests for those known misses
- update the spec to explain the DPD config round-trip contract
- keep DataSource/DataCleaner module-loading cleanup separate unless it
  directly affects DPD config round-tripping

Phase 0 is intentionally not the canonical contract. It reduces immediate risk
but does not remove the underlying hand-copy drift problem.

## Phase 1: Test-First Drift Exposure

The implementation should be changed test-first. The first step is to add
failing tests that expose the current field-copy drift, before changing the
production code. Those tests should cover:

- a DPD config-field classification invariant: every relevant
  `DatedParquetDataset` model field is either in the canonical DPD config
  field set or explicitly excluded
- YAML-to-DPD loading: non-default YAML values for every DPD-owned config field
  reach the constructed `DatedParquetDataset`
- DPD-to-metadata capture: non-default DPD config fields are stored in snapshot
  `yaml_config`
- metadata-to-update-cache reconstruction: `update-cache` reconstructs the DPD
  with the same DPD-owned config values stored in snapshot metadata
- metadata-to-registry reconstruction: `CacheRegistry` reconstructs enough DPD
  config to make reads and refresh behavior consistent
- DPDSource metadata fallback: if YAML is absent and metadata fallback is
  supported, source-DPD reconstruction preserves the layout fields required to
  read the source correctly
- NPD separation: NPD `--info-file` remains strict and rejects DPD-style config
  fields

No production refactor for DPD config round-tripping should start until these
tests exist and fail against the current drift. Existing focused tests for
Phase 0 regressions are useful, but they are not a substitute for these
contract-level drift tests.

## Phase 2: Canonical DPD Config Contract

After those tests exist, introduce a single canonical DPD config contract. The
contract should live in one place and expose helpers equivalent to:

```python
DPD_CONFIG_FIELDS = frozenset({...})

def dpd_config_from_mapping(config: dict) -> dict:
    ...

def dpd_config_from_dataset(dpd: DatedParquetDataset) -> dict:
    ...

def dpd_config_for_metadata(
    dpd: DatedParquetDataset,
    base_config: dict | None = None,
) -> dict:
    ...

def dpd_from_config(cache_dir, name, config) -> DatedParquetDataset:
    ...

def dpd_from_metadata_config(cache_dir, name, config) -> DatedParquetDataset:
    ...
```

The helpers must copy only DPD-owned fields. They must not pass the whole YAML
entry into `DatedParquetDataset`, because source, cleaner, transform,
sync-function, and watched snapshot-info fields are not DPD model fields.

`dpd_from_config` should be the strict user/YAML constructor. It should not
silently strip runtime-only fields from user-authored YAML. Trusted snapshot
metadata may contain runtime-normalized fields such as the injected
`__instrument_bucket__` partition column, so metadata reconstruction should use
a separately named helper that performs that cleanup explicitly.

Captured metadata should remain portable serialized config, not merely a dump
of runtime-normalized attributes. In particular, relative `lock_dir` values such
as `mutable-locks` should remain relative in metadata and be resolved against
the active cache root during reconstruction.

## Phase 3: Replacement Sites

Once the helper exists, replace the repeated hand-copied DPD constructor and
metadata field lists in these paths:

- `DatasetConfig.to_dpd`
- `DatasetConfig.to_yaml_config`
- `DatedParquetDataset._default_yaml_config`
- `update_datasets._discover_datasets_from_disk`
- `CacheRegistry._get_dpd`
- `DPDSource` metadata fallback in `builtin_sources.py`
- `local_subset._new_destination_dpd`; preserve source physical layout but
  clear source update-policy fields for the subset destination

## Out of Scope for the Core Refactor

These may be worthwhile cleanups, but they should not be counted as completing
the DPD config round-trip work:

- shared DataSource/DataCleaner class-loading helpers
- cleaner or source `module://` import improvements
- sync-function loader cleanup
- documentation-only updates
- native GCS locking implementation

## Fields That Stay Separate

Orchestration-owned fields remain separate and must continue to be handled by
the YAML configuration layer, not by `DatedParquetDataset`:

- `source_location`, `source_class_name`, `source_init_args`
- `cleaning_class_location`, `cleaning_class_name`, `cleaning_init_args`
- `columns_to_rename`, `columns_to_drop`, `dropna_columns`, `dedup_columns`,
  `dedup_keep`
- `sync_function_location`, `sync_function_name`, `sync_function_init_args`
- watched snapshot-info fields: `annotations`, `notes`, and
  `column_descriptions`

## Phase 4: Lock Settings

Lock settings need an explicit near-term decision during this refactor:

- `use_update_lock` should round-trip through YAML, snapshot metadata, and
  metadata reconstruction. The short-term default is `False`; users may opt
  into local lock files with `use_update_lock: true`.
- `lock_dir` may round-trip only as local or mounted-filesystem configuration
  until native GCS locking exists.
- A native `gs://...` lock directory should be rejected clearly or documented
  as unsupported until the lock implementation is backed by a storage
  abstraction instead of `Path` operations.

## Unknown YAML Keys

Unknown YAML keys should become hard errors once the canonical classifier is in
place. If compatibility requires a transition period, the temporary behavior
must be explicit and covered by tests; silent ignore should not be the long-term
behavior.

## Definition of Done

The refactor is done when:

- every DPD-owned config field is classified in one place
- YAML loading, snapshot metadata capture, update-cache reconstruction,
  registry reconstruction, and DPDSource fallback all use the shared contract
- tests fail if a new relevant `DatedParquetDataset` model field is added
  without being classified
- `use_update_lock` and the chosen `lock_dir` semantics round-trip as specified
- NPD info/provenance handling remains strict and separate
- focused tests, `ruff`, `black`, `git diff --check`, and the full test suite
  pass

## Verification

Verification for the refactor must include the new failing tests, focused
YAML/update-cache/registry/DPDSource tests, `ruff`, `black`, `git diff --check`,
and the full test suite.
