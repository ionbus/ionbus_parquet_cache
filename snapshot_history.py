"""Snapshot lineage and provenance models."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DateRange:
    """Inclusive date range recorded in snapshot lineage."""

    start_date: dt.date
    end_date: dt.date


@dataclass
class SnapshotLineage:
    """How a DPD snapshot was produced."""

    base_snapshot: str | None
    first_snapshot_id: str | None
    operation: str
    requested_date_range: DateRange | None
    added_date_ranges: list[DateRange] = field(default_factory=list)
    rewritten_date_ranges: list[DateRange] = field(default_factory=list)
    instrument_column: str | None = None
    instrument_scope: str = "unknown"
    instruments: list[str] | None = None


@dataclass
class SnapshotProvenanceRef:
    """Reference to an external provenance sidecar."""

    path: str
    checksum: str
    size_bytes: int


@dataclass
class CacheHistoryEntry:
    """One entry returned by cache_history()."""

    snapshot: str
    operation: str
    base_snapshot: str | None
    first_snapshot_id: str | None
    requested_date_range: DateRange | None
    added_date_ranges: list[DateRange]
    rewritten_date_ranges: list[DateRange]
    instrument_column: str | None
    instrument_scope: str
    instruments: list[str] | None
    status: str = "ok"
    message: str | None = None
    provenance: SnapshotProvenanceRef | None = None

    @classmethod
    def from_metadata(cls, metadata: Any) -> "CacheHistoryEntry":
        """Build a history entry from snapshot metadata."""
        lineage = getattr(metadata, "lineage", None)
        provenance = getattr(metadata, "provenance", None)
        if lineage is None:
            return cls(
                snapshot=metadata.suffix,
                operation="unknown",
                base_snapshot=None,
                first_snapshot_id=None,
                requested_date_range=None,
                added_date_ranges=[],
                rewritten_date_ranges=[],
                instrument_column=None,
                instrument_scope="unknown",
                instruments=None,
                status="missing_lineage",
                message=(
                    f"Snapshot {metadata.suffix} has no lineage metadata."
                ),
                provenance=provenance,
            )

        return cls(
            snapshot=metadata.suffix,
            operation=lineage.operation,
            base_snapshot=lineage.base_snapshot,
            first_snapshot_id=lineage.first_snapshot_id,
            requested_date_range=lineage.requested_date_range,
            added_date_ranges=list(lineage.added_date_ranges),
            rewritten_date_ranges=list(lineage.rewritten_date_ranges),
            instrument_column=lineage.instrument_column,
            instrument_scope=lineage.instrument_scope,
            instruments=(
                list(lineage.instruments)
                if lineage.instruments is not None
                else None
            ),
            provenance=provenance,
        )
