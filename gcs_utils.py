"""
GCS utilities with lazy imports.

All GCS operations are gated behind is_gcs_path checks. The gcsfs library
is only imported when a gs:// path is actually used. Install with:
    pip install gcsfs
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import gcsfs as _gcsfs_t
    import pyarrow.fs as pafs_t

_GCS_FS: "_gcsfs_t.GCSFileSystem | None" = None


def is_gcs_path(path: str | Path) -> bool:
    """Return True if path is a GCS URL (starts with gs://)."""
    return str(path).startswith("gs://")


def get_gcs_filesystem() -> "_gcsfs_t.GCSFileSystem":
    """Return the singleton gcsfs filesystem, importing lazily."""
    global _GCS_FS
    if _GCS_FS is None:
        try:
            import gcsfs
        except ImportError:
            raise ImportError(
                "gcsfs is required for GCS support. "
                "Install with: pip install gcsfs"
            ) from None
        _GCS_FS = gcsfs.GCSFileSystem()
    return _GCS_FS


def gcs_pa_filesystem() -> "pafs_t.FileSystem":
    """Return a PyArrow-compatible filesystem backed by gcsfs."""
    import pyarrow.fs as pafs
    fs = get_gcs_filesystem()
    return pafs.PyFileSystem(pafs.FSSpecHandler(fs))


def gcs_strip_prefix(gcs_url: str) -> str:
    """Strip gs:// to get the bucket/path form used by gcsfs."""
    if gcs_url.startswith("gs://"):
        return gcs_url[5:]
    return gcs_url


def gcs_join(base: str, *parts: str) -> str:
    """Join GCS path segments."""
    result = base.rstrip("/")
    for part in parts:
        result = result + "/" + part.strip("/")
    return result


def gcs_exists(gcs_url: str) -> bool:
    """Return True if the GCS blob or pseudo-directory exists."""
    return get_gcs_filesystem().exists(gcs_strip_prefix(gcs_url))


def gcs_ls(gcs_url: str) -> list[str]:
    """
    List the immediate children of a GCS pseudo-directory.

    Returns full gs:// URLs. Returns [] if the prefix does not exist.
    """
    fs = get_gcs_filesystem()
    try:
        items = fs.ls(gcs_strip_prefix(gcs_url), detail=False)
    except FileNotFoundError:
        return []
    return [f"gs://{item}" for item in items]


def gcs_find(gcs_url: str) -> list[str]:
    """
    Recursively list all blobs under a GCS prefix.

    Returns full gs:// URLs. Returns [] if the prefix does not exist.
    """
    fs = get_gcs_filesystem()
    try:
        items = fs.find(gcs_strip_prefix(gcs_url), detail=False)
    except FileNotFoundError:
        return []
    return [f"gs://{item}" for item in items]


def gcs_open(gcs_url: str, mode: str = "rb"):
    """Open a GCS blob as a file-like object."""
    return get_gcs_filesystem().open(gcs_strip_prefix(gcs_url), mode)


def gcs_size(gcs_url: str) -> int:
    """Return the size of a GCS blob in bytes."""
    return get_gcs_filesystem().size(gcs_strip_prefix(gcs_url))


def gcs_info(gcs_url: str) -> dict[str, Any]:
    """Return gcsfs info dict for a blob."""
    return get_gcs_filesystem().info(gcs_strip_prefix(gcs_url))  # type: ignore[return-value]


def gcs_upload(local_path: Path, gcs_url: str) -> None:
    """Upload a local file to GCS."""
    get_gcs_filesystem().put_file(str(local_path), gcs_strip_prefix(gcs_url))


def gcs_download(gcs_url: str, local_path: Path) -> None:
    """Download a GCS blob to a local path, creating parent dirs as needed."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    get_gcs_filesystem().get_file(gcs_strip_prefix(gcs_url), str(local_path))


def should_upload(local_path: Path, gcs_url: str) -> bool:
    """Return True if local_path should be uploaded (missing or size differs)."""
    if not gcs_exists(gcs_url):
        return True
    return local_path.stat().st_size != gcs_size(gcs_url)


def should_download(gcs_url: str, local_path: Path) -> bool:
    """Return True if gcs_url should be downloaded (missing or size differs)."""
    if not local_path.exists():
        return True
    return gcs_size(gcs_url) != local_path.stat().st_size
