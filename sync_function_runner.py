"""Loading and execution helpers for sync-cache post-sync functions."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ionbus_parquet_cache.exceptions import ConfigurationError

SyncFunction = Callable[..., None]

_BUILTIN_SYNC_FUNCTIONS: dict[str, SyncFunction] = {}


@dataclass(frozen=True)
class SyncFunctionConfig:
    """Configured sync function location, name, and constructor kwargs."""

    location: str | None
    name: str
    init_args: dict[str, Any] = field(default_factory=dict)
    context: str = "sync function"


@dataclass(frozen=True)
class SyncFunctionTarget:
    """One dataset snapshot selected for post-sync execution."""

    source_dataset_name: str
    dest_dataset_name: str
    dataset_type: str
    snapshot_id: str
    source_location: str
    dest_location: str

    def kwargs(self) -> dict[str, str]:
        """Return keyword arguments passed to sync functions."""
        return {
            "source_dataset_name": self.source_dataset_name,
            "dest_dataset_name": self.dest_dataset_name,
            "dataset_type": self.dataset_type,
            "snapshot_id": self.snapshot_id,
            "source_location": self.source_location,
            "dest_location": self.dest_location,
        }


def parse_sync_function_spec(spec: str) -> tuple[str, str]:
    """
    Parse ``LOCATION:NAME`` from the command line.

    Uses the final colon so ``module://pkg.mod:Name`` parses correctly.
    """
    if ":" not in spec:
        raise ConfigurationError(
            "--sync-function must be in LOCATION:NAME format"
        )
    location, name = spec.rsplit(":", 1)
    if not name:
        raise ConfigurationError(
            "--sync-function must include a callable name after ':'"
        )
    return location, name


def load_sync_function(
    config: SyncFunctionConfig,
    cache_dir: Path,
) -> SyncFunction:
    """
    Load and validate a sync function or callable class.

    Classes are instantiated with ``config.init_args``. Plain functions may not
    receive init args because there is nowhere unambiguous to apply them.
    """
    if not config.name:
        raise ConfigurationError(f"{config.context}: missing function name")

    init_args = config.init_args or {}
    if not isinstance(init_args, dict):
        raise ConfigurationError(
            f"{config.context}: sync_function_init_args must be a dict"
        )

    obj = _load_sync_function_object(config, cache_dir)
    if inspect.isclass(obj):
        try:
            callable_obj = obj(**init_args)
        except Exception as e:
            raise ConfigurationError(
                f"{config.context}: failed to instantiate "
                f"'{config.name}': {e}"
            ) from e
        if not callable(callable_obj):
            raise ConfigurationError(
                f"{config.context}: instance of '{config.name}' is not "
                "callable"
            )
        _validate_signature(callable_obj, config.context)
        return callable_obj

    if init_args:
        raise ConfigurationError(
            f"{config.context}: sync_function_init_args can only be used "
            "with class-based sync functions"
        )
    if not callable(obj):
        raise ConfigurationError(
            f"{config.context}: '{config.name}' is not callable"
        )
    _validate_signature(obj, config.context)
    return obj


def run_sync_function(
    sync_function: SyncFunction,
    target: SyncFunctionTarget,
) -> None:
    """Run a sync function for one selected dataset snapshot."""
    sync_function(**target.kwargs())


def _load_sync_function_object(
    config: SyncFunctionConfig,
    cache_dir: Path,
) -> Any:
    """Resolve the raw object before function/class validation."""
    location = config.location or ""
    if not location:
        return _load_builtin_sync_function(config.name, config.context)
    if location.startswith("module://"):
        module_path = location[len("module://") :]
        return _load_object_from_module(
            module_path,
            config.name,
            config.context,
        )
    return _load_object_from_file(
        _resolve_path(cache_dir, location),
        config.name,
        config.context,
    )


def _load_builtin_sync_function(name: str, context: str) -> Any:
    """Load a built-in sync function, if this package defines one."""
    sync_function = _BUILTIN_SYNC_FUNCTIONS.get(name)
    if sync_function is not None:
        return sync_function
    raise ConfigurationError(
        f"{context}: built-in sync function '{name}' not found; "
        "provide sync_function_location"
    )


def _load_object_from_module(
    module_path: str,
    name: str,
    context: str,
) -> Any:
    """Load an object from an installed/importable module."""
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise ConfigurationError(
            f"{context}: could not import module '{module_path}': {e}"
        ) from e
    except Exception as e:
        raise ConfigurationError(
            f"{context}: failed to import module '{module_path}': {e}"
        ) from e

    if not hasattr(module, name):
        raise ConfigurationError(
            f"{context}: '{name}' not found in module '{module_path}'"
        )
    return getattr(module, name)


def _load_object_from_file(
    file_path: Path,
    name: str,
    context: str,
) -> Any:
    """Load an object from a Python file."""
    if not file_path.exists():
        raise ConfigurationError(
            f"{context}: Python file not found: {file_path}",
            config_file=str(file_path),
        )

    module_name = (
        f"_ionbus_sync_function_{file_path.stem}_"
        f"{abs(hash(str(file_path.resolve())))}"
    )
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ConfigurationError(
            f"{context}: could not load Python file: {file_path}",
            config_file=str(file_path),
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.path.insert(0, str(file_path.parent))
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ConfigurationError(
            f"{context}: failed to import {file_path}: {e}",
            config_file=str(file_path),
        ) from e
    finally:
        try:
            sys.path.remove(str(file_path.parent))
        except ValueError:
            pass

    if not hasattr(module, name):
        raise ConfigurationError(
            f"{context}: '{name}' not found in {file_path}",
            config_file=str(file_path),
        )
    return getattr(module, name)


def _resolve_path(cache_dir: Path, location: str) -> Path:
    """Resolve a cache-local or absolute sync function path."""
    path = Path(location)
    if path.is_absolute():
        return path
    return cache_dir / location


def _validate_signature(sync_function: SyncFunction, context: str) -> None:
    """Verify the callable can accept the sync-function keyword contract."""
    try:
        signature = inspect.signature(sync_function)
    except (TypeError, ValueError):
        return

    try:
        signature.bind(
            source_dataset_name="source",
            dest_dataset_name="dest",
            dataset_type="dpd",
            snapshot_id="1AAAAAA",
            source_location="/source",
            dest_location="/dest",
        )
    except TypeError as e:
        raise ConfigurationError(
            f"{context}: sync function has incompatible signature: {e}"
        ) from e
