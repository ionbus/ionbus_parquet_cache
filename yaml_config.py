"""
YAML configuration loader for DatedParquetDataset definitions.

Loads DPD definitions from YAML files in the yaml/ subdirectory.
Supports dynamic loading of DataSource and DataCleaner classes.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import yaml
from ionbus_utils.general import load_class_from_file
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.data_cleaner import DataCleaner
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import ConfigurationError


@dataclass
class DatasetConfig:
    """
    Configuration for a single DatedParquetDataset from YAML.

    Contains all settings needed to create a DPD and its data source.
    """

    name: str
    cache_dir: Path

    # Core DPD settings
    description: str = ""
    date_col: str = "Date"
    date_partition: str = "day"
    partition_columns: list[str] = field(default_factory=list)
    sort_columns: list[str] | None = None
    start_date_str: str | None = None
    end_date_str: str | None = None
    repull_n_days: int = 0
    instrument_column: str | None = None
    instruments: list[str] | None = None
    num_instrument_buckets: int | None = None
    use_update_lock: bool = True
    lock_dir: Path | None = None
    row_group_size: int | None = None
    annotations: dict[str, Any] | None = None
    notes: str | None = None
    column_descriptions: dict[str, str] | None = None

    # Data source settings
    source_location: str = ""
    source_class_name: str = ""
    source_init_args: dict[str, Any] = field(default_factory=dict)

    # Optional post-sync function settings
    sync_function_location: str | None = None
    sync_function_name: str | None = None
    sync_function_init_args: dict[str, Any] = field(default_factory=dict)

    # YAML transformation settings
    columns_to_drop: list[str] = field(default_factory=list)
    columns_to_rename: dict[str, str] = field(default_factory=dict)
    dropna_columns: list[str] = field(default_factory=list)
    dedup_columns: list[str] = field(default_factory=list)
    dedup_keep: str = "last"

    # Cleaning class settings
    cleaning_class_location: str | None = None
    cleaning_class_name: str | None = None
    cleaning_init_args: dict[str, Any] = field(default_factory=dict)

    def to_dpd(self) -> "DatedParquetDataset":
        """
        Create a DatedParquetDataset from this configuration.

        Returns:
            A configured DatedParquetDataset instance.
        """
        return DatedParquetDataset(
            cache_dir=self.cache_dir,
            name=self.name,
            date_col=self.date_col,
            date_partition=self.date_partition,
            partition_columns=self.partition_columns,
            sort_columns=self.sort_columns,
            description=self.description,
            start_date_str=self.start_date_str,
            end_date_str=self.end_date_str,
            repull_n_days=self.repull_n_days,
            instrument_column=self.instrument_column,
            instruments=self.instruments,
            num_instrument_buckets=self.num_instrument_buckets,
            use_update_lock=self.use_update_lock,
            lock_dir=self.lock_dir,
            row_group_size=self.row_group_size,
        )

    def to_yaml_config(self) -> dict[str, Any]:
        """
        Convert to a dict suitable for storing in snapshot metadata.

        This includes all configuration needed to recreate the DataSource
        and update the dataset without the original YAML file.

        Returns:
            Dict with all configuration fields.
        """
        config = {
            # Core DPD settings
            "date_col": self.date_col,
            "date_partition": self.date_partition,
            "partition_columns": self.partition_columns,
            "sort_columns": self.sort_columns or [self.date_col],
            "description": self.description,
            "start_date_str": self.start_date_str,
            "end_date_str": self.end_date_str,
            "repull_n_days": self.repull_n_days,
            "instrument_column": self.instrument_column,
            "instruments": self.instruments,
            "num_instrument_buckets": self.num_instrument_buckets,
            "row_group_size": self.row_group_size,
            # Source settings
            "source_location": self.source_location,
            "source_class_name": self.source_class_name,
            "source_init_args": self.source_init_args,
            # Post-sync function settings
            "sync_function_location": self.sync_function_location,
            "sync_function_name": self.sync_function_name,
            "sync_function_init_args": self.sync_function_init_args,
            # Transform settings
            "columns_to_drop": self.columns_to_drop,
            "columns_to_rename": self.columns_to_rename,
            "dropna_columns": self.dropna_columns,
            "dedup_columns": self.dedup_columns,
            "dedup_keep": self.dedup_keep,
            # Cleaner settings
            "cleaning_class_location": self.cleaning_class_location,
            "cleaning_class_name": self.cleaning_class_name,
            "cleaning_init_args": self.cleaning_init_args,
        }
        if self.annotations is not None:
            config["annotations"] = self.annotations
        if self.notes is not None:
            config["notes"] = self.notes
        if self.column_descriptions is not None:
            config["column_descriptions"] = self.column_descriptions
        return config

    def load_source_class(self) -> type["DataSource"]:
        """
        Dynamically load the DataSource class.

        Resolution order:
        1. If source_location is empty, load from built-in sources
        2. If source_location starts with 'module://', load from installed package
        3. Otherwise, load from file path (relative to cache root or absolute)

        Returns:
            The DataSource subclass.

        Raises:
            ConfigurationError: If the class cannot be loaded.
        """
        if not self.source_class_name:
            raise ConfigurationError(
                f"Dataset '{self.name}' has no source_class_name configured",
                config_file=str(self.cache_dir / "yaml"),
            )

        return _resolve_source_class(
            self.source_location,
            self.source_class_name,
            self.cache_dir,
            f"Dataset '{self.name}'",
        )

    def create_source(self, dataset: "DatedParquetDataset") -> "DataSource":
        """
        Create a configured DataSource instance.

        Args:
            dataset: The DatedParquetDataset the source is for.

        Returns:
            A configured DataSource instance.
        """
        source_class = self.load_source_class()
        return source_class(dataset, **self.source_init_args)

    def load_cleaner_class(self) -> type["DataCleaner"] | None:
        """
        Dynamically load the DataCleaner class if configured.

        Resolution order:
        1. If cleaning_class_name is empty, return None
        2. If cleaning_class_location starts with 'module://', load from module
        3. Otherwise, load from file path (relative to cache root or absolute)

        Returns:
            The DataCleaner subclass, or None if not configured.

        Raises:
            ConfigurationError: If the class cannot be loaded.
        """
        if not self.cleaning_class_name:
            return None

        return _resolve_cleaner_class(
            self.cleaning_class_location,
            self.cleaning_class_name,
            self.cache_dir,
            f"Dataset '{self.name}'",
            config_file=str(self.cache_dir / "yaml"),
        )

    def create_cleaner(
        self, dataset: "DatedParquetDataset"
    ) -> "DataCleaner | None":
        """
        Create a configured DataCleaner instance if configured.

        Args:
            dataset: The DatedParquetDataset the cleaner is for.

        Returns:
            A configured DataCleaner instance, or None if not configured.
        """
        cleaner_class = self.load_cleaner_class()
        if cleaner_class is None:
            return None
        return cleaner_class(dataset, **self.cleaning_init_args)

    def has_yaml_transforms(self) -> bool:
        """Check if any YAML transforms are configured."""
        return bool(
            self.columns_to_drop
            or self.columns_to_rename
            or self.dropna_columns
            or self.dedup_columns
        )


def load_yaml_file(
    yaml_path: Path, cache_dir: Path
) -> dict[str, DatasetConfig]:
    """
    Load dataset configurations from a single YAML file.

    Args:
        yaml_path: Path to the YAML file.
        cache_dir: Root directory of the cache (for resolving paths).

    Returns:
        Dict mapping dataset names to their configurations.

    Raises:
        ConfigurationError: If the YAML is invalid.
    """
    logger.debug(f"Loading YAML config from {yaml_path}")
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(
            f"Invalid YAML syntax in {yaml_path}: {e}",
            config_file=str(yaml_path),
        ) from e
    except FileNotFoundError as e:
        raise ConfigurationError(
            f"YAML file not found: {yaml_path}",
            config_file=str(yaml_path),
        ) from e

    if not isinstance(data, dict):
        raise ConfigurationError(
            f"YAML file must contain a mapping, got {type(data).__name__}",
            config_file=str(yaml_path),
        )

    datasets_dict = data.get("datasets", {})
    if not isinstance(datasets_dict, dict):
        raise ConfigurationError(
            f"'datasets' must be a mapping, got {type(datasets_dict).__name__}",
            config_file=str(yaml_path),
        )

    configs = {}
    for name, settings in datasets_dict.items():
        if not isinstance(settings, dict):
            raise ConfigurationError(
                f"Dataset '{name}' settings must be a mapping",
                config_file=str(yaml_path),
            )

        annotations = settings.get("annotations")
        if "annotations" in settings and not isinstance(annotations, dict):
            raise ConfigurationError(
                f"Dataset '{name}' annotations must be a mapping",
                config_file=str(yaml_path),
            )

        notes = settings.get("notes")
        if "notes" in settings and not isinstance(notes, str):
            raise ConfigurationError(
                f"Dataset '{name}' notes must be a string",
                config_file=str(yaml_path),
            )

        column_descriptions = settings.get("column_descriptions")
        if "column_descriptions" in settings:
            if not isinstance(column_descriptions, dict):
                raise ConfigurationError(
                    f"Dataset '{name}' column_descriptions must be a mapping",
                    config_file=str(yaml_path),
                )
            for column_name, description in column_descriptions.items():
                if not isinstance(column_name, str) or not isinstance(
                    description,
                    str,
                ):
                    raise ConfigurationError(
                        f"Dataset '{name}' column_descriptions keys and "
                        "values must be strings",
                        config_file=str(yaml_path),
                    )

        sync_function_init_args = settings.get("sync_function_init_args", {})
        if sync_function_init_args is None:
            sync_function_init_args = {}
        if not isinstance(sync_function_init_args, dict):
            raise ConfigurationError(
                f"Dataset '{name}' sync_function_init_args must be a mapping",
                config_file=str(yaml_path),
            )

        use_update_lock = settings.get("use_update_lock", True)
        if not isinstance(use_update_lock, bool):
            raise ConfigurationError(
                f"Dataset '{name}' use_update_lock must be a boolean",
                config_file=str(yaml_path),
            )

        lock_dir = settings.get("lock_dir")
        if lock_dir is not None:
            if not isinstance(lock_dir, str):
                raise ConfigurationError(
                    f"Dataset '{name}' lock_dir must be a string path",
                    config_file=str(yaml_path),
                )
            lock_dir = Path(lock_dir).expanduser()
            if not lock_dir.is_absolute():
                lock_dir = cache_dir / lock_dir

        configs[name] = DatasetConfig(
            name=name,
            cache_dir=cache_dir,
            description=settings.get("description", ""),
            date_col=settings.get("date_col", "Date"),
            date_partition=settings.get("date_partition", "day"),
            partition_columns=settings.get("partition_columns", []),
            sort_columns=settings.get("sort_columns"),
            start_date_str=settings.get("start_date_str"),
            end_date_str=settings.get("end_date_str"),
            repull_n_days=settings.get("repull_n_days", 0),
            instrument_column=settings.get("instrument_column"),
            instruments=settings.get("instruments"),
            num_instrument_buckets=settings.get("num_instrument_buckets"),
            use_update_lock=use_update_lock,
            lock_dir=lock_dir,
            row_group_size=settings.get("row_group_size"),
            annotations=annotations,
            notes=notes,
            column_descriptions=column_descriptions,
            source_location=settings.get("source_location", ""),
            source_class_name=settings.get("source_class_name", ""),
            source_init_args=settings.get("source_init_args", {}),
            sync_function_location=settings.get("sync_function_location"),
            sync_function_name=settings.get("sync_function_name"),
            sync_function_init_args=sync_function_init_args,
            columns_to_drop=settings.get("columns_to_drop", []),
            columns_to_rename=settings.get("columns_to_rename", {}),
            dropna_columns=settings.get("dropna_columns", []),
            dedup_columns=settings.get("dedup_columns", []),
            dedup_keep=settings.get("dedup_keep", "last"),
            cleaning_class_location=settings.get("cleaning_class_location"),
            cleaning_class_name=settings.get("cleaning_class_name"),
            cleaning_init_args=settings.get("cleaning_init_args", {}),
        )

    return configs


def load_all_configs(cache_dir: str | Path) -> dict[str, DatasetConfig]:
    """
    Load all dataset configurations from a cache's yaml/ directory.

    Args:
        cache_dir: Root directory of the cache.

    Returns:
        Dict mapping dataset names to their configurations.

    Raises:
        ConfigurationError: If any YAML file is invalid or has duplicate names.
    """
    cache_path = Path(cache_dir)
    yaml_dir = cache_path / "yaml"

    if not yaml_dir.exists():
        return {}

    all_configs: dict[str, DatasetConfig] = {}

    logger.debug(f"Scanning YAML files in {yaml_dir}")
    for yaml_file in yaml_dir.glob("*.yaml"):
        file_configs = load_yaml_file(yaml_file, cache_path)

        for name, config in file_configs.items():
            if name in all_configs:
                raise ConfigurationError(
                    f"Duplicate dataset name '{name}' found in {yaml_file}",
                    config_file=str(yaml_file),
                )
            all_configs[name] = config

    # Also check .yml extension
    for yaml_file in yaml_dir.glob("*.yml"):
        file_configs = load_yaml_file(yaml_file, cache_path)

        for name, config in file_configs.items():
            if name in all_configs:
                raise ConfigurationError(
                    f"Duplicate dataset name '{name}' found in {yaml_file}",
                    config_file=str(yaml_file),
                )
            all_configs[name] = config

    return all_configs


def get_dataset_config(
    cache_dir: str | Path,
    dataset_name: str,
) -> DatasetConfig:
    """
    Get configuration for a specific dataset.

    Args:
        cache_dir: Root directory of the cache.
        dataset_name: Name of the dataset.

    Returns:
        The DatasetConfig for the dataset.

    Raises:
        ConfigurationError: If the dataset is not found in any YAML file.
    """
    configs = load_all_configs(cache_dir)

    if dataset_name not in configs:
        raise ConfigurationError(
            f"Dataset '{dataset_name}' not found in yaml/ directory",
            config_file=str(Path(cache_dir) / "yaml"),
        )

    return configs[dataset_name]


def _resolve_path(cache_dir: Path, location: str) -> Path:
    """
    Resolve a path that may be relative to cache_dir or absolute.

    Args:
        cache_dir: Root directory of the cache.
        location: Path string (relative or absolute).

    Returns:
        Resolved absolute Path.
    """
    path = Path(location)
    if path.is_absolute():
        return path
    return cache_dir / location


def _resolve_source_class(
    source_location: str,
    source_class_name: str,
    cache_dir: Path,
    context: str,
) -> type["DataSource"]:
    """
    Resolve and load a DataSource class from source_location.

    Resolution order:
    1. If source_location is empty, load from built-in sources
    2. If source_location starts with 'module://', load from installed package
    3. Otherwise, load from file path (relative to cache root or absolute)

    Args:
        source_location: Empty string, "module://...", or file path.
        source_class_name: Name of the DataSource class to load.
        cache_dir: Root cache directory (for resolving relative paths).
        context: Context string for error messages.

    Returns:
        The DataSource subclass.

    Raises:
        ConfigurationError: If the class cannot be loaded.
    """
    # Check for built-in sources first
    if not source_location:
        return _load_builtin_source(source_class_name)

    # Check for installed module source
    if source_location.startswith("module://"):
        module_path = source_location[len("module://") :]
        return _load_installed_module_source(
            module_path,
            source_class_name,
            context,
        )

    # Load from file
    source_path = _resolve_path(cache_dir, source_location)
    return _load_class_from_file(
        source_path,
        source_class_name,
        DataSource,
        context,
    )


def _resolve_cleaner_class(
    cleaning_class_location: str | None,
    cleaning_class_name: str,
    cache_dir: Path,
    context: str,
    config_file: str | None = None,
) -> type["DataCleaner"]:
    """
    Resolve and load a DataCleaner class from cleaning_class_location.

    Args:
        cleaning_class_location: "module://..." or file path. Required when
            cleaning_class_name is configured; blank does not imply built-in.
        cleaning_class_name: Name of the DataCleaner class to load.
        cache_dir: Root cache directory (for resolving relative paths).
        context: Context string for error messages.
        config_file: Optional config path to attach to ConfigurationError.

    Returns:
        The DataCleaner subclass.

    Raises:
        ConfigurationError: If the class cannot be loaded.
    """
    if not cleaning_class_location:
        raise ConfigurationError(
            f"{context} has cleaning_class_name but no "
            "cleaning_class_location",
            config_file=config_file,
        )

    if cleaning_class_location.startswith("module://"):
        module_path = cleaning_class_location[len("module://") :]
        return _load_class_from_module(
            module_path,
            cleaning_class_name,
            DataCleaner,
            "DataCleaner",
            context,
        )

    cleaner_path = _resolve_path(cache_dir, cleaning_class_location)
    return _load_class_from_file(
        cleaner_path,
        cleaning_class_name,
        DataCleaner,
        context,
    )


def _load_installed_module_source(
    module_path: str,
    class_name: str,
    context: str,
) -> type["DataSource"]:
    """
    Load a DataSource class from an installed Python package.

    Args:
        module_path: Importable module path (e.g., 'my_library.data_sources').
        class_name: Name of the DataSource class in that module.
        context: Context string for error messages.

    Returns:
        The DataSource subclass.

    Raises:
        ConfigurationError: If the module cannot be imported, the class is
            not found, is nested, or does not inherit from DataSource.
    """
    return _load_class_from_module(
        module_path,
        class_name,
        DataSource,
        "DataSource or BucketedDataSource",
        context,
    )


def _load_class_from_module(
    module_path: str,
    class_name: str,
    base_class: type,
    base_class_description: str,
    context: str,
) -> type:
    """
    Load and validate a class from an installed/importable Python module.

    Args:
        module_path: Importable module path (e.g., 'my_library.data_sources').
        class_name: Name of the class in that module.
        base_class: Expected base class.
        base_class_description: Human-readable base class name for errors.
        context: Context string for error messages.

    Returns:
        The loaded class.

    Raises:
        ConfigurationError: If the module cannot be imported, the class is
            not found, is nested, or does not inherit from base_class.
    """
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        raise ConfigurationError(
            f"{context}: Could not import module '{module_path}': {e}",
        ) from e
    except Exception as e:
        raise ConfigurationError(
            f"{context}: Failed to import module '{module_path}': {e}",
        ) from e

    # Get the class from the module
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ConfigurationError(
            f"{context}: Class '{class_name}' not found in module "
            f"'{module_path}'",
        )

    # Validate: must be a class
    if not inspect.isclass(cls):
        raise ConfigurationError(
            f"{context}: '{class_name}' in module '{module_path}' is not a "
            f"class",
        )

    if not issubclass(cls, base_class):
        raise ConfigurationError(
            f"{context}: Class '{class_name}' must inherit from "
            f"{base_class_description}",
        )

    return cls


def _load_builtin_source(class_name: str) -> type["DataSource"]:
    """
    Load a built-in DataSource class.

    Args:
        class_name: Name of the built-in class.

    Returns:
        The DataSource subclass.

    Raises:
        ConfigurationError: If the class is not found.
    """
    # Import built-in sources module
    try:
        from ionbus_parquet_cache import builtin_sources

        cls = getattr(builtin_sources, class_name, None)
        if cls is not None:
            return cls
    except ImportError:
        pass

    raise ConfigurationError(
        f"Built-in DataSource '{class_name}' not found. "
        f"Check source_class_name or provide source_location.",
    )


def _load_class_from_file(
    file_path: Path,
    class_name: str,
    base_class: type,
    context: str,
) -> type:
    """
    Dynamically load a class from a Python file.

    Wraps ionbus_utils.general.load_class_from_file with ConfigurationError.

    Args:
        file_path: Path to the Python file.
        class_name: Name of the class to load.
        base_class: Expected base class.
        context: Context string for error messages.

    Returns:
        The loaded class.

    Raises:
        ConfigurationError: If loading fails.
    """
    try:
        return load_class_from_file(file_path, class_name, base_class)
    except FileNotFoundError:
        raise ConfigurationError(
            f"{context}: Python file not found: {file_path}",
            config_file=str(file_path),
        )
    except (ImportError, AttributeError, TypeError) as e:
        raise ConfigurationError(
            f"{context}: {e}",
            config_file=str(file_path),
        ) from e
    except Exception as e:
        raise ConfigurationError(
            f"{context}: Failed to load '{class_name}' from {file_path}: {e}",
            config_file=str(file_path),
        ) from e


def apply_yaml_transforms(
    rel: "duckdb.DuckDBPyRelation",
    config: DatasetConfig,
) -> "duckdb.DuckDBPyRelation":
    """
    Apply YAML-specified transforms to a DuckDB relation.

    Transforms are applied in order:
    1. columns_to_rename
    2. columns_to_drop
    3. dropna_columns
    4. dedup_columns

    Args:
        rel: The input DuckDB relation.
        config: Dataset configuration with transform settings.

    Returns:
        The transformed DuckDB relation.
    """
    # 1. Rename columns
    if config.columns_to_rename:
        renames = ", ".join(
            f'"{old}" AS "{new}"'
            for old, new in config.columns_to_rename.items()
        )
        # Get columns that aren't being renamed
        current_cols = rel.columns
        other_cols = [
            f'"{c}"'
            for c in current_cols
            if c not in config.columns_to_rename
        ]
        select_clause = (
            ", ".join(other_cols + [renames]) if other_cols else renames
        )
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    # 2. Drop columns
    if config.columns_to_drop:
        current_cols = rel.columns
        keep_cols = [
            c for c in current_cols if c not in config.columns_to_drop
        ]
        select_clause = ", ".join(f'"{c}"' for c in keep_cols)
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    # 3. Drop rows with nulls
    if config.dropna_columns:
        conditions = " AND ".join(
            f'"{c}" IS NOT NULL' for c in config.dropna_columns
        )
        rel = duckdb.sql(f"SELECT * FROM rel WHERE {conditions}")

    # 4. Deduplicate
    # Preserves input order: "first" keeps earliest row, "last" keeps latest
    # row as they appear in the source data. Uses _input_order to track
    # original row position since rowid is lost during SQL transformations.
    if config.dedup_columns:
        key_cols = ", ".join(f'"{c}"' for c in config.dedup_columns)

        # Add row number to track input order
        rel = duckdb.sql(
            "SELECT *, ROW_NUMBER() OVER () AS _input_order FROM rel"
        )

        if config.dedup_keep == "first":
            rel = duckdb.sql(
                f"SELECT * FROM rel QUALIFY ROW_NUMBER() OVER "
                f"(PARTITION BY {key_cols} ORDER BY _input_order) = 1"
            )
        else:  # last
            rel = duckdb.sql(
                f"SELECT * FROM rel QUALIFY ROW_NUMBER() OVER "
                f"(PARTITION BY {key_cols} ORDER BY _input_order DESC) = 1"
            )

        # Remove helper column
        current_cols = [c for c in rel.columns if c != "_input_order"]
        select_clause = ", ".join(f'"{c}"' for c in current_cols)
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    return rel
