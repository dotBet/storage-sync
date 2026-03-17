# Storage Sync

A Python package for syncing files between different storage locations with automatic archiving.

## Features

- **Multiple Storage Backends**
  - Local file system
  - Azure Blob Storage
  - Azure Data Lake Storage Gen2 (ADLS Gen2 / DFS)
  - Microsoft Fabric LakeHouse

- **Cross-Storage Sync**
  - Local to cloud, cloud to local, cloud to cloud
  - Different storage types for source and destination

- **Authentication Options**
  - Connection string
  - Azure Identity (DefaultAzureCredential) - supports managed identity, Azure CLI, etc.

- **Smart Sync Features**
  - Wildcard pattern filtering (e.g., `*.csv`, `*.parquet`)
  - Skip unchanged files
  - Automatic archiving of existing files before overwrite
  - Dry-run mode for previewing changes

## Installation

```bash
# Install from source
pip install -e .

# Or install dependencies directly
pip install -r requirements.txt
```

## Quick Start

### Command Line

```bash
# Local to ADLS Gen2
storage-sync --source-type local --source-container "C:/data/export" \
             --dest-type dfs --dest-url "https://account.dfs.core.windows.net" \
             --dest-container mycontainer

# ADLS Gen2 to local
storage-sync --source-type dfs --source-url "https://account.dfs.core.windows.net" \
             --source-container mycontainer \
             --dest-type local --dest-container "C:/backup"

# Sync only CSV files
storage-sync --source-type local --source-container "./data" \
             --dest-type blob --dest-conn "..." --dest-container backup \
             --pattern "*.csv"
```

### Python API

```python
from storage_sync import sync_storage

stats = sync_storage(
    source_type="local",
    source_container="C:/data/export",
    source_prefix="",
    dest_type="dfs",
    dest_container="mycontainer",
    dest_prefix="data",
    dest_account_url="https://account.dfs.core.windows.net",
    patterns=["*.csv", "*.parquet"],
    archive_existing=True,
    dry_run=False
)

print(f"Copied: {stats['copied']}, Archived: {stats['archived']}")
```

## Usage

### Storage Types

| Type | Description | Authentication |
|------|-------------|----------------|
| `local` | Local file system | None required |
| `dfs` | ADLS Gen2 (Data Lake) | Connection string or Identity |
| `blob` | Azure Blob Storage | Connection string or Identity |

### Command Line Options

```
--source-type       Source storage type: local, dfs, blob (default: dfs)
--source-conn       Source connection string (for Azure storage)
--source-url        Source account URL (uses Azure Identity)
--source-container  Source container/filesystem/folder path
--source-prefix     Source prefix/subfolder path

--dest-type         Destination storage type: local, dfs, blob (default: dfs)
--dest-conn         Destination connection string (for Azure storage)
--dest-url          Destination account URL (uses Azure Identity)
--dest-container    Destination container/filesystem/folder path
--dest-prefix       Destination prefix/subfolder path

--pattern           Wildcard pattern to filter files (can be used multiple times)
--no-archive        Overwrite existing files without archiving
--archive-folder    Name of archive folder (default: Archive)
--skip-unchanged    Skip files that haven't changed
--dry-run           Preview changes without making them
--quiet, -q         Suppress progress output
```

### Wildcard Patterns

Supports standard wildcards:
- `*` - matches any characters
- `?` - matches single character

Examples:
- `*.csv` - all CSV files
- `*.parquet` - all Parquet files
- `report_*.xlsx` - files starting with "report_"
- `2024/*/*.json` - JSON files in year/month subdirectories

### Archiving

When a file exists in the destination, it's automatically moved to an Archive folder with a timestamp before being replaced:

```
data/report.csv → data/Archive/report_20260317_143052.csv
```

Disable with `--no-archive` to overwrite directly.

## Examples

### Fabric LakeHouse to Local

```bash
storage-sync --source-type dfs \
             --source-url "https://onelake.dfs.fabric.microsoft.com" \
             --source-container "workspace/lakehouse.Lakehouse/Files" \
             --dest-type local \
             --dest-container "C:/lakehouse-backup"
```

### Local to Blob Storage with Pattern

```bash
storage-sync --source-type local \
             --source-container "./exports" \
             --dest-type blob \
             --dest-conn "DefaultEndpointsProtocol=https;AccountName=..." \
             --dest-container archive \
             --pattern "*.csv" --pattern "*.parquet"
```

### Cloud to Cloud Sync

```bash
storage-sync --source-type dfs \
             --source-url "https://source.dfs.core.windows.net" \
             --source-container data \
             --dest-type dfs \
             --dest-url "https://dest.dfs.core.windows.net" \
             --dest-container backup \
             --skip-unchanged
```

### Dry Run

Preview what would be synced without making changes:

```bash
storage-sync --source-type local --source-container "./data" \
             --dest-type dfs --dest-url "..." --dest-container container \
             --dry-run
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black storage_sync/

# Lint
ruff check storage_sync/
```
