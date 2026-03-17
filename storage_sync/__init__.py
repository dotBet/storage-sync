"""
Storage Sync - A Python package for syncing files between different storage locations.

Supported storage backends:
- local - Local file system
- dfs   - ADLS Gen2 (Data Lake Storage), recommended for Fabric LakeHouse
- blob  - Azure Blob Storage
"""

from storage_sync.sync import sync_storage
from storage_sync.clients import (
    StorageClient,
    LocalFileSystemClient,
    BlobStorageClient,
    DfsStorageClient,
    create_storage_client,
)

__version__ = "1.0.0"
__all__ = [
    "sync_storage",
    "StorageClient",
    "LocalFileSystemClient",
    "BlobStorageClient",
    "DfsStorageClient",
    "create_storage_client",
]
