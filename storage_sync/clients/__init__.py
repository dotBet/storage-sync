"""Storage client implementations."""

from storage_sync.clients.base import StorageClient
from storage_sync.clients.local import LocalFileSystemClient
from storage_sync.clients.blob import BlobStorageClient
from storage_sync.clients.dfs import DfsStorageClient

__all__ = [
    "StorageClient",
    "LocalFileSystemClient",
    "BlobStorageClient",
    "DfsStorageClient",
    "create_storage_client",
]


def create_storage_client(
    storage_type: str,
    connection_string: str = None,
    account_url: str = None
) -> StorageClient:
    """
    Factory function to create appropriate storage client.
    
    Args:
        storage_type: Storage type - "local", "dfs", or "blob"
        connection_string: Connection string for Azure storage
        account_url: Account URL for Azure storage (uses Azure Identity)
    
    Returns:
        StorageClient instance
    """
    if storage_type == "local":
        return LocalFileSystemClient()
    
    if not connection_string and not account_url:
        raise ValueError(f"Either connection_string or account_url must be provided for {storage_type} storage")
    
    if storage_type == "dfs":
        return DfsStorageClient(connection_string=connection_string, account_url=account_url)
    elif storage_type == "blob":
        return BlobStorageClient(connection_string=connection_string, account_url=account_url)
    else:
        raise ValueError(f"Unknown storage type: {storage_type}. Use 'local', 'dfs', or 'blob'.")
