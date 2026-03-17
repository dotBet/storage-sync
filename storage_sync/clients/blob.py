"""Azure Blob Storage client."""

from typing import Dict

from storage_sync.clients.base import StorageClient


class BlobStorageClient(StorageClient):
    """Azure Blob Storage implementation."""
    
    def __init__(self, connection_string: str = None, account_url: str = None):
        """
        Initialize Blob Storage client.
        
        Args:
            connection_string: Azure Storage connection string
            account_url: Account URL (uses DefaultAzureCredential)
        """
        from azure.storage.blob import BlobServiceClient
        
        if connection_string:
            self.service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_url:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            self.service_client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            raise ValueError("Either connection_string or account_url must be provided")
    
    def get_container_client(self, container_name: str):
        """Get container client for the specified container."""
        return self.service_client.get_container_client(container_name)
    
    def list_files_recursive(self, container_client, prefix: str) -> Dict:
        """List all blobs under a prefix recursively."""
        files = {}
        name_starts_with = prefix if prefix else None
        for blob in container_client.list_blobs(name_starts_with=name_starts_with):
            relative_name = blob.name[len(prefix):].lstrip("/") if prefix else blob.name
            if relative_name:
                files[relative_name] = {
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified,
                    "etag": blob.etag
                }
        return files
    
    def download_file(self, container_client, file_path: str) -> bytes:
        """Download blob contents as bytes."""
        blob_client = container_client.get_blob_client(file_path)
        return blob_client.download_blob().readall()
    
    def upload_file(self, container_client, file_path: str, data: bytes, overwrite: bool = True):
        """Upload blob data to the specified path."""
        blob_client = container_client.get_blob_client(file_path)
        blob_client.upload_blob(data, overwrite=overwrite)
    
    def delete_file(self, container_client, file_path: str):
        """Delete the specified blob."""
        blob_client = container_client.get_blob_client(file_path)
        blob_client.delete_blob()
    
    def ensure_container_exists(self, container_client):
        """Ensure the container exists."""
        try:
            container_client.get_container_properties()
        except Exception:
            container_client.create_container()
    
    def ensure_directory_exists(self, container_client, file_path: str):
        """No-op for blob storage (flat namespace)."""
        pass
