"""Azure Data Lake Storage Gen2 (DFS) client."""

from pathlib import PurePosixPath
from typing import Dict

from storage_sync.clients.base import StorageClient


class DfsStorageClient(StorageClient):
    """ADLS Gen2 (DFS) implementation."""
    
    def __init__(self, connection_string: str = None, account_url: str = None):
        """
        Initialize Data Lake Storage client.
        
        Args:
            connection_string: Azure Storage connection string
            account_url: Account URL (uses DefaultAzureCredential)
        """
        from azure.storage.filedatalake import DataLakeServiceClient
        
        if connection_string:
            self.service_client = DataLakeServiceClient.from_connection_string(connection_string)
        elif account_url:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            self.service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
        else:
            raise ValueError("Either connection_string or account_url must be provided")
    
    def get_container_client(self, container_name: str):
        """Get file system client for the specified filesystem."""
        return self.service_client.get_file_system_client(container_name)
    
    def list_files_recursive(self, fs_client, prefix: str) -> Dict:
        """List all files under a prefix recursively."""
        files = {}
        paths = fs_client.get_paths(path=prefix if prefix else None, recursive=True)
        
        for path in paths:
            if path.is_directory:
                continue
            
            relative_name = path.name[len(prefix):].lstrip("/") if prefix else path.name
            if relative_name:
                files[relative_name] = {
                    "name": path.name,
                    "size": path.content_length,
                    "last_modified": path.last_modified,
                    "etag": path.etag
                }
        return files
    
    def download_file(self, fs_client, file_path: str) -> bytes:
        """Download file contents as bytes."""
        file_client = fs_client.get_file_client(file_path)
        return file_client.download_file().readall()
    
    def upload_file(self, fs_client, file_path: str, data: bytes, overwrite: bool = True):
        """Upload file data to the specified path."""
        file_client = fs_client.get_file_client(file_path)
        file_client.upload_data(data, overwrite=overwrite)
    
    def delete_file(self, fs_client, file_path: str):
        """Delete the specified file."""
        file_client = fs_client.get_file_client(file_path)
        file_client.delete_file()
    
    def ensure_container_exists(self, fs_client):
        """Ensure the file system exists."""
        try:
            fs_client.get_file_system_properties()
        except Exception:
            fs_client.create_file_system()
    
    def ensure_directory_exists(self, fs_client, file_path: str):
        """Ensure parent directory exists for the given file path."""
        parent = str(PurePosixPath(file_path).parent)
        if parent and parent != ".":
            dir_client = fs_client.get_directory_client(parent)
            try:
                dir_client.create_directory()
            except Exception:
                pass
