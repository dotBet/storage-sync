"""Abstract base class for storage clients."""

from abc import ABC, abstractmethod
from typing import Dict


class StorageClient(ABC):
    """Abstract base class for storage operations."""
    
    @abstractmethod
    def get_container_client(self, container_name: str):
        """Get a client for the specified container/filesystem/folder."""
        pass
    
    @abstractmethod
    def list_files_recursive(self, container_client, prefix: str) -> Dict:
        """
        List all files under a prefix recursively.
        
        Returns:
            Dictionary mapping relative_name -> properties dict with keys:
            - name: Full path/name of the file
            - size: File size in bytes
            - last_modified: Last modified datetime
            - etag: Entity tag (optional)
        """
        pass
    
    @abstractmethod
    def download_file(self, container_client, file_path: str) -> bytes:
        """Download file contents as bytes."""
        pass
    
    @abstractmethod
    def upload_file(self, container_client, file_path: str, data: bytes, overwrite: bool):
        """Upload file data to the specified path."""
        pass
    
    @abstractmethod
    def delete_file(self, container_client, file_path: str):
        """Delete the specified file."""
        pass
    
    @abstractmethod
    def ensure_container_exists(self, container_client):
        """Ensure the container/filesystem/folder exists."""
        pass
    
    @abstractmethod
    def ensure_directory_exists(self, container_client, file_path: str):
        """Ensure parent directory exists for the given file path."""
        pass
