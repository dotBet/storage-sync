"""Local file system storage client."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from storage_sync.clients.base import StorageClient


class LocalFileSystemClient(StorageClient):
    """Local file system implementation."""
    
    def __init__(self):
        pass
    
    def get_container_client(self, container_name: str) -> Path:
        """Get Path object for the specified directory."""
        return Path(container_name).resolve()
    
    def list_files_recursive(self, base_path: Path, prefix: str) -> Dict:
        """List all files under a prefix recursively."""
        files = {}
        search_path = base_path / prefix if prefix else base_path
        
        if not search_path.exists():
            return files
        
        for file_path in search_path.rglob("*"):
            if file_path.is_file():
                relative_to_base = file_path.relative_to(base_path)
                relative_name = str(relative_to_base.relative_to(prefix)) if prefix else str(relative_to_base)
                relative_name = relative_name.replace("\\", "/")
                
                stat = file_path.stat()
                files[relative_name] = {
                    "name": str(relative_to_base).replace("\\", "/"),
                    "size": stat.st_size,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                    "etag": None
                }
        return files
    
    def download_file(self, base_path: Path, file_path: str) -> bytes:
        """Download file contents as bytes."""
        full_path = base_path / file_path
        return full_path.read_bytes()
    
    def upload_file(self, base_path: Path, file_path: str, data: bytes, overwrite: bool = True):
        """Upload file data to the specified path."""
        full_path = base_path / file_path
        if full_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {full_path}")
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(data)
    
    def delete_file(self, base_path: Path, file_path: str):
        """Delete the specified file."""
        full_path = base_path / file_path
        full_path.unlink()
    
    def ensure_container_exists(self, base_path: Path):
        """Ensure the directory exists."""
        base_path.mkdir(parents=True, exist_ok=True)
    
    def ensure_directory_exists(self, base_path: Path, file_path: str):
        """Ensure parent directory exists for the given file path."""
        full_path = base_path / file_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
