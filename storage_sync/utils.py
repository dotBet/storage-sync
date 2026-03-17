"""Utility functions for storage sync."""

import fnmatch
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Dict, List

from storage_sync.clients.base import StorageClient


def filter_files_by_patterns(files: Dict, patterns: List[str]) -> Dict:
    """
    Filter files dictionary by wildcard patterns.
    
    Args:
        files: Dictionary of files (relative_name -> properties)
        patterns: List of wildcard patterns (e.g., ["*.csv", "*.parquet"])
    
    Returns:
        Filtered dictionary containing only files matching any pattern
    """
    if not patterns:
        return files
    
    filtered = {}
    for relative_name, props in files.items():
        filename = PurePosixPath(relative_name).name
        for pattern in patterns:
            if fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(relative_name, pattern):
                filtered[relative_name] = props
                break
    
    return filtered


def generate_archive_name(file_path: str, archive_folder: str = "Archive") -> str:
    """
    Generate archive path by moving file to Archive folder with timestamp.
    
    Args:
        file_path: Original file path
        archive_folder: Name of the archive folder
    
    Returns:
        New archive path with timestamp
        Example: data/report.csv -> data/Archive/report_20260317_143052.csv
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = PurePosixPath(file_path)
    
    parent = str(path.parent) if str(path.parent) != "." else ""
    
    if path.suffix:
        new_name = f"{path.stem}_{timestamp}{path.suffix}"
    else:
        new_name = f"{path.name}_{timestamp}"
    
    if parent:
        return f"{parent}/{archive_folder}/{new_name}"
    else:
        return f"{archive_folder}/{new_name}"


def archive_file(
    storage_client: StorageClient,
    container_client,
    file_path: str,
    archive_folder: str = "Archive",
    dry_run: bool = False
) -> str:
    """
    Archive a file by moving it to Archive folder with timestamp.
    
    Args:
        storage_client: Storage client instance
        container_client: Container/filesystem client
        file_path: Path to file to archive
        archive_folder: Name of archive folder
        dry_run: If True, don't actually move the file
    
    Returns:
        Archive path where file was moved
    """
    archive_path = generate_archive_name(file_path, archive_folder)
    
    if not dry_run:
        storage_client.ensure_directory_exists(container_client, archive_path)
        
        file_data = storage_client.download_file(container_client, file_path)
        storage_client.upload_file(container_client, archive_path, file_data, overwrite=True)
        storage_client.delete_file(container_client, file_path)
    
    return archive_path


def should_copy_file(source_props: dict, dest_props: dict, skip_unchanged: bool) -> bool:
    """
    Determine if a file should be copied based on existence and modification time.
    
    Args:
        source_props: Source file properties
        dest_props: Destination file properties (None if doesn't exist)
        skip_unchanged: If True, skip files that haven't changed
    
    Returns:
        True if the file should be copied
    """
    if dest_props is None:
        return True
    
    if not skip_unchanged:
        return True
    
    source_modified = source_props.get("last_modified")
    dest_modified = dest_props.get("last_modified")
    source_size = source_props.get("size")
    dest_size = dest_props.get("size")
    
    if source_modified and dest_modified and source_size == dest_size:
        return source_modified > dest_modified
    
    return True


def get_storage_label(storage_type: str) -> str:
    """Get human-readable label for storage type."""
    labels = {
        "local": "Local FileSystem",
        "dfs": "ADLS Gen2/DFS",
        "blob": "Blob Storage"
    }
    return labels.get(storage_type, storage_type)
