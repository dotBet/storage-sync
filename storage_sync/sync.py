"""Core sync functionality."""

from typing import Dict, List, Optional

from storage_sync.clients import create_storage_client
from storage_sync.utils import (
    filter_files_by_patterns,
    generate_archive_name,
    archive_file,
    should_copy_file,
)


def sync_storage(
    source_type: str,
    source_container: str,
    source_prefix: str,
    dest_type: str,
    dest_container: str,
    dest_prefix: str,
    source_connection_string: str = None,
    source_account_url: str = None,
    dest_connection_string: str = None,
    dest_account_url: str = None,
    patterns: List[str] = None,
    archive_existing: bool = True,
    archive_folder: str = "Archive",
    skip_unchanged: bool = False,
    dry_run: bool = False,
    verbose: bool = True
) -> Dict:
    """
    Sync files between storage locations with archiving.
    
    Flow:
    1. Scan source and destination recursively (with optional pattern filtering)
    2. For files that exist in destination: move to Archive folder with timestamp
    3. Copy new file from source to destination
    
    Args:
        source_type: Source storage type - "local", "dfs", or "blob"
        source_container: Source container/filesystem/folder path
        source_prefix: Source folder/prefix (empty string for root)
        dest_type: Destination storage type - "local", "dfs", or "blob"
        dest_container: Destination container/filesystem/folder path
        dest_prefix: Destination folder/prefix (empty string for root)
        source_connection_string: Connection string for source (Azure storage)
        source_account_url: Account URL for source (use with Azure Identity)
        dest_connection_string: Connection string for destination (Azure storage)
        dest_account_url: Account URL for destination (use with Azure Identity)
        patterns: List of wildcard patterns to filter files (e.g., ["*.csv", "*.parquet"])
        archive_existing: If True, archive existing files before copying (default)
        archive_folder: Name of the archive folder (default: "Archive")
        skip_unchanged: If True, skip files that haven't changed (same size/date)
        dry_run: If True, only print what would be done without making changes
        verbose: If True, print progress messages
    
    Returns:
        dict with counts: copied, archived, skipped, filtered, errors
    """
    source_client = create_storage_client(
        source_type,
        connection_string=source_connection_string,
        account_url=source_account_url
    )
    dest_client = create_storage_client(
        dest_type,
        connection_string=dest_connection_string,
        account_url=dest_account_url
    )
    
    source_container_client = source_client.get_container_client(source_container)
    dest_container_client = dest_client.get_container_client(dest_container)
    
    if not dry_run:
        dest_client.ensure_container_exists(dest_container_client)
    
    if verbose:
        print(f"\nScanning source ({source_type}): {source_container}/{source_prefix or '(root)'}")
    
    source_files = source_client.list_files_recursive(source_container_client, source_prefix)
    total_source_files = len(source_files)
    
    if patterns:
        source_files = filter_files_by_patterns(source_files, patterns)
        if verbose:
            print(f"Found {total_source_files} files in source, {len(source_files)} matching pattern(s): {', '.join(patterns)}")
    else:
        if verbose:
            print(f"Found {len(source_files)} files in source")
    
    if verbose:
        print(f"\nScanning destination ({dest_type}): {dest_container}/{dest_prefix or '(root)'}")
    
    dest_files = dest_client.list_files_recursive(dest_container_client, dest_prefix)
    
    if verbose:
        print(f"Found {len(dest_files)} files in destination")
    
    stats = {"copied": 0, "archived": 0, "skipped": 0, "filtered": 0, "errors": 0}
    
    if verbose:
        print("\n" + "=" * 60)
        print("Sync Operations")
        print("=" * 60)
    
    for relative_name, source_props in source_files.items():
        dest_file_path = f"{dest_prefix}/{relative_name}".lstrip("/") if dest_prefix else relative_name
        dest_props = dest_files.get(relative_name)
        
        if not should_copy_file(source_props, dest_props, skip_unchanged):
            if verbose:
                print(f"[SKIP] {relative_name} (unchanged)")
            stats["skipped"] += 1
            continue
        
        file_exists = dest_props is not None
        
        if file_exists and archive_existing:
            archive_path = generate_archive_name(dest_file_path, archive_folder)
            if verbose:
                print(f"[ARCHIVE] {dest_file_path} -> {archive_path}")
            
            if not dry_run:
                try:
                    archive_file(dest_client, dest_container_client, dest_file_path, archive_folder, dry_run=False)
                    stats["archived"] += 1
                except Exception as e:
                    if verbose:
                        print(f"  ERROR archiving: {e}")
                    stats["errors"] += 1
                    continue
            else:
                stats["archived"] += 1
        
        action = "COPY" if not file_exists else "REPLACE"
        if verbose:
            print(f"[{action}] {source_props['name']} -> {dest_file_path}")
        
        if not dry_run:
            try:
                file_data = source_client.download_file(source_container_client, source_props["name"])
                
                dest_client.ensure_directory_exists(dest_container_client, dest_file_path)
                dest_client.upload_file(dest_container_client, dest_file_path, file_data, overwrite=True)
                stats["copied"] += 1
            except Exception as e:
                if verbose:
                    print(f"  ERROR copying: {e}")
                stats["errors"] += 1
        else:
            stats["copied"] += 1
    
    return stats
