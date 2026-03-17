"""Command-line interface for storage sync."""

import argparse
import sys

from storage_sync.sync import sync_storage
from storage_sync.utils import get_storage_label


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Sync files between different storage locations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Storage Types:
  local - Local file system
  dfs   - ADLS Gen2 (Data Lake Storage), recommended for Fabric LakeHouse
  blob  - Azure Blob Storage

Authentication (for Azure storage):
  Option 1: Connection string (--source-conn, --dest-conn)
  Option 2: Account URL with Azure Identity (--source-url, --dest-url)
            Uses DefaultAzureCredential (managed identity, CLI, env vars, etc.)
  Note: Local storage doesn't require authentication

Wildcard Patterns (--pattern):
  Supports standard wildcards: * (any chars), ? (single char)
  Can match filename or full relative path
  Examples: "*.csv", "*.parquet", "report_*.csv", "2024/*/*.json"

Flow:
  1. Recursively scan source and destination (with optional pattern filtering)
  2. For each file that exists in target: move to Archive folder with timestamp
     Example: data/report.csv -> data/Archive/report_20260317_143052.csv
  3. Copy new file from source to destination

Examples:
  # Local to ADLS Gen2 (upload)
  storage-sync --source-type local --source-container "C:/data/export" \\
               --dest-type dfs --dest-url "https://account.dfs.core.windows.net" \\
               --dest-container mycontainer

  # Sync only CSV files
  storage-sync --source-type local --source-container "C:/data" \\
               --dest-type dfs --dest-url "..." --dest-container data \\
               --pattern "*.csv"

  # Sync multiple file types
  storage-sync --source-type dfs --source-url "..." --source-container lakehouse \\
               --dest-type local --dest-container "C:/backup" \\
               --pattern "*.csv" --pattern "*.parquet"

  # ADLS Gen2 to local (download)
  storage-sync --source-type dfs --source-url "https://account.dfs.core.windows.net" \\
               --source-container mycontainer --source-prefix raw/2024 \\
               --dest-type local --dest-container "C:/backup/data"

  # Fabric LakeHouse to local
  storage-sync --source-type dfs --source-url "https://onelake.dfs.fabric.microsoft.com" \\
               --source-container "workspace/lakehouse.Lakehouse/Files" \\
               --dest-type local --dest-container "C:/lakehouse-backup"

  # DFS to DFS (cloud to cloud)
  storage-sync --source-type dfs --source-url "https://source.dfs.core.windows.net" \\
               --source-container data \\
               --dest-type dfs --dest-url "https://dest.dfs.core.windows.net" \\
               --dest-container backup

  # Dry run to preview changes
  storage-sync --source-type local --source-container "./data" \\
               --dest-type dfs --dest-url "..." --dest-container container \\
               --dry-run
        """
    )
    
    parser.add_argument("--source-type", choices=["local", "dfs", "blob"], default="dfs",
                        help="Source storage type (default: dfs)")
    parser.add_argument("--source-conn",
                        help="Source storage connection string (for Azure storage)")
    parser.add_argument("--source-url",
                        help="Source account URL (uses Azure Identity)")
    parser.add_argument("--source-container", required=True,
                        help="Source container/filesystem/folder path")
    parser.add_argument("--source-prefix", default="",
                        help="Source prefix/subfolder path (default: root)")
    
    parser.add_argument("--dest-type", choices=["local", "dfs", "blob"], default="dfs",
                        help="Destination storage type (default: dfs)")
    parser.add_argument("--dest-conn",
                        help="Destination storage connection string (for Azure storage)")
    parser.add_argument("--dest-url",
                        help="Destination account URL (uses Azure Identity)")
    parser.add_argument("--dest-container", required=True,
                        help="Destination container/filesystem/folder path")
    parser.add_argument("--dest-prefix", default="",
                        help="Destination prefix/subfolder path (default: root)")
    
    parser.add_argument("--pattern", action="append", dest="patterns",
                        help="Wildcard pattern to filter files (e.g., *.csv). Can be used multiple times.")
    parser.add_argument("--no-archive", action="store_true",
                        help="Overwrite existing files without archiving (default: archive)")
    parser.add_argument("--archive-folder", default="Archive",
                        help="Name of archive folder (default: Archive)")
    parser.add_argument("--skip-unchanged", action="store_true",
                        help="Skip files that haven't changed (same size and not newer)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without making them")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Suppress progress output")
    
    args = parser.parse_args()
    
    if args.source_type != "local" and not args.source_conn and not args.source_url:
        parser.error(f"--source-conn or --source-url is required for {args.source_type} storage")
    
    if args.dest_type != "local" and not args.dest_conn and not args.dest_url:
        parser.error(f"--dest-conn or --dest-url is required for {args.dest_type} storage")
    
    archive_existing = not args.no_archive
    source_label = get_storage_label(args.source_type)
    dest_label = get_storage_label(args.dest_type)
    verbose = not args.quiet
    
    if args.source_type == "local":
        source_auth_method = "N/A"
    else:
        source_auth_method = "Connection String" if args.source_conn else "Azure Identity"
    
    if args.dest_type == "local":
        dest_auth_method = "N/A"
    else:
        dest_auth_method = "Connection String" if args.dest_conn else "Azure Identity"
    
    patterns_display = ", ".join(args.patterns) if args.patterns else "(all files)"
    
    if verbose:
        print("=" * 60)
        print(f"Storage Sync: {source_label} -> {dest_label}")
        print("=" * 60)
        print(f"Source type:        {args.source_type}")
        print(f"Source auth:        {source_auth_method}")
        print(f"Source location:    {args.source_container}/{args.source_prefix or '(root)'}")
        print(f"Dest type:          {args.dest_type}")
        print(f"Dest auth:          {dest_auth_method}")
        print(f"Dest location:      {args.dest_container}/{args.dest_prefix or '(root)'}")
        print(f"File patterns:      {patterns_display}")
        print(f"Archive existing:   {archive_existing}")
        print(f"Archive folder:     {args.archive_folder}")
        print(f"Skip unchanged:     {args.skip_unchanged}")
        print(f"Dry run:            {args.dry_run}")
        
        if args.dry_run:
            print("\n*** DRY RUN MODE - No changes will be made ***")
    
    try:
        stats = sync_storage(
            source_type=args.source_type,
            source_container=args.source_container,
            source_prefix=args.source_prefix,
            dest_type=args.dest_type,
            dest_container=args.dest_container,
            dest_prefix=args.dest_prefix,
            source_connection_string=args.source_conn,
            source_account_url=args.source_url,
            dest_connection_string=args.dest_conn,
            dest_account_url=args.dest_url,
            patterns=args.patterns,
            archive_existing=archive_existing,
            archive_folder=args.archive_folder,
            skip_unchanged=args.skip_unchanged,
            dry_run=args.dry_run,
            verbose=verbose
        )
        
        if verbose:
            print("\n" + "=" * 60)
            print("Summary")
            print("=" * 60)
            print(f"Copied:   {stats['copied']}")
            print(f"Archived: {stats['archived']}")
            print(f"Skipped:  {stats['skipped']}")
            print(f"Errors:   {stats['errors']}")
            
            if args.dry_run:
                print("\n*** This was a dry run - no actual changes were made ***")
        
        sys.exit(0 if stats['errors'] == 0 else 1)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
