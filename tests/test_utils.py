"""Tests for utility functions."""

import pytest
from storage_sync.utils import (
    filter_files_by_patterns,
    generate_archive_name,
    should_copy_file,
    get_storage_label,
)
from datetime import datetime, timezone


class TestFilterFilesByPatterns:
    """Tests for filter_files_by_patterns function."""
    
    def test_no_patterns_returns_all(self):
        files = {
            "file1.csv": {"name": "file1.csv"},
            "file2.parquet": {"name": "file2.parquet"},
            "file3.json": {"name": "file3.json"},
        }
        result = filter_files_by_patterns(files, None)
        assert result == files
        
        result = filter_files_by_patterns(files, [])
        assert result == files
    
    def test_single_pattern(self):
        files = {
            "file1.csv": {"name": "file1.csv"},
            "file2.parquet": {"name": "file2.parquet"},
            "data.csv": {"name": "data.csv"},
        }
        result = filter_files_by_patterns(files, ["*.csv"])
        assert len(result) == 2
        assert "file1.csv" in result
        assert "data.csv" in result
        assert "file2.parquet" not in result
    
    def test_multiple_patterns(self):
        files = {
            "file1.csv": {"name": "file1.csv"},
            "file2.parquet": {"name": "file2.parquet"},
            "file3.json": {"name": "file3.json"},
        }
        result = filter_files_by_patterns(files, ["*.csv", "*.parquet"])
        assert len(result) == 2
        assert "file1.csv" in result
        assert "file2.parquet" in result
        assert "file3.json" not in result
    
    def test_pattern_with_prefix(self):
        files = {
            "report_2024.csv": {"name": "report_2024.csv"},
            "data_2024.csv": {"name": "data_2024.csv"},
            "report_2023.csv": {"name": "report_2023.csv"},
        }
        result = filter_files_by_patterns(files, ["report_*.csv"])
        assert len(result) == 2
        assert "report_2024.csv" in result
        assert "report_2023.csv" in result
    
    def test_path_pattern(self):
        files = {
            "2024/01/data.csv": {"name": "2024/01/data.csv"},
            "2024/02/data.csv": {"name": "2024/02/data.csv"},
            "2023/12/data.csv": {"name": "2023/12/data.csv"},
        }
        result = filter_files_by_patterns(files, ["2024/*/*.csv"])
        assert len(result) == 2


class TestGenerateArchiveName:
    """Tests for generate_archive_name function."""
    
    def test_simple_file(self):
        result = generate_archive_name("report.csv")
        assert result.startswith("Archive/report_")
        assert result.endswith(".csv")
    
    def test_file_with_path(self):
        result = generate_archive_name("data/raw/report.csv")
        assert result.startswith("data/raw/Archive/report_")
        assert result.endswith(".csv")
    
    def test_file_without_extension(self):
        result = generate_archive_name("README")
        assert result.startswith("Archive/README_")
    
    def test_custom_archive_folder(self):
        result = generate_archive_name("report.csv", archive_folder="Backup")
        assert result.startswith("Backup/report_")


class TestShouldCopyFile:
    """Tests for should_copy_file function."""
    
    def test_dest_not_exists(self):
        source = {"size": 100, "last_modified": datetime.now(timezone.utc)}
        assert should_copy_file(source, None, skip_unchanged=True) is True
        assert should_copy_file(source, None, skip_unchanged=False) is True
    
    def test_skip_unchanged_false(self):
        now = datetime.now(timezone.utc)
        source = {"size": 100, "last_modified": now}
        dest = {"size": 100, "last_modified": now}
        assert should_copy_file(source, dest, skip_unchanged=False) is True
    
    def test_skip_unchanged_same_file(self):
        now = datetime.now(timezone.utc)
        source = {"size": 100, "last_modified": now}
        dest = {"size": 100, "last_modified": now}
        assert should_copy_file(source, dest, skip_unchanged=True) is False
    
    def test_skip_unchanged_source_newer(self):
        from datetime import timedelta
        older = datetime.now(timezone.utc)
        newer = older + timedelta(hours=1)
        source = {"size": 100, "last_modified": newer}
        dest = {"size": 100, "last_modified": older}
        assert should_copy_file(source, dest, skip_unchanged=True) is True
    
    def test_different_size_always_copy(self):
        now = datetime.now(timezone.utc)
        source = {"size": 200, "last_modified": now}
        dest = {"size": 100, "last_modified": now}
        assert should_copy_file(source, dest, skip_unchanged=True) is True


class TestGetStorageLabel:
    """Tests for get_storage_label function."""
    
    def test_known_types(self):
        assert get_storage_label("local") == "Local FileSystem"
        assert get_storage_label("dfs") == "ADLS Gen2/DFS"
        assert get_storage_label("blob") == "Blob Storage"
    
    def test_unknown_type(self):
        assert get_storage_label("unknown") == "unknown"
