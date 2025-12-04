import pandas as pd
import pytest

from src.read import read_csv, read_json, read


def test_read_csv_success(tmp_path):
    """read_csv should return a DataFrame matching the CSV contents."""
    original_df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    csv_path = tmp_path / "test.csv"
    original_df.to_csv(csv_path, index=False)

    result = read_csv(str(csv_path))

    pd.testing.assert_frame_equal(result, original_df)


def test_read_json_success(tmp_path):
    """read_json should return a DataFrame matching the JSON contents."""
    original_df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    json_path = tmp_path / "test.json"
    original_df.to_json(json_path)

    result = read_json(str(json_path))

    pd.testing.assert_frame_equal(result, original_df)


def test_read_dispatches_to_csv(tmp_path):
    """read() should call read_csv when type is 'csv'."""
    original_df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    csv_path = tmp_path / "test.csv"
    original_df.to_csv(csv_path, index=False)

    cfg = {"type": "csv", "path": str(csv_path)}

    result = read(cfg)

    pd.testing.assert_frame_equal(result, original_df)


def test_read_dispatches_to_json(tmp_path):
    """read() should call read_json when type is 'json'."""
    original_df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    json_path = tmp_path / "test.json"
    original_df.to_json(json_path)

    cfg = {"type": "json", "path": str(json_path)}

    result = read(cfg)

    pd.testing.assert_frame_equal(result, original_df)


def test_read_csv_missing_file_raises(tmp_path):
    """read_csv should propagate FileNotFoundError for missing files."""
    missing_path = tmp_path / "does_not_exist.csv"

    with pytest.raises(FileNotFoundError):
        read_csv(str(missing_path))


def test_read_json_missing_file_raises(tmp_path):
    """read_json should propagate FileNotFoundError for missing files."""
    missing_path = tmp_path / "does_not_exist.json"

    with pytest.raises(FileNotFoundError):
        read_json(str(missing_path))


def test_read_unsupported_source_type_raises():
    """read() should raise ValueError for unsupported types."""
    cfg = {"type": "xml", "path": "some/path/file.xml"}

    with pytest.raises(ValueError, match="Unsupported source type"):
        read(cfg)
