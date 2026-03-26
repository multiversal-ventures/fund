import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from upload import upload_to_storage, collect_parquet_files

def test_collect_parquet_files(tmp_path):
    (tmp_path / "census").mkdir()
    (tmp_path / "census" / "acs_2023.parquet").write_bytes(b"fake")
    (tmp_path / "hud").mkdir()
    (tmp_path / "hud" / "fha_multifamily.parquet").write_bytes(b"fake")
    (tmp_path / "scored").mkdir()
    (tmp_path / "scored" / "properties.parquet").write_bytes(b"fake")

    files = collect_parquet_files(str(tmp_path))
    assert len(files) == 3
    assert any("census/acs_2023.parquet" in f[1] for f in files)

@patch("upload.storage")
@patch("upload.firebase_admin")
def test_upload_to_storage(mock_admin, mock_storage, tmp_path):
    (tmp_path / "test.parquet").write_bytes(b"fake parquet data")

    mock_bucket = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    upload_to_storage([(str(tmp_path / "test.parquet"), "data/test.parquet")])

    mock_bucket.blob.assert_called_once_with("data/test.parquet")
    mock_blob.upload_from_filename.assert_called_once()
