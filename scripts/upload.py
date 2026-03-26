# scripts/upload.py
"""Upload Parquet files to Firebase Storage."""
import json
import firebase_admin
from firebase_admin import storage
from pathlib import Path
from datetime import datetime, timezone


def collect_parquet_files(data_dir: str) -> list[tuple[str, str]]:
    data_path = Path(data_dir)
    files = []
    for pq_file in data_path.rglob("*.parquet"):
        relative = pq_file.relative_to(data_path)
        storage_path = f"data/{relative}"
        files.append((str(pq_file), storage_path))
    return sorted(files, key=lambda x: x[1])


def upload_to_storage(file_pairs: list[tuple[str, str]], bucket_name: str = None):
    if not firebase_admin._apps:
        firebase_admin.initialize_app()
    bucket = storage.bucket(bucket_name)
    for local_path, storage_path in file_pairs:
        blob = bucket.blob(storage_path)
        blob.upload_from_filename(local_path, content_type="application/octet-stream")
        print(f"  Uploaded {storage_path} ({Path(local_path).stat().st_size:,} bytes)")


def upload_meta(data_dir: str, config: dict, bucket_name: str = None):
    if not firebase_admin._apps:
        firebase_admin.initialize_app()
    bucket = storage.bucket(bucket_name)
    blob = bucket.blob("data/meta/last_run.json")
    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "files": [f[1] for f in collect_parquet_files(data_dir)],
    }
    blob.upload_from_string(json.dumps(meta, indent=2, default=str), content_type="application/json")
    print(f"  Uploaded data/meta/last_run.json")


def upload_all(data_dir: str, config: dict, bucket_name: str = None):
    print("Uploading to Firebase Storage...")
    file_pairs = collect_parquet_files(data_dir)
    upload_to_storage(file_pairs, bucket_name)
    upload_meta(data_dir, config, bucket_name)
    print(f"  Done — {len(file_pairs)} files uploaded.")
