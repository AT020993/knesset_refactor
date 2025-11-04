"""
One-time script to upload local data to Google Cloud Storage.

Usage:
    python upload_to_gcs.py

Prerequisites:
    1. Create a GCS bucket in Google Cloud Console
    2. Create a service account with Storage Admin role
    3. Download the JSON key file
    4. Set environment variable: export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
    5. Update BUCKET_NAME below with your bucket name
"""

from pathlib import Path
from google.cloud import storage
import sys

# CONFIGURATION - UPDATE THIS WITH YOUR BUCKET NAME
BUCKET_NAME = "knesset-data-yourname-2025"  # Change this to your bucket name

def upload_data_to_gcs():
    """Upload all local data files to Google Cloud Storage."""

    # Initialize GCS client
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        print(f"✓ Connected to bucket: {BUCKET_NAME}")
    except Exception as e:
        print(f"✗ Failed to connect to GCS: {e}")
        print("\nMake sure you:")
        print("  1. Created a GCS bucket")
        print("  2. Downloaded service account JSON key")
        print("  3. Set environment variable:")
        print('     export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"')
        sys.exit(1)

    # Files to upload
    project_root = Path(__file__).parent
    files_to_upload = [
        ("data/warehouse.duckdb", "data/warehouse.duckdb"),
        ("data/faction_coalition_status.csv", "data/faction_coalition_status.csv"),
    ]

    # Add all parquet files
    parquet_dir = project_root / "data" / "parquet"
    if parquet_dir.exists():
        for parquet_file in parquet_dir.glob("*.parquet"):
            local_path = f"data/parquet/{parquet_file.name}"
            gcs_path = f"data/parquet/{parquet_file.name}"
            files_to_upload.append((local_path, gcs_path))

    # Upload each file
    success_count = 0
    fail_count = 0

    for local_path, gcs_path in files_to_upload:
        full_local_path = project_root / local_path

        if not full_local_path.exists():
            print(f"⊘ Skipping {local_path} (not found)")
            continue

        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(str(full_local_path))

            size_mb = full_local_path.stat().st_size / (1024 * 1024)
            print(f"✓ Uploaded {local_path} ({size_mb:.2f} MB) → gs://{BUCKET_NAME}/{gcs_path}")
            success_count += 1
        except Exception as e:
            print(f"✗ Failed to upload {local_path}: {e}")
            fail_count += 1

    # Summary
    print("\n" + "="*60)
    print(f"Upload complete: {success_count} succeeded, {fail_count} failed")
    print("="*60)

    if success_count > 0:
        print("\nNext steps:")
        print("  1. Go to Streamlit Cloud app settings")
        print("  2. Navigate to 'Secrets' section")
        print("  3. Add your GCS configuration (see .streamlit/secrets.toml.example)")
        print("  4. Redeploy your app")
        print("\nYour app will then automatically download data from GCS on startup!")

if __name__ == "__main__":
    upload_data_to_gcs()
