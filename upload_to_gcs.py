#!/usr/bin/env python3
"""Upload local data files to Google Cloud Storage.

Usage:
    # Use credential resolver (auto-detects from env, .env, or secrets)
    python upload_to_gcs.py

    # Or set bucket name via environment variable
    export GCS_BUCKET_NAME="your-bucket-name"
    export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
    python upload_to_gcs.py

    # Or pass bucket name as argument
    python upload_to_gcs.py --bucket your-bucket-name

    # Preview what would be uploaded without actually uploading
    python upload_to_gcs.py --dry-run

Prerequisites:
    1. Create a GCS bucket in Google Cloud Console
    2. Create a service account with Storage Admin role
    3. Download the JSON key file
    4. Either:
       a. Set GOOGLE_APPLICATION_CREDENTIALS environment variable, OR
       b. Create a .env file with credentials, OR
       c. Configure .streamlit/secrets.toml
"""

import argparse
import os
import sys
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account


def get_credentials_from_resolver():
    """Try to load credentials using the GCSCredentialResolver."""
    try:
        from data.storage.credential_resolver import GCSCredentialResolver
        credentials_dict, bucket_name = GCSCredentialResolver.resolve()
        if credentials_dict and bucket_name:
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            return bucket_name, credentials
    except ImportError:
        print(
            "Import failed. Run with `PYTHONPATH=./src python upload_to_gcs.py ...`."
        )
    except Exception as e:
        print(f"Warning: Credential resolver error: {e}")
    return None, None


def get_bucket_and_credentials(args_bucket: str = None):
    """Get bucket name and credentials from various sources.

    Priority: CLI argument > environment variable > credential resolver
    """
    # Try credential resolver first (handles Streamlit secrets, env vars, .env)
    resolver_bucket, resolver_credentials = get_credentials_from_resolver()

    # Determine bucket name
    if args_bucket:
        bucket_name = args_bucket
    elif os.environ.get("GCS_BUCKET_NAME"):
        bucket_name = os.environ["GCS_BUCKET_NAME"]
    elif resolver_bucket:
        bucket_name = resolver_bucket
        print(f"Using bucket from credential resolver: {bucket_name}")
    else:
        print("ERROR: No GCS bucket specified.")
        print("\nPlease specify bucket name via:")
        print("  1. Command line: python upload_to_gcs.py --bucket YOUR_BUCKET")
        print("  2. Environment variable: export GCS_BUCKET_NAME=YOUR_BUCKET")
        print("  3. .env file: GCS_BUCKET_NAME=YOUR_BUCKET")
        print("  4. Streamlit secrets: .streamlit/secrets.toml [storage] gcs_bucket_name")
        sys.exit(1)

    # Use resolved credentials if available, otherwise let GCS use env var
    credentials = resolver_credentials

    return bucket_name, credentials


def main():
    """Main entry point for upload script."""
    parser = argparse.ArgumentParser(
        description="Upload data files to Google Cloud Storage"
    )
    parser.add_argument("--bucket", "-b", help="GCS bucket name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading",
    )
    args = parser.parse_args()

    bucket_name, credentials = get_bucket_and_credentials(args.bucket)
    print(f"Using GCS bucket: {bucket_name}")

    # Project root for finding data files
    project_root = Path(__file__).parent

    # Files to upload
    files_to_upload = [
        ("data/warehouse.duckdb", "data/warehouse.duckdb"),
        ("data/faction_coalition_status.csv", "data/faction_coalition_status.csv"),
    ]

    # Add all parquet files
    parquet_dir = project_root / "data" / "parquet"
    if parquet_dir.exists():
        for pq_file in parquet_dir.glob("*.parquet"):
            files_to_upload.append(
                (f"data/parquet/{pq_file.name}", f"data/parquet/{pq_file.name}")
            )

    # Dry run mode - just show what would be uploaded
    if args.dry_run:
        print("\n🔍 Dry run - would upload:")
        for local_path, remote_path in files_to_upload:
            full_path = project_root / local_path
            if full_path.exists():
                size_mb = full_path.stat().st_size / (1024 * 1024)
                print(f"  {local_path} -> gs://{bucket_name}/{remote_path} ({size_mb:.1f} MB)")
            else:
                print(f"  {local_path} -> SKIPPED (file not found)")
        return

    # Connect to GCS
    try:
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        print(f"✓ Connected to bucket: {bucket_name}")
    except Exception as e:
        print(f"✗ Failed to connect to GCS: {e}")
        print("\nMake sure you:")
        print("  1. Created a GCS bucket")
        print("  2. Downloaded service account JSON key")
        print("  3. Set environment variable:")
        print('     export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"')
        sys.exit(1)

    # Upload files
    success_count = 0
    fail_count = 0

    for local_path, remote_path in files_to_upload:
        full_local_path = project_root / local_path

        if not full_local_path.exists():
            print(f"⊘ Skipping {local_path} (not found)")
            continue

        try:
            print(f"Uploading {local_path}...")
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(str(full_local_path))

            size_mb = full_local_path.stat().st_size / (1024 * 1024)
            print(f"  ✓ -> gs://{bucket_name}/{remote_path} ({size_mb:.2f} MB)")
            success_count += 1
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            fail_count += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"Upload complete: {success_count} succeeded, {fail_count} failed")
    print("=" * 60)

    if success_count > 0:
        print("\nNext steps:")
        print("  1. Go to Streamlit Cloud app settings")
        print("  2. Navigate to 'Secrets' section")
        print("  3. Add your GCS configuration (see .streamlit/secrets.toml.example)")
        print("  4. Redeploy your app")
        print("\nYour app will then automatically download data from GCS on startup!")


if __name__ == "__main__":
    main()
