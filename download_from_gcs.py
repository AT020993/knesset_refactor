#!/usr/bin/env python3
"""Download data files from Google Cloud Storage to local.

Usage:
    # Use credential resolver (auto-detects from env, .env, or secrets)
    python download_from_gcs.py

    # Or set bucket name via environment variable
    export GCS_BUCKET_NAME="your-bucket-name"
    export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
    python download_from_gcs.py

    # Or pass bucket name as argument
    python download_from_gcs.py --bucket your-bucket-name

    # Preview what would be downloaded
    python download_from_gcs.py --dry-run

    # Download only the database (skip parquet files)
    python download_from_gcs.py --db-only

Prerequisites:
    1. Have access to the GCS bucket
    2. Either:
       a. Set GOOGLE_APPLICATION_CREDENTIALS environment variable, OR
       b. Create a .env file with credentials, OR
       c. Configure .streamlit/secrets.toml
"""

import argparse
import os
import sys
from pathlib import Path

try:
    from google.cloud import storage
    from google.oauth2 import service_account
except ImportError:
    print("ERROR: google-cloud-storage not installed.")
    print("Install with: pip install google-cloud-storage")
    sys.exit(1)


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
            "Import failed. Run with `PYTHONPATH=./src python download_from_gcs.py ...`."
        )
    except Exception as e:
        print(f"Warning: Credential resolver error: {e}")
    return None, None


def get_credentials_from_streamlit_secrets():
    """Try to load credentials from .streamlit/secrets.toml (legacy fallback)."""
    secrets_path = Path(__file__).parent / ".streamlit" / "secrets.toml"

    if not secrets_path.exists():
        return None, None

    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            print("Warning: Could not import tomllib/tomli to read secrets.toml")
            return None, None

    try:
        with open(secrets_path, "rb") as f:
            secrets = tomllib.load(f)

        bucket_name = secrets.get("storage", {}).get("gcs_bucket_name")
        gcp_secrets = secrets.get("gcp_service_account", {})

        credentials = None
        if gcp_secrets:
            # Check for base64 encoded credentials
            if "credentials_base64" in gcp_secrets:
                import base64
                import json
                decoded = base64.b64decode(gcp_secrets["credentials_base64"]).decode("utf-8")
                creds_dict = json.loads(decoded)
                credentials = service_account.Credentials.from_service_account_info(creds_dict)
            # Check for direct fields
            elif "client_email" in gcp_secrets and "private_key" in gcp_secrets:
                credentials = service_account.Credentials.from_service_account_info(dict(gcp_secrets))

        return bucket_name, credentials
    except Exception as e:
        print(f"Warning: Could not parse secrets.toml: {e}")
        return None, None


def get_bucket_and_credentials(args_bucket: str = None):
    """Get bucket name and credentials from various sources.

    Priority: CLI argument > environment variable > credential resolver > Streamlit secrets
    """
    # Try credential resolver first (handles env vars, .env)
    resolver_bucket, resolver_credentials = get_credentials_from_resolver()

    # Also try legacy Streamlit secrets
    secrets_bucket, secrets_credentials = get_credentials_from_streamlit_secrets()

    # Determine bucket name
    if args_bucket:
        bucket_name = args_bucket
    elif os.environ.get("GCS_BUCKET_NAME"):
        bucket_name = os.environ["GCS_BUCKET_NAME"]
    elif resolver_bucket:
        bucket_name = resolver_bucket
        print(f"Using bucket from credential resolver: {bucket_name}")
    elif secrets_bucket:
        bucket_name = secrets_bucket
        print(f"Using bucket from .streamlit/secrets.toml: {bucket_name}")
    else:
        print("ERROR: No GCS bucket specified.")
        print("\nPlease specify bucket name via:")
        print("  1. Command line: python download_from_gcs.py --bucket YOUR_BUCKET")
        print("  2. Environment variable: export GCS_BUCKET_NAME=YOUR_BUCKET")
        print("  3. .env file: GCS_BUCKET_NAME=YOUR_BUCKET")
        print("  4. Streamlit secrets: .streamlit/secrets.toml [storage] gcs_bucket_name")
        sys.exit(1)

    # Determine credentials (resolver takes priority)
    credentials = resolver_credentials or secrets_credentials

    if credentials:
        print("Using credentials from resolver or secrets")
    elif os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print(f"Using credentials from GOOGLE_APPLICATION_CREDENTIALS")

    return bucket_name, credentials


def main():
    """Main entry point for download script."""
    parser = argparse.ArgumentParser(
        description="Download data files from Google Cloud Storage"
    )
    parser.add_argument("--bucket", "-b", help="GCS bucket name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading",
    )
    parser.add_argument(
        "--db-only",
        action="store_true",
        help="Download only the database file, skip parquet files",
    )
    args = parser.parse_args()

    bucket_name, credentials = get_bucket_and_credentials(args.bucket)
    print(f"Using GCS bucket: {bucket_name}")

    # Project root for saving data files
    project_root = Path(__file__).parent

    # Files to download
    files_to_download = [
        ("data/warehouse.duckdb", project_root / "data" / "warehouse.duckdb"),
        ("data/faction_coalition_status.csv", project_root / "data" / "faction_coalition_status.csv"),
    ]

    # Connect to GCS
    try:
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        print(f"Connected to bucket: {bucket_name}")
    except Exception as e:
        print(f"Failed to connect to GCS: {e}")
        print("\nMake sure you have valid credentials configured.")
        sys.exit(1)

    # List parquet files in bucket if not db-only
    if not args.db_only:
        try:
            blobs = client.list_blobs(bucket_name, prefix="data/parquet/")
            for blob in blobs:
                if blob.name.endswith(".parquet"):
                    local_path = project_root / blob.name
                    files_to_download.append((blob.name, local_path))
        except Exception as e:
            print(f"Warning: Could not list parquet files: {e}")

    # Dry run mode - show what would be downloaded
    if args.dry_run:
        print("\nDry run - would download:")
        for gcs_path, local_path in files_to_download:
            blob = bucket.blob(gcs_path)
            try:
                blob.reload()
                size_mb = blob.size / (1024 * 1024)
                status = "EXISTS" if local_path.exists() else "NEW"
                print(f"  gs://{bucket_name}/{gcs_path} -> {local_path.name} ({size_mb:.1f} MB) [{status}]")
            except Exception:
                print(f"  gs://{bucket_name}/{gcs_path} -> NOT FOUND in bucket")
        return

    # Download files
    success_count = 0
    fail_count = 0
    skip_count = 0

    for gcs_path, local_path in files_to_download:
        try:
            blob = bucket.blob(gcs_path)

            if not blob.exists():
                print(f"Skipping {gcs_path} (not found in bucket)")
                skip_count += 1
                continue

            # Ensure directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            print(f"Downloading {gcs_path}...")
            blob.download_to_filename(str(local_path))

            size_mb = local_path.stat().st_size / (1024 * 1024)
            print(f"  -> {local_path.name} ({size_mb:.2f} MB)")
            success_count += 1
        except Exception as e:
            print(f"  Failed: {e}")
            fail_count += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"Download complete: {success_count} succeeded, {fail_count} failed, {skip_count} skipped")
    print("=" * 60)

    if success_count > 0:
        print("\nYour local database is now synced with the cloud!")
        print("Restart your local Streamlit app to see the changes.")


if __name__ == "__main__":
    main()
