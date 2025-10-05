# Streamlit Cloud Deployment Guide with Google Cloud Storage

This guide will walk you through deploying your Knesset data platform to Streamlit Cloud with persistent storage using Google Cloud Storage (GCS).

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Google Cloud Setup](#google-cloud-setup)
3. [Streamlit Cloud Deployment](#streamlit-cloud-deployment)
4. [Configuration](#configuration)
5. [Testing](#testing)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

- GitHub account (with your private knesset_refactor repository)
- Google Cloud account (free tier is sufficient)
- Streamlit Cloud account (free, sign up at https://share.streamlit.io)

---

## Google Cloud Setup

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click **"Create Project"** or select an existing project
3. Name it something like `knesset-data-platform`
4. Note your **Project ID** (you'll need this later)

### Step 2: Create a Storage Bucket

1. In Google Cloud Console, go to **Storage** → **Browser**
2. Click **"Create Bucket"**
3. Configure your bucket:
   - **Name**: Choose a globally unique name (e.g., `knesset-data-institute-2025`)
   - **Location type**: Region (choose closest to your users, or Multi-region)
   - **Storage class**: Standard
   - **Access control**: Uniform
   - **Protection tools**: Leave defaults
4. Click **"Create"**
5. **Save your bucket name** - you'll need it for configuration

**Cost**: Your data is ~56 MB, well within the 5 GB free tier (always free)

### Step 3: Create a Service Account

1. Go to **IAM & Admin** → **Service Accounts**
2. Click **"Create Service Account"**
3. Configure:
   - **Name**: `knesset-streamlit-storage`
   - **Description**: "Service account for Knesset Streamlit app storage access"
4. Click **"Create and Continue"**
5. **Grant permissions**:
   - Click **"Select a role"**
   - Search for and select: **Storage Admin**
   - Click **"Continue"**
6. Click **"Done"**

### Step 4: Create and Download JSON Key

1. Find your newly created service account in the list
2. Click on it to open details
3. Go to the **"Keys"** tab
4. Click **"Add Key"** → **"Create new key"**
5. Select **"JSON"** format
6. Click **"Create"**
7. **Save the downloaded JSON file** - you'll need it for Streamlit secrets

**⚠️ IMPORTANT**: Keep this JSON file secure! Never commit it to git.

---

## Streamlit Cloud Deployment

### Step 1: Push Code to GitHub

Your code is already in a private GitHub repository, so this step is done!

### Step 2: Deploy to Streamlit Cloud

1. Go to [Streamlit Cloud](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click **"New app"**
4. Configure your app:
   - **Repository**: Select `your-username/knesset_refactor`
   - **Branch**: `master` (or your main branch)
   - **Main file path**: `src/ui/data_refresh.py`
   - **App URL**: Choose a custom name (e.g., `knesset-data`)
5. Click **"Deploy"** (don't worry, it will fail initially - we need to add secrets)

### Step 3: Configure Secrets

1. While your app is deploying (or after it fails), click on **"⚙️ Settings"**
2. Go to the **"Secrets"** section
3. Open your downloaded JSON key file from Step 4 of Google Cloud Setup
4. Copy the **entire contents** of the JSON file
5. In Streamlit secrets, paste this structure (replace values with your actual data):

```toml
# Storage Configuration
[storage]
gcs_bucket_name = "your-bucket-name-here"  # From Step 2 of GCS setup
enable_cloud_storage = true

# GCP Service Account (from your JSON key file)
[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "abc123..."
private_key = "-----BEGIN PRIVATE KEY-----\nYour actual key here\n-----END PRIVATE KEY-----\n"
client_email = "knesset-streamlit-storage@your-project.iam.gserviceaccount.com"
client_id = "123456789..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com"
universe_domain = "googleapis.com"
```

6. Click **"Save"**
7. The app will automatically redeploy with the new secrets

**📝 Tip**: You can also use the `.streamlit/secrets.toml.example` file in this repository as a template.

---

## Configuration

### How Cloud Storage Works

#### First Deployment (No Data Yet)
1. App starts → No local data, no cloud data
2. Researchers see: "No data found. Please refresh using sidebar."
3. Researcher clicks **"🔄 Refresh Selected Data"**
4. Data fetched from Knesset OData API
5. Saved locally **and** uploaded to Google Cloud Storage
6. Done! Data persists

#### Subsequent App Restarts
1. App starts → Checks GCS for data
2. Downloads database + parquet files from GCS
3. App ready immediately with data
4. Researchers can use app without refreshing

#### When Researchers Refresh
1. Fetches latest data from Knesset API
2. Saves locally
3. Uploads to GCS automatically
4. Cloud and local stay in sync

### Disabling Cloud Storage

If you want to disable cloud storage (use local only):

**Option 1: In Streamlit Secrets**
```toml
[storage]
enable_cloud_storage = false
```

**Option 2: Remove secrets entirely**
Just delete the `[storage]` and `[gcp_service_account]` sections

---

## Testing

### 1. Check Logs

After deployment, check the app logs:
- Go to your app → Click **"⚙️"** → **"Logs"**
- Look for: `"Storage sync service initialized with GCS"`
- If you see this, cloud storage is working!

### 2. Test Data Refresh

1. In the app sidebar, select all tables
2. Click **"🔄 Refresh Selected Data"**
3. Wait for data to download (5-15 minutes)
4. Check logs for: `"Syncing data to cloud storage..."`
5. Should see: `"Successfully synced data to cloud storage"`

### 3. Verify in Google Cloud

1. Go to [Google Cloud Storage Browser](https://console.cloud.google.com/storage/browser)
2. Click on your bucket
3. You should see:
   - `data/warehouse.duckdb` (~40-50 MB)
   - `data/parquet/` folder with `.parquet` files
   - `data/faction_coalition_status.csv`
   - `data/.resume_state.json`

### 4. Test Persistence

1. In Streamlit Cloud, go to **"⚙️ Settings"** → **"Reboot app"**
2. Wait for app to restart
3. Check logs for: `"Syncing data from cloud storage..."`
4. App should load with data immediately (no refresh needed!)

---

## Troubleshooting

### "Cloud storage not enabled"

**Check**:
- Secrets are properly configured in Streamlit Cloud
- `enable_cloud_storage = true` in secrets
- Bucket name is correct

### "Permission denied" errors

**Solutions**:
- Verify service account has **Storage Admin** role
- Check that bucket name matches exactly
- Regenerate JSON key if needed

### "Bucket not found"

**Solutions**:
- Double-check bucket name in secrets
- Ensure bucket exists in Google Cloud Console
- Verify service account has access to the bucket

### Data not syncing after refresh

**Check logs for**:
- `"Cloud storage sync disabled"` → Enable in secrets
- `"Error during cloud sync"` → Check service account permissions
- `"Successfully synced"` → It's working!

### App is slow on startup

**This is normal** if downloading from cloud storage:
- First download: ~30-60 seconds for 56 MB
- Subsequent restarts: Faster (uses cached data)
- To speed up: Consider regional bucket closer to Streamlit servers

---

## Cost Breakdown

### Google Cloud Storage (Free Tier)

**Your Usage**: ~56 MB total
- Database: ~40 MB
- Parquet files: ~13 MB
- CSV/JSON: < 1 MB

**Free Tier Limits** (always free):
- Storage: 5 GB/month → You use 0.056 GB (1.1%)
- Class A operations: 5,000/month → You use ~10/day (refreshes)
- Class B operations: 50,000/month → You use ~10/day (downloads)
- Egress: 1 GB/month → You use ~0.056 GB per download

**Estimated Cost**: $0.00/month (within free tier)

**Even if you exceed free tier** (unlikely):
- Storage: $0.020/GB/month = $0.001/month for 56 MB
- Operations: $0.05/10,000 = negligible
- **Total**: < $0.05/month

### Streamlit Cloud

**Free tier**: 1 app, unlimited users
**Your usage**: 1 app
**Cost**: $0.00/month

---

## Next Steps

1. ✅ Share the app URL with researchers
2. ✅ First researcher runs data refresh (one-time setup)
3. ✅ All subsequent users have instant access
4. ✅ Data automatically syncs when refreshed
5. ✅ Your code stays private, data is public

## Support

If you encounter issues:
1. Check the app logs first
2. Verify Google Cloud Console → Storage → Your bucket has data
3. Test service account permissions
4. Review this guide's troubleshooting section

---

**Your app is now deployed with persistent cloud storage! 🎉**

Researchers can access it at: `https://your-app-name.streamlit.app`
