# Streamlit Cloud Deployment with Google Cloud Storage

Deploy Knesset data platform to Streamlit Cloud with persistent storage using GCS.

## Prerequisites

- GitHub account with private knesset_refactor repository
- Google Cloud account (free tier sufficient)
- Streamlit Cloud account (free at https://share.streamlit.io)

---

## Google Cloud Setup

### 1. Create Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create project: `knesset-data-platform`
3. Note **Project ID**

### 2. Create Storage Bucket
1. Go to **Storage** → **Browser** → **"Create Bucket"**
2. Configure:
   - **Name**: Globally unique (e.g., `knesset-data-institute-2025`)
   - **Location**: Region closest to users
   - **Storage class**: Standard
   - **Access control**: Uniform
3. Save bucket name

**Cost**: ~56 MB data, well within 5 GB free tier (always free)

### 3. Create Service Account
1. **IAM & Admin** → **Service Accounts** → **"Create Service Account"**
2. Configure:
   - **Name**: `knesset-streamlit-storage`
   - **Role**: **Storage Admin**
3. Done

### 4. Create JSON Key
1. Open service account → **"Keys"** tab
2. **"Add Key"** → **"Create new key"** → **JSON**
3. Download and save JSON file securely

**⚠️ IMPORTANT**: Never commit JSON key to git!

---

## Streamlit Cloud Deployment

### 1. Push Code to GitHub
Already done with private repository!

### 2. Deploy App
1. Go to [Streamlit Cloud](https://share.streamlit.io)
2. Sign in with GitHub
3. **"New app"**:
   - **Repository**: `your-username/knesset_refactor`
   - **Branch**: `main`
   - **Main file**: `src/ui/data_refresh.py`
   - **App URL**: Custom name (e.g., `knesset-data`)
4. Click **"Deploy"** (will fail initially - need secrets)

### 3. Configure Secrets
1. Click **"⚙️ Settings"** → **"Secrets"**
2. Paste this structure (replace with your values):

```toml
# Storage Configuration
[storage]
gcs_bucket_name = "your-bucket-name-here"
enable_cloud_storage = true

# GCP Service Account (from JSON key file)
[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "abc123..."
private_key = "-----BEGIN PRIVATE KEY-----\nYour key\n-----END PRIVATE KEY-----\n"
client_email = "knesset-streamlit-storage@your-project.iam.gserviceaccount.com"
client_id = "123456789..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
universe_domain = "googleapis.com"
```

3. Click **"Save"** → App redeploys automatically

**Tip**: Use `.streamlit/secrets.toml.example` as template

---

## How Cloud Storage Works

**First Deployment**: No data → Researcher refreshes → Data fetched from API → Saved locally + uploaded to GCS → Persists

**Subsequent Restarts**: App checks GCS → Downloads data → Ready immediately

**Data Refresh**: Fetches from API → Saves locally → Uploads to GCS → Stays in sync

**Disable Cloud Storage**: Set `enable_cloud_storage = false` in secrets or remove storage sections

---

## Testing

### 1. Check Logs
App → **"⚙️"** → **"Logs"** → Look for: `"Storage sync service initialized with GCS"`

### 2. Test Data Refresh
1. Select all tables → **"🔄 Refresh Selected Data"**
2. Wait 5-15 minutes
3. Check logs: `"Successfully synced data to cloud storage"`

### 3. Verify in Google Cloud
Go to [Storage Browser](https://console.cloud.google.com/storage/browser) → Your bucket → Should see:
- `data/warehouse.duckdb` (~40-50 MB)
- `data/parquet/` folder
- `data/faction_coalition_status.csv`

### 4. Test Persistence
1. **"⚙️ Settings"** → **"Reboot app"**
2. Check logs: `"Syncing data from cloud storage..."`
3. App loads with data immediately!

---

## Troubleshooting

**"Cloud storage not enabled"**: Check secrets configured, `enable_cloud_storage = true`, bucket name correct

**"Permission denied"**: Verify service account has **Storage Admin** role, bucket name matches, regenerate key if needed

**"Bucket not found"**: Check bucket name in secrets, bucket exists in Console, service account has access

**Data not syncing**: Check logs for errors, verify permissions, ensure secrets enabled

**Slow startup**: Normal for first download (~30-60s for 56 MB), subsequent restarts faster, use regional bucket

---

## Cost Breakdown

**Google Cloud Storage (Free Tier)**:
- Your usage: ~56 MB (0.056 GB)
- Free tier: 5 GB/month (you use 1.1%)
- **Cost**: $0.00/month (within free tier)
- Even if exceeded: < $0.05/month

**Streamlit Cloud**: Free tier (1 app, unlimited users), **Cost**: $0.00/month

---

## Next Steps

1. ✅ Share app URL with researchers
2. ✅ First researcher runs data refresh (one-time)
3. ✅ All users have instant access
4. ✅ Data auto-syncs when refreshed
5. ✅ Code stays private, data is public

**Your app is deployed with persistent cloud storage! 🎉**

Access at: `https://your-app-name.streamlit.app`
