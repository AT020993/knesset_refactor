# Cloud Compatibility Test Suite

Tests ensuring the application works identically in local development and Streamlit Cloud environments.

## Test Categories

### Unit Tests (`test_cloud_compatibility.py`)

| Class | Tests | Coverage |
|-------|-------|----------|
| `TestCloudCredentialLoading` | 5 | GCS credential loading from base64, JSON fields, env vars, graceful fallback |
| `TestCloudStorageOperations` | 6 | Upload/download with graceful degradation, sync service behavior |
| `TestAsyncStreamlitPatterns` | 5 | Async handling in Streamlit vs CLI contexts, thread isolation, tqdm safety |
| `TestDatabasePersistence` | 6 | Connection management, leak detection, sequences, concurrent access |
| `TestSessionStatePatterns` | 6 | State persistence, timeout detection, logout clearing, widget state |
| `TestResourceConstraints` | 6 | Memory limits, caching patterns, lazy loading, connection pool limits |

**Total: 34 unit tests**

### Integration Tests (`test_cloud_integration.py`)

| Class | Tests | Coverage |
|-------|-------|----------|
| `TestCloudDeploymentScenarios` | 5 | Fresh deployment, annotation sync, startup without GCS, sync cycle |
| `TestSecretsConfiguration` | 3 | Base64 decoding, missing sections handling, CAP secrets structure |
| `TestConcurrentAccess` | 2 | Multi-researcher annotation, upsert behavior |

**Total: 10 integration tests**

## Running Tests

```bash
# All cloud tests (mocked)
pytest tests/test_cloud_compatibility.py tests/test_cloud_integration.py -v

# With real GCS (requires credentials)
GCS_TEST_BUCKET=your-bucket pytest tests/test_cloud_integration.py -v

# Fast subset (unit tests only)
pytest tests/test_cloud_compatibility.py -v

# Just integration tests
pytest tests/test_cloud_integration.py -v -m integration
```

## Fixtures (`tests/fixtures/cloud_fixtures.py`)

| Fixture | Purpose |
|---------|---------|
| `mock_streamlit_secrets` | Factory for mocked st.secrets with configurable GCS/CAP options |
| `mock_gcs_client` | Mocked GCS client with bucket/blob chain operations |
| `streamlit_context` | Simulates running event loop (Streamlit's Tornado context) |
| `cli_context` | Simulates no event loop (CLI execution mode) |
| `mock_session_state` | Dict-like session state mock with attribute/dict access |
| `resource_limited_environment` | Simulates Streamlit Cloud free tier memory constraints |
| `mock_gcs_manager` | Complete mock of GCSManager class |
| `mock_storage_sync_service` | Mock of StorageSyncService for sync operations |
| `cloud_environment_vars` | Factory for setting cloud environment variables |

## Key Patterns Tested

### 1. Credential Loading Priority
```
credentials_base64 > JSON fields > GOOGLE_APPLICATION_CREDENTIALS env var
```
Tests verify fallback chain works correctly.

### 2. Graceful Degradation
All cloud operations return `False`/`None` on failure instead of raising exceptions:
- Upload failures return `False`
- Download of non-existent files returns `False`
- Missing GCS credentials disable sync service gracefully

### 3. Thread Isolation for Async Code
Streamlit runs on Tornado with its own event loop. Tests verify:
- `asyncio.get_running_loop()` detection works
- Thread isolation pattern executes async code correctly
- `DataRefreshService.refresh_tables_sync()` handles both contexts

### 4. Session Timeout
- 2-hour default timeout
- Checked via `CAPAuthHandler.is_session_valid()`
- Expired sessions require re-authentication

### 5. Connection Management
- Context manager pattern (`get_db_connection`)
- Leak detection via `_connection_monitor`
- Read-only connections prevent writes
- Sequences persist across connections

### 6. Multi-Annotator Support
- `UNIQUE(BillID, ResearcherID)` constraint
- Upsert via `ON CONFLICT ... DO UPDATE`
- Multiple researchers can annotate same bill

## Test Data

Mock credentials in `cloud_fixtures.py`:
- `MOCK_GCS_CREDENTIALS`: Dict with fake service account fields
- `MOCK_GCS_CREDENTIALS_BASE64`: Base64-encoded version for Streamlit secrets

## Notes

- Tests marked `@pytest.mark.integration` are integration tests
- Tests marked `@pytest.mark.slow` involve timeouts/delays
- Real GCS tests require `GCS_TEST_BUCKET` environment variable
- The `conftest.py` autouse fixture disables GCS sync by default in tests
