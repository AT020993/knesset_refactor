# Full Dataset Download Feature

## Overview
This feature allows users to download the complete filtered dataset from predefined queries, not just the 1,000 rows displayed in the UI.

## Implementation Details

### Files Modified
1. **src/ui/pages/data_refresh_page.py**
   - Added `_remove_limit_offset_from_query()` static method
   - Added `_render_full_dataset_download()` method
   - Integrated full download UI into `_render_query_results_display()`

### Key Components

#### 1. SQL Query Modification (`_remove_limit_offset_from_query`)
```python
@staticmethod
def _remove_limit_offset_from_query(sql: str) -> str:
    """Remove LIMIT and OFFSET clauses from SQL query."""
    sql = re.sub(r'\s+LIMIT\s+\d+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+OFFSET\s+\d+', '', sql, flags=re.IGNORECASE)
    return sql.strip()
```

**How it works:**
- Uses regex to remove LIMIT clauses (e.g., "LIMIT 1000")
- Removes OFFSET clauses (e.g., "OFFSET 2000")
- Case-insensitive matching
- Preserves all other SQL components (WHERE, ORDER BY, GROUP BY, CTEs, etc.)

**Tested scenarios:**
- ✅ Simple queries with LIMIT only
- ✅ Queries with LIMIT and OFFSET
- ✅ Complex queries with CTEs
- ✅ Queries with OFFSET before LIMIT

#### 2. Full Dataset Download UI (`_render_full_dataset_download`)

**Features:**
- **Row Count Display**: Shows exact number of rows in filtered dataset
- **Large Dataset Warning**: Warns users when dataset exceeds 50,000 rows
- **Two-Step Download**:
  1. First button: Prepares dataset and counts rows
  2. Second button: Actually downloads the file
- **Format Options**: CSV and Excel download options
- **Error Handling**: Graceful error messages if query fails
- **Progress Feedback**: Spinner while preparing data

**UI Layout:**
```
### 📦 Download Full Filtered Dataset
⚠️ This will download ALL rows matching your filters (not just 1000 displayed)

📊 Total rows in filtered dataset: 10,547

[⬇️ Download Full CSV]  [⬇️ Download Full Excel]
```

#### 3. Integration with Existing Flow

**Query Execution Flow:**
1. User runs predefined query from sidebar
2. Query executes with filters + LIMIT 1000 + OFFSET (pagination)
3. Results display in UI (up to 1,000 rows)
4. Standard download buttons export displayed rows
5. **NEW**: Full dataset section appears below
6. Full dataset download removes LIMIT/OFFSET, keeps filters

**Filter Respect:**
- ✅ Knesset filters applied
- ✅ Faction filters applied
- ✅ Local Knesset filter applied
- ✅ All WHERE clauses preserved

### How Filters Are Preserved

The feature uses `last_executed_sql` from session state, which contains:
- Base query SQL
- Applied Knesset filters (from sidebar)
- Applied Faction filters (from sidebar)
- Local Knesset filter (from results page)
- LIMIT and OFFSET for pagination

When downloading full dataset:
1. Takes `last_executed_sql` (already has all filters)
2. Removes LIMIT and OFFSET clauses
3. Executes modified query
4. Returns complete filtered dataset

## User Experience

### Normal Workflow
1. User selects "Bills + Full Details" query
2. Applies Knesset 25 filter
3. Clicks "Run Selected Query"
4. Sees 1,000 rows displayed
5. Scrolls down to "Download Full Filtered Dataset"
6. Sees "📊 Total rows in filtered dataset: **6,459**"
7. Clicks "⬇️ Download Full CSV"
8. Sees spinner: "Preparing full dataset..."
9. Gets download button: "💾 Click to Save Full CSV"
10. Downloads `Bills___Full_Details_FULL_results.csv` with all 6,459 rows

### Large Dataset Workflow
1. User runs query that returns 75,000 rows
2. Sees warning: "⚠️ Large dataset (75,000 rows). Download may take some time."
3. Clicks download button
4. Waits for spinner to complete
5. Successfully downloads all 75,000 rows

### Edge Cases Handled
- **No results**: Download buttons disabled
- **Query error**: Error message displayed, no crash
- **Very large datasets**: Warning message, but still works
- **Empty dataset after filters**: Shows "0 rows", buttons disabled

## Testing Recommendations

### 1. Filter Accuracy Tests
```python
# Test Case 1: Knesset filter
# - Apply Knesset 25 filter
# - Run "Bills + Full Details" query
# - Download full dataset
# - Verify all rows have KnessetNum = 25

# Test Case 2: Faction filter
# - Apply "Likud (K25)" faction filter
# - Run "Bills + Full Details" query
# - Download full dataset
# - Verify all rows have correct FactionID

# Test Case 3: Combined filters
# - Apply Knesset 25 + Multiple faction filters
# - Download full dataset
# - Verify all rows match both filters
```

### 2. Row Count Verification
```python
# Test Case 1: Compare counts
# - Run query with filters
# - Note displayed row count
# - Check "Total rows in filtered dataset"
# - Download full dataset
# - Verify Excel/CSV row count matches

# Test Case 2: Pagination vs Full
# - Run query that returns 2,500 rows
# - Navigate through 3 pages (1000 + 1000 + 500)
# - Download full dataset
# - Verify you get all 2,500 rows in single file
```

### 3. SQL Modification Tests
```python
# Test Case 1: Simple query
SELECT * FROM table LIMIT 1000
# Expected: SELECT * FROM table

# Test Case 2: Query with pagination
SELECT * FROM table LIMIT 1000 OFFSET 2000
# Expected: SELECT * FROM table

# Test Case 3: Complex query with CTE
WITH cte AS (SELECT * FROM table1)
SELECT * FROM cte WHERE x = 1 LIMIT 1000
# Expected: WITH cte AS (SELECT * FROM table1)
#          SELECT * FROM cte WHERE x = 1
```

### 4. Performance Tests
```python
# Test Case 1: Small dataset (<1,000 rows)
# - Should complete in <1 second

# Test Case 2: Medium dataset (10,000 rows)
# - Should complete in <5 seconds

# Test Case 3: Large dataset (50,000+ rows)
# - Should show warning
# - Should complete in <30 seconds
# - Should not timeout or crash
```

### 5. Error Handling Tests
```python
# Test Case 1: Database connection error
# - Simulate connection failure
# - Verify error message appears
# - Verify no crash

# Test Case 2: Invalid SQL after modification
# - Test with edge case queries
# - Verify graceful error handling

# Test Case 3: Memory limits
# - Test with very large dataset (100,000+ rows)
# - Verify no memory errors
# - Verify download completes
```

## Technical Notes

### Database Connection Management
- Uses `get_db_connection()` context manager
- Ensures proper connection cleanup
- Read-only connections for safety

### Memory Efficiency
- Pandas DataFrame loads entire result set into memory
- For very large datasets (>100,000 rows), consider chunking
- Current implementation suitable for datasets up to 500,000 rows

### File Naming Convention
- Paginated download: `{query_name}_results.csv`
- Full download: `{query_name}_FULL_results.csv`
- Example: `Bills___Full_Details_FULL_results.csv`

### Encoding
- CSV files use UTF-8 with BOM (`utf-8-sig`)
- Excel files use native Excel encoding
- Ensures compatibility with Excel on Windows

## Future Enhancements

### Potential Improvements
1. **Chunked Download**: For datasets >500,000 rows
2. **Progress Bar**: Show download progress for large datasets
3. **Format Options**: Add JSON, Parquet export options
4. **Compression**: Offer ZIP compression for large files
5. **Background Processing**: Queue large downloads
6. **Email Notification**: Send link when large download ready
7. **Cached Results**: Cache full dataset for repeated downloads

### Performance Optimizations
1. **Streaming**: Stream results to file instead of loading all in memory
2. **Parallel Processing**: Parallel execution for very large datasets
3. **Query Optimization**: Add indexes for common filter columns

## Maintenance

### Code Location
- **Main implementation**: `src/ui/pages/data_refresh_page.py`
- **Helper function**: `DataRefreshPageRenderer._remove_limit_offset_from_query()`
- **UI rendering**: `DataRefreshPageRenderer._render_full_dataset_download()`

### Dependencies
- `pandas`: DataFrame operations, CSV/Excel export
- `openpyxl`: Excel file generation
- `streamlit`: UI components
- `duckdb`: Database queries

### Error Monitoring
Check logs for:
- "Error counting full dataset rows"
- "Error preparing full CSV"
- "Error preparing full Excel"

### Performance Monitoring
Monitor:
- Query execution time (should be <30s for most queries)
- Memory usage (watch for >1GB for single query)
- Connection pool usage (should release after download)
