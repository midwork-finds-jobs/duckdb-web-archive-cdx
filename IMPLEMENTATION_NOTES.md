# Common Crawl DuckDB Extension - Implementation Notes

## What Was Implemented

This extension provides a table-valued function `common_crawl_index` to query Common Crawl data directly from DuckDB.

### Core Features

1. **Table-Valued Function**: `common_crawl_index(index_name)`
   - Takes a Common Crawl index name as parameter (e.g., 'CC-MAIN-2025-43')
   - Returns table with columns: url, timestamp, mime_type, status_code, digest, filename, offset, length, response

2. **Auto-loading httpfs Extension**
   - The extension automatically loads httpfs on initialization
   - Uses httpfs for HTTP requests to Common Crawl APIs

3. **CDX API Integration**
   - Queries the Common Crawl CDX API at https://index.commoncrawl.org/
   - Parses JSON responses to extract metadata about crawled pages
   - Uses `&fl=` parameter for field optimization (predicate pushdown)

4. **Field Optimization**
   - Only requests needed fields from CDX API
   - Avoids fetching WARC file data if only URL metadata is needed

### Architecture

#### Data Structures

- **CDXRecord**: Holds data for a single crawled page (url, filename, offset, length, etc.)
- **CommonCrawlBindData**: Bind-time data including index name, column info, and URL filter
- **CommonCrawlGlobalState**: Global state holding all CDX records and current position

#### Function Flow

1. **Bind Phase** (`CommonCrawlBind`):
   - Validates input parameters
   - Defines output schema
   - Sets up field tracking for optimization

2. **Init Phase** (`CommonCrawlInitGlobal`):
   - Queries CDX API with URL pattern and field list
   - Parses JSON responses into CDXRecord structures
   - Stores records in global state

3. **Scan Phase** (`CommonCrawlScan`):
   - Iterates through CDX records
   - Populates output DataChunk with record data
   - Optionally fetches WARC response bodies (currently disabled)

### Current Limitations

1. **WARC Response Fetching**:
   - Response column is included in schema but returns NULL
   - Requires implementing HTTP range requests + gzip decompression
   - Can be enabled by setting `bind_data->fetch_response = true` and implementing proper decompression

2. **URL Pattern Filtering**:
   - URL filter is hardcoded to "*" (all URLs)
   - Filter pushdown framework is partially implemented but not fully functional
   - Need to parse WHERE clauses to extract URL patterns

3. **Error Handling**:
   - Basic error handling is in place
   - Should add more robust error messages and recovery

4. **Performance**:
   - Single-threaded execution
   - All CDX records loaded into memory at init time
   - Could be optimized with streaming/pagination

### Example Usage

```sql
LOAD 'build/debug/extension/quack/quack.duckdb_extension';

-- Query Common Crawl index
SELECT url, timestamp, status_code, mime_type
FROM common_crawl_index('CC-MAIN-2025-43')
LIMIT 10;

-- Filter results (note: URL filtering not yet pushed down to CDX API)
SELECT url, status_code
FROM common_crawl_index('CC-MAIN-2025-43')
WHERE url LIKE '%example.com%'
LIMIT 10;
```

### Next Steps

1. **Complete WARC Fetching**:
   - Implement HTTP range requests via httpfs
   - Add gzip decompression for WARC data
   - Parse WARC/HTTP response format

2. **Implement Filter Pushdown**:
   - Add proper filter pushdown function
   - Parse WHERE clauses to extract URL patterns
   - Convert SQL patterns to CDX API format (wildcards)

3. **Add More Query Parameters**:
   - Support for date range filtering
   - Support for mime type filtering
   - Support for status code filtering

4. **Testing**:
   - Add unit tests
   - Add integration tests
   - Test with real Common Crawl indexes

5. **Documentation**:
   - Add user-facing documentation
   - Add examples for common use cases
   - Document all supported parameters and filters

## References

- Common Crawl CDX API: https://github.com/webrecorder/pywb/wiki/CDX-Server-API
- Common Crawl Index Info: https://index.commoncrawl.org/collinfo.json
- DuckDB Extension Development: https://duckdb.org/docs/extensions/overview
