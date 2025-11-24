SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Query with automatic LIMIT pushdown (DuckDB v1.5+)
-- For v1.4.2: LIMIT is applied post-fetch, use WHERE crawl_id to limit scope
.mode line
.timer on

SELECT url, timestamp, response.headers->'Content-Type' AS mime_type
-- TODO: we need to use max_results instead of LIMIT because there's no way to push down LIMIT to CDX API
FROM common_crawl_index(max_results := 10)  -- Limit CDX API to 10 results per crawl_id
WHERE crawl_id IN ('CC-MAIN-2024-46', 'CC-MAIN-2024-42')  -- Valid 2024 crawl IDs
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
  AND mime_type != 'application/pdf'
LIMIT 2;