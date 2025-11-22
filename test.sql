SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Query with URL pattern and limit (3 parameters: index, url_pattern, cdx_limit)
-- This limits CDX API to return only 10 results, making the query much faster
CALL enable_logging('HTTP');

SELECT url, timestamp, response
FROM common_crawl_index(2)
WHERE crawl_id IN ('CC-MAIN-2025-43', 'CC-MAIN-2025-38')
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
  AND mime_type != 'application/pdf'
LIMIT 10;

.mode line
FROM duckdb_logs_parsed('HTTP')
WHERE request.url LIKE 'https://index.commoncrawl.org/%'

