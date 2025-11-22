SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test 1: Query with specific crawl_id
SELECT url, crawl_id, timestamp
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 3;
