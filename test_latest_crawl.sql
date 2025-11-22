SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test: Query without crawl_id (should fetch latest from collinfo.json)
SELECT url, crawl_id, timestamp
FROM common_crawl_index()
WHERE url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 3;
