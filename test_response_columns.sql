SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

LOAD 'build/debug/extension/common_crawl/common_crawl.duckdb_extension';

-- Test 1: Query with response_headers only
SELECT url, LENGTH(response_headers) as headers_size
FROM common_crawl_index(10)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 2;

-- Test 2: Query with response_body only
-- Note: response_body is BLOB type, use OCTET_LENGTH() instead of LENGTH()
SELECT url, OCTET_LENGTH(response_body) as body_size
FROM common_crawl_index(10)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 2;

-- Test 3: Query with both response_headers and response_body
SELECT url,
       LENGTH(response_headers) as headers_size,
       OCTET_LENGTH(response_body) as body_size,
       SUBSTRING(response_headers, 1, 150) as first_150_chars_headers
FROM common_crawl_index(10)
WHERE crawl_id = 'CC-MAIN-2025-43'
  AND url LIKE '%.teamtailor.com/%'
  AND status_code = 200
LIMIT 1;
