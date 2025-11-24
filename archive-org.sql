SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

FROM internet_archive()
WHERE url = 'archive.org'
AND status_code = 200
LIMIT 5;

-- WHAT! the internet_archive can push the LIMIT down to the API??
-- Check how this is possible and move this to common_crawl_index as well!