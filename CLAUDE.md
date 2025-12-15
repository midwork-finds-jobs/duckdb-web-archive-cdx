# duckdb-web-archive-cdx - DuckDB extension to query archived web pages

This extension provides two table functions:

1. **common_crawl_index()** - Query Common Crawl web archive (2008-present, monthly snapshots)
2. **wayback_machine()** - Query Internet Archive Wayback Machine (1996-present, continuous)

## Do not remove the whole release between builds

```sh
# This is much slower
$ rm -rf build/release && make release GEN=ninja

# Than this:
$ make release GEN=ninja
```

Only delete the `build` folder if you encounter weird hard to debug issues.

## Example of how duckdb can be used with this extension

```sql
SELECT response, url
FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-43'   -- Specify crawl via WHERE (defaults to latest)
  AND url LIKE '*.example.com/*'  -- URL filtering via WHERE clause
  AND statuscode = 200              -- Pushed down to CDX API as filter
  AND mimetype != 'application/pdf' -- Pushed down to CDX API as filter
LIMIT 10;                            -- LIMIT is automatically pushed down to CDX API
```

LIMIT and result size:

```sql
-- The extension fetches up to 10000 results from CDX API by default
-- Use WHERE clauses to narrow down the result set
-- In future DuckDB versions (v1.5+), LIMIT will be automatically pushed down to CDX API
SELECT url FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' LIMIT 10;
```

The extension automatically uses the `&fl=` query parameter to fetch only the fields which are needed from the index server.

If response is requested you need these 3 parameters from the index server:

```text
&fl=filename,offset,length
```

If only `url` is requested then you don't need to query the warc files with range requests. `&url=` query parameter for the index server supports wildcards:
`https://example.com/*` will search for all paths

See index server API docs: <https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference>

All common crawl indexes and their metadata can be found from: <https://index.commoncrawl.org/collinfo.json>

## Internet Archive Wayback Machine

```sql
-- Query archived snapshots with automatic LIMIT pushdown
SELECT url, timestamp, mimetype, statuscode
FROM wayback_machine()
WHERE url = 'archive.org'       -- Exact URL match
  AND statuscode = 200          -- Only successful responses
  AND mimetype = 'text/html'    -- Only HTML pages
LIMIT 10;                        -- LIMIT automatically pushed to CDX API

-- Fetch archived page content
SELECT url, timestamp, response
FROM wayback_machine()
WHERE url = 'archive.org/about/'
  AND statuscode = 200
ORDER BY timestamp DESC
LIMIT 1;

-- URL matching modes (auto-detected from pattern):
WHERE url = 'archive.org'           -- Exact match
WHERE url LIKE 'archive.org/%'      -- Prefix match (all paths under archive.org/)
WHERE url LIKE 'archive.org/about%' -- Prefix match
WHERE url LIKE '*.archive.org'      -- Domain match (includes subdomains)
```

**Key differences from common_crawl_index():**

- Much simpler data retrieval (no WARC parsing)
- Historical data from 1996 onwards
- Response column is raw HTTP body (not structured WARC)
- Direct download via `https://web.archive.org/web/{timestamp}id_/{url}`

See [docs/INTERNET_ARCHIVE.md](docs/INTERNET_ARCHIVE.md) for complete documentation.

## Implementation details

Do not create a new http client. You want to autoload `httpfs` in the extension and then use `httputil`. Something like this:

```c++
// In your Extension::Load function
void MyExtension::Load(DuckDB &db) {
    Connection con(db);
    con.BeginTransaction();

    // Auto-load httpfs
    auto result = con.Query("INSTALL httpfs; LOAD httpfs;");
    if (result->HasError()) {
        throw std::runtime_error("Failed to load httpfs: " + result->GetError());
    }

    con.Commit();
    // ... rest of load logic
}
```

## Example url with filters

Here's an example which only loads pages with statuscode:200 and where mime is not a pdf:

```sh
curl -v 'https://index.commoncrawl.org/CC-MAIN-2025-43-index?url=*.example.com/*&output=json&fl=url,timestamp,filename,offset,length&limit=2&filter==statuscode:200&filter=!mime:application/pdf'
```

This query works as is! There's no need to urlencode the '!' character for example.

## Example working script with curl

The included `./common-crawl.sh` shows an example of how to query common crawl index server and then using the returned values to get the body of the http response.

example output from: `./common-crawl.sh 'https://careers.swappie.com/jobs/6559247-maintenance-and-engineering-lead-operations'`

is here below:

```text
WARC/1.0
WARC-Type: response
WARC-Date: 2025-10-09T05:36:22Z
WARC-Record-ID: <urn:uuid:186e4395-5db8-442a-ad8b-26448ca21165>
Content-Length: 82998
Content-Type: application/http; msgtype=response
WARC-Warcinfo-ID: <urn:uuid:ae58b7f4-9b1c-430c-83a7-2f1ffa2b0032>
WARC-Concurrent-To: <urn:uuid:784ada4e-2a5f-4850-8610-99a85157dbb2>
WARC-IP-Address: 207.120.32.236
WARC-Target-URI: https://careers.swappie.com/jobs/6559247-maintenance-and-engineering-lead-operations
WARC-Protocol: h2
WARC-Protocol: tls/1.3
WARC-Cipher-Suite: TLS_AES_256_GCM_SHA384
WARC-Payload-Digest: sha1:5O7CMMB63K7IEXRKPNR42NLPZI5TH4OT
WARC-Block-Digest: sha1:QRVPTH5G3KD25BFP6DSFHY3WYD5IT5HI
WARC-Identified-Payload-Type: text/html

HTTP/1.1 200
date: Thu, 09 Oct 2025 05:36:22 GMT
content-type: text/html; charset=utf-8
vary: Accept-Encoding
x-frame-options: SAMEORIGIN
x-xss-protection: 0
x-content-type-options: nosniff
x-permitted-cross-domain-policies: none
referrer-policy: strict-origin-when-cross-origin
content-security-policy: frame-ancestors 'self' careers.swappie.com app.example.com
cache-control: max-age=0, public, s-maxage=5184000
link: <https://assets-aws.example-cdn.com/assets/packs/css/careersite-68472310.css>; rel=preload; as=style; nopush
etag: W/"922544fef93fe74557fb8bb6175d76ed"
x-request-id: c34c5d28935d6f9393a2f6dba9d256a7
x-runtime: 0.334679
strict-transport-security: max-age=63072000; includeSubDomains
vary: Accept-Encoding
x-varnish: 517020139
age: 0
section-io-cache: Miss
X-Crawler-content-encoding: gzip
section-io-id: c34c5d28935d6f9393a2f6dba9d256a7
Content-Length: 82088

<!DOCTYPE html>
<html lang="en-GB" dir="ltr" class="h-screen">
<head>
    ...
</head>
<body>
    ...
</body>
```
