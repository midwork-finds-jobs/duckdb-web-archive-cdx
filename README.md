# duckdb-web-archive-cdx

DuckDB extension to query web archive CDX APIs directly from SQL.

## Features

- **Two data sources**: Wayback Machine (1996-present) and Common Crawl (2008-present)
- **Smart pushdowns**: `SELECT`, `WHERE`, `LIMIT`, and `DISTINCT ON` are pushed to the CDX API
- **Minimal requests**: Only fetches what you need - no wasted bandwidth
- **Response fetching**: Archived page content is fetched only when `response.body` is selected

## Installation

```sql
INSTALL web_archive FROM community;
LOAD web_archive;
```

## Quick Start: Track GitHub Stars Over Time

Query the Wayback Machine to see how DuckDB's GitHub stars grew over the years.

Using the [html_query](https://github.com/midwork-finds-jobs/duckdb_html_query) extension to extract data from archived HTML:

```sql
SELECT DISTINCT ON(year)
    url[9:] as url,
    strftime(timestamp::TIMESTAMP, '%Y-%m-%d') AS date,
    html_query(response.body, '#repo-stars-counter-star', '@text') AS stars,
    html_query(response.body, '#repo-network-counter', '@text') AS forks,
    html_query(response.body, 'a[href="/duckdb/duckdb/releases"] span', '@text') AS releases,
    html_query(response.body, 'a[href="/duckdb/duckdb/graphs/contributors"] span', '@text') AS contributors
FROM wayback_machine()
WHERE url = 'github.com/duckdb/duckdb'
  AND timestamp > '2018-01-01'
  AND statuscode = 200
  AND mimetype = 'text/html'
ORDER BY date
LIMIT 10;
```

```text
┌──────────────────────────┬────────────┬─────────┬─────────┬──────────┬──────────────┐
│           url            │    date    │  stars  │  forks  │ releases │ contributors │
│         varchar          │  varchar   │ varchar │ varchar │ varchar  │   varchar    │
├──────────────────────────┼────────────┼─────────┼─────────┼──────────┼──────────────┤
│ github.com/duckdb/duckdb │ 2021-04-06 │ NULL    │ NULL    │ 17       │ 59           │
│ github.com/duckdb/duckdb │ 2022-01-07 │ 4.1k    │ 358     │ 22       │ 91           │
│ github.com/duckdb/duckdb │ 2023-01-08 │ 7.9k    │ 745     │ 29       │ 170          │
│ github.com/duckdb/duckdb │ 2024-01-23 │ 14k     │ 1.3k    │ 36       │ 277          │
│ github.com/duckdb/duckdb │ 2025-01-04 │ 25.5k   │ 2k      │ 45       │ 389          │
└──────────────────────────┴────────────┴─────────┴─────────┴──────────┴──────────────┘
```

**This query makes only 6 HTTP requests** despite potentially matching thousands of archived snapshots:

| Request | URL |
|---------|-----|
| 1 | CDX API query with all filters and `&collapse=timestamp:4&limit=10` |
| 2-6 | Fetch 5 archived pages (one per year) |

The extension automatically:

- Pushes `WHERE` filters (`statuscode`, `mimetype`) to the CDX API
- Pushes `LIMIT 10` to the API
- Pushes `DISTINCT ON(year)` as `&collapse=timestamp:4` (dedupe by year)
- Only requests `response.body` for the 5 matching rows
- Only fetches needed fields (`timestamp`, `original`) via `&fl=` parameter

## How Pushdown Works

### 1. Filter Pushdown (WHERE)

WHERE clauses are converted to CDX API parameters:

```sql
SELECT url FROM wayback_machine()
WHERE url LIKE 'example.com/%'      -- Pushed as: url=example.com/*
  AND statuscode = 200              -- Pushed as: &filter=statuscode:200
  AND mimetype = 'text/html'        -- Pushed as: &filter=mimetype:text/html
  AND mimetype != 'application/pdf' -- Pushed as: &filter=!mimetype:application/pdf
  AND timestamp > '2020-01-01'      -- Pushed as: &from=20200101
LIMIT 10;
```

### 2. LIMIT Pushdown

LIMIT goes directly to the CDX API - no over-fetching:

```sql
-- CDX API receives &limit=5, returns exactly 5 records
SELECT url FROM wayback_machine()
WHERE url LIKE 'example.com/%'
LIMIT 5;
```

### 3. SELECT Pushdown (Projection)

Only requested columns are fetched via `&fl=` parameter:

```sql
-- Fast: CDX returns only url field (&fl=original)
SELECT url FROM wayback_machine()
WHERE url LIKE 'example.com/%' LIMIT 10;

-- Slower: Also downloads archived page content
SELECT url, response.body FROM wayback_machine()
WHERE url LIKE 'example.com/%' LIMIT 10;
```

### 4. DISTINCT ON Pushdown (Collapse)

`DISTINCT ON` is pushed as the CDX `&collapse=` parameter:

```sql
-- One snapshot per year: &collapse=timestamp:4
SELECT DISTINCT ON(year) url, timestamp
FROM wayback_machine() WHERE url = 'example.com';

-- One snapshot per year+month: &collapse=timestamp:6
SELECT DISTINCT ON(year, month) url, timestamp
FROM wayback_machine() WHERE url = 'example.com';

-- One per unique digest (content hash): &collapse=digest
SELECT DISTINCT ON(digest) url, timestamp
FROM wayback_machine() WHERE url LIKE 'example.com/%';

-- Multiple collapse params: &collapse=digest&collapse=statuscode
SELECT DISTINCT ON(digest, statuscode) url, timestamp
FROM wayback_machine() WHERE url LIKE 'example.com/%';

-- Prefix collapse (first 6 chars of urlkey): &collapse=urlkey:6
SELECT DISTINCT ON(urlkey[:6]) url, timestamp
FROM wayback_machine() WHERE url LIKE 'example.com/%';
```

## Response Fetching

When you select `response.body`, the extension fetches archived pages:

```sql
-- No page fetching (CDX API only)
SELECT url, timestamp FROM wayback_machine()
WHERE url = 'example.com' LIMIT 5;

-- Fetches 5 archived pages from web.archive.org
SELECT url, timestamp, response.body FROM wayback_machine()
WHERE url = 'example.com' LIMIT 5;
```

Each archived page is fetched via:

```text
https://web.archive.org/web/{timestamp}id_/{url}
```

## Wayback Machine

```sql
-- Find archived snapshots
SELECT url, timestamp, statuscode
FROM wayback_machine()
WHERE url LIKE 'example.com/%'
  AND statuscode = 200
LIMIT 10;

-- Get latest snapshot
SELECT url, timestamp, response.body
FROM wayback_machine()
WHERE url = 'example.com/about'
  AND statuscode = 200
ORDER BY timestamp DESC
LIMIT 1;
```

### Wayback Machine URL Patterns

```sql
WHERE url = 'example.com'           -- Exact match
WHERE url LIKE 'example.com/%'      -- Prefix match (paths)
WHERE url LIKE '%.example.com'      -- Domain match (subdomains)
```

### Wayback Machine Columns

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Original URL |
| urlkey | VARCHAR | SURT-formatted URL |
| timestamp | TIMESTAMP | Archive timestamp |
| year | INTEGER | Year extracted from timestamp |
| month | INTEGER | Month extracted from timestamp |
| statuscode | INTEGER | HTTP status code |
| mimetype | VARCHAR | Content MIME type |
| digest | VARCHAR | Content hash |
| length | BIGINT | Content length |
| response | STRUCT | Response with `body` and `error` fields |

### Wayback Machine Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| max_results | BIGINT | 100 | Maximum results from CDX API |
| collapse | VARCHAR | - | Manual collapse field (prefer DISTINCT ON) |
| debug | BOOLEAN | false | Show `cdx_url` column with generated API URL |
| timeout | BIGINT | 180 | Timeout in seconds for fetching responses |

## Common Crawl Index

```sql
-- Find HTML pages from a domain
SELECT url, timestamp, statuscode
FROM common_crawl_index()
WHERE url LIKE '%.example.com/%'
  AND statuscode = 200
  AND mimetype = 'text/html'
LIMIT 10;

-- Fetch page content (downloads from WARC files)
SELECT url, response.body
FROM common_crawl_index()
WHERE url LIKE 'https://example.com/%'
  AND statuscode = 200
LIMIT 1;

-- Use specific crawl
SELECT url FROM common_crawl_index()
WHERE crawl_id = 'CC-MAIN-2025-47'
  AND url LIKE '%.example.com/%'
LIMIT 10;
```

### Common Crawl URL Patterns

```sql
WHERE url LIKE '%.example.com/%'        -- Domain wildcard (subdomains)
WHERE url LIKE 'https://example.com/%'  -- Prefix match
WHERE url SIMILAR TO '.*example\.com/$' -- Regex match
```

### Common Crawl Columns

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Original URL |
| timestamp | TIMESTAMP | Crawl timestamp |
| crawl_id | VARCHAR | Crawl identifier (e.g., CC-MAIN-2025-47) |
| statuscode | INTEGER | HTTP status code |
| mimetype | VARCHAR | Content MIME type |
| digest | VARCHAR | Content hash |
| filename | VARCHAR | WARC filename |
| offset | BIGINT | Offset in WARC |
| length | BIGINT | Content length |
| response | STRUCT | Parsed WARC response (headers + body) |
| warc | STRUCT | WARC metadata |

### Common Crawl Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| max_results | BIGINT | 100 | Maximum results from CDX API |

## Examples

### Track website changes over time

```sql
SELECT DISTINCT ON(year)
    timestamp,
    length,
    digest
FROM wayback_machine()
WHERE url = 'example.com'
  AND statuscode = 200
ORDER BY timestamp;
```

### Find all unique domains from a TLD

```sql
SELECT DISTINCT regexp_extract(url, 'https?://([^/]+)', 1) as domain
FROM wayback_machine()
WHERE url LIKE '%.gov/%'
  AND statuscode = 200
LIMIT 1000;
```

### Export URLs to file

```sql
COPY (
  SELECT url, timestamp
  FROM common_crawl_index()
  WHERE url LIKE '%.example.com/%'
    AND statuscode = 200
  LIMIT 10000
) TO 'urls.csv';
```

## Data Sources

- **Wayback Machine**: <https://web.archive.org> - Continuous archiving since 1996
- **Common Crawl**: <https://commoncrawl.org> - Monthly web crawls since 2008

## CDX API Documentation

- **CDX Server API Reference**: <https://github.com/webrecorder/pywb/wiki/CDX-Server-API>
- **Common Crawl Index API**: <https://index.commoncrawl.org>
- **Wayback Machine CDX API**: <https://web.archive.org/cdx/search/cdx>

## License

MIT
