#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

#include <zlib.h>
#include <vector>
#include <sstream>
#include <chrono>
#include <future>
#include <set>
#include <unordered_map>

namespace duckdb {

// ========================================
// TIMING UTILITIES
// ========================================

// Global start time for timing debug
extern std::chrono::steady_clock::time_point g_start_time;

double ElapsedMs();

// ========================================
// STRING UTILITIES
// ========================================

// Convert SQL LIKE pattern to regex
// % -> .* and _ -> . with anchoring
string LikeToRegex(const string &like_pattern);

// Convert timestamp string to CDX format, stripping trailing zeros
// E.g., "2024-06-01 00:00:00" -> "20240601" (not "20240601000000")
string ToCdxTimestamp(const string &ts_str);

// Helper to sanitize strings for DuckDB (remove invalid UTF-8 sequences)
string SanitizeUTF8(const string &str);

// Safe wrapper for creating DuckDB string Values
Value SafeStringValue(const string &str);

// Simple JSON value extractor - extracts value for a given key from a JSON object line
string ExtractJSONValue(const string &json_line, const string &key);

// Helper function to convert SQL LIKE wildcards to CDX API wildcards
string ConvertSQLWildcardsToCDX(const string &pattern);

// Parse CDX timestamp format (YYYYMMDDHHmmss) to DuckDB timestamp
timestamp_t ParseCDXTimestamp(const string &cdx_timestamp);

// ========================================
// GZIP DECOMPRESSION
// ========================================

// Helper function to decompress gzip data using zlib
string DecompressGzip(const char *compressed_data, size_t compressed_size);

// ========================================
// HTTP/WARC PARSING
// ========================================

// Structure to hold parsed WARC response
struct WARCResponse {
	string warc_version;                        // e.g., "1.0" from "WARC/1.0"
	unordered_map<string, string> warc_headers; // WARC header fields as map
	string http_version;                        // e.g., "1.1" from "HTTP/1.1"
	int http_status_code;                       // e.g., 200 from "HTTP/1.1 200"
	unordered_map<string, string> http_headers; // HTTP header fields as map
	string body;                                // HTTP response body
	string error;                               // Error message if fetch failed (empty on success)

	WARCResponse() : http_status_code(0) {
	}
};

// Helper function to parse headers into a map
// Handles multi-value headers by concatenating with ", "
unordered_map<string, string> ParseHeaders(const string &header_text);

// Helper function to parse WARC format and extract structured WARC/HTTP headers and body
WARCResponse ParseWARCResponse(const string &warc_data);

// ========================================
// CDX RECORD TYPES
// ========================================

// Structure to hold CDX record data (Common Crawl)
struct CDXRecord {
	string url;
	string filename;
	int64_t offset;
	int64_t length;
	string timestamp;
	string mime_type;
	string digest;
	int32_t status_code;
	string crawl_id;

	CDXRecord() : offset(0), length(0), status_code(0) {
	}
};

// Structure to hold Internet Archive CDX record data
struct ArchiveOrgRecord {
	string urlkey;       // SURT-formatted URL key
	string timestamp;    // YYYYMMDDhhmmss format
	string original;     // Original URL
	string mime_type;    // MIME type
	int32_t status_code; // HTTP status code
	string digest;       // SHA-1 hash
	int64_t length;      // Content length

	ArchiveOrgRecord() : status_code(0), length(0) {
	}
};

// ========================================
// COLLINFO CACHE
// ========================================

// Structure to hold crawl information from collinfo.json
struct CrawlInfo {
	string id;           // e.g., "CC-MAIN-2025-47"
	string name;         // e.g., "November 2025 Index"
	timestamp_t from_ts; // Start of crawl period
	timestamp_t to_ts;   // End of crawl period

	CrawlInfo() : from_ts(timestamp_t(0)), to_ts(timestamp_t(0)) {
	}
};

// Cache for collinfo.json (1 day TTL)
struct CollInfoCache {
	string latest_crawl_id;
	vector<CrawlInfo> crawl_infos; // Full list of crawl info with dates
	std::chrono::system_clock::time_point cached_at;
	bool is_valid;

	CollInfoCache() : is_valid(false) {
	}

	bool IsExpired() const {
		if (!is_valid)
			return true;
		auto now = std::chrono::system_clock::now();
		auto age = std::chrono::duration_cast<std::chrono::hours>(now - cached_at).count();
		return age >= 24; // 1 day = 24 hours
	}
};

extern CollInfoCache g_collinfo_cache;

// Helper function to fetch the latest crawl_id from collinfo.json (with 1-day caching)
string GetLatestCrawlId(ClientContext &context);

// Helper function to get all crawl infos (with 1-day caching)
const vector<CrawlInfo> &GetCrawlInfos(ClientContext &context);

// Helper function to find crawl_ids that overlap with a given timestamp range
vector<string> GetCrawlIdsForTimestampRange(ClientContext &context, timestamp_t from_ts, timestamp_t to_ts);

// ========================================
// FORWARD DECLARATIONS FOR TABLE FUNCTIONS
// ========================================

// Common Crawl table function
void RegisterCommonCrawlFunction(ExtensionLoader &loader);

// Wayback Machine (Internet Archive) table function
void RegisterWaybackMachineFunction(ExtensionLoader &loader);

// Optimizer for LIMIT pushdown
void CommonCrawlOptimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
