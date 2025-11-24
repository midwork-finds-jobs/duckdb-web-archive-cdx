#define DUCKDB_EXTENSION_MAIN

#include "common_crawl_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

// zlib for gzip decompression
#include <zlib.h>
#include <vector>
#include <sstream>
#include <chrono>
#include <future>
#include <thread>

namespace duckdb {

// Cache for collinfo.json (1 day TTL)
struct CollInfoCache {
	string latest_crawl_id;
	std::chrono::system_clock::time_point cached_at;
	bool is_valid;

	CollInfoCache() : is_valid(false) {}

	bool IsExpired() const {
		if (!is_valid) return true;
		auto now = std::chrono::system_clock::now();
		auto age = std::chrono::duration_cast<std::chrono::hours>(now - cached_at).count();
		return age >= 24; // 1 day = 24 hours
	}
};

static CollInfoCache g_collinfo_cache;

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
};

// Structure to hold bind data for the table function
struct CommonCrawlBindData : public TableFunctionData {
	string index_name;  // Single index (for backwards compatibility)
	vector<string> crawl_ids;  // Multiple crawl_ids for IN clause support
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	string url_filter;
	vector<string> cdx_filters; // CDX API filter parameters (e.g., "=status:200", "=mime:text/html")
	idx_t max_results; // Maximum number of results to fetch from CDX API

	// Default CDX limit set to 100 to prevent fetching too many results
	// FIXME: When DuckDB v1.5+ support is added, LIMIT will be pushed down and override this value
	// See FIXME comment in CommonCrawlInitGlobal for implementation details
	CommonCrawlBindData(string index) : index_name(std::move(index)), fetch_response(false), url_filter("*"), max_results(100) {}
};

// Structure to hold global state for the table function
struct CommonCrawlGlobalState : public GlobalTableFunctionState {
	vector<CDXRecord> records;
	idx_t current_position;
	vector<column_t> column_ids; // Which columns are actually selected

	CommonCrawlGlobalState() : current_position(0) {}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// Helper to sanitize strings for DuckDB (remove invalid UTF-8 sequences)
static string SanitizeUTF8(const string &str) {
	string result;
	result.reserve(str.size());

	for (size_t i = 0; i < str.size(); ) {
		unsigned char c = static_cast<unsigned char>(str[i]);

		// ASCII character (0-127)
		if (c < 0x80) {
			result += c;
			i++;
			continue;
		}

		// Multi-byte UTF-8 character
		int len = 0;
		if ((c & 0xE0) == 0xC0) len = 2;      // 2-byte
		else if ((c & 0xF0) == 0xE0) len = 3; // 3-byte
		else if ((c & 0xF8) == 0xF0) len = 4; // 4-byte
		else {
			// Invalid start byte, replace with ?
			result += '?';
			i++;
			continue;
		}

		// Check if we have enough bytes
		if (i + len > str.size()) {
			// Truncated sequence, replace with ?
			result += '?';
			break;
		}

		// Validate continuation bytes
		bool valid = true;
		for (int j = 1; j < len; j++) {
			if ((static_cast<unsigned char>(str[i + j]) & 0xC0) != 0x80) {
				valid = false;
				break;
			}
		}

		if (valid) {
			// Add the valid multi-byte sequence
			result.append(str, i, len);
			i += len;
		} else {
			// Invalid sequence, replace with ?
			result += '?';
			i++;
		}
	}

	return result;
}

// Safe wrapper for creating DuckDB string Values
static Value SafeStringValue(const string &str) {
	try {
		string sanitized = SanitizeUTF8(str);
		return Value(sanitized);
	} catch (const std::exception &ex) {
		// If sanitization still fails, return empty string
		fprintf(stderr, "[ERROR] Failed to create Value for string: %s (error: %s)\n",
		        str.substr(0, 100).c_str(), ex.what());
		return Value("");
	} catch (...) {
		fprintf(stderr, "[ERROR] Failed to create Value for string: %s (unknown error)\n",
		        str.substr(0, 100).c_str());
		return Value("");
	}
}

// Simple JSON value extractor - extracts value for a given key from a JSON object line
static string ExtractJSONValue(const string &json_line, const string &key) {
	string search = "\"" + key + "\": \"";
	size_t start = json_line.find(search);
	if (start == string::npos) {
		// Try without space after colon
		search = "\"" + key + "\":\"";
		start = json_line.find(search);
		if (start == string::npos) {
			return "";
		}
	}
	start += search.length();
	size_t end = json_line.find("\"", start);
	if (end == string::npos) {
		return "";
	}
	return SanitizeUTF8(json_line.substr(start, end - start));
}

// Helper function to query CDX API using FileSystem
static vector<CDXRecord> QueryCDXAPI(ClientContext &context, const string &index_name, const string &url_pattern,
                                      const vector<string> &fields_needed, const vector<string> &cdx_filters, idx_t max_results) {
	fprintf(stderr, "[DEBUG] QueryCDXAPI started\n");
	vector<CDXRecord> records;

	// Helper lambda to map DuckDB column names to CDX API field names
	auto map_column_to_field = [](const string &col_name) -> string {
		if (col_name == "mime_type") return "mime";
		if (col_name == "status_code") return "status";
		return col_name; // url, digest, timestamp, filename, offset, length stay the same
	};

	// Construct field list for &fl= parameter to optimize the query
	// Only request fields that are actually needed
	string field_list = "";
	bool need_warc_fields = false;
	for (size_t i = 0; i < fields_needed.size(); i++) {
		if (i > 0) {
			field_list += ",";
		}
		field_list += map_column_to_field(fields_needed[i]);

		// Check if we need WARC fields for parsing
		if (fields_needed[i] == "filename" || fields_needed[i] == "offset" || fields_needed[i] == "length") {
			need_warc_fields = true;
		}
	}

	// Construct the CDX API URL
	// Add limit parameter to control how many results we fetch
	string cdx_url = "https://index.commoncrawl.org/" + index_name + "-index?url=" + url_pattern +
	                 "&output=json&fl=" + field_list + "&limit=" + to_string(max_results);

	// Add filter parameters (e.g., filter==statuscode:200)
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	// Debug: print the final CDX URL
	fprintf(stderr, "[CDX URL] %s\n", cdx_url.c_str());

	try {
		fprintf(stderr, "[DEBUG] Opening CDX URL\n");
		// Use FileSystem to fetch the CDX data
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(cdx_url, FileFlags::FILE_FLAGS_READ);

		fprintf(stderr, "[DEBUG] Reading CDX response\n");
		// Read the entire response
		string response_data;
		const idx_t buffer_size = 8192;
		auto buffer = unique_ptr<char[]>(new char[buffer_size]);

		while (true) {
			int64_t bytes_read = file_handle->Read(buffer.get(), buffer_size);
			if (bytes_read <= 0) {
				break;
			}
			response_data.append(buffer.get(), bytes_read);
		}

		fprintf(stderr, "[DEBUG] Got %lu bytes, sanitizing UTF-8\n", (unsigned long)response_data.size());
		// Sanitize the entire response to ensure valid UTF-8
		response_data = SanitizeUTF8(response_data);
		fprintf(stderr, "[DEBUG] UTF-8 sanitization complete\n");

		// Parse newline-delimited JSON
		fprintf(stderr, "[DEBUG] Parsing JSON lines\n");
		std::istringstream stream(response_data);
		string line;
		int line_count = 0;

		while (std::getline(stream, line)) {
			if (line.empty() || line[0] != '{') {
				continue;
			}
			line_count++;

			CDXRecord record;

			// Extract fields from JSON line
			record.url = ExtractJSONValue(line, "url");
			if (record.url.empty()) {
				continue; // Skip invalid records
			}

			record.timestamp = ExtractJSONValue(line, "timestamp");
			record.mime_type = ExtractJSONValue(line, "mime");
			record.digest = ExtractJSONValue(line, "digest");
			record.crawl_id = index_name;

			string status_str = ExtractJSONValue(line, "status");
			record.status_code = status_str.empty() ? 0 : std::stoi(status_str);

			if (need_warc_fields) {
				record.filename = ExtractJSONValue(line, "filename");
				string offset_str = ExtractJSONValue(line, "offset");
				string length_str = ExtractJSONValue(line, "length");

				record.offset = offset_str.empty() ? 0 : std::stoll(offset_str);
				record.length = length_str.empty() ? 0 : std::stoll(length_str);
			}

			records.push_back(record);
		}
		fprintf(stderr, "[DEBUG] Parsed %d JSON lines, got %lu records\n", line_count, (unsigned long)records.size());

	} catch (std::exception &ex) {
		throw IOException("Error querying CDX API: " + string(ex.what()));
	} catch (...) {
		throw IOException("Unknown error querying CDX API");
	}

	return records;
}

// Structure to hold parsed WARC response
struct WARCResponse {
	string warc_version;                    // e.g., "1.0" from "WARC/1.0"
	unordered_map<string, string> warc_headers;  // WARC header fields as map
	string http_version;                    // e.g., "1.1" from "HTTP/1.1"
	int http_status_code;                   // e.g., 200 from "HTTP/1.1 200"
	unordered_map<string, string> http_headers;  // HTTP header fields as map
	string body;                            // HTTP response body
};

// Helper function to parse headers into a map
// Handles multi-value headers by concatenating with ", "
static unordered_map<string, string> ParseHeaders(const string &header_text) {
	unordered_map<string, string> headers;

	size_t pos = 0;
	size_t line_end;

	while (pos < header_text.length()) {
		// Find end of line
		line_end = header_text.find("\r\n", pos);
		if (line_end == string::npos) {
			line_end = header_text.find("\n", pos);
			if (line_end == string::npos) {
				break;
			}
		}

		string line = header_text.substr(pos, line_end - pos);

		// Find colon separator
		size_t colon_pos = line.find(": ");
		if (colon_pos != string::npos) {
			string key = line.substr(0, colon_pos);
			string value = line.substr(colon_pos + 2);

			// If key already exists, concatenate with ", "
			auto it = headers.find(key);
			if (it != headers.end()) {
				it->second += ", " + value;
			} else {
				headers[key] = value;
			}
		}

		// Move to next line
		pos = line_end + 1;
		if (pos < header_text.length() && header_text[pos] == '\n') {
			pos++;
		}
	}

	return headers;
}

// Helper function to parse WARC format and extract structured WARC/HTTP headers and body
static WARCResponse ParseWARCResponse(const string &warc_data) {
	WARCResponse result;
	result.http_status_code = 0; // Default

	// WARC format structure:
	// 1. WARC version line + headers (metadata about the record)
	// 2. HTTP status line + headers
	// 3. HTTP body (actual content)

	// Find the end of WARC headers (double newline)
	size_t warc_headers_end = warc_data.find("\r\n\r\n");
	size_t newline_size = 4;
	if (warc_headers_end == string::npos) {
		warc_headers_end = warc_data.find("\n\n");
		newline_size = 2;
		if (warc_headers_end == string::npos) {
			return result; // Invalid WARC format
		}
	}

	// Extract WARC section
	string warc_section = warc_data.substr(0, warc_headers_end);

	// Parse WARC version from first line (e.g., "WARC/1.0")
	size_t first_line_end = warc_section.find("\r\n");
	if (first_line_end == string::npos) {
		first_line_end = warc_section.find("\n");
	}

	if (first_line_end != string::npos) {
		string version_line = warc_section.substr(0, first_line_end);
		if (version_line.find("WARC/") == 0) {
			result.warc_version = version_line.substr(5); // Extract version after "WARC/"
		}

		// Parse remaining WARC headers (skip version line)
		size_t warc_headers_start = first_line_end + 1;
		if (warc_headers_start < warc_section.length() && warc_section[warc_headers_start] == '\n') {
			warc_headers_start++;
		}
		string warc_headers_text = warc_section.substr(warc_headers_start);
		result.warc_headers = ParseHeaders(warc_headers_text);
	}

	// After WARC headers comes the HTTP response
	size_t http_start = warc_headers_end + newline_size;
	size_t http_headers_end = warc_data.find("\r\n\r\n", http_start);
	size_t http_newline_size = 4;
	if (http_headers_end == string::npos) {
		http_headers_end = warc_data.find("\n\n", http_start);
		http_newline_size = 2;
		if (http_headers_end == string::npos) {
			return result; // Invalid HTTP format
		}
	}

	// Extract HTTP section
	string http_section = warc_data.substr(http_start, http_headers_end - http_start);

	// Parse HTTP status line (e.g., "HTTP/1.1 200")
	size_t http_first_line_end = http_section.find("\r\n");
	if (http_first_line_end == string::npos) {
		http_first_line_end = http_section.find("\n");
	}

	if (http_first_line_end != string::npos) {
		string status_line = http_section.substr(0, http_first_line_end);

		// Parse "HTTP/1.1 200 OK" format
		size_t space1 = status_line.find(" ");
		if (space1 != string::npos && status_line.find("HTTP/") == 0) {
			// Extract version (e.g., "1.1" from "HTTP/1.1")
			result.http_version = status_line.substr(5, space1 - 5);

			// Extract status code
			size_t space2 = status_line.find(" ", space1 + 1);
			string status_str;
			if (space2 != string::npos) {
				status_str = status_line.substr(space1 + 1, space2 - space1 - 1);
			} else {
				status_str = status_line.substr(space1 + 1);
			}

			try {
				result.http_status_code = std::stoi(status_str);
			} catch (...) {
				result.http_status_code = 0;
			}
		}

		// Parse HTTP headers (skip status line)
		size_t http_headers_start = http_first_line_end + 1;
		if (http_headers_start < http_section.length() && http_section[http_headers_start] == '\n') {
			http_headers_start++;
		}
		string http_headers_text = http_section.substr(http_headers_start);
		result.http_headers = ParseHeaders(http_headers_text);
	}

	// Extract HTTP body
	result.body = warc_data.substr(http_headers_end + http_newline_size);

	return result;
}

// Helper function to decompress gzip data using zlib
static string DecompressGzip(const char *compressed_data, size_t compressed_size) {
	// Initialize zlib stream
	z_stream stream;
	memset(&stream, 0, sizeof(stream));

	// Initialize for gzip decompression (windowBits = 15 + 16 for gzip format)
	int ret = inflateInit2(&stream, 15 + 16);
	if (ret != Z_OK) {
		return "[Error: Failed to initialize gzip decompression]";
	}

	// Set input
	stream.avail_in = compressed_size;
	stream.next_in = (Bytef *)compressed_data;

	// Prepare output buffer (estimate 10x compression ratio)
	std::vector<char> decompressed_buffer;
	decompressed_buffer.reserve(compressed_size * 10);

	// Decompress in chunks
	const size_t chunk_size = 32768; // 32 KB chunks
	char out_buffer[chunk_size];

	do {
		stream.avail_out = chunk_size;
		stream.next_out = (Bytef *)out_buffer;

		ret = inflate(&stream, Z_NO_FLUSH);

		if (ret != Z_OK && ret != Z_STREAM_END) {
			inflateEnd(&stream);
			return "[Error: Gzip decompression failed with code " + to_string(ret) + "]";
		}

		size_t produced = chunk_size - stream.avail_out;
		decompressed_buffer.insert(decompressed_buffer.end(), out_buffer, out_buffer + produced);

	} while (ret != Z_STREAM_END);

	inflateEnd(&stream);

	// Convert to string
	return string(decompressed_buffer.begin(), decompressed_buffer.end());
}

// Helper function to fetch WARC response using FileSystem API
static WARCResponse FetchWARCResponse(ClientContext &context, const CDXRecord &record) {
	WARCResponse result;

	if (record.filename.empty() || record.offset == 0 || record.length == 0) {
		return result; // Invalid record - return empty
	}

	try {
		// Construct the WARC URL
		string warc_url = "https://data.commoncrawl.org/" + record.filename;

		// Get the file system from the database context
		auto &fs = FileSystem::GetFileSystem(context);

		// Open the file through httpfs (HTTP URLs are handled when httpfs is loaded)
		auto file_handle = fs.OpenFile(warc_url, FileFlags::FILE_FLAGS_READ);

		// Allocate buffer for the compressed data
		auto buffer = unique_ptr<char[]>(new char[record.length]);

		// Seek to the offset and read the specified length
		// httpfs should translate this into HTTP Range request: bytes=offset-(offset+length-1)
		file_handle->Seek(record.offset);
		int64_t bytes_read = file_handle->Read(buffer.get(), record.length);

		if (bytes_read <= 0) {
			result.body = "[Error: Failed to read data from WARC file]";
			return result;
		}

		// The data we read is gzip compressed
		// We need to decompress it to get the WARC content
		string decompressed = DecompressGzip(buffer.get(), bytes_read);

		// Parse the WARC format to extract HTTP response headers and body
		if (decompressed.find("[Error") == 0) {
			// If decompression returned an error message, put it in body
			result.body = decompressed;
			return result;
		}

		return ParseWARCResponse(decompressed);

	} catch (Exception &ex) {
		// Return error message in body for debugging
		result.body = "[Error fetching WARC: " + string(ex.what()) + "]";
		return result;
	} catch (std::exception &ex) {
		result.body = "[Error fetching WARC: " + string(ex.what()) + "]";
		return result;
	} catch (...) {
		result.body = "[Unknown error fetching WARC]";
		return result;
	}
}

// Forward declarations
static string GetLatestCrawlId(ClientContext &context);

// Bind function for the table function
static unique_ptr<FunctionData> CommonCrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	fprintf(stderr, "[DEBUG] CommonCrawlBind called\n");
	fflush(stderr);

	// Optional max_results named parameter to control CDX API result size
	// If provided, overrides the default max_results (100)
	auto bind_data = make_uniq<CommonCrawlBindData>("");

	// Handle named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "max_results") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT) {
				throw BinderException("common_crawl_index max_results parameter must be an integer");
			}
			bind_data->max_results = kv.second.GetValue<int64_t>();
			fprintf(stderr, "[DEBUG] CDX API max_results set to: %lu\n", (unsigned long)bind_data->max_results);
		} else {
			throw BinderException("Unknown parameter '%s' for common_crawl_index", kv.first.c_str());
		}
	}

	// Define output columns
	names.push_back("url");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("url");

	names.push_back("timestamp");
	return_types.push_back(LogicalType::TIMESTAMP_TZ);
	bind_data->fields_needed.push_back("timestamp");

	names.push_back("mime_type");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mime_type");

	names.push_back("status_code");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("status_code");

	names.push_back("digest");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("digest");

	names.push_back("filename");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("filename");

	names.push_back("offset");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("offset");

	names.push_back("length");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("length");

	// Add crawl_id column (populated from index_name)
	names.push_back("crawl_id");
	return_types.push_back(LogicalType::VARCHAR);

	// Add warc STRUCT with WARC metadata
	// Fields: version (VARCHAR), headers (MAP)
	names.push_back("warc");
	child_list_t<LogicalType> warc_children;
	warc_children.push_back(make_pair("version", LogicalType::VARCHAR));
	warc_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	return_types.push_back(LogicalType::STRUCT(warc_children));

	// Add response STRUCT with HTTP response details
	// Fields: body (BLOB), headers (MAP), http_version (VARCHAR)
	names.push_back("response");
	child_list_t<LogicalType> response_children;
	response_children.push_back(make_pair("body", LogicalType::BLOB));
	response_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	response_children.push_back(make_pair("http_version", LogicalType::VARCHAR));
	return_types.push_back(LogicalType::STRUCT(response_children));

	// Enable response fetching (implemented with HTTP range requests + gzip decompression)
	// Note: This will fetch WARC files which can be slow for large result sets
	// Projection pushdown will control whether this is actually fetched
	bind_data->fetch_response = true;

	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function
static unique_ptr<GlobalTableFunctionState> CommonCrawlInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	fprintf(stderr, "[DEBUG] CommonCrawlInitGlobal called\n");
	auto &bind_data = const_cast<CommonCrawlBindData&>(input.bind_data->Cast<CommonCrawlBindData>());
	auto state = make_uniq<CommonCrawlGlobalState>();

	// FIXME: LIMIT pushdown for DuckDB v1.5+
	// ===========================================
	// Current limitation: DuckDB v1.4.2 does not expose LIMIT value in TableFunctionInitInput
	//
	// When upgrading to DuckDB v1.5 or later:
	// 1. Check if TableFunctionInitInput has a 'max_rows' field
	// 2. Uncomment the following code to enable automatic LIMIT pushdown:
	//
	// if (input.max_rows != NumericLimits<idx_t>::Maximum()) {
	//     bind_data.max_results = input.max_rows;
	//     fprintf(stderr, "[DEBUG] LIMIT pushdown: max_results set to %lu\n",
	//             (unsigned long)bind_data.max_results);
	// }
	//
	// 3. Test with queries like: SELECT * FROM common_crawl_index() LIMIT 10
	//    and verify that CDX API receives &limit=10 instead of &limit=10000
	// 4. Update CLAUDE.md to document automatic LIMIT pushdown capability
	// 5. Remove this FIXME comment
	//
	// Current behavior: Uses default max_results=10000 for all queries
	// Users should use WHERE clauses to narrow down result sets

	// If no crawl_id was specified in WHERE clause, fetch the latest one
	// Don't fetch if crawl_ids vector is populated (from IN clause)
	if (bind_data.index_name.empty() && bind_data.crawl_ids.empty()) {
		bind_data.index_name = GetLatestCrawlId(context);
		fprintf(stderr, "[DEBUG] Using latest crawl_id: %s\n", bind_data.index_name.c_str());
	}

	// Store which columns are selected for projection pushdown
	state->column_ids = input.column_ids;
	fprintf(stderr, "[DEBUG] Projected columns: ");
	for (auto &col_id : input.column_ids) {
		fprintf(stderr, "%lu ", (unsigned long)col_id);
	}
	fprintf(stderr, "\n");

	// CDX API fields that can be requested from index.commoncrawl.org
	// All other columns are computed by our extension
	static const std::unordered_set<string> cdx_fields = {
		"url", "timestamp", "mime_type", "status_code",
		"digest", "filename", "offset", "length"
	};

	// Determine which fields are actually needed based on projection
	vector<string> needed_fields;
	bool need_response = false;
	bool need_warc = false;

	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			fprintf(stderr, "[DEBUG] Column %lu = %s\n", (unsigned long)col_id, col_name.c_str());

			// Check if we need to fetch WARC data for this column
			if (col_name == "warc" || col_name == "response") {
				need_response = true;
				need_warc = true; // WARC response parsing requires filename/offset/length
			} else if (col_name == "filename" || col_name == "offset" || col_name == "length") {
				need_warc = true;
			}

			// Only add CDX fields to needed_fields (whitelist approach)
			// Non-CDX columns are computed by the extension:
			// - crawl_id (from index name)
			// - warc, response (from WARC file)
			if (cdx_fields.count(col_name) > 0) {
				needed_fields.push_back(col_name);
			}
		}
	}

	// If we need WARC data, ensure we have filename, offset, length
	if (need_warc) {
		if (std::find(needed_fields.begin(), needed_fields.end(), "filename") == needed_fields.end()) {
			needed_fields.push_back("filename");
		}
		if (std::find(needed_fields.begin(), needed_fields.end(), "offset") == needed_fields.end()) {
			needed_fields.push_back("offset");
		}
		if (std::find(needed_fields.begin(), needed_fields.end(), "length") == needed_fields.end()) {
			needed_fields.push_back("length");
		}
	}

	// Override fetch_response based on projection
	bind_data.fetch_response = need_response;
	fprintf(stderr, "[DEBUG] fetch_response = %d, need_warc = %d\n", bind_data.fetch_response, need_warc);

	// Ensure we always request at least 'url' field from CDX API
	// (e.g., when only crawl_id is selected, we still need a CDX field to get records)
	if (needed_fields.empty()) {
		needed_fields.push_back("url");
	}

	// Use the URL filter from bind data (could be set via filter pushdown)
	string url_pattern = bind_data.url_filter;
	fprintf(stderr, "[DEBUG] About to call QueryCDXAPI with %lu fields\n", (unsigned long)needed_fields.size());

	// Query CDX API - handle multiple crawl_ids if IN clause was used
	if (!bind_data.crawl_ids.empty()) {
		// IN clause detected: query each crawl_id in parallel and combine results
		fprintf(stderr, "[DEBUG] Launching %lu parallel CDX API requests\n", (unsigned long)bind_data.crawl_ids.size());

		std::vector<std::future<vector<CDXRecord>>> futures;
		futures.reserve(bind_data.crawl_ids.size());

		// Launch async requests for each crawl_id
		for (const auto &crawl_id : bind_data.crawl_ids) {
			futures.push_back(std::async(std::launch::async, [&context, crawl_id, url_pattern, &needed_fields, &bind_data]() {
				return QueryCDXAPI(context, crawl_id, url_pattern, needed_fields, bind_data.cdx_filters, bind_data.max_results);
			}));
		}

		// Collect results from all futures
		for (auto &future : futures) {
			auto records = future.get();
			state->records.insert(state->records.end(), records.begin(), records.end());
		}

		fprintf(stderr, "[DEBUG] All parallel requests completed, total records: %lu\n", (unsigned long)state->records.size());
	} else {
		// Single crawl_id: use index_name
		state->records = QueryCDXAPI(context, bind_data.index_name, url_pattern, needed_fields, bind_data.cdx_filters, bind_data.max_results);
		fprintf(stderr, "[DEBUG] QueryCDXAPI returned %lu records\n", (unsigned long)state->records.size());
	}

	return std::move(state);
}

// Helper function to parse CDX timestamp format (YYYYMMDDHHmmss) to DuckDB timestamp
static timestamp_t ParseCDXTimestamp(const string &cdx_timestamp) {
	if (cdx_timestamp.length() != 14) {
		return timestamp_t(0); // Return epoch if invalid format
	}

	try {
		int32_t year = std::stoi(cdx_timestamp.substr(0, 4));
		int32_t month = std::stoi(cdx_timestamp.substr(4, 2));
		int32_t day = std::stoi(cdx_timestamp.substr(6, 2));
		int32_t hour = std::stoi(cdx_timestamp.substr(8, 2));
		int32_t minute = std::stoi(cdx_timestamp.substr(10, 2));
		int32_t second = std::stoi(cdx_timestamp.substr(12, 2));

		// Use DuckDB's Timestamp::FromDatetime to create timestamp
		date_t date = Date::FromDate(year, month, day);
		dtime_t time = Time::FromTime(hour, minute, second, 0);
		return Timestamp::FromDatetime(date, time);
	} catch (...) {
		return timestamp_t(0); // Return epoch on parse error
	}
}

// Helper function to fetch the latest crawl_id from collinfo.json (with 1-day caching)
static string GetLatestCrawlId(ClientContext &context) {
	// Check if cache is valid and not expired
	if (!g_collinfo_cache.IsExpired()) {
		return g_collinfo_cache.latest_crawl_id;
	}

	// Fetch collinfo.json using httpfs and json extension
	fprintf(stderr, "[DEBUG] Fetching latest crawl_id from collinfo.json\n");

	string collinfo_url = "https://index.commoncrawl.org/collinfo.json";
	Connection con(context.db->GetDatabase(context));

	// Ensure json extension is loaded
	con.Query("INSTALL json");
	con.Query("LOAD json");

	// Read the first entry from collinfo.json (newest crawl)
	auto result = con.Query("SELECT id FROM read_json('" + collinfo_url + "') LIMIT 1");

	if (result->HasError()) {
		throw IOException("Failed to fetch collinfo.json: " + result->GetError());
	}

	if (result->RowCount() == 0) {
		throw IOException("collinfo.json returned no results");
	}

	auto latest_id = result->GetValue(0, 0).ToString();

	// Update cache
	g_collinfo_cache.latest_crawl_id = latest_id;
	g_collinfo_cache.cached_at = std::chrono::system_clock::now();
	g_collinfo_cache.is_valid = true;

	fprintf(stderr, "[DEBUG] Latest crawl_id: %s\n", latest_id.c_str());
	return latest_id;
}

// Scan function for the table function
static void CommonCrawlScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	fprintf(stderr, "[DEBUG] CommonCrawlScan called\n");
	auto &bind_data = data.bind_data->Cast<CommonCrawlBindData>();
	auto &gstate = data.global_state->Cast<CommonCrawlGlobalState>();
	fprintf(stderr, "[DEBUG] Have %lu records to process\n", (unsigned long)gstate.records.size());

	// Pre-fetch WARCs in parallel for this chunk if needed
	std::vector<WARCResponse> warc_responses;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		fprintf(stderr, "[DEBUG] Pre-fetching %lu WARCs in parallel\n", (unsigned long)chunk_size);
		std::vector<std::future<WARCResponse>> warc_futures;
		warc_futures.reserve(chunk_size);

		// Launch parallel WARC fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			warc_futures.push_back(std::async(std::launch::async, [&context, record]() {
				return FetchWARCResponse(context, record);
			}));
		}

		// Collect results
		warc_responses.reserve(chunk_size);
		for (auto &future : warc_futures) {
			warc_responses.push_back(future.get());
		}
		fprintf(stderr, "[DEBUG] All %lu WARCs fetched\n", (unsigned long)chunk_size);
	}

	idx_t output_offset = 0;
	while (gstate.current_position < gstate.records.size() && output_offset < STANDARD_VECTOR_SIZE) {
		auto &record = gstate.records[gstate.current_position];

		bool row_success = true;

		// Process each projected column
		for (idx_t proj_idx = 0; proj_idx < gstate.column_ids.size(); proj_idx++) {
			auto col_id = gstate.column_ids[proj_idx];
			string col_name = bind_data.column_names[col_id];

			try {
				if (col_name == "url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.url));
				} else if (col_name == "timestamp") {
					auto data_ptr = FlatVector::GetData<timestamp_t>(output.data[proj_idx]);
					data_ptr[output_offset] = ParseCDXTimestamp(record.timestamp);
				} else if (col_name == "mime_type") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.mime_type));
				} else if (col_name == "status_code") {
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.status_code;
				} else if (col_name == "digest") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.digest));
				} else if (col_name == "filename") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.filename));
				} else if (col_name == "offset") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.offset;
				} else if (col_name == "length") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.length;
				} else if (col_name == "crawl_id") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], record.crawl_id);
				} else if (col_name == "warc" || col_name == "response") {
					if (bind_data.fetch_response && !warc_responses.empty()) {
						WARCResponse &warc_response = warc_responses[output_offset];

						if (col_name == "warc") {
							// WARC STRUCT with version and headers
							auto &struct_vector = output.data[proj_idx];
							auto &struct_children = StructVector::GetEntries(struct_vector);

							// Child 0: version (VARCHAR)
							auto &version_vector = struct_children[0];
							auto version_data = FlatVector::GetData<string_t>(*version_vector);
							version_data[output_offset] = StringVector::AddString(*version_vector,
							                                                        SanitizeUTF8(warc_response.warc_version));

							// Child 1: headers (MAP)
							auto &headers_map = struct_children[1];
							auto &map_keys = MapVector::GetKeys(*headers_map);
							auto &map_values = MapVector::GetValues(*headers_map);

							idx_t map_offset = ListVector::GetListSize(*headers_map);
							idx_t new_size = map_offset + warc_response.warc_headers.size();
							ListVector::Reserve(*headers_map, new_size);

							auto key_data = FlatVector::GetData<string_t>(map_keys);
							auto value_data = FlatVector::GetData<string_t>(map_values);

							for (const auto &header : warc_response.warc_headers) {
								key_data[map_offset] = StringVector::AddString(map_keys, header.first);
								value_data[map_offset] = StringVector::AddString(map_values, SanitizeUTF8(header.second));
								map_offset++;
							}

							auto map_data = FlatVector::GetData<list_entry_t>(*headers_map);
							map_data[output_offset].offset = ListVector::GetListSize(*headers_map);
							map_data[output_offset].length = warc_response.warc_headers.size();
							ListVector::SetListSize(*headers_map, map_offset);

						} else if (col_name == "response") {
							// Response STRUCT with body, headers, http_version
							auto &struct_vector = output.data[proj_idx];
							auto &struct_children = StructVector::GetEntries(struct_vector);

							// Child 0: body (BLOB)
							auto &body_vector = struct_children[0];
							auto body_data = FlatVector::GetData<string_t>(*body_vector);
							body_data[output_offset] = StringVector::AddStringOrBlob(*body_vector, warc_response.body);

							// Child 1: headers (MAP)
							auto &headers_map = struct_children[1];
							auto &map_keys = MapVector::GetKeys(*headers_map);
							auto &map_values = MapVector::GetValues(*headers_map);

							idx_t map_offset = ListVector::GetListSize(*headers_map);
							idx_t new_size = map_offset + warc_response.http_headers.size();
							ListVector::Reserve(*headers_map, new_size);

							auto key_data = FlatVector::GetData<string_t>(map_keys);
							auto value_data = FlatVector::GetData<string_t>(map_values);

							for (const auto &header : warc_response.http_headers) {
								key_data[map_offset] = StringVector::AddString(map_keys, header.first);
								value_data[map_offset] = StringVector::AddString(map_values, SanitizeUTF8(header.second));
								map_offset++;
							}

							auto map_data = FlatVector::GetData<list_entry_t>(*headers_map);
							map_data[output_offset].offset = ListVector::GetListSize(*headers_map);
							map_data[output_offset].length = warc_response.http_headers.size();
							ListVector::SetListSize(*headers_map, map_offset);

							// Child 2: http_version (VARCHAR)
							auto &version_vector = struct_children[2];
							auto version_data = FlatVector::GetData<string_t>(*version_vector);
							version_data[output_offset] = StringVector::AddString(*version_vector,
							                                                        SanitizeUTF8(warc_response.http_version));
						}
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				}
			} catch (const std::exception &ex) {
				fprintf(stderr, "[ERROR] Failed to process column %s for row %lu: %s\n",
				        col_name.c_str(), (unsigned long)gstate.current_position, ex.what());
				row_success = false;
				break;
			}
		}

		if (row_success) {
			output_offset++;
		}
		gstate.current_position++;
	}

	output.SetCardinality(output_offset);
}

// Helper function to convert SQL LIKE wildcards to CDX API wildcards
static string ConvertSQLWildcardsToCDX(const string &pattern) {
	string result;
	result.reserve(pattern.length());

	for (char ch : pattern) {
		if (ch == '%') {
			// SQL % (zero or more chars) -> CDX * (zero or more chars)
			result += '*';
		} else if (ch == '_') {
			// SQL _ (single char) -> CDX ? (single char, if supported)
			// Note: CDX API may not support ?, but we'll convert it anyway
			result += '?';
		} else {
			result += ch;
		}
	}

	return result;
}

// Filter pushdown function to handle WHERE clauses
static void CommonCrawlPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                               vector<unique_ptr<Expression>> &filters) {
	fprintf(stderr, "[DEBUG] CommonCrawlPushdownComplexFilter called with %lu filters\n", (unsigned long)filters.size());
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();


	// Build a map of column names to their indices
	std::unordered_map<string, idx_t> column_map;
	for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
		column_map[bind_data.column_names[i]] = i;
	}

	// Look for filters we can push down
	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		fprintf(stderr, "[DEBUG] Filter %lu: class=%d\n",
		        (unsigned long)i, (int)filter->GetExpressionClass());

		// Handle BOUND_OPERATOR for IN clauses (e.g., crawl_id IN ('id1', 'id2'))
		// DuckDB represents IN as a BOUND_OPERATOR with children: [column, value1, value2, ...]
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op = filter->Cast<BoundOperatorExpression>();

			// Check if first child is a crawl_id column and rest are constants
			if (op.children.size() >= 2 &&
			    op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();

				if (col_ref.GetName() == "crawl_id") {
					vector<string> crawl_id_values;
					bool all_constants = true;

					// Extract all constant values (children[1] onwards)
					for (size_t j = 1; j < op.children.size(); j++) {
						if (op.children[j]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
							auto &constant = op.children[j]->Cast<BoundConstantExpression>();
							if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
								crawl_id_values.push_back(constant.value.ToString());
								continue;
							}
						}
						all_constants = false;
						break;
					}

					if (all_constants && !crawl_id_values.empty()) {
						bind_data.crawl_ids = crawl_id_values;
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}

			// Alternative: Check if this is an IN operator (children[0] should be wrapped in CONJUNCTION due to optimization)
			if (op.children.size() == 1 &&
			    op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
				auto &conjunction = op.children[0]->Cast<BoundConjunctionExpression>();
				fprintf(stderr, "[DEBUG] Found CONJUNCTION inside OPERATOR, type=%d, children=%lu\n",
				        (int)conjunction.type, (unsigned long)conjunction.children.size());

				// Only handle OR conjunctions (IN clauses become OR of equalities)
				if (conjunction.type == ExpressionType::CONJUNCTION_OR) {
					vector<string> crawl_id_values;
					bool all_crawl_id_comparisons = true;

					// Check if all children are crawl_id = 'value' comparisons
					for (auto &child : conjunction.children) {
						if (child->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON &&
						    child->type == ExpressionType::COMPARE_EQUAL) {
							auto &comp = child->Cast<BoundComparisonExpression>();

							if (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
							    comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
								auto &col_ref = comp.left->Cast<BoundColumnRefExpression>();
								auto &constant = comp.right->Cast<BoundConstantExpression>();

								if (col_ref.GetName() == "crawl_id" &&
								    constant.value.type().id() == LogicalTypeId::VARCHAR) {
									crawl_id_values.push_back(constant.value.ToString());
									continue;
								}
							}
						}
						all_crawl_id_comparisons = false;
						break;
					}

					// If we successfully extracted all crawl_id values from IN clause
					if (all_crawl_id_comparisons && !crawl_id_values.empty()) {
						bind_data.crawl_ids = crawl_id_values;
						fprintf(stderr, "[DEBUG] IN clause detected with %lu crawl_ids\n",
						        (unsigned long)crawl_id_values.size());
						for (const auto &id : crawl_id_values) {
							fprintf(stderr, "[DEBUG]   - %s\n", id.c_str());
						}
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle BOUND_FUNCTION for LIKE expressions
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();
			fprintf(stderr, "[DEBUG] Function name: %s\n", func.function.name.c_str());

			// Check if this is a CONTAINS function (DuckDB optimizes LIKE '%string%' to contains)
			if (func.function.name == "contains") {
				// CONTAINS has 2 children: column and search string
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string search_string = constant.value.ToString();
						fprintf(stderr, "[DEBUG] CONTAINS URL filter search string: '%s'\n", search_string.c_str());
						// Convert to CDX wildcard pattern: *search_string*
						bind_data.url_filter = "*" + search_string + "*";
						fprintf(stderr, "[DEBUG] CONTAINS URL filter pattern: '%s'\n", bind_data.url_filter.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
			// Check if this is a LIKE function
			else if (func.function.name == "like" || func.function.name == "~~") {
				// LIKE has 2 children: column and pattern
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string original_pattern = constant.value.ToString();
						fprintf(stderr, "[DEBUG] LIKE URL filter original pattern: '%s'\n", original_pattern.c_str());
						// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
						bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
						fprintf(stderr, "[DEBUG] LIKE URL filter converted pattern: '%s'\n", bind_data.url_filter.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Check if this is a comparison expression
		if (filter->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}
		if (filter->type != ExpressionType::COMPARE_EQUAL &&
		    filter->type != ExpressionType::COMPARE_NOTEQUAL) {
			continue;
		}

		auto &comparison = filter->Cast<BoundComparisonExpression>();

		// Check if left side is a bound column reference
		if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			continue;
		}
		auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();

		// Check if right side is a constant value
		if (comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			continue;
		}
		auto &constant = comparison.right->Cast<BoundConstantExpression>();

		// Get the column binding
		idx_t table_index = col_ref.binding.table_index;

		// Get the column name from the expression itself (this is the most reliable way)
		string column_name = col_ref.GetName();

		// Validate table index matches our table
		if (table_index != get.table_index) {
			continue;
		}

		// Handle URL filtering (special case - uses url parameter)
		if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			string original_pattern = constant.value.ToString();
			fprintf(stderr, "[DEBUG] URL filter original pattern: '%s'\n", original_pattern.c_str());
			// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
			bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
			fprintf(stderr, "[DEBUG] URL filter converted pattern: '%s'\n", bind_data.url_filter.c_str());
			filters_to_remove.push_back(i);
		}
		// Handle crawl_id filtering (sets the index_name to use)
		else if (column_name == "crawl_id" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				bind_data.index_name = constant.value.ToString();
				fprintf(stderr, "[DEBUG] crawl_id filter set index_name to: %s\n", bind_data.index_name.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle status_code filtering (uses CDX filter parameter)
		// CDX API syntax: filter==statuscode:200 means &filter==statuscode:200
		// (one = for parameter, one = for exact match operator)
		else if (column_name == "status_code" &&
		         (constant.value.type().id() == LogicalTypeId::INTEGER ||
		          constant.value.type().id() == LogicalTypeId::BIGINT)) {
			string op = (filter->type == ExpressionType::COMPARE_EQUAL) ? "=" : "!";
			string filter_str = op + "statuscode:" + to_string(constant.value.GetValue<int32_t>());
			bind_data.cdx_filters.push_back(filter_str);
			filters_to_remove.push_back(i);
		}
		// Handle mime_type filtering (uses CDX filter parameter)
		else if (column_name == "mime_type" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			string op = (filter->type == ExpressionType::COMPARE_EQUAL) ? "=" : "!";
			string filter_str = op + "mime:" + constant.value.ToString();
			bind_data.cdx_filters.push_back(filter_str);
			filters_to_remove.push_back(i);
		}
	}

	// Remove filters that we've pushed down (in reverse order to maintain indices)
	for (idx_t i = filters_to_remove.size(); i > 0; i--) {
		filters.erase(filters.begin() + filters_to_remove[i - 1]);
	}
}

// Cardinality function to provide row count estimates to DuckDB optimizer
static unique_ptr<NodeStatistics> CommonCrawlCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();
	// Return the max results as an estimate - this helps DuckDB optimize the query plan
	// FIXME: With DuckDB v1.5+ LIMIT pushdown, this will accurately reflect the LIMIT value
	// which will enable better query optimization (e.g., choosing more efficient join strategies)
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// INTERNET ARCHIVE TABLE FUNCTION
// ========================================

// Structure to hold bind data for internet_archive table function
struct InternetArchiveBindData : public TableFunctionData {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	string url_filter;
	string match_type; // exact, prefix, host, domain
	vector<string> cdx_filters; // filter=field:regex
	string from_date; // YYYYMMDDhhmmss
	string to_date;   // YYYYMMDDhhmmss
	idx_t max_results; // Default limit

	InternetArchiveBindData() : fetch_response(false), url_filter("*"), match_type("exact"), max_results(100) {}
};

// Structure to hold global state for internet_archive table function
struct InternetArchiveGlobalState : public GlobalTableFunctionState {
	vector<ArchiveOrgRecord> records;
	idx_t current_position;
	vector<column_t> column_ids;

	InternetArchiveGlobalState() : current_position(0) {}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded
	}
};

// Helper function to query Internet Archive CDX API
static vector<ArchiveOrgRecord> QueryArchiveOrgCDX(ClientContext &context, const string &url_pattern,
                                                     const string &match_type, const vector<string> &fields_needed,
                                                     const vector<string> &cdx_filters, const string &from_date,
                                                     const string &to_date, idx_t max_results) {
	fprintf(stderr, "[DEBUG] QueryArchiveOrgCDX started\n");
	vector<ArchiveOrgRecord> records;

	// Construct field list for &fl= parameter
	string field_list = "urlkey,timestamp,original,mimetype,statuscode,digest,length";

	// Construct the CDX API URL
	string cdx_url = "https://web.archive.org/cdx/search/cdx?url=" + url_pattern +
	                 "&output=json&fl=" + field_list;

	// Add matchType if not exact (default)
	if (match_type != "exact") {
		cdx_url += "&matchType=" + match_type;
	}

	// Add date range filters
	if (!from_date.empty()) {
		cdx_url += "&from=" + from_date;
	}
	if (!to_date.empty()) {
		cdx_url += "&to=" + to_date;
	}

	// Add limit
	cdx_url += "&limit=" + to_string(max_results);

	// Add filter parameters
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	fprintf(stderr, "[CDX URL] %s\n", cdx_url.c_str());

	try {
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(cdx_url, FileFlags::FILE_FLAGS_READ);

		// Read the response
		string response_data;
		const idx_t buffer_size = 8192;
		auto buffer = unique_ptr<char[]>(new char[buffer_size]);

		while (true) {
			int64_t bytes_read = file_handle->Read(buffer.get(), buffer_size);
			if (bytes_read <= 0) {
				break;
			}
			response_data.append(buffer.get(), bytes_read);
		}

		// Sanitize UTF-8
		response_data = SanitizeUTF8(response_data);

		// Parse newline-delimited JSON
		std::istringstream stream(response_data);
		string line;
		int line_count = 0;
		bool first_line = true;

		while (std::getline(stream, line)) {
			if (line.empty() || line[0] != '[') {
				continue;
			}
			line_count++;

			// Skip header line (first line with field names)
			if (first_line) {
				first_line = false;
				continue;
			}

			ArchiveOrgRecord record;

			// Extract fields from JSON line
			// Format: ["urlkey","timestamp","original","mimetype","statuscode","digest","length"]
			record.urlkey = ExtractJSONValue(line, "");  // Will need better parsing
			record.timestamp = ExtractJSONValue(line, "");
			record.original = ExtractJSONValue(line, "");

			// For now, parse the JSON array manually
			// Remove brackets and quotes, split by comma
			// This is a simple parser - a proper JSON parser would be better
			size_t start = line.find('[');
			size_t end = line.rfind(']');
			if (start != string::npos && end != string::npos && end > start) {
				string data = line.substr(start + 1, end - start - 1);

				// Parse array elements
				vector<string> values;
				size_t pos = 0;
				bool in_quotes = false;
				string current;

				for (size_t i = 0; i < data.length(); i++) {
					char c = data[i];
					if (c == '"' && (i == 0 || data[i-1] != '\\')) {
						in_quotes = !in_quotes;
					} else if (c == ',' && !in_quotes) {
						values.push_back(current);
						current.clear();
					} else if (c != '"') {
						current += c;
					}
				}
				if (!current.empty()) {
					values.push_back(current);
				}

				// Assign values if we have enough
				if (values.size() >= 7) {
					record.urlkey = values[0];
					record.timestamp = values[1];
					record.original = values[2];
					record.mime_type = values[3];
					record.status_code = values[4].empty() ? 0 : std::stoi(values[4]);
					record.digest = values[5];
					record.length = values[6].empty() ? 0 : std::stoll(values[6]);

					records.push_back(record);
				}
			}
		}
		fprintf(stderr, "[DEBUG] Parsed %d JSON lines, got %lu records\n", line_count, (unsigned long)records.size());

	} catch (std::exception &ex) {
		throw IOException("Error querying Internet Archive CDX API: " + string(ex.what()));
	}

	return records;
}

// Helper function to fetch archived page from Internet Archive
static string FetchArchivedPage(ClientContext &context, const ArchiveOrgRecord &record) {
	if (record.timestamp.empty() || record.original.empty()) {
		return "[Error: Missing timestamp or URL]";
	}

	try {
		// Construct the download URL with id_ suffix to get raw content
		string download_url = "https://web.archive.org/web/" + record.timestamp + "id_/" + record.original;
		fprintf(stderr, "[DEBUG] Fetching: %s\n", download_url.c_str());

		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(download_url, FileFlags::FILE_FLAGS_READ);

		// Read the response
		string response_data;
		const idx_t buffer_size = 8192;
		auto buffer = unique_ptr<char[]>(new char[buffer_size]);

		while (true) {
			int64_t bytes_read = file_handle->Read(buffer.get(), buffer_size);
			if (bytes_read <= 0) {
				break;
			}
			response_data.append(buffer.get(), bytes_read);
		}

		return response_data;

	} catch (Exception &ex) {
		return "[Error fetching archived page: " + string(ex.what()) + "]";
	} catch (std::exception &ex) {
		return "[Error fetching archived page: " + string(ex.what()) + "]";
	}
}

// Bind function for internet_archive table function
static unique_ptr<FunctionData> InternetArchiveBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	fprintf(stderr, "[DEBUG] InternetArchiveBind called\n");

	auto bind_data = make_uniq<InternetArchiveBindData>();

	// Handle named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "max_results") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT) {
				throw BinderException("internet_archive max_results parameter must be an integer");
			}
			bind_data->max_results = kv.second.GetValue<int64_t>();
			fprintf(stderr, "[DEBUG] CDX API max_results set to: %lu\n", (unsigned long)bind_data->max_results);
		} else {
			throw BinderException("Unknown parameter '%s' for internet_archive", kv.first.c_str());
		}
	}

	// Define output columns
	names.push_back("url");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("original");

	names.push_back("timestamp");
	return_types.push_back(LogicalType::TIMESTAMP_TZ);
	bind_data->fields_needed.push_back("timestamp");

	names.push_back("urlkey");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("urlkey");

	names.push_back("mime_type");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mimetype");

	names.push_back("status_code");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("statuscode");

	names.push_back("digest");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("digest");

	names.push_back("length");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("length");

	// Add response column (BLOB for raw HTTP body)
	names.push_back("response");
	return_types.push_back(LogicalType::BLOB);

	// Don't set fetch_response here - will be determined by projection pushdown
	bind_data->fetch_response = false;
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function for internet_archive
static unique_ptr<GlobalTableFunctionState> InternetArchiveInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	fprintf(stderr, "[DEBUG] InternetArchiveInitGlobal called\n");
	auto &bind_data = const_cast<InternetArchiveBindData&>(input.bind_data->Cast<InternetArchiveBindData>());
	auto state = make_uniq<InternetArchiveGlobalState>();

	// Store projected columns
	state->column_ids = input.column_ids;

	// Determine if we need to fetch response based on projection
	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			fprintf(stderr, "[DEBUG] Projected column: %s\n", col_name.c_str());
			if (col_name == "response") {
				bind_data.fetch_response = true;
				fprintf(stderr, "[DEBUG] Will fetch response bodies\n");
			}
		}
	}

	// Query Internet Archive CDX API
	state->records = QueryArchiveOrgCDX(context, bind_data.url_filter, bind_data.match_type,
	                                     bind_data.fields_needed, bind_data.cdx_filters,
	                                     bind_data.from_date, bind_data.to_date, bind_data.max_results);

	fprintf(stderr, "[DEBUG] QueryArchiveOrgCDX returned %lu records\n", (unsigned long)state->records.size());

	return std::move(state);
}

// Scan function for internet_archive table function
static void InternetArchiveScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<InternetArchiveBindData>();
	auto &gstate = data.global_state->Cast<InternetArchiveGlobalState>();

	// Pre-fetch responses in parallel for this chunk if needed
	std::vector<string> response_bodies;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		fprintf(stderr, "[DEBUG] Pre-fetching %lu archived pages in parallel\n", (unsigned long)chunk_size);
		std::vector<std::future<string>> response_futures;
		response_futures.reserve(chunk_size);

		// Launch parallel fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			response_futures.push_back(std::async(std::launch::async, [&context, record]() {
				return FetchArchivedPage(context, record);
			}));
		}

		// Collect results
		response_bodies.reserve(chunk_size);
		for (auto &future : response_futures) {
			response_bodies.push_back(future.get());
		}
		fprintf(stderr, "[DEBUG] All %lu archived pages fetched\n", (unsigned long)chunk_size);
	}

	idx_t output_offset = 0;
	while (gstate.current_position < gstate.records.size() && output_offset < STANDARD_VECTOR_SIZE) {
		auto &record = gstate.records[gstate.current_position];

		// Process each projected column
		for (idx_t proj_idx = 0; proj_idx < gstate.column_ids.size(); proj_idx++) {
			auto col_id = gstate.column_ids[proj_idx];
			string col_name = bind_data.column_names[col_id];

			try {
				if (col_name == "url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.original));
				} else if (col_name == "timestamp") {
					auto data_ptr = FlatVector::GetData<timestamp_t>(output.data[proj_idx]);
					data_ptr[output_offset] = ParseCDXTimestamp(record.timestamp);
				} else if (col_name == "urlkey") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.urlkey));
				} else if (col_name == "mime_type") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.mime_type));
				} else if (col_name == "status_code") {
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.status_code;
				} else if (col_name == "digest") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.digest));
				} else if (col_name == "length") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.length;
				} else if (col_name == "response") {
					if (bind_data.fetch_response && !response_bodies.empty()) {
						string &body = response_bodies[output_offset];
						auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
						data_ptr[output_offset] = StringVector::AddStringOrBlob(output.data[proj_idx], body);
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				}
			} catch (const std::exception &ex) {
				fprintf(stderr, "[ERROR] Failed to process column %s: %s\n", col_name.c_str(), ex.what());
			}
		}

		output_offset++;
		gstate.current_position++;
	}

	output.SetCardinality(output_offset);
}

// Filter pushdown for internet_archive
static void InternetArchivePushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                   vector<unique_ptr<Expression>> &filters) {
	fprintf(stderr, "[DEBUG] InternetArchivePushdownComplexFilter called with %lu filters\n", (unsigned long)filters.size());
	auto &bind_data = bind_data_p->Cast<InternetArchiveBindData>();

	// Build column map
	std::unordered_map<string, idx_t> column_map;
	for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
		column_map[bind_data.column_names[i]] = i;
	}

	vector<idx_t> filters_to_remove;

	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];

		// Handle LIKE/CONTAINS for URL filtering
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();

			if ((func.function.name == "contains" || func.function.name == "like" || func.function.name == "~~") &&
			    func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					bind_data.url_filter = constant.value.ToString();

					// Detect matchType from pattern
					if (bind_data.url_filter.find("*") == 0) {
						bind_data.match_type = "domain";
						bind_data.url_filter = bind_data.url_filter.substr(1); // Remove leading *
					} else if (bind_data.url_filter.find("/*") != string::npos) {
						bind_data.match_type = "prefix";
						bind_data.url_filter = bind_data.url_filter.substr(0, bind_data.url_filter.find("/*"));
					}

					fprintf(stderr, "[DEBUG] URL filter: %s, matchType: %s\n",
					        bind_data.url_filter.c_str(), bind_data.match_type.c_str());
					filters_to_remove.push_back(i);
					continue;
				}
			}
		}

		// Handle comparison filters
		if (filter->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}

		auto &comparison = filter->Cast<BoundComparisonExpression>();
		if (comparison.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    comparison.right->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			continue;
		}

		auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
		auto &constant = comparison.right->Cast<BoundConstantExpression>();
		string column_name = col_ref.GetName();

		// Handle URL filtering via equality/LIKE
		if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				bind_data.url_filter = constant.value.ToString();
				bind_data.match_type = "exact";
				fprintf(stderr, "[DEBUG] URL filter (exact): %s\n", bind_data.url_filter.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle status_code filtering
		if (column_name == "status_code" &&
		    (constant.value.type().id() == LogicalTypeId::INTEGER ||
		     constant.value.type().id() == LogicalTypeId::BIGINT)) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				string filter_str = "statuscode:" + to_string(constant.value.GetValue<int32_t>());
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				string filter_str = "!statuscode:" + to_string(constant.value.GetValue<int32_t>());
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			}
		}
		// Handle mime_type filtering
		else if (column_name == "mime_type" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				string filter_str = "mimetype:" + constant.value.ToString();
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				string filter_str = "!mimetype:" + constant.value.ToString();
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			}
		}
	}

	// Remove pushed down filters
	for (idx_t i = filters_to_remove.size(); i > 0; i--) {
		filters.erase(filters.begin() + filters_to_remove[i - 1]);
	}
}

// Cardinality function for internet_archive
static unique_ptr<NodeStatistics> InternetArchiveCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<InternetArchiveBindData>();
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// EXTENSION LOADER
// ========================================

// Optimizer function to push down LIMIT to internet_archive function
static void OptimizeInternetArchiveLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find the GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "internet_archive") {
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits (not expressions)
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			OptimizeInternetArchiveLimitPushdown(op->children[0]);
			return;
		}

		// Extract limit value and store in bind_data
		auto &bind_data = get.bind_data->Cast<InternetArchiveBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			bind_data.max_results = limit.limit_val.GetConstantValue();
			fprintf(stderr, "[DEBUG] LIMIT pushdown: max_results set to %lu\n",
			        (unsigned long)bind_data.max_results);

			// Remove the LIMIT node from the plan since we've pushed it down
			op = std::move(op->children[0]);
			return;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeInternetArchiveLimitPushdown(child);
	}
}

static void CommonCrawlOptimizer(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeInternetArchiveLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
	fprintf(stderr, "\n[DEBUG] *** COMMON_CRAWL EXTENSION LOADING ***\n");
	fflush(stderr);

	// Note: httpfs extension must be loaded before using this extension
	// Users should run: INSTALL httpfs; LOAD httpfs; before loading this extension
	// Or set autoload_known_extensions=1 and autoinstall_known_extensions=1

	// Register the common_crawl_index table function
	// Usage: SELECT * FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' LIMIT 10
	// Usage with max_results: SELECT * FROM common_crawl_index(max_results := 500) WHERE crawl_id = 'CC-MAIN-2025-43'
	// - crawl_id filtering is done via WHERE clause (defaults to latest if not specified)
	// - URL filtering: WHERE url LIKE '*.example.com/*'
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet common_crawl_set("common_crawl_index");

	auto func = TableFunction({},
	                          CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);
	func.cardinality = CommonCrawlCardinality;
	func.pushdown_complex_filter = CommonCrawlPushdownComplexFilter;
	func.projection_pushdown = true;

	// Add named parameter
	func.named_parameters["max_results"] = LogicalType::BIGINT;

	common_crawl_set.AddFunction(func);

	loader.RegisterFunction(common_crawl_set);

	// Register the internet_archive table function
	// Usage: SELECT * FROM internet_archive() WHERE url = 'archive.org' LIMIT 10
	// Usage with max_results: SELECT * FROM internet_archive(max_results := 500) WHERE url = 'archive.org'
	// - URL filtering via WHERE clause
	// - Supports matchType detection (exact, prefix, host, domain)
	// - Much simpler than common_crawl - no WARC parsing needed
	// - Projection pushdown: only fetches response when needed
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet internet_archive_set("internet_archive");

	auto ia_func = TableFunction({},
	                              InternetArchiveScan, InternetArchiveBind, InternetArchiveInitGlobal);
	ia_func.cardinality = InternetArchiveCardinality;
	ia_func.pushdown_complex_filter = InternetArchivePushdownComplexFilter;
	ia_func.projection_pushdown = true;

	// Add named parameter
	ia_func.named_parameters["max_results"] = LogicalType::BIGINT;

	internet_archive_set.AddFunction(ia_func);

	loader.RegisterFunction(internet_archive_set);

	// Register optimizer extension for LIMIT pushdown
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension optimizer;
	optimizer.optimize_function = CommonCrawlOptimizer;
	config.optimizer_extensions.push_back(std::move(optimizer));
	fprintf(stderr, "[DEBUG] Optimizer extension registered for LIMIT pushdown\n");
}

void CommonCrawlExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string CommonCrawlExtension::Name() {
	return "common_crawl";
}

std::string CommonCrawlExtension::Version() const {
#ifdef EXT_VERSION_COMMON_CRAWL
	return EXT_VERSION_COMMON_CRAWL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(common_crawl, loader) {
	duckdb::LoadInternal(loader);
}
}
