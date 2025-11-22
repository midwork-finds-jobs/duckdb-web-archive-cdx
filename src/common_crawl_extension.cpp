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
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
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

// Structure to hold CDX record data
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

	// Default CDX limit reduced to 1000 for better performance with small LIMIT clauses
	// Users can override with: common_crawl_index(10000) if they need more records
	CommonCrawlBindData(string index) : index_name(std::move(index)), fetch_response(false), url_filter("*"), max_results(1000) {}
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
	string headers;
	string body;
};

// Helper function to parse WARC format and extract HTTP response headers and body
static WARCResponse ParseWARCResponse(const string &warc_data) {
	WARCResponse result;

	// WARC format has WARC headers followed by HTTP response (which has HTTP headers + body)

	// Find the end of WARC headers (double newline)
	size_t warc_headers_end = warc_data.find("\r\n\r\n");
	if (warc_headers_end == string::npos) {
		warc_headers_end = warc_data.find("\n\n");
		if (warc_headers_end == string::npos) {
			return result; // Invalid WARC format - return empty
		}
		warc_headers_end += 2;
	} else {
		warc_headers_end += 4;
	}

	// After WARC headers comes the HTTP response
	// Find the end of HTTP headers (double newline)
	size_t http_headers_start = warc_headers_end;
	size_t http_headers_end = warc_data.find("\r\n\r\n", http_headers_start);
	if (http_headers_end == string::npos) {
		http_headers_end = warc_data.find("\n\n", http_headers_start);
		if (http_headers_end == string::npos) {
			return result; // Invalid HTTP format - return empty
		}
		http_headers_end += 2;
	} else {
		http_headers_end += 4;
	}

	// Extract HTTP headers (from after WARC headers to before body)
	result.headers = warc_data.substr(http_headers_start, http_headers_end - http_headers_start);

	// Extract HTTP body
	result.body = warc_data.substr(http_headers_end);

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

	// No parameters required - crawl_id will be set from WHERE clause or default to latest
	// Optional: single parameter for cdx_limit
	if (input.inputs.size() > 1) {
		throw BinderException("common_crawl_index requires 0-1 parameters: optional cdx_limit");
	}

	// Start with empty index_name - will be set from filter or default to latest
	auto bind_data = make_uniq<CommonCrawlBindData>("");

	// Check for optional limit parameter
	if (input.inputs.size() == 1) {
		if (input.inputs[0].type().id() != LogicalTypeId::BIGINT &&
		    input.inputs[0].type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("common_crawl_index cdx_limit must be an integer");
		}
		bind_data->max_results = input.inputs[0].GetValue<int64_t>();
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

	// Add response_headers column (HTTP headers from WARC)
	// Using VARCHAR for headers (text content)
	names.push_back("response_headers");
	return_types.push_back(LogicalType::VARCHAR);

	// Add response_body column (HTTP body from WARC)
	// Using BLOB type to handle binary content (PDFs, images, etc.)
	names.push_back("response_body");
	return_types.push_back(LogicalType::BLOB);

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

	// Determine which fields are actually needed based on projection
	vector<string> needed_fields;
	bool need_response = false;
	bool need_warc = false;

	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			fprintf(stderr, "[DEBUG] Column %lu = %s\n", (unsigned long)col_id, col_name.c_str());

			if (col_name == "response_headers" || col_name == "response_body") {
				need_response = true;
				need_warc = true; // Response requires filename/offset/length
			} else if (col_name == "filename" || col_name == "offset" || col_name == "length") {
				need_warc = true;
			}

			// Add all selected columns to needed_fields (except response_headers, response_body, and crawl_id which are computed)
			// crawl_id, response_headers, response_body are not CDX fields - they're populated by our code
			if (col_name != "response_headers" && col_name != "response_body" && col_name != "crawl_id") {
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
		// IN clause detected: query each crawl_id separately and combine results
		for (const auto &crawl_id : bind_data.crawl_ids) {
			auto records = QueryCDXAPI(context, crawl_id, url_pattern, needed_fields, bind_data.cdx_filters, bind_data.max_results);
			// Append records to state
			state->records.insert(state->records.end(), records.begin(), records.end());
		}
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
				} else if (col_name == "response_headers" || col_name == "response_body") {
					if (bind_data.fetch_response) {
						WARCResponse warc_response = FetchWARCResponse(context, record);
						auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);

						if (col_name == "response_headers") {
							// Headers are text, use AddString with UTF-8 validation
							data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx],
							                                                    SanitizeUTF8(warc_response.headers));
						} else {
							// Body can be binary (images, PDFs, etc.), use AddStringOrBlob
							data_ptr[output_offset] = StringVector::AddStringOrBlob(output.data[proj_idx],
							                                                         warc_response.body);
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

// Cardinality function to handle LIMIT pushdown
static unique_ptr<NodeStatistics> CommonCrawlCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();
	// Return the max results as an estimate - this helps DuckDB optimize the query plan
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

static void LoadInternal(ExtensionLoader &loader) {
	fprintf(stderr, "\n[DEBUG] *** COMMON_CRAWL EXTENSION LOADING ***\n");
	fflush(stderr);

	// Note: httpfs extension must be loaded before using this extension
	// Users should run: INSTALL httpfs; LOAD httpfs; before loading this extension
	// Or set autoload_known_extensions=1 and autoinstall_known_extensions=1

	// Register the common_crawl_index table function with variable arguments
	// crawl_id filtering is done via WHERE clause
	// Defaults to latest crawl_id from collinfo.json if not specified
	TableFunctionSet common_crawl_set("common_crawl_index");

	// No parameter version: common_crawl_index()
	// Use: WHERE crawl_id = 'CC-MAIN-2025-43' to specify crawl
	// Use: WHERE url LIKE '*.example.com/*' for URL filtering
	auto func0 = TableFunction({},
	                           CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);
	func0.cardinality = CommonCrawlCardinality;
	func0.pushdown_complex_filter = CommonCrawlPushdownComplexFilter;
	func0.projection_pushdown = true; // Enable projection pushdown
	common_crawl_set.AddFunction(func0);

	// Single parameter version: common_crawl_index(100)
	// Parameter is CDX API result limit
	auto func1 = TableFunction({LogicalType::BIGINT},
	                           CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);
	func1.cardinality = CommonCrawlCardinality;
	func1.pushdown_complex_filter = CommonCrawlPushdownComplexFilter;
	func1.projection_pushdown = true; // Enable projection pushdown
	common_crawl_set.AddFunction(func1);

	loader.RegisterFunction(common_crawl_set);
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
