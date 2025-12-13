#include "web_archive_utils.hpp"
#include <thread>
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

// ========================================
// BIND DATA AND STATE
// ========================================

// Structure to hold bind data for the table function
struct CommonCrawlBindData : public TableFunctionData {
	string index_name;        // Single index (for backwards compatibility)
	vector<string> crawl_ids; // Multiple crawl_ids for IN clause support
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	string url_filter;
	vector<string> cdx_filters; // CDX API filter parameters (e.g., "=status:200", "=mime:text/html")
	idx_t max_results;          // Maximum number of results to fetch from CDX API
	timestamp_t timestamp_from; // Timestamp range filter (from collinfo.json lookup)
	timestamp_t timestamp_to;   // Timestamp range filter (from collinfo.json lookup)
	bool has_timestamp_filter;  // Whether timestamp filters were applied
	bool debug;                 // Show cdx_url column when true
	string cdx_url;             // The constructed CDX API URL (populated after query)
	int timeout_seconds;        // Timeout for fetch operations (default 180)

	// Default CDX limit set to 100 to prevent fetching too many results
	CommonCrawlBindData(string index)
	    : index_name(std::move(index)), fetch_response(false), url_filter("*"), max_results(100),
	      timestamp_from(timestamp_t(0)), timestamp_to(timestamp_t(0)), has_timestamp_filter(false), debug(false),
	      timeout_seconds(180) {
	}
};

// Structure to hold global state for the table function
struct CommonCrawlGlobalState : public GlobalTableFunctionState {
	vector<CDXRecord> records;
	idx_t current_position;
	vector<column_t> column_ids; // Which columns are actually selected

	CommonCrawlGlobalState() : current_position(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// ========================================
// CDX API QUERY
// ========================================

// Helper function to query CDX API using FileSystem
static vector<CDXRecord> QueryCDXAPI(ClientContext &context, const string &index_name, const string &url_pattern,
                                     const vector<string> &fields_needed, const vector<string> &cdx_filters,
                                     idx_t max_results, timestamp_t ts_from, timestamp_t ts_to, string &out_cdx_url) {
	DUCKDB_LOG_DEBUG(context, "QueryCDXAPI started +%.0fms", ElapsedMs());
	vector<CDXRecord> records;

	// Helper lambda to map DuckDB column names to CDX API field names
	auto map_column_to_field = [](const string &col_name) -> string {
		if (col_name == "mimetype")
			return "mime";
		if (col_name == "statuscode")
			return "status";
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

	// Add timestamp range parameters (from/to) if specified
	// CDX API expects timestamps in YYYYMMDDHHMMSS format (14 digits)
	if (ts_from.value != 0) {
		string from_str = ToCdxTimestamp(Timestamp::ToString(ts_from));
		cdx_url += "&from=" + from_str;
	}
	if (ts_to.value != 0) {
		string to_str = ToCdxTimestamp(Timestamp::ToString(ts_to));
		cdx_url += "&to=" + to_str;
	}

	// Add filter parameters (e.g., filter==statuscode:200)
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	// Debug: print the final CDX URL
	DUCKDB_LOG_DEBUG(context, "CDX URL: %s +%.0fms", cdx_url.c_str(), ElapsedMs());

	// Store the CDX URL for output
	out_cdx_url = cdx_url;

	try {
		DUCKDB_LOG_DEBUG(context, "Opening CDX URL +%.0fms", ElapsedMs());
		// Set force_download to skip HEAD request
		context.db->GetDatabase(context).config.SetOption("force_download", Value(true));

		// Use FileSystem to fetch the CDX data
		auto &fs = FileSystem::GetFileSystem(context);
		auto file_handle = fs.OpenFile(cdx_url, FileFlags::FILE_FLAGS_READ);

		DUCKDB_LOG_DEBUG(context, "Reading CDX response +%.0fms", ElapsedMs());
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

		DUCKDB_LOG_DEBUG(context, "Got %lu bytes, sanitizing UTF-8 +%.0fms", (unsigned long)response_data.size(),
		                 ElapsedMs());
		// Sanitize the entire response to ensure valid UTF-8
		response_data = SanitizeUTF8(response_data);
		DUCKDB_LOG_DEBUG(context, "UTF-8 sanitization complete +%.0fms", ElapsedMs());

		// Parse newline-delimited JSON
		DUCKDB_LOG_DEBUG(context, "Parsing JSON lines +%.0fms", ElapsedMs());
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
		DUCKDB_LOG_DEBUG(context, "Parsed %d JSON lines, got %lu records +%.0fms", line_count,
		                 (unsigned long)records.size(), ElapsedMs());

	} catch (std::exception &ex) {
		throw IOException("Error querying CDX API: " + string(ex.what()));
	} catch (...) {
		throw IOException("Unknown error querying CDX API");
	}

	return records;
}

// ========================================
// WARC FETCHING
// ========================================

// Helper function to fetch WARC response using FileSystem API with retry and timeout
static WARCResponse FetchWARCResponse(ClientContext &context, const CDXRecord &record,
                                      std::chrono::steady_clock::time_point start_time, int timeout_seconds) {
	WARCResponse result;

	if (record.filename.empty() || record.offset == 0 || record.length == 0) {
		return result; // Invalid record - return empty
	}

	// Construct the WARC URL
	string warc_url = "https://data.commoncrawl.org/" + record.filename;

	// Retry with exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
	const int max_retries = 5;
	int retry_delay_ms = 100;
	string last_error;

	for (int attempt = 0; attempt < max_retries; attempt++) {
		// Check if timeout exceeded
		auto elapsed =
		    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time).count();
		if (elapsed >= timeout_seconds) {
			result.error = "Timeout after " + to_string(elapsed) + "s (limit: " + to_string(timeout_seconds) + "s)";
			DUCKDB_LOG_DEBUG(context, "Fetch timeout for WARC: %s", warc_url.c_str());
			return result;
		}

		try {
			if (attempt > 0) {
				DUCKDB_LOG_DEBUG(context, "Retry %d/%d after %dms for WARC: %s", attempt, max_retries - 1,
				                 retry_delay_ms / 2, warc_url.c_str());
				std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
				retry_delay_ms *= 2; // Exponential backoff
			}

			// Set force_download to skip HEAD request
			context.db->GetDatabase(context).config.SetOption("force_download", Value(true));

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
				last_error = "Failed to read data from WARC file";
				continue; // Retry
			}

			// The data we read is gzip compressed
			// We need to decompress it to get the WARC content
			string decompressed = DecompressGzip(buffer.get(), bytes_read);

			// Parse the WARC format to extract HTTP response headers and body
			if (decompressed.find("[Error") == 0) {
				// If decompression returned an error message, set it as error
				result.error = decompressed;
				return result;
			}

			return ParseWARCResponse(decompressed);

		} catch (Exception &ex) {
			last_error = ex.what();
			// Check for retryable HTTP errors (503, 504)
			string error_str = last_error;
			bool is_retryable = error_str.find("503") != string::npos || error_str.find("504") != string::npos ||
			                    error_str.find("Service Unavailable") != string::npos ||
			                    error_str.find("Gateway Timeout") != string::npos ||
			                    error_str.find("connection") != string::npos ||
			                    error_str.find("timeout") != string::npos;
			if (!is_retryable && attempt == 0) {
				// Non-retryable error on first attempt, fail immediately
				result.error = last_error;
				return result;
			}
			// Continue to retry on retryable errors
		} catch (std::exception &ex) {
			last_error = ex.what();
			// Continue to retry on connection errors
		} catch (...) {
			last_error = "Unknown error";
			// Continue to retry
		}
	}

	// All retries failed
	result.error = "Failed after " + to_string(max_retries) + " retries: " + last_error;
	return result;
}

// ========================================
// TABLE FUNCTION IMPLEMENTATION
// ========================================

// Bind function for the table function
static unique_ptr<FunctionData> CommonCrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	DUCKDB_LOG_DEBUG(context, "CommonCrawlBind called +%.0fms", ElapsedMs());

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
			DUCKDB_LOG_DEBUG(context, "CDX API max_results set to: %lu", (unsigned long)bind_data->max_results);
		} else if (kv.first == "debug") {
			if (kv.second.type().id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("common_crawl_index debug parameter must be a boolean");
			}
			bind_data->debug = kv.second.GetValue<bool>();
			DUCKDB_LOG_DEBUG(context, "Debug mode: %s", bind_data->debug ? "true" : "false");
		} else if (kv.first == "timeout") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT && kv.second.type().id() != LogicalTypeId::INTEGER) {
				throw BinderException("common_crawl_index timeout parameter must be an integer (seconds)");
			}
			bind_data->timeout_seconds = kv.second.GetValue<int64_t>();
			DUCKDB_LOG_DEBUG(context, "Timeout set to: %d seconds", bind_data->timeout_seconds);
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

	names.push_back("mimetype");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mimetype");

	names.push_back("statuscode");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("statuscode");

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
	// Fields: body (BLOB), headers (MAP), http_version (VARCHAR), error (VARCHAR)
	names.push_back("response");
	child_list_t<LogicalType> response_children;
	response_children.push_back(make_pair("body", LogicalType::BLOB));
	response_children.push_back(make_pair("headers", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	response_children.push_back(make_pair("http_version", LogicalType::VARCHAR));
	response_children.push_back(make_pair("error", LogicalType::VARCHAR));
	return_types.push_back(LogicalType::STRUCT(response_children));

	// Add cdx_url column only when debug := true
	if (bind_data->debug) {
		names.push_back("cdx_url");
		return_types.push_back(LogicalType::VARCHAR);
	}

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
	DUCKDB_LOG_DEBUG(context, "CommonCrawlInitGlobal called +%.0fms", ElapsedMs());
	auto &bind_data = const_cast<CommonCrawlBindData &>(input.bind_data->Cast<CommonCrawlBindData>());

	// Validate URL filter - don't allow queries without a specific URL
	if (bind_data.url_filter == "*" || bind_data.url_filter.empty()) {
		throw InvalidInputException("common_crawl_index() requires a URL filter. Use WHERE url LIKE '%.example.com/%' "
		                            "or WHERE url LIKE 'https://example.com/%'");
	}

	auto state = make_uniq<CommonCrawlGlobalState>();

	// Determine crawl_ids based on filters:
	// 1. If explicit crawl_id(s) specified via WHERE clause, use those
	// 2. If timestamp range filter specified, look up matching crawl_ids from collinfo.json
	// 3. Otherwise, use the latest crawl_id
	if (bind_data.index_name.empty() && bind_data.crawl_ids.empty()) {
		if (bind_data.has_timestamp_filter) {
			// Look up crawl_ids that overlap with the timestamp range
			auto matching_ids = GetCrawlIdsForTimestampRange(context, bind_data.timestamp_from, bind_data.timestamp_to);
			if (!matching_ids.empty()) {
				bind_data.crawl_ids = matching_ids;
				string ids_str;
				for (const auto &id : matching_ids) {
					ids_str += id + " ";
				}
				DUCKDB_LOG_DEBUG(context, "Timestamp filter matched %lu crawl_ids: %s +%.0fms",
				                 (unsigned long)matching_ids.size(), ids_str.c_str(), ElapsedMs());
			} else {
				// No matching crawls found, fall back to latest
				bind_data.index_name = GetLatestCrawlId(context);
				DUCKDB_LOG_DEBUG(context, "No crawl_ids match timestamp range, using latest: %s +%.0fms",
				                 bind_data.index_name.c_str(), ElapsedMs());
			}
		} else {
			bind_data.index_name = GetLatestCrawlId(context);
			DUCKDB_LOG_DEBUG(context, "Using latest crawl_id: %s +%.0fms", bind_data.index_name.c_str(), ElapsedMs());
		}
	}

	// Store which columns are selected for projection pushdown
	state->column_ids = input.column_ids;
	string proj_cols_str;
	for (auto &col_id : input.column_ids) {
		proj_cols_str += to_string(col_id) + " ";
	}
	DUCKDB_LOG_DEBUG(context, "Projected columns: %s +%.0fms", proj_cols_str.c_str(), ElapsedMs());

	// CDX API fields that can be requested from index.commoncrawl.org
	// All other columns are computed by our extension
	static const std::unordered_set<string> cdx_fields = {"url",    "timestamp", "mimetype", "statuscode",
	                                                      "digest", "filename",  "offset",   "length"};

	// Determine which fields are actually needed based on projection
	vector<string> needed_fields;
	bool need_response = false;
	bool need_warc = false;

	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			DUCKDB_LOG_DEBUG(context, "Column %lu = %s", (unsigned long)col_id, col_name.c_str());

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
	DUCKDB_LOG_DEBUG(context, "fetch_response = %d, need_warc = %d +%.0fms", bind_data.fetch_response, need_warc,
	                 ElapsedMs());

	// Ensure we always request at least 'url' field from CDX API
	// (e.g., when only crawl_id is selected, we still need a CDX field to get records)
	if (needed_fields.empty()) {
		needed_fields.push_back("url");
	}

	// Use the URL filter from bind data (could be set via filter pushdown)
	string url_pattern = bind_data.url_filter;
	DUCKDB_LOG_DEBUG(context, "About to call QueryCDXAPI with %lu fields +%.0fms", (unsigned long)needed_fields.size(),
	                 ElapsedMs());

	// Query CDX API - handle multiple crawl_ids if IN clause was used
	if (!bind_data.crawl_ids.empty()) {
		// IN clause detected: query each crawl_id in parallel and combine results
		DUCKDB_LOG_DEBUG(context, "Launching %lu parallel CDX API requests +%.0fms",
		                 (unsigned long)bind_data.crawl_ids.size(), ElapsedMs());

		// For parallel queries, store each CDX URL
		vector<string> cdx_urls;
		cdx_urls.resize(bind_data.crawl_ids.size());

		std::vector<std::future<vector<CDXRecord>>> futures;
		futures.reserve(bind_data.crawl_ids.size());

		// Launch async requests for each crawl_id
		for (size_t i = 0; i < bind_data.crawl_ids.size(); i++) {
			const auto &crawl_id = bind_data.crawl_ids[i];
			futures.push_back(std::async(
			    std::launch::async, [&context, crawl_id, url_pattern, &needed_fields, &bind_data, &cdx_urls, i]() {
				    return QueryCDXAPI(context, crawl_id, url_pattern, needed_fields, bind_data.cdx_filters,
				                       bind_data.max_results, bind_data.timestamp_from, bind_data.timestamp_to,
				                       cdx_urls[i]);
			    }));
		}

		// Collect results from all futures
		for (auto &future : futures) {
			auto records = future.get();
			state->records.insert(state->records.end(), records.begin(), records.end());
		}

		// Store all CDX URLs (joined with newlines for debug output)
		for (size_t i = 0; i < cdx_urls.size(); i++) {
			if (i > 0)
				bind_data.cdx_url += "\n";
			bind_data.cdx_url += cdx_urls[i];
		}

		DUCKDB_LOG_DEBUG(context, "All parallel requests completed, total records: %lu +%.0fms",
		                 (unsigned long)state->records.size(), ElapsedMs());
	} else {
		// Single crawl_id: use index_name
		state->records =
		    QueryCDXAPI(context, bind_data.index_name, url_pattern, needed_fields, bind_data.cdx_filters,
		                bind_data.max_results, bind_data.timestamp_from, bind_data.timestamp_to, bind_data.cdx_url);
		DUCKDB_LOG_DEBUG(context, "QueryCDXAPI returned %lu records +%.0fms", (unsigned long)state->records.size(),
		                 ElapsedMs());
	}

	return std::move(state);
}

// Scan function for the table function
static void CommonCrawlScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	DUCKDB_LOG_DEBUG(context, "CommonCrawlScan called +%.0fms", ElapsedMs());
	auto &bind_data = data.bind_data->Cast<CommonCrawlBindData>();
	auto &gstate = data.global_state->Cast<CommonCrawlGlobalState>();
	DUCKDB_LOG_DEBUG(context, "Have %lu records to process +%.0fms", (unsigned long)gstate.records.size(), ElapsedMs());

	// Pre-fetch WARCs in parallel for this chunk if needed
	std::vector<WARCResponse> warc_responses;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		DUCKDB_LOG_DEBUG(context, "Pre-fetching %lu WARCs in parallel +%.0fms", (unsigned long)chunk_size, ElapsedMs());
		std::vector<std::future<WARCResponse>> warc_futures;
		warc_futures.reserve(chunk_size);

		// Start timer for timeout tracking
		auto fetch_start = std::chrono::steady_clock::now();
		int timeout = bind_data.timeout_seconds;

		// Launch parallel WARC fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			warc_futures.push_back(std::async(std::launch::async, [&context, record, fetch_start, timeout]() {
				return FetchWARCResponse(context, record, fetch_start, timeout);
			}));
		}

		// Collect results
		warc_responses.reserve(chunk_size);
		for (auto &future : warc_futures) {
			warc_responses.push_back(future.get());
		}
		DUCKDB_LOG_DEBUG(context, "All %lu WARCs fetched +%.0fms", (unsigned long)chunk_size, ElapsedMs());
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
				} else if (col_name == "mimetype") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] =
					    StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.mime_type));
				} else if (col_name == "statuscode") {
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.status_code;
				} else if (col_name == "digest") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] =
					    StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.digest));
				} else if (col_name == "filename") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] =
					    StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.filename));
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
							version_data[output_offset] =
							    StringVector::AddString(*version_vector, SanitizeUTF8(warc_response.warc_version));

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
								value_data[map_offset] =
								    StringVector::AddString(map_values, SanitizeUTF8(header.second));
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
								value_data[map_offset] =
								    StringVector::AddString(map_values, SanitizeUTF8(header.second));
								map_offset++;
							}

							auto map_data = FlatVector::GetData<list_entry_t>(*headers_map);
							map_data[output_offset].offset = ListVector::GetListSize(*headers_map);
							map_data[output_offset].length = warc_response.http_headers.size();
							ListVector::SetListSize(*headers_map, map_offset);

							// Child 2: http_version (VARCHAR)
							auto &version_vector = struct_children[2];
							auto version_data = FlatVector::GetData<string_t>(*version_vector);
							version_data[output_offset] =
							    StringVector::AddString(*version_vector, SanitizeUTF8(warc_response.http_version));
						}
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				} else if (col_name == "cdx_url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], bind_data.cdx_url);
				}
			} catch (const std::exception &ex) {
				DUCKDB_LOG_ERROR(context, "Failed to process column %s for row %lu: %s", col_name.c_str(),
				                 (unsigned long)gstate.current_position, ex.what());
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

// ========================================
// FILTER PUSHDOWN
// ========================================

// Columns that support CDX regex filtering
// NOTE: Common Crawl uses 'status' and 'mime' in CDX API (not statuscode/mimetype like Internet Archive)
static const std::set<string> CC_CDX_REGEX_COLUMNS = {"mimetype", "statuscode"};

// Escape regex special characters in a literal string
// Uses backslash escaping (raw backslashes work with httpfs)
static string EscapeRegexSpecialChars(const string &literal) {
	string result;
	for (char c : literal) {
		if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '+' || c == '?' ||
		    c == '*' || c == '^' || c == '$' || c == '\\' || c == '|') {
			result += '\\';
			result += c;
		} else {
			result += c;
		}
	}
	return result;
}

// Convert SQL SIMILAR TO pattern to regex (anchored with ^ and $)
// Handles: % -> .*, _ -> ., * -> .*
static string SqlRegexToRegex(const string &sql_regex) {
	string regex = "^";
	for (size_t i = 0; i < sql_regex.size(); i++) {
		char c = sql_regex[i];
		if (c == '%') {
			regex += ".*";
		} else if (c == '_') {
			regex += ".";
		} else if (c == '*') {
			// SQL SIMILAR TO uses * for regex-like patterns
			regex += ".*";
		} else if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '+' ||
		           c == '?' || c == '$' || c == '\\') {
			// Escape regex special chars (but not ^ since we add it ourselves)
			regex += '\\';
			regex += c;
		} else {
			regex += c;
		}
	}
	regex += "$";
	return regex;
}

// Map DuckDB column name to Common Crawl CDX filter field name
static string MapColumnToCdxFilterField(const string &col_name) {
	if (col_name == "mimetype")
		return "mime";
	if (col_name == "statuscode")
		return "status";
	return col_name;
}

// Try to add a CDX regex filter for a column
static bool TryAddCdxRegexFilter(ClientContext &context, CommonCrawlBindData &bind_data, const string &col_name,
                                 const string &regex_pattern, const string &source, bool negated = false) {
	if (CC_CDX_REGEX_COLUMNS.find(col_name) == CC_CDX_REGEX_COLUMNS.end()) {
		return false;
	}
	string cdx_field = MapColumnToCdxFilterField(col_name);
	string filter_str = (negated ? "!~" : "~") + cdx_field + ":" + regex_pattern;
	bind_data.cdx_filters.push_back(filter_str);
	DUCKDB_LOG_DEBUG(context, "%s %s regex: %s +%.0fms", col_name.c_str(), source.c_str(), filter_str.c_str(),
	                 ElapsedMs());
	return true;
}

// Handle IN expression for CDX columns (statuscode, mimetype)
// Converts IN (val1, val2, ...) to regex alternation ~status:(val1|val2|...)
static bool TryHandleInExpression(ClientContext &context, CommonCrawlBindData &bind_data, BoundOperatorExpression &op,
                                  const string &col_name, bool is_integer) {
	if (CC_CDX_REGEX_COLUMNS.find(col_name) == CC_CDX_REGEX_COLUMNS.end()) {
		return false;
	}

	vector<string> values;
	// children[0] is the column ref, children[1..n] are the values
	for (size_t i = 1; i < op.children.size(); i++) {
		if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false; // Non-constant value, can't push down
		}
		auto &constant = op.children[i]->Cast<BoundConstantExpression>();
		if (is_integer) {
			if (constant.value.type().id() != LogicalTypeId::INTEGER &&
			    constant.value.type().id() != LogicalTypeId::BIGINT) {
				return false;
			}
			values.push_back(to_string(constant.value.GetValue<int32_t>()));
		} else {
			if (constant.value.type().id() != LogicalTypeId::VARCHAR) {
				return false;
			}
			values.push_back(constant.value.ToString());
		}
	}

	if (values.empty()) {
		return false;
	}

	// Build regex alternation: (val1|val2|val3)
	string regex_pattern = "(";
	for (size_t i = 0; i < values.size(); i++) {
		if (i > 0)
			regex_pattern += "|";
		regex_pattern += values[i];
	}
	regex_pattern += ")";

	string cdx_field = MapColumnToCdxFilterField(col_name);
	string filter_str = "~" + cdx_field + ":" + regex_pattern;
	bind_data.cdx_filters.push_back(filter_str);
	DUCKDB_LOG_DEBUG(context, "%s IN pushdown: %s +%.0fms", col_name.c_str(), filter_str.c_str(), ElapsedMs());
	return true;
}

// Filter pushdown function to handle WHERE clauses
static void CommonCrawlPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                             vector<unique_ptr<Expression>> &filters) {
	DUCKDB_LOG_DEBUG(context, "CommonCrawlPushdownComplexFilter called with %lu filters +%.0fms",
	                 (unsigned long)filters.size(), ElapsedMs());
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
		DUCKDB_LOG_DEBUG(context, "Filter %lu: class=%d", (unsigned long)i, (int)filter->GetExpressionClass());

		// Handle BOUND_OPERATOR for IN clauses (e.g., crawl_id IN ('id1', 'id2'))
		// DuckDB represents IN as a BOUND_OPERATOR with children: [column, value1, value2, ...]
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
			auto &op = filter->Cast<BoundOperatorExpression>();

			// Check if first child is a column and rest are constants
			if (op.children.size() >= 2 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();
				string col_name = col_ref.GetName();

				// Handle statuscode IN (200, 301, 302) -> ~status:(200|301|302)
				if (col_name == "statuscode") {
					if (TryHandleInExpression(context, bind_data, op, col_name, true)) {
						filters_to_remove.push_back(i);
						continue;
					}
				}
				// Handle mimetype IN ('text/html', 'application/json') -> ~mime:(text/html|application/json)
				else if (col_name == "mimetype") {
					if (TryHandleInExpression(context, bind_data, op, col_name, false)) {
						filters_to_remove.push_back(i);
						continue;
					}
				}
				// Handle crawl_id IN (...)
				else if (col_name == "crawl_id") {
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

			// Alternative: Check if this is an IN operator (children[0] should be wrapped in CONJUNCTION due to
			// optimization)
			if (op.children.size() == 1 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
				auto &conjunction = op.children[0]->Cast<BoundConjunctionExpression>();
				DUCKDB_LOG_DEBUG(context, "Found CONJUNCTION inside OPERATOR, type=%d, children=%lu",
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
						string ids_str;
						for (const auto &id : crawl_id_values) {
							ids_str += id + " ";
						}
						DUCKDB_LOG_DEBUG(context, "IN clause detected with %lu crawl_ids: %s",
						                 (unsigned long)crawl_id_values.size(), ids_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle BOUND_FUNCTION for LIKE expressions
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &func = filter->Cast<BoundFunctionExpression>();
			DUCKDB_LOG_DEBUG(context, "Function name: %s, children: %lu", func.function.name.c_str(),
			                 (unsigned long)func.children.size());

			// Check if this is a SUFFIX function (DuckDB optimizes LIKE '%string' to suffix)
			if (func.function.name == "suffix") {
				// SUFFIX has 2 children: column and suffix string
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string suffix_string = constant.value.ToString();
						// Convert to CDX wildcard pattern: *suffix_string
						bind_data.url_filter = "*" + suffix_string;
						DUCKDB_LOG_DEBUG(context, "SUFFIX URL filter: '%s' -> '%s'", suffix_string.c_str(),
						                 bind_data.url_filter.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
			// Check if this is a PREFIX function (DuckDB optimizes LIKE 'string%' to prefix)
			else if (func.function.name == "prefix") {
				// PREFIX has 2 children: column and prefix string
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string prefix_string = constant.value.ToString();
						if (column_name == "url") {
							// Convert to CDX wildcard pattern: prefix_string*
							bind_data.url_filter = prefix_string + "*";
							DUCKDB_LOG_DEBUG(context, "PREFIX URL filter: '%s' -> '%s'", prefix_string.c_str(),
							                 bind_data.url_filter.c_str());
							filters_to_remove.push_back(i);
							continue;
						}
						// Handle mimetype/statuscode prefix patterns -> regex ^prefix_string.*
						else if (CC_CDX_REGEX_COLUMNS.find(column_name) != CC_CDX_REGEX_COLUMNS.end()) {
							string regex_pattern = "^" + EscapeRegexSpecialChars(prefix_string) + ".*";
							if (TryAddCdxRegexFilter(context, bind_data, column_name, regex_pattern, "PREFIX")) {
								filters_to_remove.push_back(i);
								continue;
							}
						}
					}
				}
			}
			// Check if this is a CONTAINS function (DuckDB optimizes LIKE '%string%' to contains)
			else if (func.function.name == "contains") {
				// CONTAINS has 2 children: column and search string
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string search_string = constant.value.ToString();
						// Convert to CDX wildcard pattern: *search_string*
						bind_data.url_filter = "*" + search_string + "*";
						DUCKDB_LOG_DEBUG(context, "CONTAINS URL filter: '%s' -> '%s'", search_string.c_str(),
						                 bind_data.url_filter.c_str());
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

					if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string original_pattern = constant.value.ToString();

						if (column_name == "url") {
							// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
							bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
							DUCKDB_LOG_DEBUG(context, "LIKE URL filter: '%s' -> '%s'", original_pattern.c_str(),
							                 bind_data.url_filter.c_str());
							filters_to_remove.push_back(i);
							continue;
						}
						// Handle mimetype LIKE 'text/%' -> ~mime:^text/.*$
						else if (column_name == "mimetype") {
							string regex_pattern = SqlRegexToRegex(original_pattern);
							if (TryAddCdxRegexFilter(context, bind_data, column_name, regex_pattern, "LIKE")) {
								filters_to_remove.push_back(i);
								continue;
							}
						}
					}
				}
			}
			// Check if this is a NOT LIKE function (!~~ or not_like)
			// url NOT LIKE 'pattern' -> &filter=!url:regex
			else if (func.function.name == "!~~" || func.function.name == "not_like") {
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string like_pattern = constant.value.ToString();
						string regex_pattern = SqlRegexToRegex(like_pattern);

						if (column_name == "url") {
							// url NOT LIKE -> !~url:regex (negated regex match)
							string filter_str = "!~url:" + regex_pattern;
							bind_data.cdx_filters.push_back(filter_str);
							DUCKDB_LOG_DEBUG(context, "url NOT LIKE: '%s' -> %s", like_pattern.c_str(),
							                 filter_str.c_str());
							filters_to_remove.push_back(i);
							continue;
						}
						// Handle mimetype/statuscode NOT LIKE
						else if (CC_CDX_REGEX_COLUMNS.find(column_name) != CC_CDX_REGEX_COLUMNS.end()) {
							if (TryAddCdxRegexFilter(context, bind_data, column_name, regex_pattern, "NOT LIKE",
							                         true)) {
								filters_to_remove.push_back(i);
								continue;
							}
						}
					}
				}
			}
			// Check if this is a SIMILAR TO function (regexp_matches / regexp_full_match)
			// Used for regex matching on url/statuscode/mimetype columns
			else if (func.function.name == "regexp_matches" || func.function.name == "regexp_full_match") {
				if (func.children.size() >= 2 &&
				    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string regex_pattern = constant.value.ToString();

						// Handle url SIMILAR TO -> ~url:regex
						// DuckDB already converts SIMILAR TO to regex, so just add anchors
						if (column_name == "url") {
							// Add anchors if not present (regexp_full_match is implicitly anchored)
							string anchored_regex = regex_pattern;
							if (anchored_regex.empty() || anchored_regex[0] != '^') {
								anchored_regex = "^" + anchored_regex;
							}
							if (anchored_regex.empty() || anchored_regex.back() != '$') {
								anchored_regex = anchored_regex + "$";
							}
							string filter_str = "~url:" + anchored_regex;
							bind_data.cdx_filters.push_back(filter_str);
							DUCKDB_LOG_DEBUG(context, "url SIMILAR TO: '%s' -> %s", regex_pattern.c_str(),
							                 filter_str.c_str());
							filters_to_remove.push_back(i);
							continue;
						}
						// Try to push down regex filter for statuscode/mimetype
						if (TryAddCdxRegexFilter(context, bind_data, column_name, regex_pattern, "SIMILAR TO")) {
							filters_to_remove.push_back(i);
							continue;
						}
					}
				}
			}
		}

		// Handle OPERATOR_NOT wrapping functions like prefix(), suffix(), contains(), like()
		// DuckDB sometimes wraps NOT LIKE as NOT(prefix()) or NOT(like())
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
		    filter->type == ExpressionType::OPERATOR_NOT) {
			auto &op = filter->Cast<BoundOperatorExpression>();
			if (op.children.size() >= 1 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {

				auto &inner_func = op.children[0]->Cast<BoundFunctionExpression>();

				// NOT prefix(url, 'pattern') -> !~url:^pattern.*$ (negated regex match)
				if (inner_func.function.name == "prefix" && inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string prefix_str = constant.value.ToString();
						string regex_pattern = "^" + EscapeRegexSpecialChars(prefix_str) + ".*$";
						string filter_str = "!~url:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "url NOT PREFIX: '%s' -> %s", prefix_str.c_str(), filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT like(url, 'pattern') or NOT ~~(url, 'pattern') -> !~url:regex (negated regex match)
				if ((inner_func.function.name == "like" || inner_func.function.name == "~~") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string like_pattern = constant.value.ToString();
						string regex_pattern = SqlRegexToRegex(like_pattern);
						string filter_str = "!~url:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "url NOT LIKE (wrapped): '%s' -> %s", like_pattern.c_str(),
						                 filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT regexp_matches(url, 'pattern') or NOT regexp_full_match(url, 'pattern') -> !~url:regex
				// This handles NOT SIMILAR TO for the url column
				// DuckDB already converts SIMILAR TO to regex, so just add anchors
				if ((inner_func.function.name == "regexp_matches" || inner_func.function.name == "regexp_full_match") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();
					string column_name = col_ref.GetName();

					if (column_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string regex_pattern = constant.value.ToString();
						// Add anchors if not present
						string anchored_regex = regex_pattern;
						if (anchored_regex.empty() || anchored_regex[0] != '^') {
							anchored_regex = "^" + anchored_regex;
						}
						if (anchored_regex.empty() || anchored_regex.back() != '$') {
							anchored_regex = anchored_regex + "$";
						}
						string filter_str = "!~url:" + anchored_regex;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "url NOT SIMILAR TO: '%s' -> %s", regex_pattern.c_str(),
						                 filter_str.c_str());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle BOUND_BETWEEN for timestamp range (e.g., timestamp BETWEEN '2025-11-12' AND '2025-11-14')
		// DuckDB also converts `timestamp >= X AND timestamp < Y` to BETWEEN
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = filter->Cast<BoundBetweenExpression>();

			// Check if input is a timestamp column reference
			if (between.input->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = between.input->Cast<BoundColumnRefExpression>();

				if (col_ref.GetName() == "timestamp") {
					// Extract lower bound
					if (between.lower->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
						auto &lower_const = between.lower->Cast<BoundConstantExpression>();
						timestamp_t lower_ts;
						if (lower_const.value.type().id() == LogicalTypeId::TIMESTAMP ||
						    lower_const.value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
							lower_ts = timestamp_t(lower_const.value.GetValue<int64_t>());
						} else if (lower_const.value.type().id() == LogicalTypeId::VARCHAR) {
							lower_ts = Timestamp::FromString(lower_const.value.ToString(), false);
						} else {
							lower_ts = timestamp_t(0);
						}
						if (lower_ts.value != 0) {
							bind_data.timestamp_from = lower_ts;
							bind_data.has_timestamp_filter = true;
							DUCKDB_LOG_DEBUG(context, "BETWEEN timestamp FROM: %s",
							                 Timestamp::ToString(lower_ts).c_str());
						}
					}

					// Extract upper bound
					if (between.upper->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
						auto &upper_const = between.upper->Cast<BoundConstantExpression>();
						timestamp_t upper_ts;
						if (upper_const.value.type().id() == LogicalTypeId::TIMESTAMP ||
						    upper_const.value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
							upper_ts = timestamp_t(upper_const.value.GetValue<int64_t>());
						} else if (upper_const.value.type().id() == LogicalTypeId::VARCHAR) {
							upper_ts = Timestamp::FromString(upper_const.value.ToString(), false);
						} else {
							upper_ts = timestamp_t(0);
						}
						if (upper_ts.value != 0) {
							bind_data.timestamp_to = upper_ts;
							bind_data.has_timestamp_filter = true;
							DUCKDB_LOG_DEBUG(context, "BETWEEN timestamp TO: %s",
							                 Timestamp::ToString(upper_ts).c_str());
						}
					}
					// Don't remove filter - DuckDB will still apply it for exact row filtering
					continue;
				}
			}
		}

		// Check if this is a comparison expression
		if (filter->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}

		// Handle timestamp range comparisons (>, >=, <, <=) for automatic crawl_id lookup
		if (filter->type == ExpressionType::COMPARE_GREATERTHAN ||
		    filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		    filter->type == ExpressionType::COMPARE_LESSTHAN ||
		    filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();

			// Check if left side is a timestamp column reference
			if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();

				if (col_ref.GetName() == "timestamp" &&
				    comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
					auto &constant = comparison.right->Cast<BoundConstantExpression>();

					// Handle both TIMESTAMP and VARCHAR types
					timestamp_t ts_value;
					if (constant.value.type().id() == LogicalTypeId::TIMESTAMP ||
					    constant.value.type().id() == LogicalTypeId::TIMESTAMP_TZ) {
						// timestamp_t is internally int64_t, get it directly
						ts_value = timestamp_t(constant.value.GetValue<int64_t>());
					} else if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
						// Parse string like '2024-01-01' to timestamp
						string ts_str = constant.value.ToString();
						ts_value = Timestamp::FromString(ts_str, false); // false = don't use offset
					} else {
						continue; // Unsupported type
					}

					// Set timestamp_from or timestamp_to based on comparison type
					if (filter->type == ExpressionType::COMPARE_GREATERTHAN ||
					    filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
						bind_data.timestamp_from = ts_value;
						bind_data.has_timestamp_filter = true;
						DUCKDB_LOG_DEBUG(context, "Timestamp FROM filter: %s", Timestamp::ToString(ts_value).c_str());
					} else {
						bind_data.timestamp_to = ts_value;
						bind_data.has_timestamp_filter = true;
						DUCKDB_LOG_DEBUG(context, "Timestamp TO filter: %s", Timestamp::ToString(ts_value).c_str());
					}
					// Don't remove filter - DuckDB will still apply it for exact row filtering
					continue;
				}
			}
		}

		if (filter->type != ExpressionType::COMPARE_EQUAL && filter->type != ExpressionType::COMPARE_NOTEQUAL) {
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
			// Convert SQL LIKE wildcards (%) to CDX API wildcards (*)
			bind_data.url_filter = ConvertSQLWildcardsToCDX(original_pattern);
			DUCKDB_LOG_DEBUG(context, "URL filter: '%s' -> '%s'", original_pattern.c_str(),
			                 bind_data.url_filter.c_str());
			filters_to_remove.push_back(i);
		}
		// Handle crawl_id filtering (sets the index_name to use)
		else if (column_name == "crawl_id" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				bind_data.index_name = constant.value.ToString();
				DUCKDB_LOG_DEBUG(context, "crawl_id filter set index_name to: %s", bind_data.index_name.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle statuscode filtering (uses CDX filter parameter)
		// CDX API syntax: filter==status:200 means &filter==status:200
		// NOTE: Common Crawl uses 'status' field name (not 'statuscode')
		else if (column_name == "statuscode" && (constant.value.type().id() == LogicalTypeId::INTEGER ||
		                                         constant.value.type().id() == LogicalTypeId::BIGINT)) {
			string op = (filter->type == ExpressionType::COMPARE_EQUAL) ? "=" : "!";
			string filter_str = op + "status:" + to_string(constant.value.GetValue<int32_t>());
			bind_data.cdx_filters.push_back(filter_str);
			filters_to_remove.push_back(i);
		}
		// Handle mimetype filtering (uses CDX filter parameter)
		// NOTE: Common Crawl uses 'mime' field name (not 'mimetype')
		else if (column_name == "mimetype" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
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
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// OPTIMIZER FOR LIMIT PUSHDOWN
// ========================================

// Optimizer function to push down LIMIT to common_crawl_index function
void OptimizeCommonCrawlLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection operators to find the GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "common_crawl_index") {
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits (not expressions)
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			OptimizeCommonCrawlLimitPushdown(op->children[0]);
			return;
		}

		// Extract limit value and store in bind_data
		auto &bind_data = get.bind_data->Cast<CommonCrawlBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto limit_value = limit.limit_val.GetConstantValue();
			// For common_crawl with multiple crawl_ids, divide limit across crawl_ids
			if (!bind_data.crawl_ids.empty()) {
				limit_value = (limit_value + bind_data.crawl_ids.size() - 1) / bind_data.crawl_ids.size();
			}
			bind_data.max_results = limit_value;

			// Remove the LIMIT node from the plan since we've pushed it down
			op = std::move(op->children[0]);
			return;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeCommonCrawlLimitPushdown(child);
	}
}

// ========================================
// REGISTRATION
// ========================================

void RegisterCommonCrawlFunction(ExtensionLoader &loader) {
	// Register the common_crawl_index table function
	// Usage: SELECT * FROM common_crawl_index() WHERE crawl_id = 'CC-MAIN-2025-43' LIMIT 10
	// Usage with max_results: SELECT * FROM common_crawl_index(max_results := 500) WHERE crawl_id = 'CC-MAIN-2025-43'
	// - crawl_id filtering is done via WHERE clause (defaults to latest if not specified)
	// - URL filtering: WHERE url LIKE '*.example.com/*'
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet common_crawl_set("common_crawl_index");

	auto func = TableFunction({}, CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);
	func.cardinality = CommonCrawlCardinality;
	func.pushdown_complex_filter = CommonCrawlPushdownComplexFilter;
	func.projection_pushdown = true;

	// Add named parameters
	func.named_parameters["max_results"] = LogicalType::BIGINT;
	func.named_parameters["debug"] = LogicalType::BOOLEAN;
	func.named_parameters["timeout"] = LogicalType::BIGINT;

	common_crawl_set.AddFunction(func);

	loader.RegisterFunction(common_crawl_set);
}

} // namespace duckdb
