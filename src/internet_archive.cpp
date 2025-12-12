#include "web_archive_cdx_utils.hpp"
#include <set>
#include <thread>
#include <unordered_map>
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

// ========================================
// BIND DATA AND STATE
// ========================================

// Response fetch result with body and optional error
struct FetchResult {
	string body;
	string error; // Empty if successful, error message otherwise
};

// Structure to hold bind data for wayback_machine table function
struct WaybackMachineBindData : public TableFunctionData {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	bool cdx_url_only; // True if only cdx_url column is selected (skip network request)
	string url_filter;
	string match_type;                                      // exact, prefix, host, domain
	vector<string> cdx_filters;                             // filter=field:regex
	string from_date;                                       // YYYYMMDDhhmmss
	string to_date;                                         // YYYYMMDDhhmmss
	idx_t max_results;                                      // Default limit
	string collapse;                                        // collapse parameter (e.g., "urlkey", "timestamp:8")
	string cdx_url;                                         // The constructed CDX API URL (populated after query)
	bool fast_latest;                                       // Use fastLatest=true for efficient ORDER BY timestamp DESC
	bool order_desc;                                        // ORDER BY timestamp DESC detected
	idx_t offset;                                           // offset parameter for pagination
	bool debug;                                             // Show cdx_url column when true
	int timeout_seconds;                                    // Timeout for fetch operations (default 180)
	std::chrono::steady_clock::time_point fetch_start_time; // Track fetch start time

	WaybackMachineBindData()
	    : fetch_response(false), cdx_url_only(false), url_filter("*"), match_type("exact"), max_results(100),
	      collapse(""), cdx_url(""), fast_latest(false), order_desc(false), offset(0), debug(false),
	      timeout_seconds(180) {
	}
};

// Structure to hold global state for wayback_machine table function
struct WaybackMachineGlobalState : public GlobalTableFunctionState {
	vector<ArchiveOrgRecord> records;
	idx_t current_position;
	vector<column_t> column_ids;

	WaybackMachineGlobalState() : current_position(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded
	}
};

// ========================================
// CDX URL BUILDING
// ========================================

// Helper function to build Internet Archive CDX URL (without making request)
static string BuildArchiveOrgCDXUrl(const string &url_pattern, const string &match_type,
                                    const vector<string> &fields_needed, const vector<string> &cdx_filters,
                                    const string &from_date, const string &to_date, idx_t max_results,
                                    const string &collapse, bool fast_latest, idx_t offset) {
	// Construct field list for &fl= parameter from fields_needed
	std::set<string> needed_set(fields_needed.begin(), fields_needed.end());

	string field_list;
	// Order matters for parsing - keep consistent order
	vector<string> ordered_fields = {"urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"};
	for (const auto &f : ordered_fields) {
		if (needed_set.count(f)) {
			if (!field_list.empty())
				field_list += ",";
			field_list += f;
		}
	}

	// Construct the CDX API URL (use CSV format - space delimited, fields in fl order)
	string cdx_url = "https://web.archive.org/cdx/search/cdx?url=" + url_pattern + "&output=csv&fl=" + field_list;

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

	// Add limit (negative for fastLatest to get latest results)
	if (fast_latest) {
		cdx_url += "&fastLatest=true&limit=-" + to_string(max_results);
	} else {
		cdx_url += "&limit=" + to_string(max_results);
	}

	// Add offset parameter for pagination
	if (offset > 0) {
		cdx_url += "&offset=" + to_string(offset);
	}

	// Add filter parameters
	for (const auto &filter : cdx_filters) {
		cdx_url += "&filter=" + filter;
	}

	// Add collapse parameter if specified
	if (!collapse.empty()) {
		cdx_url += "&collapse=" + collapse;
	}

	return cdx_url;
}

// ========================================
// CDX API QUERY
// ========================================

// Helper function to query Internet Archive CDX API
static vector<ArchiveOrgRecord> QueryArchiveOrgCDX(ClientContext &context, const string &url_pattern,
                                                   const string &match_type, const vector<string> &fields_needed,
                                                   const vector<string> &cdx_filters, const string &from_date,
                                                   const string &to_date, idx_t max_results, const string &collapse,
                                                   bool fast_latest, idx_t offset, string &out_cdx_url) {
	DUCKDB_LOG_DEBUG(context, "QueryArchiveOrgCDX started +%.0fms", ElapsedMs());
	vector<ArchiveOrgRecord> records;

	// Build the CDX URL
	string cdx_url = BuildArchiveOrgCDXUrl(url_pattern, match_type, fields_needed, cdx_filters, from_date, to_date,
	                                       max_results, collapse, fast_latest, offset);

	// Construct field list for parsing (same logic as BuildArchiveOrgCDXUrl)
	std::set<string> needed_set(fields_needed.begin(), fields_needed.end());
	string field_list;
	vector<string> ordered_fields = {"urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"};
	for (const auto &f : ordered_fields) {
		if (needed_set.count(f)) {
			if (!field_list.empty())
				field_list += ",";
			field_list += f;
		}
	}
	DUCKDB_LOG_DEBUG(context, "Internet Archive CDX fields: %s", field_list.c_str());
	DUCKDB_LOG_DEBUG(context, "CDX URL +%.0fms: %s", ElapsedMs(), cdx_url.c_str());

	// Store the CDX URL for output
	out_cdx_url = cdx_url;

	try {
		// Set force_download to skip HEAD request
		context.db->GetDatabase(context).config.SetOption("force_download", Value(true));

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

		// Build list of fields we're requesting (in order)
		vector<string> fields_in_order;
		for (const auto &f : ordered_fields) {
			if (needed_set.count(f)) {
				fields_in_order.push_back(f);
			}
		}

		// Parse tab-delimited CSV (fields in same order as fl parameter)
		std::istringstream stream(response_data);
		string line;
		int line_count = 0;

		while (std::getline(stream, line)) {
			if (line.empty()) {
				continue;
			}
			line_count++;

			// Split by space (Internet Archive CDX uses space delimiter for CSV)
			vector<string> values;
			std::istringstream line_stream(line);
			string value;
			while (line_stream >> value) {
				values.push_back(value);
			}

			if (values.size() < fields_in_order.size()) {
				continue; // Skip malformed lines
			}

			// Parse data row using known field order
			ArchiveOrgRecord record;
			for (size_t i = 0; i < fields_in_order.size(); i++) {
				const string &field = fields_in_order[i];
				const string &val = values[i];

				if (field == "urlkey") {
					record.urlkey = val;
				} else if (field == "timestamp") {
					record.timestamp = val;
				} else if (field == "original") {
					record.original = val;
				} else if (field == "mimetype") {
					record.mime_type = val;
				} else if (field == "statuscode") {
					record.status_code = val.empty() || val == "-" ? 0 : std::stoi(val);
				} else if (field == "digest") {
					record.digest = val;
				} else if (field == "length") {
					record.length = val.empty() || val == "-" ? 0 : std::stoll(val);
				}
			}
			records.push_back(record);
		}
		DUCKDB_LOG_DEBUG(context, "Parsed %d CSV lines, got %lu records", line_count, (unsigned long)records.size());

	} catch (std::exception &ex) {
		throw IOException("Error querying Internet Archive CDX API: " + string(ex.what()));
	}

	return records;
}

// ========================================
// ARCHIVED PAGE FETCHING
// ========================================

// Helper function to fetch archived page from Internet Archive with retry and timeout
static FetchResult FetchArchivedPage(ClientContext &context, const ArchiveOrgRecord &record,
                                     std::chrono::steady_clock::time_point start_time, int timeout_seconds) {
	FetchResult result;

	if (record.timestamp.empty() || record.original.empty()) {
		result.error = "Missing timestamp or URL";
		return result;
	}

	// Construct the download URL with id_ suffix to get raw content
	string download_url = "https://web.archive.org/web/" + record.timestamp + "id_/" + record.original;

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
			DUCKDB_LOG_DEBUG(context, "Fetch timeout for: %s", download_url.c_str());
			return result;
		}

		try {
			if (attempt > 0) {
				DUCKDB_LOG_DEBUG(context, "Retry %d/%d after %dms for: %s", attempt, max_retries - 1,
				                 retry_delay_ms / 2, download_url.c_str());
				std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
				retry_delay_ms *= 2; // Exponential backoff
			} else {
				DUCKDB_LOG_DEBUG(context, "Fetching archived page: %s", download_url.c_str());
			}

			// Set force_download to skip HEAD request
			context.db->GetDatabase(context).config.SetOption("force_download", Value(true));

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

			result.body = response_data;
			return result; // Success - error is empty

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
		}
	}

	result.error = "Failed after " + to_string(max_retries) + " retries: " + last_error;
	return result;
}

// ========================================
// TABLE FUNCTION IMPLEMENTATION
// ========================================

// Bind function for wayback_machine table function
static unique_ptr<FunctionData> WaybackMachineBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	g_start_time = std::chrono::steady_clock::now();
	DUCKDB_LOG_DEBUG(context, "WaybackMachineBind called +%.0fms", ElapsedMs());

	auto bind_data = make_uniq<WaybackMachineBindData>();

	// Handle named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "max_results") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT) {
				throw BinderException("wayback_machine max_results parameter must be an integer");
			}
			bind_data->max_results = kv.second.GetValue<int64_t>();
			DUCKDB_LOG_DEBUG(context, "CDX API max_results set to: %lu", (unsigned long)bind_data->max_results);
		} else if (kv.first == "collapse") {
			if (kv.second.type().id() != LogicalTypeId::VARCHAR) {
				throw BinderException("wayback_machine collapse parameter must be a string");
			}
			bind_data->collapse = kv.second.GetValue<string>();
			DUCKDB_LOG_DEBUG(context, "CDX API collapse set to: %s", bind_data->collapse.c_str());
		} else if (kv.first == "debug") {
			if (kv.second.type().id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("wayback_machine debug parameter must be a boolean");
			}
			bind_data->debug = kv.second.GetValue<bool>();
			DUCKDB_LOG_DEBUG(context, "Debug mode: %s", bind_data->debug ? "true" : "false");
		} else if (kv.first == "timeout") {
			if (kv.second.type().id() != LogicalTypeId::BIGINT && kv.second.type().id() != LogicalTypeId::INTEGER) {
				throw BinderException("wayback_machine timeout parameter must be an integer (seconds)");
			}
			bind_data->timeout_seconds = kv.second.GetValue<int64_t>();
			DUCKDB_LOG_DEBUG(context, "Timeout set to: %d seconds", bind_data->timeout_seconds);
		} else {
			throw BinderException("Unknown parameter '%s' for wayback_machine", kv.first.c_str());
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

	names.push_back("mimetype");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("mimetype");

	names.push_back("statuscode");
	return_types.push_back(LogicalType::INTEGER);
	bind_data->fields_needed.push_back("statuscode");

	names.push_back("digest");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("digest");

	names.push_back("length");
	return_types.push_back(LogicalType::BIGINT);
	bind_data->fields_needed.push_back("length");

	// Add response column as STRUCT with body and error fields
	names.push_back("response");
	child_list_t<LogicalType> response_children;
	response_children.push_back(make_pair("body", LogicalType::BLOB));
	response_children.push_back(make_pair("error", LogicalType::VARCHAR));
	return_types.push_back(LogicalType::STRUCT(response_children));

	// Add year column (extracted from timestamp)
	names.push_back("year");
	return_types.push_back(LogicalType::INTEGER);

	// Add month column (extracted from timestamp)
	names.push_back("month");
	return_types.push_back(LogicalType::INTEGER);

	// Add cdx_url column only when debug := true
	if (bind_data->debug) {
		names.push_back("cdx_url");
		return_types.push_back(LogicalType::VARCHAR);
	}

	// Don't set fetch_response here - will be determined by projection pushdown
	bind_data->fetch_response = false;
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function for wayback_machine
static unique_ptr<GlobalTableFunctionState> WaybackMachineInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	DUCKDB_LOG_DEBUG(context, "WaybackMachineInitGlobal called +%.0fms", ElapsedMs());
	auto &bind_data = const_cast<WaybackMachineBindData &>(input.bind_data->Cast<WaybackMachineBindData>());

	// Validate URL filter - don't allow queries without a specific URL
	if (bind_data.url_filter == "*" || bind_data.url_filter.empty()) {
		throw InvalidInputException("wayback_machine() requires a URL filter. Use WHERE url = 'example.com', WHERE url "
		                            "LIKE 'example.com/%', or WHERE url LIKE '%.example.com' for subdomains");
	}

	auto state = make_uniq<WaybackMachineGlobalState>();

	// Store projected columns
	state->column_ids = input.column_ids;

	// Rebuild fields_needed based on projection pushdown
	// Map column names to CDX API field names
	bind_data.fields_needed.clear();
	for (auto &col_id : input.column_ids) {
		if (col_id < bind_data.column_names.size()) {
			string col_name = bind_data.column_names[col_id];
			DUCKDB_LOG_DEBUG(context, "Projected column: %s", col_name.c_str());

			if (col_name == "url") {
				bind_data.fields_needed.push_back("original");
			} else if (col_name == "timestamp") {
				bind_data.fields_needed.push_back("timestamp");
			} else if (col_name == "urlkey") {
				bind_data.fields_needed.push_back("urlkey");
			} else if (col_name == "mimetype") {
				bind_data.fields_needed.push_back("mimetype");
			} else if (col_name == "statuscode") {
				bind_data.fields_needed.push_back("statuscode");
			} else if (col_name == "digest") {
				bind_data.fields_needed.push_back("digest");
			} else if (col_name == "length") {
				bind_data.fields_needed.push_back("length");
			} else if (col_name == "response") {
				bind_data.fetch_response = true;
				DUCKDB_LOG_DEBUG(context, "Will fetch response bodies");
			} else if (col_name == "year" || col_name == "month") {
				// year and month need timestamp field
				if (std::find(bind_data.fields_needed.begin(), bind_data.fields_needed.end(), "timestamp") ==
				    bind_data.fields_needed.end()) {
					bind_data.fields_needed.push_back("timestamp");
				}
			} else if (col_name == "cdx_url") {
				// cdx_url doesn't need any CDX fields
			}
		}
	}

	// Check if only cdx_url is selected (debug mode, fields_needed is empty and no response)
	bind_data.cdx_url_only = bind_data.debug && bind_data.fields_needed.empty() && !bind_data.fetch_response;

	// Enhanced check: if collapse is set and all needed fields are covered by collapse base, skip fetch
	// This enables testing DISTINCT ON collapse optimization without network requests
	if (!bind_data.cdx_url_only && bind_data.debug && !bind_data.collapse.empty() && !bind_data.fetch_response) {
		// Parse collapse base field (e.g., "timestamp:4" -> "timestamp")
		std::string collapse_base = bind_data.collapse;
		auto colon_pos = collapse_base.find(':');
		if (colon_pos != std::string::npos) {
			collapse_base = collapse_base.substr(0, colon_pos);
		}

		// Check if all fields_needed are derived from collapse base
		bool all_covered = true;
		for (const auto &field : bind_data.fields_needed) {
			// timestamp collapse covers timestamp field (used for year/month)
			if (collapse_base == "timestamp" && field == "timestamp") {
				continue;
			}
			// Other fields not covered
			all_covered = false;
			break;
		}

		if (all_covered) {
			DUCKDB_LOG_DEBUG(context, "All fields covered by collapse=%s - skipping network request",
			                 bind_data.collapse.c_str());
			bind_data.cdx_url_only = true;
		}
	}

	if (bind_data.cdx_url_only) {
		// Only cdx_url is selected - build URL without network request
		DUCKDB_LOG_DEBUG(context, "cdx_url_only mode - skipping network request");
		bind_data.cdx_url =
		    BuildArchiveOrgCDXUrl(bind_data.url_filter, bind_data.match_type, bind_data.fields_needed,
		                          bind_data.cdx_filters, bind_data.from_date, bind_data.to_date, bind_data.max_results,
		                          bind_data.collapse, bind_data.fast_latest, bind_data.offset);
		DUCKDB_LOG_DEBUG(context, "CDX URL +%.0fms: %s", ElapsedMs(), bind_data.cdx_url.c_str());

		// Create a single dummy record so we return one row with the cdx_url
		ArchiveOrgRecord dummy;
		dummy.timestamp = "202501010000"; // Dummy timestamp for year/month extraction
		state->records.push_back(dummy);
	} else {
		// Query Internet Archive CDX API
		state->records =
		    QueryArchiveOrgCDX(context, bind_data.url_filter, bind_data.match_type, bind_data.fields_needed,
		                       bind_data.cdx_filters, bind_data.from_date, bind_data.to_date, bind_data.max_results,
		                       bind_data.collapse, bind_data.fast_latest, bind_data.offset, bind_data.cdx_url);
	}

	DUCKDB_LOG_DEBUG(context, "QueryArchiveOrgCDX returned %lu records +%.0fms", (unsigned long)state->records.size(),
	                 ElapsedMs());

	return std::move(state);
}

// Scan function for wayback_machine table function
static void WaybackMachineScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<WaybackMachineBindData>();
	auto &gstate = data.global_state->Cast<WaybackMachineGlobalState>();

	// Pre-fetch responses in parallel for this chunk if needed
	std::vector<FetchResult> response_results;
	idx_t chunk_size = std::min<idx_t>(STANDARD_VECTOR_SIZE, gstate.records.size() - gstate.current_position);

	if (bind_data.fetch_response && chunk_size > 0) {
		DUCKDB_LOG_DEBUG(context, "Pre-fetching %lu archived pages in parallel", (unsigned long)chunk_size);
		std::vector<std::future<FetchResult>> response_futures;
		response_futures.reserve(chunk_size);

		// Record start time for timeout tracking
		auto fetch_start = std::chrono::steady_clock::now();
		int timeout = bind_data.timeout_seconds;

		// Launch parallel fetches
		for (idx_t i = 0; i < chunk_size; i++) {
			auto &record = gstate.records[gstate.current_position + i];
			response_futures.push_back(std::async(std::launch::async, [&context, record, fetch_start, timeout]() {
				return FetchArchivedPage(context, record, fetch_start, timeout);
			}));
		}

		// Collect results
		response_results.reserve(chunk_size);
		for (auto &future : response_futures) {
			response_results.push_back(future.get());
		}
		DUCKDB_LOG_DEBUG(context, "All %lu archived pages fetched", (unsigned long)chunk_size);
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
					data_ptr[output_offset] =
					    StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.original));
				} else if (col_name == "timestamp") {
					auto data_ptr = FlatVector::GetData<timestamp_t>(output.data[proj_idx]);
					data_ptr[output_offset] = ParseCDXTimestamp(record.timestamp);
				} else if (col_name == "urlkey") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] =
					    StringVector::AddString(output.data[proj_idx], SanitizeUTF8(record.urlkey));
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
				} else if (col_name == "length") {
					auto data_ptr = FlatVector::GetData<int64_t>(output.data[proj_idx]);
					data_ptr[output_offset] = record.length;
				} else if (col_name == "response") {
					if (bind_data.fetch_response && !response_results.empty()) {
						FetchResult &result = response_results[output_offset];
						// Response STRUCT with body and error fields
						auto &struct_vector = output.data[proj_idx];
						auto &struct_children = StructVector::GetEntries(struct_vector);
						// Child 0: body (BLOB)
						auto &body_vector = struct_children[0];
						auto body_data = FlatVector::GetData<string_t>(*body_vector);
						body_data[output_offset] = StringVector::AddStringOrBlob(*body_vector, result.body);
						// Child 1: error (VARCHAR)
						auto &error_vector = struct_children[1];
						auto error_data = FlatVector::GetData<string_t>(*error_vector);
						if (result.error.empty()) {
							FlatVector::SetNull(*error_vector, output_offset, true);
						} else {
							error_data[output_offset] = StringVector::AddString(*error_vector, result.error);
						}
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				} else if (col_name == "year") {
					// Extract year from timestamp (format: YYYYMMDDhhmmss)
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					if (record.timestamp.length() >= 4) {
						data_ptr[output_offset] = std::stoi(record.timestamp.substr(0, 4));
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				} else if (col_name == "month") {
					// Extract month from timestamp (format: YYYYMMDDhhmmss)
					auto data_ptr = FlatVector::GetData<int32_t>(output.data[proj_idx]);
					if (record.timestamp.length() >= 6) {
						data_ptr[output_offset] = std::stoi(record.timestamp.substr(4, 2));
					} else {
						FlatVector::SetNull(output.data[proj_idx], output_offset, true);
					}
				} else if (col_name == "cdx_url") {
					auto data_ptr = FlatVector::GetData<string_t>(output.data[proj_idx]);
					data_ptr[output_offset] = StringVector::AddString(output.data[proj_idx], bind_data.cdx_url);
				}
			} catch (const std::exception &ex) {
				DUCKDB_LOG_ERROR(context, "Failed to process column %s: %s", col_name.c_str(), ex.what());
			}
		}

		output_offset++;
		gstate.current_position++;
	}

	output.SetCardinality(output_offset);
}

// ========================================
// FILTER PUSHDOWN
// ========================================

// Columns that support CDX regex filtering
static const std::set<string> CDX_REGEX_COLUMNS = {"urlkey", "mimetype", "statuscode"};

// Convert SQL SIMILAR TO pattern to Java regex (anchored)
// Handles: % -> .*, _ -> ., * -> .*
static string SqlRegexToJavaRegex(const string &sql_regex) {
	string regex = "^";
	for (char c : sql_regex) {
		if (c == '%' || c == '*') {
			regex += ".*";
		} else if (c == '_') {
			regex += ".";
		} else {
			regex += c;
		}
	}
	regex += "$";
	return regex;
}

// Escape regex special characters for contains() patterns
static string EscapeRegex(const string &val) {
	string escaped;
	for (char c : val) {
		if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '+' || c == '?' ||
		    c == '^' || c == '$' || c == '|' || c == '\\' || c == '*') {
			escaped += '\\';
		}
		escaped += c;
	}
	return escaped;
}

// Helper to check if column supports CDX regex and add filter
// Returns true if filter was added
static bool TryAddCdxRegexFilter(ClientContext &context, WaybackMachineBindData &bind_data, const string &col_name,
                                 const string &filter_pattern, const string &debug_label, bool negate = false) {
	if (CDX_REGEX_COLUMNS.find(col_name) == CDX_REGEX_COLUMNS.end()) {
		return false;
	}
	string filter_str = (negate ? "!" : "") + col_name + ":" + filter_pattern;
	bind_data.cdx_filters.push_back(filter_str);
	DUCKDB_LOG_DEBUG(context, "%s %s: %s +%.0fms", col_name.c_str(), debug_label.c_str(), filter_str.c_str(),
	                 ElapsedMs());
	return true;
}

// Helper to handle IN expression for CDX columns (statuscode, mimetype)
// Converts IN (val1, val2, ...) to regex alternation (val1|val2|...)
static bool TryHandleInExpression(ClientContext &context, WaybackMachineBindData &bind_data,
                                  BoundOperatorExpression &op, const string &col_name, bool is_integer) {
	if (CDX_REGEX_COLUMNS.find(col_name) == CDX_REGEX_COLUMNS.end()) {
		return false;
	}

	vector<string> values;
	for (idx_t j = 1; j < op.children.size(); j++) {
		if (op.children[j]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &const_expr = op.children[j]->Cast<BoundConstantExpression>();

		if (is_integer) {
			if (const_expr.value.type().id() != LogicalTypeId::INTEGER &&
			    const_expr.value.type().id() != LogicalTypeId::BIGINT) {
				return false;
			}
			values.push_back(to_string(const_expr.value.GetValue<int32_t>()));
		} else {
			if (const_expr.value.type().id() != LogicalTypeId::VARCHAR) {
				return false;
			}
			// Escape regex special chars for string values
			values.push_back(EscapeRegex(const_expr.value.ToString()));
		}
	}

	if (values.empty()) {
		return false;
	}

	// Build regex alternation: (val1|val2|val3)
	string regex_pattern = "(";
	for (idx_t j = 0; j < values.size(); j++) {
		if (j > 0)
			regex_pattern += "|";
		regex_pattern += values[j];
	}
	regex_pattern += ")";

	string filter_str = col_name + ":" + regex_pattern;
	bind_data.cdx_filters.push_back(filter_str);
	DUCKDB_LOG_DEBUG(context, "%s IN -> %s +%.0fms", col_name.c_str(), filter_str.c_str(), ElapsedMs());
	return true;
}

// Filter pushdown for wayback_machine
static void WaybackMachinePushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                                vector<unique_ptr<Expression>> &filters) {
	DUCKDB_LOG_DEBUG(context, "WaybackMachinePushdownComplexFilter called with %lu filters +%.0fms",
	                 (unsigned long)filters.size(), ElapsedMs());
	auto &bind_data = bind_data_p->Cast<WaybackMachineBindData>();

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

			// Handle LIKE for url column - just replace % with * for CDX API
			if ((func.function.name == "like" || func.function.name == "~~") && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();

				if (col_ref.GetName() == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					bind_data.url_filter = constant.value.ToString();
					// Replace SQL % wildcards with CDX API * wildcards
					for (size_t pos = 0; pos < bind_data.url_filter.size(); ++pos) {
						if (bind_data.url_filter[pos] == '%') {
							bind_data.url_filter[pos] = '*';
						}
					}
					DUCKDB_LOG_DEBUG(context, "URL LIKE: %s +%.0fms", bind_data.url_filter.c_str(), ElapsedMs());
					filters_to_remove.push_back(i);
					continue;
				}

				// Handle LIKE for CDX regex columns
				string col_name = col_ref.GetName();
				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string like_pattern = constant.value.ToString();
					string regex_pattern = LikeToRegex(like_pattern);
					TryAddCdxRegexFilter(context, bind_data, col_name, regex_pattern, "LIKE");
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle NOT LIKE (!~~) for url column -> !original:regex filter
			// CDX API uses 'original' field name for URL
			if ((func.function.name == "!~~" || func.function.name == "not_like") && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				if (col_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string like_pattern = constant.value.ToString();
					string regex_pattern = LikeToRegex(like_pattern);
					string filter_str = "!original:" + regex_pattern;
					bind_data.cdx_filters.push_back(filter_str);
					DUCKDB_LOG_DEBUG(context, "url NOT LIKE: %s -> %s +%.0fms", like_pattern.c_str(),
					                 filter_str.c_str(), ElapsedMs());
					filters_to_remove.push_back(i);
					continue;
				}

				// Handle NOT LIKE for CDX regex columns (urlkey, mimetype, statuscode)
				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string like_pattern = constant.value.ToString();
					string regex_pattern = LikeToRegex(like_pattern);
					string filter_str = "!" + col_name + ":" + regex_pattern;
					bind_data.cdx_filters.push_back(filter_str);
					DUCKDB_LOG_DEBUG(context, "%s NOT LIKE: %s -> %s +%.0fms", col_name.c_str(), like_pattern.c_str(),
					                 filter_str.c_str(), ElapsedMs());
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle suffix() - DuckDB optimizes LIKE '%x' to suffix()
			if (func.function.name == "suffix" && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				// Handle suffix(url, '.domain.com') -> url=*.domain.com
				// CDX server auto-detects matchType from wildcard pattern, no need to specify
				if (col_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string suffix_val = constant.value.ToString();
					// For URL suffix like '.example.com', use wildcard prefix
					bind_data.url_filter = "*" + suffix_val;
					// Don't set match_type - let CDX server auto-detect from pattern
					DUCKDB_LOG_DEBUG(context, "URL suffix: %s +%.0fms", bind_data.url_filter.c_str(), ElapsedMs());
					filters_to_remove.push_back(i);
					continue;
				}

				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string suffix_val = constant.value.ToString();
					TryAddCdxRegexFilter(context, bind_data, col_name, ".*" + suffix_val + "$", "suffix");
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle prefix() - DuckDB optimizes LIKE 'x%' to prefix()
			if (func.function.name == "prefix" && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				// Handle prefix(url, 'pattern') -> url=pattern* (special case)
				if (col_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
					bind_data.url_filter = constant.value.ToString() + "*";
					DUCKDB_LOG_DEBUG(context, "URL prefix: %s +%.0fms", bind_data.url_filter.c_str(), ElapsedMs());
					filters_to_remove.push_back(i);
					continue;
				}

				// Handle prefix for CDX regex columns
				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string prefix_val = constant.value.ToString();
					TryAddCdxRegexFilter(context, bind_data, col_name, "^" + prefix_val + ".*", "prefix");
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle contains() - DuckDB optimizes LIKE '%x%' to contains()
			if (func.function.name == "contains" && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string escaped = EscapeRegex(constant.value.ToString());
					TryAddCdxRegexFilter(context, bind_data, col_name, ".*" + escaped + ".*", "contains");
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle regexp_matches: column ~ 'regex'
			if ((func.function.name == "regexp_matches" || func.function.name == "~") && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string regex_pattern = constant.value.ToString();
					TryAddCdxRegexFilter(context, bind_data, col_name, regex_pattern, "regex");
					filters_to_remove.push_back(i);
					continue;
				}
			}

			// Handle regexp_full_match: SIMILAR TO converts to this
			if (func.function.name == "regexp_full_match" && func.children.size() >= 2 &&
			    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

				auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant = func.children[1]->Cast<BoundConstantExpression>();
				string col_name = col_ref.GetName();

				if (CDX_REGEX_COLUMNS.find(col_name) != CDX_REGEX_COLUMNS.end() &&
				    constant.value.type().id() == LogicalTypeId::VARCHAR) {
					string sql_regex = constant.value.ToString();
					string regex_pattern = SqlRegexToJavaRegex(sql_regex);
					TryAddCdxRegexFilter(context, bind_data, col_name, regex_pattern, "SIMILAR TO");
					filters_to_remove.push_back(i);
					continue;
				}
			}
		}

		// Handle NOT (OPERATOR_NOT) for urlkey: NOT regexp_matches() or NOT LIKE
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
		    filter->type == ExpressionType::OPERATOR_NOT) {
			auto &op = filter->Cast<BoundOperatorExpression>();
			if (op.children.size() >= 1 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {

				auto &inner_func = op.children[0]->Cast<BoundFunctionExpression>();

				// NOT regexp_matches(urlkey, 'regex')
				if ((inner_func.function.name == "regexp_matches" || inner_func.function.name == "~") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string regex_pattern = constant.value.ToString();
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "urlkey NOT regex: %s +%.0fms", filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT (urlkey LIKE '%pattern') -> !urlkey:.*pattern$
				// NOT (url LIKE 'pattern') -> !original:regex  (CDX uses 'original' for URL field)
				if ((inner_func.function.name == "like" || inner_func.function.name == "~~") &&
				    inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();
					string col_name = col_ref.GetName();

					if (col_name == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string like_pattern = constant.value.ToString();
						string regex_pattern = LikeToRegex(like_pattern);
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "urlkey NOT LIKE: %s -> %s +%.0fms", like_pattern.c_str(),
						                 filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}

					// url NOT LIKE -> !original:regex (CDX field name is 'original')
					if (col_name == "url" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string like_pattern = constant.value.ToString();
						string regex_pattern = LikeToRegex(like_pattern);
						string filter_str = "!original:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "url NOT LIKE: %s -> %s +%.0fms", like_pattern.c_str(),
						                 filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT suffix(urlkey, 'pattern') -> !urlkey:.*pattern$
				if (inner_func.function.name == "suffix" && inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string suffix_val = constant.value.ToString();
						string filter_str = "!urlkey:.*" + suffix_val + "$";
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "urlkey NOT suffix: %s +%.0fms", filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT prefix(urlkey, 'pattern') -> !urlkey:^pattern.*
				if (inner_func.function.name == "prefix" && inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string prefix_val = constant.value.ToString();
						string filter_str = "!urlkey:^" + prefix_val + ".*";
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "urlkey NOT prefix: %s +%.0fms", filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT SIMILAR TO -> NOT regexp_full_match
				if (inner_func.function.name == "regexp_full_match" && inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();

					if (col_ref.GetName() == "urlkey" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string sql_regex = constant.value.ToString();
						string regex_pattern = SqlRegexToJavaRegex(sql_regex);
						string filter_str = "!urlkey:" + regex_pattern;
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "urlkey NOT SIMILAR TO: %s -> %s +%.0fms", sql_regex.c_str(),
						                 filter_str.c_str(), ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}

				// NOT contains(urlkey/url, 'pattern') -> !urlkey:.*pattern.* or !original:.*pattern.*
				if (inner_func.function.name == "contains" && inner_func.children.size() >= 2 &&
				    inner_func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    inner_func.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {

					auto &col_ref = inner_func.children[0]->Cast<BoundColumnRefExpression>();
					auto &constant = inner_func.children[1]->Cast<BoundConstantExpression>();
					string col_name = col_ref.GetName();

					if ((col_name == "urlkey" || col_name == "url") &&
					    constant.value.type().id() == LogicalTypeId::VARCHAR) {
						string contains_val = constant.value.ToString();
						// Escape regex special chars
						string escaped;
						for (char c : contains_val) {
							if (c == '.' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' ||
							    c == '+' || c == '?' || c == '^' || c == '$' || c == '|' || c == '\\' || c == '*') {
								escaped += '\\';
							}
							escaped += c;
						}
						// CDX API uses 'original' for URL field
						string cdx_field = (col_name == "url") ? "original" : col_name;
						string filter_str = "!" + cdx_field + ":.*" + escaped + ".*";
						bind_data.cdx_filters.push_back(filter_str);
						DUCKDB_LOG_DEBUG(context, "%s NOT contains: %s +%.0fms", col_name.c_str(), filter_str.c_str(),
						                 ElapsedMs());
						filters_to_remove.push_back(i);
						continue;
					}
				}
			}
		}

		// Handle IN expressions: statuscode IN (200, 301, 302) or mimetype IN ('text/html', 'text/plain')
		// DuckDB converts IN to COMPARE_IN operator expression
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
		    filter->type == ExpressionType::COMPARE_IN) {
			auto &op = filter->Cast<BoundOperatorExpression>();
			// First child is the column, rest are the values
			if (op.children.size() >= 2 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {

				auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();
				string col_name = col_ref.GetName();

				// statuscode is integer, mimetype is string
				bool is_integer = (col_name == "statuscode");
				if (TryHandleInExpression(context, bind_data, op, col_name, is_integer)) {
					filters_to_remove.push_back(i);
					continue;
				}
			}
		}

		// Handle BETWEEN expressions (timestamp BETWEEN x AND y)
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
			auto &between = filter->Cast<BoundBetweenExpression>();
			if (between.input->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    between.lower->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
			    between.upper->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				auto &col_ref = between.input->Cast<BoundColumnRefExpression>();
				if (col_ref.GetName() == "timestamp") {
					auto &lower_const = between.lower->Cast<BoundConstantExpression>();
					auto &upper_const = between.upper->Cast<BoundConstantExpression>();

					bind_data.from_date = ToCdxTimestamp(lower_const.value.ToString());
					bind_data.to_date = ToCdxTimestamp(upper_const.value.ToString());
					DUCKDB_LOG_DEBUG(context, "BETWEEN from=%s to=%s +%.0fms", bind_data.from_date.c_str(),
					                 bind_data.to_date.c_str(), ElapsedMs());

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
				DUCKDB_LOG_DEBUG(context, "URL filter (exact): %s", bind_data.url_filter.c_str());
				filters_to_remove.push_back(i);
			}
		}
		// Handle statuscode filtering
		if (column_name == "statuscode" && (constant.value.type().id() == LogicalTypeId::INTEGER ||
		                                    constant.value.type().id() == LogicalTypeId::BIGINT)) {
			int32_t val = constant.value.GetValue<int32_t>();
			if (filter->type == ExpressionType::COMPARE_EQUAL) {
				string filter_str = "statuscode:" + to_string(val);
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_NOTEQUAL) {
				string filter_str = "!statuscode:" + to_string(val);
				bind_data.cdx_filters.push_back(filter_str);
				filters_to_remove.push_back(i);
			}
		}
		// Handle mimetype filtering
		else if (column_name == "mimetype" && constant.value.type().id() == LogicalTypeId::VARCHAR) {
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
		// Handle timestamp filtering (from/to date range)
		else if (column_name == "timestamp" && (constant.value.type().id() == LogicalTypeId::TIMESTAMP ||
		                                        constant.value.type().id() == LogicalTypeId::TIMESTAMP_TZ ||
		                                        constant.value.type().id() == LogicalTypeId::DATE ||
		                                        constant.value.type().id() == LogicalTypeId::VARCHAR)) {

			string cdx_timestamp = ToCdxTimestamp(constant.value.ToString());

			if (filter->type == ExpressionType::COMPARE_GREATERTHAN ||
			    filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				bind_data.from_date = cdx_timestamp;
				DUCKDB_LOG_DEBUG(context, "Timestamp from: %s +%.0fms", cdx_timestamp.c_str(), ElapsedMs());
				filters_to_remove.push_back(i);
			} else if (filter->type == ExpressionType::COMPARE_LESSTHAN ||
			           filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				bind_data.to_date = cdx_timestamp;
				DUCKDB_LOG_DEBUG(context, "Timestamp to: %s +%.0fms", cdx_timestamp.c_str(), ElapsedMs());
				filters_to_remove.push_back(i);
			}
		}
	}

	// Remove pushed down filters
	for (idx_t i = filters_to_remove.size(); i > 0; i--) {
		filters.erase(filters.begin() + filters_to_remove[i - 1]);
	}
}

// Cardinality function for wayback_machine
static unique_ptr<NodeStatistics> WaybackMachineCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<WaybackMachineBindData>();
	return make_uniq<NodeStatistics>(bind_data.max_results);
}

// ========================================
// OPTIMIZER FOR LIMIT PUSHDOWN
// ========================================

// Helper function to check if TOP_N orders by timestamp DESC
static bool IsTimestampDescTopN(LogicalTopN &top_n, const WaybackMachineBindData &bind_data) {
	if (top_n.orders.empty()) {
		return false;
	}

	// Check if the first (and ideally only) order is timestamp DESC
	auto &first_order = top_n.orders[0];
	if (first_order.type != OrderType::DESCENDING) {
		return false;
	}

	// Check if ordering by the timestamp column
	auto expr_class = first_order.expression->GetExpressionClass();

	if (expr_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = first_order.expression->Cast<BoundColumnRefExpression>();
		string col_name = col_ref.GetName();

		// Check if column name contains "timestamp" (handles qualified names like 'wayback_machine."timestamp"')
		if (col_name.find("timestamp") != string::npos || col_ref.alias.find("timestamp") != string::npos) {
			return true;
		}

		// Also check by column binding - timestamp is column index 1 in our schema
		// (url=0, timestamp=1, urlkey=2, mimetype=3, statuscode=4, digest=5, length=6, response=7, year=8, month=9,
		// cdx_url=10 if debug)
		if (col_ref.binding.column_index == 1) {
			return true;
		}
	}

	return false;
}

// Optimizer function to push down LIMIT to wayback_machine function
void OptimizeWaybackMachineLimitPushdown(unique_ptr<LogicalOperator> &op) {
	// Handle TOP_N (ORDER BY + LIMIT combined)
	if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op->Cast<LogicalTopN>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection and filter operators to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       child.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeWaybackMachineLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "wayback_machine") {
			OptimizeWaybackMachineLimitPushdown(op->children[0]);
			return;
		}

		auto &bind_data = get.bind_data->Cast<WaybackMachineBindData>();

		// Check if ORDER BY timestamp DESC
		if (IsTimestampDescTopN(top_n, bind_data)) {
			bind_data.max_results = top_n.limit;
			bind_data.fast_latest = true;
			bind_data.order_desc = true;
			// Push down offset if present
			if (top_n.offset > 0) {
				bind_data.offset = top_n.offset;
			}
			// Keep TOP_N in plan - API returns latest results but not in DESC order
			// DuckDB will sort them after fetching
			return;
		} else {
			// Regular TOP_N - just push down the limit and offset
			bind_data.max_results = top_n.limit;
			if (top_n.offset > 0) {
				bind_data.offset = top_n.offset;
			}
			// Keep TOP_N in plan for non-DESC ordering
		}
	}

	// Handle plain LIMIT (no ORDER BY)
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op->Cast<LogicalLimit>();
		reference<LogicalOperator> child = *op->children[0];

		// Skip projection and filter operators to find GET
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       child.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			OptimizeWaybackMachineLimitPushdown(op->children[0]);
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "wayback_machine") {
			OptimizeWaybackMachineLimitPushdown(op->children[0]);
			return;
		}

		// Only push down constant limits and offsets (not expressions)
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant or unset limit
			OptimizeWaybackMachineLimitPushdown(op->children[0]);
			return;
		}

		// Check offset type too
		bool has_constant_offset = false;
		switch (limit.offset_val.Type()) {
		case LimitNodeType::CONSTANT_VALUE:
			has_constant_offset = true;
			break;
		case LimitNodeType::UNSET:
			break;
		default:
			// not a constant offset - can still push down limit but not offset
			break;
		}

		// Extract limit value and store in bind_data
		auto &bind_data = get.bind_data->Cast<WaybackMachineBindData>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			bind_data.max_results = limit.limit_val.GetConstantValue();

			// Also push down offset if it's a constant
			if (has_constant_offset) {
				bind_data.offset = limit.offset_val.GetConstantValue();
			}

			// Remove the LIMIT node from the plan since we've pushed it down
			op = std::move(op->children[0]);
			return;
		}
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeWaybackMachineLimitPushdown(child);
	}
}

// ========================================
// DISTINCT ON PUSHDOWN OPTIMIZER
// ========================================

// Columns that support CDX collapse parameter
// Maps DuckDB column name to CDX API field name
static const std::unordered_map<string, string> COLLAPSE_COLUMNS = {
    {"digest", "digest"}, {"timestamp", "timestamp"}, {"length", "length"},    {"statuscode", "statuscode"},
    {"urlkey", "urlkey"}, {"url", "original"},        {"mimetype", "mimetype"}};

// Helper to find all distinct target column names
static std::set<string> GetDistinctOnColumnNames(const LogicalDistinct &distinct, const LogicalGet &get) {
	std::set<string> result;
	auto &bind_data = get.bind_data->Cast<WaybackMachineBindData>();
	auto &column_ids = get.GetColumnIds();

	for (const auto &target : distinct.distinct_targets) {
		if (target->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = target->Cast<BoundColumnRefExpression>();
			idx_t binding_col = col_ref.binding.column_index;

			// Try mapping binding directly to column_ids position
			if (binding_col < column_ids.size()) {
				idx_t orig_col_idx = column_ids[binding_col].GetPrimaryIndex();
				if (orig_col_idx < bind_data.column_names.size()) {
					result.insert(bind_data.column_names[orig_col_idx]);
				}
			}
		}
	}

	// Fallback: if no columns found via binding, look for single collapsible column
	if (result.empty()) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			idx_t orig_idx = column_ids[i].GetPrimaryIndex();
			if (orig_idx < bind_data.column_names.size()) {
				const string &col_name = bind_data.column_names[orig_idx];
				// Check if it's a collapsible column or year/month
				if (COLLAPSE_COLUMNS.count(col_name) || col_name == "year" || col_name == "month") {
					result.insert(col_name);
				}
			}
		}
		// Only use fallback if exactly one collapsible column found
		if (result.size() != 1) {
			result.clear();
		}
	}

	return result;
}

// Helper to check if a DISTINCT ON targets a collapsible column
// Returns the CDX API collapse parameter value if found, empty string otherwise
static string GetDistinctOnCollapseField(const LogicalDistinct &distinct, const LogicalGet &get) {
	if (distinct.distinct_type != DistinctType::DISTINCT_ON) {
		return "";
	}

	auto distinct_cols = GetDistinctOnColumnNames(distinct, get);
	if (distinct_cols.empty()) {
		return "";
	}

	// Special handling for year and month columns
	// timestamp format is YYYYMMDDhhmmss
	// year = first 4 chars, year+month = first 6 chars
	bool has_year = distinct_cols.count("year") > 0;
	bool has_month = distinct_cols.count("month") > 0;

	if (has_year && has_month) {
		// DISTINCT ON(year, month) -> collapse on first 6 chars of timestamp
		return "timestamp:6";
	} else if (has_year) {
		// DISTINCT ON(year) -> collapse on first 4 chars of timestamp
		return "timestamp:4";
	} else if (has_month) {
		// DISTINCT ON(month) alone -> collapse on first 6 chars (year+month)
		// (month alone doesn't make sense without year context)
		return "timestamp:6";
	}

	// Check for regular collapsible columns
	for (const auto &col_name : distinct_cols) {
		auto it = COLLAPSE_COLUMNS.find(col_name);
		if (it != COLLAPSE_COLUMNS.end()) {
			return it->second;
		}
	}

	return "";
}

// Optimizer function to push down DISTINCT ON to collapse parameter
void OptimizeWaybackMachineDistinctOnPushdown(unique_ptr<LogicalOperator> &op) {
	// Handle DISTINCT ON
	if (op->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		auto &distinct = op->Cast<LogicalDistinct>();

		// Only handle DISTINCT ON, not regular DISTINCT
		if (distinct.distinct_type != DistinctType::DISTINCT_ON) {
			for (auto &child : op->children) {
				OptimizeWaybackMachineDistinctOnPushdown(child);
			}
			return;
		}

		// Find the GET node first (skip projections, filters)
		reference<LogicalOperator> child = *op->children[0];
		while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION ||
		       child.get().type == LogicalOperatorType::LOGICAL_FILTER) {
			child = *child.get().children[0];
		}

		if (child.get().type != LogicalOperatorType::LOGICAL_GET) {
			for (auto &c : op->children) {
				OptimizeWaybackMachineDistinctOnPushdown(c);
			}
			return;
		}

		auto &get = child.get().Cast<LogicalGet>();
		if (get.function.name != "wayback_machine") {
			for (auto &c : op->children) {
				OptimizeWaybackMachineDistinctOnPushdown(c);
			}
			return;
		}

		// Check if DISTINCT ON targets a collapsible column
		string collapse_field = GetDistinctOnCollapseField(distinct, get);
		if (collapse_field.empty()) {
			for (auto &c : op->children) {
				OptimizeWaybackMachineDistinctOnPushdown(c);
			}
			return;
		}

		// Found DISTINCT ON over wayback_machine with collapsible column!
		auto &bind_data = get.bind_data->Cast<WaybackMachineBindData>();
		if (bind_data.collapse.empty()) {
			bind_data.collapse = collapse_field;
		}

		// Recurse into children
		for (auto &c : op->children) {
			OptimizeWaybackMachineDistinctOnPushdown(c);
		}
		return;
	}

	// Recurse into children
	for (auto &child : op->children) {
		OptimizeWaybackMachineDistinctOnPushdown(child);
	}
}

// ========================================
// REGISTRATION
// ========================================

void RegisterWaybackMachineFunction(ExtensionLoader &loader) {
	// Register the wayback_machine table function
	// Usage: SELECT * FROM wayback_machine() WHERE url = 'archive.org' LIMIT 10
	// Usage with max_results: SELECT * FROM wayback_machine(max_results := 500) WHERE url = 'archive.org'
	// - URL filtering via WHERE clause
	// - Supports matchType detection (exact, prefix, host, domain)
	// - Much simpler than common_crawl - no WARC parsing needed
	// - Projection pushdown: only fetches response when needed
	// - Optional max_results parameter controls CDX API result size (default: 100)
	TableFunctionSet wayback_machine_set("wayback_machine");

	auto ia_func = TableFunction({}, WaybackMachineScan, WaybackMachineBind, WaybackMachineInitGlobal);
	ia_func.cardinality = WaybackMachineCardinality;
	ia_func.pushdown_complex_filter = WaybackMachinePushdownComplexFilter;
	ia_func.projection_pushdown = true;

	// Add named parameters
	ia_func.named_parameters["max_results"] = LogicalType::BIGINT;
	ia_func.named_parameters["collapse"] = LogicalType::VARCHAR;
	ia_func.named_parameters["debug"] = LogicalType::BOOLEAN;
	ia_func.named_parameters["timeout"] = LogicalType::BIGINT;

	wayback_machine_set.AddFunction(ia_func);

	loader.RegisterFunction(wayback_machine_set);
}

} // namespace duckdb
