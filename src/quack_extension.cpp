#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

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
};

// Structure to hold bind data for the table function
struct CommonCrawlBindData : public TableFunctionData {
	string index_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> fields_needed;
	bool fetch_response;
	string url_filter;

	CommonCrawlBindData(string index) : index_name(std::move(index)), fetch_response(false), url_filter("*") {}
};

// Structure to hold global state for the table function
struct CommonCrawlGlobalState : public GlobalTableFunctionState {
	vector<CDXRecord> records;
	idx_t current_position;

	CommonCrawlGlobalState() : current_position(0) {}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// Helper function to query CDX API
static vector<CDXRecord> QueryCDXAPI(ClientContext &context, const string &index_name, const string &url_pattern,
                                      const vector<string> &fields_needed) {
	vector<CDXRecord> records;

	// Construct field list for &fl= parameter to optimize the query
	string field_list = "url,timestamp,mime,status,digest";
	bool need_warc_fields = false;
	for (const auto &field : fields_needed) {
		if (field == "response" || field == "filename" || field == "offset" || field == "length") {
			need_warc_fields = true;
			break;
		}
	}
	if (need_warc_fields) {
		field_list += ",filename,offset,length";
	}

	// Construct the CDX API URL
	string cdx_url = "https://index.commoncrawl.org/" + index_name + "-index?url=" + url_pattern +
	                 "&output=json&fl=" + field_list;

	// Use DuckDB's read_json_auto to parse JSON lines directly
	string query = "SELECT "
		"json_extract_string(json, '$.url') as url, "
		"json_extract_string(json, '$.filename') as filename, "
		"TRY_CAST(json_extract_string(json, '$.offset') AS BIGINT) as offset, "
		"TRY_CAST(json_extract_string(json, '$.length') AS BIGINT) as length, "
		"json_extract_string(json, '$.timestamp') as timestamp, "
		"json_extract_string(json, '$.mime') as mime_type, "
		"json_extract_string(json, '$.digest') as digest, "
		"TRY_CAST(json_extract_string(json, '$.status') AS INTEGER) as status_code "
		"FROM read_text('" + cdx_url + "', columns={'json': 'VARCHAR'})";

	try {
		auto result = context.Query(query);
		if (result->HasError()) {
			throw IOException("Failed to query CDX API: " + result->GetError());
		}

		// Fetch all records
		while (true) {
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}

			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				CDXRecord record;

				// Extract values from the chunk
				auto url_val = chunk->GetValue(0, row_idx);
				auto filename_val = chunk->GetValue(1, row_idx);
				auto offset_val = chunk->GetValue(2, row_idx);
				auto length_val = chunk->GetValue(3, row_idx);
				auto timestamp_val = chunk->GetValue(4, row_idx);
				auto mime_val = chunk->GetValue(5, row_idx);
				auto digest_val = chunk->GetValue(6, row_idx);
				auto status_val = chunk->GetValue(7, row_idx);

				// Only add valid records
				if (!url_val.IsNull()) {
					record.url = url_val.ToString();
					record.filename = filename_val.IsNull() ? "" : filename_val.ToString();
					record.offset = offset_val.IsNull() ? 0 : offset_val.GetValue<int64_t>();
					record.length = length_val.IsNull() ? 0 : length_val.GetValue<int64_t>();
					record.timestamp = timestamp_val.IsNull() ? "" : timestamp_val.ToString();
					record.mime_type = mime_val.IsNull() ? "" : mime_val.ToString();
					record.digest = digest_val.IsNull() ? "" : digest_val.ToString();
					record.status_code = status_val.IsNull() ? 0 : status_val.GetValue<int32_t>();

					records.push_back(record);
				}
			}
		}
	} catch (Exception &ex) {
		throw IOException("Error querying CDX API: " + ex.what());
	}

	return records;
}

// Helper function to fetch WARC response
static string FetchWARCResponse(ClientContext &context, const CDXRecord &record) {
	if (record.filename.empty() || record.offset == 0 || record.length == 0) {
		return ""; // Invalid record
	}

	// Construct the WARC URL
	string warc_url = "https://data.commoncrawl.org/" + record.filename;

	// Calculate byte range
	int64_t end_byte = record.offset + record.length - 1;

	// The WARC files are gzip compressed (.warc.gz)
	// When we make a range request, we get the compressed bytes
	// We need to decompress them

	// Strategy: Use DuckDB's httpfs to fetch the byte range, then decompress
	// For now, we'll try to use read_blob with range and then decompress
	// Note: This is a simplified approach - proper implementation would handle streaming decompression

	try {
		// First, let's try to fetch using httpfs
		// We'll use read_blob to get binary data, then try to decompress
		string query = "SELECT * FROM read_blob('" + warc_url + "', "
			"compression='gzip', "
			"byte_range_start=" + to_string(record.offset) + ", "
			"byte_range_end=" + to_string(end_byte) + ")";

		auto result = context.Query(query);
		if (!result->HasError()) {
			auto chunk = result->Fetch();
			if (chunk && chunk->size() > 0) {
				return chunk->GetValue(0, 0).ToString();
			}
		}

		// If the above doesn't work (read_blob might not support these parameters),
		// try an alternative approach using read_text
		query = "SELECT content FROM read_text('" + warc_url + "')";
		result = context.Query(query);
		if (!result->HasError()) {
			auto chunk = result->Fetch();
			if (chunk && chunk->size() > 0) {
				// This would fetch the entire file, which is not ideal
				// In production, we'd need proper HTTP range request support
				// For now, return a placeholder message
				return "[WARC response data - decompression needed]";
			}
		}
	} catch (Exception &ex) {
		// Silently ignore errors and return empty string
	} catch (...) {
		// Catch all other exceptions
	}

	return "";
}

// Bind function for the table function
static unique_ptr<FunctionData> CommonCrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	// Expect one parameter: the index name
	if (input.inputs.size() != 1) {
		throw BinderException("common_crawl_index requires exactly one parameter: the index name");
	}

	if (input.inputs[0].type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("common_crawl_index index name must be a string");
	}

	string index_name = input.inputs[0].ToString();
	auto bind_data = make_uniq<CommonCrawlBindData>(index_name);

	// Define output columns
	names.push_back("url");
	return_types.push_back(LogicalType::VARCHAR);
	bind_data->fields_needed.push_back("url");

	names.push_back("timestamp");
	return_types.push_back(LogicalType::VARCHAR);
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

	// Add response column (optional - will be NULL if not fetched)
	// In a full implementation, we'd use projection pushdown to detect if this column is needed
	// For now, we always include it in the schema but control fetching via a flag
	names.push_back("response");
	return_types.push_back(LogicalType::VARCHAR);

	// For MVP, disable response fetching (requires HTTP range requests + gzip decompression)
	// This can be enabled once we have proper implementation
	bind_data->fetch_response = false; // Set to true when response fetching is fully implemented

	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

// Init global state function
static unique_ptr<GlobalTableFunctionState> CommonCrawlInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CommonCrawlBindData>();
	auto state = make_uniq<CommonCrawlGlobalState>();

	// Use the URL filter from bind data (could be set via filter pushdown)
	string url_pattern = bind_data.url_filter;

	// Query CDX API with optimized field list
	state->records = QueryCDXAPI(context, bind_data.index_name, url_pattern, bind_data.fields_needed);

	return std::move(state);
}

// Scan function for the table function
static void CommonCrawlScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<CommonCrawlBindData>();
	auto &gstate = data.global_state->Cast<CommonCrawlGlobalState>();

	idx_t output_offset = 0;
	while (gstate.current_position < gstate.records.size() && output_offset < STANDARD_VECTOR_SIZE) {
		auto &record = gstate.records[gstate.current_position];

		idx_t col_idx = 0;

		// url
		output.SetValue(col_idx++, output_offset, Value(record.url));

		// timestamp
		output.SetValue(col_idx++, output_offset, Value(record.timestamp));

		// mime_type
		output.SetValue(col_idx++, output_offset, Value(record.mime_type));

		// status_code
		output.SetValue(col_idx++, output_offset, Value::INTEGER(record.status_code));

		// digest
		output.SetValue(col_idx++, output_offset, Value(record.digest));

		// filename
		output.SetValue(col_idx++, output_offset, Value(record.filename));

		// offset
		output.SetValue(col_idx++, output_offset, Value::BIGINT(record.offset));

		// length
		output.SetValue(col_idx++, output_offset, Value::BIGINT(record.length));

		// response (if requested)
		if (bind_data.fetch_response) {
			string response = FetchWARCResponse(context, record);
			output.SetValue(col_idx++, output_offset, Value(response));
		} else {
			// Return NULL for response column when not fetching
			output.SetValue(col_idx++, output_offset, Value());
		}

		gstate.current_position++;
		output_offset++;
	}

	output.SetCardinality(output_offset);
}

// Filter pushdown function to handle WHERE clauses
static void CommonCrawlPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                               vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<CommonCrawlBindData>();

	// Look for filters on the 'url' column
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];

		// Check if this is a simple comparison on the url column
		// For now, we'll do a basic extraction - a full implementation would parse the filter tree
		// and extract URL patterns from various comparison expressions

		// This is a simplified version - in production, we'd properly traverse the expression tree
		// and extract URL filter patterns
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// Auto-load httpfs extension
	auto &config = DBConfig::GetConfig(*loader.db);
	if (!config.options.load_extensions) {
		throw InvalidInputException("httpfs extension is required but extension loading is disabled");
	}

	// Load httpfs by executing INSTALL and LOAD commands
	Connection con(*loader.db);
	auto result = con.Query("INSTALL httpfs; LOAD httpfs;");
	if (result->HasError()) {
		throw IOException("Failed to load httpfs extension: " + result->GetError());
	}

	// Register the common_crawl_index table function
	TableFunction common_crawl_func("common_crawl_index", {LogicalType::VARCHAR},
	                                 CommonCrawlScan, CommonCrawlBind, CommonCrawlInitGlobal);

	ExtensionUtil::RegisterFunction(*loader.db, common_crawl_func);
}

void QuackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack, loader) {
	duckdb::LoadInternal(loader);
}
}
