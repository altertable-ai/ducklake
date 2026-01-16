#include "common/ducklake_util.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_stats.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeDataFile> transaction_local_files_p,
                                             shared_ptr<DuckLakeInlinedData> transaction_local_data_p,
                                             unique_ptr<FilterPushdownInfo> filter_info_p)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(false),
      transaction_local_files(std::move(transaction_local_files_p)),
      transaction_local_data(std::move(transaction_local_data_p)), filter_info(std::move(filter_info_p)) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             vector<DuckLakeFileListEntry> files_to_scan)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info),
      files(std::move(files_to_scan)), read_file_list(true), is_complex_filter_pruned_list(true) {
}

DuckLakeMultiFileList::DuckLakeMultiFileList(DuckLakeFunctionInfo &read_info,
                                             const DuckLakeInlinedTableInfo &inlined_table)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), read_info(read_info), read_file_list(true) {
	DuckLakeFileListEntry file_entry;
	file_entry.file.path = inlined_table.table_name;
	file_entry.row_id_start = 0;
	file_entry.data_type = DuckLakeDataType::INLINED_DATA;
	files.push_back(std::move(file_entry));
	inlined_data_tables.push_back(inlined_table);
}

//! Information about a JSON extraction filter that can be used for pruning
struct JsonExtractFilterInfo {
	//! The field index of the JSON column
	idx_t column_field_index;
	//! The JSON key path being extracted (e.g., "a.b.c" from "$.a.b.c")
	string json_key;
	//! The comparison type
	ExpressionType comparison_type;
	//! The constant value being compared against
	Value constant_value;
	//! The type of the JSON column
	LogicalType column_type;
};

//! Try to extract a flattened key path from a JSON path like "$.a.b.c"
//! Returns the flattened path (e.g., "a.b.c") which matches how we store stats
static bool TryExtractJsonPath(const string &path, string &out_key) {
	// Must start with "$."
	if (path.size() < 3 || path[0] != '$' || path[1] != '.') {
		return false;
	}

	// Extract everything after "$." - this is the flattened key path
	out_key = path.substr(2);

	// Validate: must not be empty and must not contain brackets (array access not supported)
	if (out_key.empty() || out_key.find('[') != string::npos) {
		return false;
	}

	return true;
}

//! Try to extract a JsonExtractFilterInfo from a comparison expression
static bool TryExtractJsonFilter(Expression &expr, const vector<string> &column_names,
                                 const vector<column_t> &column_ids, const DuckLakeTableEntry &table,
                                 JsonExtractFilterInfo &out_info) {
	if (expr.type != ExpressionType::COMPARE_EQUAL && expr.type != ExpressionType::COMPARE_NOTEQUAL &&
	    expr.type != ExpressionType::COMPARE_LESSTHAN && expr.type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
	    expr.type != ExpressionType::COMPARE_GREATERTHAN && expr.type != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
		return false;
	}

	auto &comparison = expr.Cast<BoundComparisonExpression>();
	Expression *func_expr = nullptr;
	Expression *const_expr = nullptr;
	bool flip_comparison = false;

	// Determine which side is the function and which is the constant
	if (comparison.left->expression_class == ExpressionClass::BOUND_FUNCTION &&
	    comparison.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
		func_expr = comparison.left.get();
		const_expr = comparison.right.get();
	} else if (comparison.right->expression_class == ExpressionClass::BOUND_FUNCTION &&
	           comparison.left->expression_class == ExpressionClass::BOUND_CONSTANT) {
		func_expr = comparison.right.get();
		const_expr = comparison.left.get();
		flip_comparison = true;
	} else {
		return false;
	}

	auto &func = func_expr->Cast<BoundFunctionExpression>();
	auto &constant = const_expr->Cast<BoundConstantExpression>();

	// Check if it's json_extract_string or json_extract
	string func_name = StringUtil::Lower(func.function.name);
	if (func_name != "json_extract_string" && func_name != "->>") {
		return false;
	}

	// json_extract_string takes 2 arguments: (json_column, path)
	if (func.children.size() != 2) {
		return false;
	}

	// First argument should be a column reference
	if (func.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}

	// Second argument should be a constant (the path)
	if (func.children[1]->expression_class != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}

	auto &col_ref = func.children[0]->Cast<BoundColumnRefExpression>();
	auto &path_const = func.children[1]->Cast<BoundConstantExpression>();

	if (path_const.value.type().id() != LogicalTypeId::VARCHAR) {
		return false;
	}

	string path = StringValue::Get(path_const.value);
	string json_key;
	if (!TryExtractJsonPath(path, json_key)) {
		return false;
	}

	// Get the column index from the column binding
	auto column_idx = col_ref.binding.column_index;
	if (column_idx >= column_ids.size()) {
		return false;
	}

	auto column_id = column_ids[column_idx];
	if (IsVirtualColumn(column_id)) {
		return false;
	}

	// Get the field index for this column
	auto column_index = PhysicalIndex(column_id);
	auto &root_id = table.GetFieldId(column_index);

	// Verify it's a JSON column
	if (!root_id.Type().IsJSONType()) {
		return false;
	}

	out_info.column_field_index = root_id.GetFieldIndex().index;
	out_info.json_key = json_key;
	out_info.comparison_type = flip_comparison ? FlipComparisonExpression(expr.type) : expr.type;
	out_info.constant_value = constant.value;
	out_info.column_type = root_id.Type();

	return true;
}

//! Check if a file can be pruned based on JSON key stats
static bool CanPruneFileByJsonKeyStats(const DuckLakeFileListEntry &file_entry, const JsonExtractFilterInfo &filter) {
	// Find the extra_stats for this column
	auto extra_stats_it = file_entry.column_extra_stats.find(filter.column_field_index);
	if (extra_stats_it == file_entry.column_extra_stats.end()) {
		// No extra stats available - can't prune
		return false;
	}

	// Parse the JSON key stats
	DuckLakeColumnJsonKeyStats json_stats;
	try {
		json_stats.Deserialize(extra_stats_it->second);
	} catch (...) {
		// Failed to parse - can't prune
		return false;
	}

	// Get stats for the specific key
	auto key_stats = json_stats.GetKeyStats(filter.json_key);
	if (!key_stats) {
		// Key not found in any row of this file - all extractions will return NULL
		// For equality/range comparisons with non-NULL constant, we can prune
		if (!filter.constant_value.IsNull() && filter.comparison_type != ExpressionType::COMPARE_NOTEQUAL) {
			return true;
		}
		return false;
	}

	// If the key has mixed types, we can't safely prune
	if (key_stats->type == JsonKeyValueType::MIXED) {
		return false;
	}

	// If all values for this key are NULL, we can prune for non-NULL comparisons
	if (key_stats->value_count == 0 && key_stats->null_count > 0) {
		if (!filter.constant_value.IsNull()) {
			return true;
		}
		return false;
	}

	// Check if we can prune based on min/max
	if (key_stats->min_value.empty() || key_stats->max_value.empty()) {
		return false;
	}

	// Convert constant to string for comparison
	string constant_str;
	if (filter.constant_value.type().id() == LogicalTypeId::VARCHAR) {
		constant_str = StringValue::Get(filter.constant_value);
	} else {
		constant_str = filter.constant_value.ToString();
	}

	// Perform the comparison based on the stored type
	// Note: JSON key stats stores min/max as strings, and for VARCHAR comparison we use lexicographic ordering
	auto compare = [&](const string &a, const string &b) -> int {
		if (key_stats->type == JsonKeyValueType::DOUBLE || key_stats->type == JsonKeyValueType::BIGINT) {
			try {
				double val_a = std::stod(a);
				double val_b = std::stod(b);
				if (val_a < val_b) {
					return -1;
				}
				if (val_a > val_b) {
					return 1;
				}
				return 0;
			} catch (...) {
				// Fall back to string comparison
			}
		}
		return a.compare(b);
	};

	int min_cmp = compare(key_stats->min_value, constant_str);
	int max_cmp = compare(key_stats->max_value, constant_str);

	switch (filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// Prune if constant is outside [min, max]
		if (min_cmp > 0 || max_cmp < 0) {
			return true;
		}
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		// Prune if all values equal the constant (min == max == constant)
		if (min_cmp == 0 && max_cmp == 0 && key_stats->null_count == 0) {
			return true;
		}
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		// Prune if min >= constant
		if (min_cmp >= 0) {
			return true;
		}
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// Prune if min > constant
		if (min_cmp > 0) {
			return true;
		}
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		// Prune if max <= constant
		if (max_cmp <= 0) {
			return true;
		}
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// Prune if max < constant
		if (max_cmp < 0) {
			return true;
		}
		break;
	default:
		break;
	}

	return false;
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                       const MultiFileOptions &options,
                                                                       MultiFilePushdownInfo &info,
                                                                       vector<unique_ptr<Expression>> &filters) {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.empty()) {
		return nullptr;
	}

	// Extract JSON extraction filters from the filter expressions
	vector<JsonExtractFilterInfo> json_filters;
	for (auto &filter : filters) {
		JsonExtractFilterInfo json_filter;
		if (TryExtractJsonFilter(*filter, info.column_names, info.column_ids, read_info.table, json_filter)) {
			json_filters.push_back(std::move(json_filter));
		}
	}

	if (json_filters.empty()) {
		return nullptr;
	}

	// Build filter pushdown info for the JSON columns so extra_stats gets loaded
	auto pushdown_info = make_uniq<FilterPushdownInfo>();
	for (auto &json_filter : json_filters) {
		// Create a dummy table filter for this column to trigger extra_stats loading
		auto dummy_filter = make_uniq<IsNotNullFilter>();
		ColumnFilterInfo filter_info(json_filter.column_field_index, json_filter.column_type, std::move(dummy_filter));
		if (pushdown_info->column_filters.find(json_filter.column_field_index) == pushdown_info->column_filters.end()) {
			pushdown_info->column_filters.emplace(json_filter.column_field_index, std::move(filter_info));
		}
	}

	// Create a new multi-file list with the filter info
	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                               std::move(pushdown_info));

	// Get the files with extra_stats loaded
	auto &files = result->GetFiles();

	// Prune files based on JSON key stats
	vector<DuckLakeFileListEntry> pruned_files;
	for (auto &file_entry : files) {
		bool should_prune = false;
		for (auto &json_filter : json_filters) {
			if (CanPruneFileByJsonKeyStats(file_entry, json_filter)) {
				should_prune = true;
				break;
			}
		}
		if (!should_prune) {
			pruned_files.push_back(file_entry);
		}
	}

	// If no files were pruned, return nullptr to indicate no benefit
	if (pruned_files.size() == files.size()) {
		return nullptr;
	}

	// Return a new file list with only the non-pruned files
	return make_uniq<DuckLakeMultiFileList>(read_info, std::move(pruned_files));
}

unique_ptr<MultiFileList>
DuckLakeMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                             const vector<string> &names, const vector<LogicalType> &types,
                                             const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (read_info.scan_type != DuckLakeScanType::SCAN_TABLE || filters.filters.empty()) {
		// filter pushdown is only supported when scanning full tables
		return nullptr;
	}

	auto pushdown_info = make_uniq<FilterPushdownInfo>();

	for (auto &entry : filters.filters) {
		auto column_index_val = entry.first;
		idx_t column_idx = column_index_val;
		auto column_id = column_ids[column_idx];

		if (IsVirtualColumn(column_id)) {
			continue;
		}

		auto column_index = PhysicalIndex(column_id);
		auto &root_id = read_info.table.GetFieldId(column_index);
		auto field_index = root_id.GetFieldIndex().index;

		auto filter_copy = entry.second->Copy();
		// Get the column type from the table schema, not from the scan types array
		const auto &column_type = read_info.column_types[column_index.index];

		ColumnFilterInfo filter_info(field_index, column_type, std::move(filter_copy));
		pushdown_info->column_filters.emplace(field_index, std::move(filter_info));
	}

	if (pushdown_info->column_filters.empty()) {
		// no pushdown possible
		return nullptr;
	}

	// If this list was created by ComplexFilterPushdown (a pruned list), preserve the files.
	// The pruned files should be kept, but we still pass the new pushdown_info for runtime filtering.
	// We don't preserve files for regular lists because they may not have the column_min_max
	// data needed for the new dynamic filters.
	if (is_complex_filter_pruned_list) {
		// Create a new list with the already-loaded (possibly pruned) files
		auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
		                                               std::move(pushdown_info));
		result->files = files;
		result->read_file_list = true;
		result->is_complex_filter_pruned_list = true;
		result->delete_scans = delete_scans;
		result->inlined_data_tables = inlined_data_tables;
		return result;
	}

	return make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                        std::move(pushdown_info));
}

vector<OpenFileInfo> DuckLakeMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> file_list;
	for (idx_t i = 0; i < GetTotalFileCount(); i++) {
		file_list.push_back(GetFile(i));
	}
	return file_list;
}

FileExpandResult DuckLakeMultiFileList::GetExpandResult() {
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t DuckLakeMultiFileList::GetTotalFileCount() {
	return GetFiles().size();
}

unique_ptr<NodeStatistics> DuckLakeMultiFileList::GetCardinality(ClientContext &context) {
	auto stats = read_info.table.GetTableStats(context);
	if (!stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(stats->record_count);
}

DuckLakeTableEntry &DuckLakeMultiFileList::GetTable() {
	return read_info.table;
}

OpenFileInfo DuckLakeMultiFileList::GetFile(idx_t i) {
	auto &files = GetFiles();
	if (i >= files.size()) {
		return OpenFileInfo();
	}
	auto &file_entry = files[i];
	auto &file = file_entry.file;
	OpenFileInfo result(file.path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	idx_t inlined_data_file_start = files.size() - inlined_data_tables.size();
	if (transaction_local_data) {
		inlined_data_file_start--;
	}
	if (transaction_local_data && i + 1 == files.size()) {
		// scanning transaction local data
		extended_info->options["transaction_local_data"] = Value::BOOLEAN(true);
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		if (file_entry.row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(file_entry.row_id_start.GetIndex());
		}
		extended_info->options["snapshot_id"] = Value(LogicalType::BIGINT);
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	} else if (i >= inlined_data_file_start) {
		// scanning inlined data
		auto inlined_data_index = i - inlined_data_file_start;
		auto &inlined_data_table = inlined_data_tables[inlined_data_index];
		extended_info->options["table_name"] = inlined_data_table.table_name;
		extended_info->options["inlined_data"] = Value::BOOLEAN(true);
		extended_info->options["schema_version"] =
		    Value::BIGINT(NumericCast<int64_t>(inlined_data_table.schema_version));
	} else {
		extended_info->options["file_size"] = Value::UBIGINT(file.file_size_bytes);
		if (file.footer_size.IsValid()) {
			extended_info->options["footer_size"] = Value::UBIGINT(file.footer_size.GetIndex());
		}
		if (files[i].row_id_start.IsValid()) {
			extended_info->options["row_id_start"] = Value::UBIGINT(files[i].row_id_start.GetIndex());
		}
		Value snapshot_id;
		if (files[i].snapshot_id.IsValid()) {
			snapshot_id = Value::BIGINT(NumericCast<int64_t>(files[i].snapshot_id.GetIndex()));
		} else {
			snapshot_id = Value(LogicalType::BIGINT);
		}
		extended_info->options["snapshot_id"] = std::move(snapshot_id);
		if (!file.encryption_key.empty()) {
			extended_info->options["encryption_key"] = Value::BLOB_RAW(file.encryption_key);
		}
		// files managed by DuckLake are never modified - we can keep them cached
		extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		// etag / last modified time can be set to dummy values
		extended_info->options["etag"] = Value("");
		extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		if (!file_entry.delete_file.path.empty() || file_entry.max_row_count.IsValid()) {
			extended_info->options["has_deletes"] = Value::BOOLEAN(true);
		}
		if (file_entry.mapping_id.IsValid()) {
			extended_info->options["mapping_id"] = Value::UBIGINT(file_entry.mapping_id.index);
		}
	}
	result.extended_info = std::move(extended_info);
	return result;
}

unique_ptr<MultiFileList> DuckLakeMultiFileList::Copy() {
	unique_ptr<FilterPushdownInfo> filter_copy;
	if (filter_info) {
		filter_copy = filter_info->Copy();
	}

	auto result = make_uniq<DuckLakeMultiFileList>(read_info, transaction_local_files, transaction_local_data,
	                                               std::move(filter_copy));
	result->files = GetFiles();
	result->read_file_list = read_file_list;
	result->delete_scans = delete_scans;
	result->inlined_data_tables = inlined_data_tables;
	return result;
}

const DuckLakeFileListEntry &DuckLakeMultiFileList::GetFileEntry(idx_t file_idx) {
	auto &files = GetFiles();
	return files[file_idx];
}

DuckLakeFileData GetFileData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	result.path = file.file_name;
	result.encryption_key = file.encryption_key;
	result.file_size_bytes = file.file_size_bytes;
	result.footer_size = file.footer_size;
	return result;
}

DuckLakeFileData GetDeleteData(const DuckLakeDataFile &file) {
	DuckLakeFileData result;
	if (!file.delete_file) {
		return result;
	}
	auto &delete_file = *file.delete_file;
	result.path = delete_file.file_name;
	result.encryption_key = delete_file.encryption_key;
	result.file_size_bytes = delete_file.file_size_bytes;
	result.footer_size = delete_file.footer_size;
	return result;
}

vector<DuckLakeFileListExtendedEntry> DuckLakeMultiFileList::GetFilesExtended() {
	lock_guard<mutex> l(file_lock);
	vector<DuckLakeFileListExtendedEntry> result;
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		result = metadata_manager.GetExtendedFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < result.size(); file_idx++) {
			if (transaction.FileIsDropped(result[file_idx].file.path)) {
				result.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : result) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = file.row_count;
		file_entry.file = GetFileData(file);
		file_entry.delete_file = GetDeleteData(file);
		file_entry.row_id_start = transaction_row_start;
		transaction_row_start += file.row_count;
		result.push_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = 0;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.file_id = DataFileIndex();
		file_entry.delete_file_id = DataFileIndex();
		file_entry.row_count = transaction_local_data->data->Count();
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		result.push_back(std::move(file_entry));
	}
	if (!read_file_list) {
		// we have not read the file list yet - construct it from the extended file list
		for (auto &file : result) {
			DuckLakeFileListEntry file_entry;
			file_entry.file = file.file;
			file_entry.row_id_start = file.row_id_start;
			file_entry.delete_file = file.delete_file;
			files.emplace_back(std::move(file_entry));
		}
		read_file_list = true;
	}
	return result;
}

void DuckLakeMultiFileList::GetFilesForTable() {
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	if (!read_info.table_id.IsTransactionLocal()) {
		// not a transaction local table - read the file list from the metadata store
		auto &metadata_manager = transaction.GetMetadataManager();
		files = metadata_manager.GetFilesForTable(read_info.table, read_info.snapshot, filter_info.get());
	}
	if (transaction.HasDroppedFiles()) {
		for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
			if (transaction.FileIsDropped(files[file_idx].file.path)) {
				files.erase_at(file_idx);
				file_idx--;
			}
		}
	}
	// if the transaction has any local deletes - apply them to the file list
	if (transaction.HasLocalDeletes(read_info.table_id)) {
		for (auto &file_entry : files) {
			transaction.GetLocalDeleteForFile(read_info.table_id, file_entry.file.path, file_entry.delete_file);
		}
	}
	idx_t transaction_row_start = TRANSACTION_LOCAL_ID_START;
	for (auto &file : transaction_local_files) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = GetFileData(file);
		file_entry.row_id_start = transaction_row_start;
		file_entry.delete_file = GetDeleteData(file);
		file_entry.mapping_id = file.mapping_id;
		transaction_row_start += file.row_count;
		files.emplace_back(std::move(file_entry));
	}
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
	if (transaction_local_data) {
		// we have transaction local inlined data - create the dummy file entry
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = DUCKLAKE_TRANSACTION_LOCAL_INLINED_FILENAME;
		file_entry.row_id_start = transaction_row_start;
		file_entry.data_type = DuckLakeDataType::TRANSACTION_LOCAL_INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableInsertions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	files = metadata_manager.GetTableInsertions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

void DuckLakeMultiFileList::GetTableDeletions() {
	if (read_info.table_id.IsTransactionLocal()) {
		throw InternalException("Cannot get changes between snapshots for transaction-local files");
	}
	auto transaction_ref = read_info.GetTransaction();
	auto &transaction = *transaction_ref;
	auto &metadata_manager = transaction.GetMetadataManager();
	delete_scans = metadata_manager.GetTableDeletions(read_info.table, *read_info.start_snapshot, read_info.snapshot);
	for (auto &file : delete_scans) {
		DuckLakeFileListEntry file_entry;
		file_entry.file = file.file;
		file_entry.row_id_start = file.row_id_start;
		file_entry.snapshot_id = file.snapshot_id;
		file_entry.mapping_id = file.mapping_id;
		files.emplace_back(std::move(file_entry));
	}
	// add inlined data tables as sources (if any)
	inlined_data_tables = read_info.table.GetInlinedDataTables();
	for (auto &table : inlined_data_tables) {
		DuckLakeFileListEntry file_entry;
		file_entry.file.path = table.table_name;
		file_entry.row_id_start = 0;
		file_entry.data_type = DuckLakeDataType::INLINED_DATA;
		files.push_back(std::move(file_entry));
	}
}

bool DuckLakeMultiFileList::IsDeleteScan() const {
	return read_info.scan_type == DuckLakeScanType::SCAN_DELETIONS;
}

const DuckLakeDeleteScanEntry &DuckLakeMultiFileList::GetDeleteScanEntry(idx_t file_idx) {
	return delete_scans[file_idx];
}

const vector<DuckLakeFileListEntry> &DuckLakeMultiFileList::GetFiles() {
	lock_guard<mutex> l(file_lock);
	if (!read_file_list) {
		// we have not read the file list yet - read it
		switch (read_info.scan_type) {
		case DuckLakeScanType::SCAN_TABLE:
			GetFilesForTable();
			break;
		case DuckLakeScanType::SCAN_INSERTIONS:
			GetTableInsertions();
			break;
		case DuckLakeScanType::SCAN_DELETIONS:
			GetTableDeletions();
			break;
		default:
			throw InternalException("Unknown DuckLake scan type");
		}
		read_file_list = true;
	}
	return files;
}

} // namespace duckdb
