//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_compaction_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;
class DuckLakeCatalog;
class DuckLakeTransaction;
class Binder;
struct DuckLakeSort;

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class DuckLakeLogicalCompaction : public LogicalExtensionOperator {
public:
	DuckLakeLogicalCompaction(idx_t table_index, DuckLakeTableEntry &table,
	                          vector<DuckLakeCompactionFileEntry> source_files_p, string encryption_key_p,
	                          optional_idx partition_id, vector<string> partition_values_p, optional_idx row_id_start,
	                          CompactionType type);

	idx_t table_index;
	DuckLakeTableEntry &table;
	vector<DuckLakeCompactionFileEntry> source_files;
	string encryption_key;
	optional_idx partition_id;
	vector<string> partition_values;
	optional_idx row_id_start;
	CompactionType type;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;

	string GetExtensionName() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	void ResolveTypes() override;
};

//===--------------------------------------------------------------------===//
// Compaction Command Generator
//===--------------------------------------------------------------------===//
class DuckLakeCompactor {
public:
	DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                  Binder &binder, TableIndex table_id, DuckLakeMergeAdjacentOptions options);
	DuckLakeCompactor(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                  Binder &binder, TableIndex table_id, double delete_threshold);
	void GenerateCompactions(DuckLakeTableEntry &table, vector<unique_ptr<LogicalOperator>> &compactions);
	unique_ptr<LogicalOperator> GenerateCompactionCommand(vector<DuckLakeCompactionFileEntry> source_files);

	static unique_ptr<LogicalOperator> InsertSort(Binder &binder, unique_ptr<LogicalOperator> &plan,
	                                              DuckLakeTableEntry &table, optional_ptr<DuckLakeSort> sort_data);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeTransaction &transaction;
	Binder &binder;
	TableIndex table_id;
	double delete_threshold = 0.95;
	DuckLakeMergeAdjacentOptions options;

	CompactionType type;
};

} // namespace duckdb
