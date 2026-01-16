//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/index.hpp"

#include <unordered_map>

namespace duckdb {
class BaseStatistics;

struct DuckLakeColumnExtraStats {
	virtual ~DuckLakeColumnExtraStats() = default;

	virtual void Merge(const DuckLakeColumnExtraStats &new_stats) = 0;
	virtual unique_ptr<DuckLakeColumnExtraStats> Copy() const = 0;

	// Convert the stats into a string representation for storage (e.g. JSON)
	virtual string Serialize() const = 0;
	// Parse the stats from a string
	virtual void Deserialize(const string &stats) = 0;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct DuckLakeColumnGeoStats final : public DuckLakeColumnExtraStats {

	DuckLakeColumnGeoStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

public:
	double xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax;
	set<string> geo_types;
};

//! Type of values for a JSON key
enum class JsonKeyValueType : uint8_t {
	UNKNOWN = 0,   //! Not yet determined
	VARCHAR = 1,   //! String values
	DOUBLE = 2,    //! Floating point numbers
	BIGINT = 3,    //! Integer numbers
	BOOLEAN = 4,   //! Boolean values
	JSON_NULL = 5, //! Only NULL values seen
	MIXED = 6      //! Mixed types (cannot use for pruning)
};

//! Stats for a single JSON key
struct JsonKeyStats {
	//! The inferred type of values for this key
	JsonKeyValueType type = JsonKeyValueType::UNKNOWN;
	//! Stringified min value (for comparison)
	string min_value;
	//! Stringified max value (for comparison)
	string max_value;
	//! Number of NULL values (including when key is missing from an object)
	idx_t null_count = 0;
	//! Number of non-NULL values
	idx_t value_count = 0;

	void Merge(const JsonKeyStats &other);
	bool HasStats() const;

	//! Convert type enum to string for serialization
	static const char *TypeToString(JsonKeyValueType type);
	//! Convert string to type enum for deserialization
	static JsonKeyValueType StringToType(const string &str);
};

//! Extra stats for JSON columns - tracks per-top-level-key statistics
struct DuckLakeColumnJsonKeyStats final : public DuckLakeColumnExtraStats {

	DuckLakeColumnJsonKeyStats();
	void Merge(const DuckLakeColumnExtraStats &new_stats) override;
	unique_ptr<DuckLakeColumnExtraStats> Copy() const override;

	string Serialize() const override;
	void Deserialize(const string &stats) override;

	//! Update stats with a single JSON value (string representation)
	void UpdateStats(const string &json_value);
	//! Called when a NULL JSON value is encountered
	void UpdateNullStats();
	//! Get stats for a specific key (returns nullptr if not found)
	const JsonKeyStats *GetKeyStats(const string &key) const;

public:
	//! Map from top-level key name to stats
	unordered_map<string, JsonKeyStats> key_stats;
	//! Total number of JSON values processed (for tracking keys that are missing)
	idx_t total_count = 0;
};

struct DuckLakeColumnStats {
	explicit DuckLakeColumnStats(LogicalType type_p) : type(std::move(type_p)) {
		if (DuckLakeTypes::IsGeoType(type)) {
			extra_stats = make_uniq<DuckLakeColumnGeoStats>();
		} else if (type.IsJSONType()) {
			extra_stats = make_uniq<DuckLakeColumnJsonKeyStats>();
		}
	}

	// Copy constructor
	DuckLakeColumnStats(const DuckLakeColumnStats &other);
	DuckLakeColumnStats &operator=(const DuckLakeColumnStats &other);
	DuckLakeColumnStats(DuckLakeColumnStats &&other) noexcept = default;
	DuckLakeColumnStats &operator=(DuckLakeColumnStats &&other) noexcept = default;

	LogicalType type;
	string min;
	string max;
	idx_t null_count = 0;
	idx_t column_size_bytes = 0;
	bool contains_nan = false;
	bool has_null_count = false;
	bool has_min = false;
	bool has_max = false;
	bool any_valid = true;
	bool has_contains_nan = false;

	unique_ptr<DuckLakeColumnExtraStats> extra_stats;

public:
	unique_ptr<BaseStatistics> ToStats() const;
	void MergeStats(const DuckLakeColumnStats &new_stats);
	DuckLakeColumnStats Copy() const;

private:
	unique_ptr<BaseStatistics> CreateNumericStats() const;
	unique_ptr<BaseStatistics> CreateStringStats() const;
};

//! These are the global, table-wide stats
struct DuckLakeTableStats {
	idx_t record_count = 0;
	idx_t table_size_bytes = 0;
	idx_t next_row_id = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;

	void MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats);
};

struct DuckLakeStats {
	map<TableIndex, unique_ptr<DuckLakeTableStats>> table_stats;
};

} // namespace duckdb
