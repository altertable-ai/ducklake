#include "storage/ducklake_stats.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"

#include "yyjson.hpp"

#include <unordered_set>

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

DuckLakeColumnStats::DuckLakeColumnStats(const DuckLakeColumnStats &other) {
	type = other.type;
	min = other.min;
	max = other.max;
	null_count = other.null_count;
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
	has_min = other.has_min;
	has_max = other.has_max;
	any_valid = other.any_valid;
	has_contains_nan = other.has_contains_nan;

	if (other.extra_stats) {
		extra_stats = other.extra_stats->Copy();
	}
}

DuckLakeColumnStats &DuckLakeColumnStats::operator=(const DuckLakeColumnStats &other) {
	if (this == &other) {
		return *this;
	}
	type = other.type;
	min = other.min;
	max = other.max;
	null_count = other.null_count;
	column_size_bytes = other.column_size_bytes;
	contains_nan = other.contains_nan;
	has_null_count = other.has_null_count;
	has_min = other.has_min;
	has_max = other.has_max;
	any_valid = other.any_valid;
	has_contains_nan = other.has_contains_nan;

	if (other.extra_stats) {
		extra_stats = other.extra_stats->Copy();
	} else {
		extra_stats.reset();
	}
	return *this;
}

void DuckLakeColumnStats::MergeStats(const DuckLakeColumnStats &new_stats) {
	if (type != new_stats.type) {
		// handle type promotion - adopt the new type
		type = new_stats.type;
	}
	if (!new_stats.has_null_count) {
		has_null_count = false;
	} else if (has_null_count) {
		// both stats have a null count - add them up
		null_count += new_stats.null_count;
	}
	column_size_bytes += new_stats.column_size_bytes;
	if (!new_stats.has_contains_nan) {
		has_contains_nan = false;
	} else if (has_contains_nan) {
		// both stats have a null count - add them up
		if (new_stats.contains_nan) {
			contains_nan = true;
		}
	}

	if (!new_stats.any_valid) {
		// all values in the source are NULL - don't update min/max
		return;
	}
	if (!any_valid) {
		// all values in the current stats are null - copy the min/max
		min = new_stats.min;
		has_min = new_stats.has_min;
		max = new_stats.max;
		has_max = new_stats.has_max;
		any_valid = true;
		return;
	}
	if (!new_stats.has_min) {
		has_min = false;
	} else if (has_min) {
		// both stats have a min - select the smallest
		if (type.IsNumeric()) {
			// for numerics we need to parse the stats
			auto current_min = Value(min).DefaultCastAs(type);
			auto new_min = Value(new_stats.min).DefaultCastAs(type);
			if (new_min < current_min) {
				min = new_stats.min;
			}
		} else if (new_stats.min < min) {
			// for other types we can compare the strings directly
			min = new_stats.min;
		}
	}

	if (!new_stats.has_max) {
		has_max = false;
	} else if (has_max) {
		// both stats have a min - select the smallest
		if (type.IsNumeric()) {
			// for numerics we need to parse the stats
			auto current_max = Value(max).DefaultCastAs(type);
			auto new_max = Value(new_stats.max).DefaultCastAs(type);
			if (new_max > current_max) {
				max = new_stats.max;
			}
		} else if (new_stats.max > max) {
			// for other types we can compare the strings directly
			max = new_stats.max;
		}
	}

	if (new_stats.extra_stats) {
		if (extra_stats) {
			extra_stats->Merge(*new_stats.extra_stats);
		} else {
			extra_stats = new_stats.extra_stats->Copy();
		}
	}
}

void DuckLakeTableStats::MergeStats(FieldIndex col_id, const DuckLakeColumnStats &file_stats) {
	auto entry = column_stats.find(col_id);
	if (entry == column_stats.end()) {
		column_stats.insert(make_pair(col_id, file_stats));
		return;
	}
	// merge the stats
	auto &current_stats = entry->second;
	current_stats.MergeStats(file_stats);
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateNumericStats() const {
	if (!has_min || !has_max) {
		return nullptr;
	}
	auto stats = NumericStats::CreateEmpty(type);
	// set min
	Value min_val(min);
	NumericStats::SetMin(stats, min_val.DefaultCastAs(type));
	// set max
	Value max_val(max);
	NumericStats::SetMax(stats, max_val.DefaultCastAs(type));
	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNull();
	}
	stats.SetHasNoNull();
	return stats.ToUnique();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::CreateStringStats() const {
	if (!has_min || !has_max) {
		return nullptr;
	}
	auto stats = StringStats::CreateEmpty(type);

	StringStats::Update(stats, string_t(min));
	StringStats::Update(stats, string_t(max));
	StringStats::ResetMaxStringLength(stats);
	StringStats::SetContainsUnicode(stats);
	// set null count
	if (!has_null_count || null_count > 0) {
		stats.SetHasNull();
	}
	stats.SetHasNoNull();
	return stats.ToUnique();
}

unique_ptr<BaseStatistics> DuckLakeColumnStats::ToStats() const {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return CreateNumericStats();
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		// we only create stats if we know there are no NaN values
		// FIXME: we can just set Max to NaN instead
		if (has_contains_nan && !contains_nan) {
			return CreateNumericStats();
		}
		return nullptr;
	case LogicalTypeId::VARCHAR:
		return CreateStringStats();
	default:
		return nullptr;
	}
}

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats() {
	xmin = NumericLimits<double>::Maximum();
	xmax = NumericLimits<double>::Minimum();
	ymin = NumericLimits<double>::Maximum();
	ymax = NumericLimits<double>::Minimum();
	zmin = NumericLimits<double>::Maximum();
	zmax = NumericLimits<double>::Minimum();
	mmin = NumericLimits<double>::Maximum();
	mmax = NumericLimits<double>::Minimum();
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnGeoStats::Copy() const {
	return make_uniq<DuckLakeColumnGeoStats>(*this);
}

void DuckLakeColumnGeoStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &geo_stats = new_stats.Cast<DuckLakeColumnGeoStats>();

	xmin = MinValue(xmin, geo_stats.xmin);
	xmax = MaxValue(xmax, geo_stats.xmax);
	ymin = MinValue(ymin, geo_stats.ymin);
	ymax = MaxValue(ymax, geo_stats.ymax);
	zmin = MinValue(zmin, geo_stats.zmin);
	zmax = MaxValue(zmax, geo_stats.zmax);
	mmin = MinValue(mmin, geo_stats.mmin);
	mmax = MaxValue(mmax, geo_stats.mmax);

	geo_types.insert(geo_stats.geo_types.begin(), geo_stats.geo_types.end());
}

string DuckLakeColumnGeoStats::Serialize() const {

	// Format as JSON
	auto xmin_val = xmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(xmin);
	auto xmax_val = xmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(xmax);
	auto ymin_val = ymin == NumericLimits<double>::Maximum() ? "null" : std::to_string(ymin);
	auto ymax_val = ymax == NumericLimits<double>::Minimum() ? "null" : std::to_string(ymax);
	auto zmin_val = zmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(zmin);
	auto zmax_val = zmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(zmax);
	auto mmin_val = mmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(mmin);
	auto mmax_val = mmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(mmax);

	auto bbox = StringUtil::Format(
	    R"({"xmin": %s, "xmax": %s, "ymin": %s, "ymax": %s, "zmin": %s, "zmax": %s, "mmin": %s, "mmax": %s})", xmin_val,
	    xmax_val, ymin_val, ymax_val, zmin_val, zmax_val, mmin_val, mmax_val);

	string types = "[";
	for (auto &type : geo_types) {
		if (types.size() > 1) {
			types += ", ";
		}
		types += StringUtil::Format("\"%s\"", type);
	}
	types += "]";

	return StringUtil::Format(R"('{"bbox": %s, "types": %s}')", bbox, types);
}

void DuckLakeColumnGeoStats::Deserialize(const string &stats) {
	auto doc = yyjson_read(stats.c_str(), stats.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse geo stats JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid geo stats JSON");
	}

	auto bbox_json = yyjson_obj_get(root, "bbox");
	if (yyjson_is_obj(bbox_json)) {
		auto xmin_json = yyjson_obj_get(bbox_json, "xmin");
		if (yyjson_is_num(xmin_json)) {
			xmin = yyjson_get_real(xmin_json);
		}
		auto xmax_json = yyjson_obj_get(bbox_json, "xmax");
		if (yyjson_is_num(xmax_json)) {
			xmax = yyjson_get_real(xmax_json);
		}
		auto ymin_json = yyjson_obj_get(bbox_json, "ymin");
		if (yyjson_is_num(ymin_json)) {
			ymin = yyjson_get_real(ymin_json);
		}
		auto ymax_json = yyjson_obj_get(bbox_json, "ymax");
		if (yyjson_is_num(ymax_json)) {
			ymax = yyjson_get_real(ymax_json);
		}
		auto zmin_json = yyjson_obj_get(bbox_json, "zmin");
		if (yyjson_is_num(zmin_json)) {
			zmin = yyjson_get_real(zmin_json);
		}
		auto zmax_json = yyjson_obj_get(bbox_json, "zmax");
		if (yyjson_is_num(zmax_json)) {
			zmax = yyjson_get_real(zmax_json);
		}
		auto mmin_json = yyjson_obj_get(bbox_json, "mmin");
		if (yyjson_is_num(mmin_json)) {
			mmin = yyjson_get_real(mmin_json);
		}
		auto mmax_json = yyjson_obj_get(bbox_json, "mmax");
		if (yyjson_is_num(mmax_json)) {
			mmax = yyjson_get_real(mmax_json);
		}
	}

	auto types_json = yyjson_obj_get(root, "types");
	if (yyjson_is_arr(types_json)) {
		yyjson_arr_iter iter;
		yyjson_arr_iter_init(types_json, &iter);
		yyjson_val *type_json;
		while ((type_json = yyjson_arr_iter_next(&iter))) {
			if (yyjson_is_str(type_json)) {
				geo_types.insert(yyjson_get_str(type_json));
			}
		}
	}
	yyjson_doc_free(doc);
}

//===----------------------------------------------------------------------===//
// DuckLakeColumnJsonKeyStats
//===----------------------------------------------------------------------===//

const char *JsonKeyStats::TypeToString(JsonKeyValueType type) {
	switch (type) {
	case JsonKeyValueType::VARCHAR:
		return "VARCHAR";
	case JsonKeyValueType::DOUBLE:
		return "DOUBLE";
	case JsonKeyValueType::BIGINT:
		return "BIGINT";
	case JsonKeyValueType::BOOLEAN:
		return "BOOLEAN";
	case JsonKeyValueType::JSON_NULL:
		return "NULL";
	case JsonKeyValueType::MIXED:
		return "MIXED";
	case JsonKeyValueType::UNKNOWN:
	default:
		return "UNKNOWN";
	}
}

JsonKeyValueType JsonKeyStats::StringToType(const string &str) {
	if (str == "VARCHAR") {
		return JsonKeyValueType::VARCHAR;
	} else if (str == "DOUBLE") {
		return JsonKeyValueType::DOUBLE;
	} else if (str == "BIGINT") {
		return JsonKeyValueType::BIGINT;
	} else if (str == "BOOLEAN") {
		return JsonKeyValueType::BOOLEAN;
	} else if (str == "NULL") {
		return JsonKeyValueType::JSON_NULL;
	} else if (str == "MIXED") {
		return JsonKeyValueType::MIXED;
	}
	return JsonKeyValueType::UNKNOWN;
}

static JsonKeyValueType GetJsonValueType(yyjson_val *val) {
	if (yyjson_is_str(val)) {
		return JsonKeyValueType::VARCHAR;
	} else if (yyjson_is_real(val)) {
		return JsonKeyValueType::DOUBLE;
	} else if (yyjson_is_sint(val) || yyjson_is_uint(val)) {
		return JsonKeyValueType::BIGINT;
	} else if (yyjson_is_bool(val)) {
		return JsonKeyValueType::BOOLEAN;
	} else if (yyjson_is_null(val)) {
		return JsonKeyValueType::JSON_NULL;
	} else {
		// Arrays, objects, etc. - we don't track stats for these
		return JsonKeyValueType::MIXED;
	}
}

static string GetJsonValueString(yyjson_val *val) {
	if (yyjson_is_str(val)) {
		return yyjson_get_str(val);
	} else if (yyjson_is_real(val)) {
		return std::to_string(yyjson_get_real(val));
	} else if (yyjson_is_sint(val)) {
		return std::to_string(yyjson_get_sint(val));
	} else if (yyjson_is_uint(val)) {
		return std::to_string(yyjson_get_uint(val));
	} else if (yyjson_is_bool(val)) {
		return yyjson_get_bool(val) ? "true" : "false";
	}
	return string();
}

static int CompareTypedValues(JsonKeyValueType type, const string &a, const string &b) {
	if (type == JsonKeyValueType::DOUBLE || type == JsonKeyValueType::BIGINT) {
		// Numeric comparison
		double val_a = std::stod(a);
		double val_b = std::stod(b);
		if (val_a < val_b) {
			return -1;
		}
		if (val_a > val_b) {
			return 1;
		}
		return 0;
	}
	// String/lexicographic comparison for VARCHAR and BOOLEAN
	if (a < b) {
		return -1;
	}
	if (a > b) {
		return 1;
	}
	return 0;
}

bool JsonKeyStats::HasStats() const {
	return value_count > 0 || null_count > 0;
}

void JsonKeyStats::Merge(const JsonKeyStats &other) {
	// Merge types
	if (type == JsonKeyValueType::UNKNOWN) {
		type = other.type;
	} else if (other.type != JsonKeyValueType::UNKNOWN && type != other.type) {
		// If one is NULL, adopt the other's type
		if (type == JsonKeyValueType::JSON_NULL) {
			type = other.type;
		} else if (other.type != JsonKeyValueType::JSON_NULL) {
			// Different non-null types - mark as mixed
			type = JsonKeyValueType::MIXED;
		}
	}

	// Merge min/max
	if (type != JsonKeyValueType::MIXED && type != JsonKeyValueType::JSON_NULL) {
		if (min_value.empty()) {
			min_value = other.min_value;
		} else if (!other.min_value.empty()) {
			if (CompareTypedValues(type, other.min_value, min_value) < 0) {
				min_value = other.min_value;
			}
		}

		if (max_value.empty()) {
			max_value = other.max_value;
		} else if (!other.max_value.empty()) {
			if (CompareTypedValues(type, other.max_value, max_value) > 0) {
				max_value = other.max_value;
			}
		}
	}

	// Merge counts
	null_count += other.null_count;
	value_count += other.value_count;
}

DuckLakeColumnJsonKeyStats::DuckLakeColumnJsonKeyStats() : DuckLakeColumnExtraStats() {
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnJsonKeyStats::Copy() const {
	auto result = make_uniq<DuckLakeColumnJsonKeyStats>();
	result->key_stats = key_stats;
	result->total_count = total_count;
	return result;
}

void DuckLakeColumnJsonKeyStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &json_stats = new_stats.Cast<DuckLakeColumnJsonKeyStats>();

	// Merge total count
	total_count += json_stats.total_count;

	// Merge key stats
	for (auto &entry : json_stats.key_stats) {
		auto it = key_stats.find(entry.first);
		if (it == key_stats.end()) {
			key_stats.insert(entry);
		} else {
			it->second.Merge(entry.second);
		}
	}
}

void DuckLakeColumnJsonKeyStats::UpdateNullStats() {
	total_count++;
	// When the entire JSON value is NULL, all keys get a null count increment
	// But we don't know which keys exist, so we track this via total_count
}

//! Helper to update stats for a single value at a given path
static void UpdateStatsForValue(unordered_map<string, JsonKeyStats> &key_stats, const string &path, yyjson_val *val) {
	JsonKeyValueType val_type = GetJsonValueType(val);
	auto &stats = key_stats[path];

	if (val_type == JsonKeyValueType::JSON_NULL) {
		stats.null_count++;
		if (stats.type == JsonKeyValueType::UNKNOWN) {
			stats.type = JsonKeyValueType::JSON_NULL;
		}
	} else if (val_type == JsonKeyValueType::MIXED) {
		// Complex value (array/object) - we'll recurse into it, but also mark this path as MIXED
		if (stats.type != JsonKeyValueType::UNKNOWN && stats.type != JsonKeyValueType::MIXED &&
		    stats.type != JsonKeyValueType::JSON_NULL) {
			stats.type = JsonKeyValueType::MIXED;
		} else if (stats.type == JsonKeyValueType::UNKNOWN) {
			stats.type = JsonKeyValueType::MIXED;
		}
		stats.value_count++;
	} else {
		// Scalar value
		string val_str = GetJsonValueString(val);

		if (stats.type == JsonKeyValueType::UNKNOWN || stats.type == JsonKeyValueType::JSON_NULL) {
			stats.type = val_type;
		} else if (stats.type != val_type && stats.type != JsonKeyValueType::MIXED) {
			// Type mismatch
			stats.type = JsonKeyValueType::MIXED;
		}

		if (stats.type != JsonKeyValueType::MIXED) {
			// Update min/max
			if (stats.min_value.empty() || CompareTypedValues(stats.type, val_str, stats.min_value) < 0) {
				stats.min_value = val_str;
			}
			if (stats.max_value.empty() || CompareTypedValues(stats.type, val_str, stats.max_value) > 0) {
				stats.max_value = val_str;
			}
		}

		stats.value_count++;
	}
}

//! Recursively traverse JSON and collect stats for all paths
static void TraverseJson(unordered_map<string, JsonKeyStats> &key_stats, const string &prefix, yyjson_val *val) {
	if (yyjson_is_obj(val)) {
		// Traverse object keys
		yyjson_obj_iter iter;
		yyjson_obj_iter_init(val, &iter);
		yyjson_val *key;
		while ((key = yyjson_obj_iter_next(&iter))) {
			const char *key_str = yyjson_get_str(key);
			if (!key_str) {
				continue;
			}

			// Build path: prefix.key or just key if prefix is empty
			string path = prefix.empty() ? string(key_str) : prefix + "." + key_str;

			yyjson_val *child_val = yyjson_obj_iter_get_val(key);

			// Update stats for this path
			UpdateStatsForValue(key_stats, path, child_val);

			// Recurse into nested objects (but not arrays - array elements don't have stable paths)
			if (yyjson_is_obj(child_val)) {
				TraverseJson(key_stats, path, child_val);
			}
		}
	}
}

void DuckLakeColumnJsonKeyStats::UpdateStats(const string &json_value) {
	total_count++;

	auto doc = yyjson_read(json_value.c_str(), json_value.size(), 0);
	if (!doc) {
		// Invalid JSON - treat as null
		return;
	}

	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		// Not an object - we only track keys of objects
		yyjson_doc_free(doc);
		return;
	}

	// Recursively traverse and collect stats for all paths
	TraverseJson(key_stats, "", root);

	yyjson_doc_free(doc);
}

const JsonKeyStats *DuckLakeColumnJsonKeyStats::GetKeyStats(const string &key) const {
	auto it = key_stats.find(key);
	if (it == key_stats.end()) {
		return nullptr;
	}
	return &it->second;
}

string DuckLakeColumnJsonKeyStats::Serialize() const {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Add json_key_stats object
	auto key_stats_obj = yyjson_mut_obj(doc);
	yyjson_mut_obj_add_val(doc, root, "json_key_stats", key_stats_obj);

	for (auto &entry : key_stats) {
		auto &key = entry.first;
		auto &stats = entry.second;

		auto stats_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_str(doc, stats_obj, "type", JsonKeyStats::TypeToString(stats.type));
		if (!stats.min_value.empty()) {
			yyjson_mut_obj_add_str(doc, stats_obj, "min", stats.min_value.c_str());
		}
		if (!stats.max_value.empty()) {
			yyjson_mut_obj_add_str(doc, stats_obj, "max", stats.max_value.c_str());
		}
		yyjson_mut_obj_add_uint(doc, stats_obj, "null_count", stats.null_count);
		yyjson_mut_obj_add_uint(doc, stats_obj, "value_count", stats.value_count);

		yyjson_mut_obj_add_val(doc, key_stats_obj, key.c_str(), stats_obj);
	}

	// Add total_count
	yyjson_mut_obj_add_uint(doc, root, "total_count", total_count);

	// Write to string
	auto json_str = yyjson_mut_write(doc, 0, nullptr);
	string result;
	if (json_str) {
		result = string("'") + json_str + "'";
		free(json_str);
	}

	yyjson_mut_doc_free(doc);
	return result;
}

void DuckLakeColumnJsonKeyStats::Deserialize(const string &stats) {
	auto doc = yyjson_read(stats.c_str(), stats.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse JSON key stats");
	}

	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid JSON key stats format");
	}

	// Read json_key_stats
	auto key_stats_json = yyjson_obj_get(root, "json_key_stats");
	if (yyjson_is_obj(key_stats_json)) {
		yyjson_obj_iter iter;
		yyjson_obj_iter_init(key_stats_json, &iter);
		yyjson_val *key;
		while ((key = yyjson_obj_iter_next(&iter))) {
			const char *key_str = yyjson_get_str(key);
			if (!key_str) {
				continue;
			}

			yyjson_val *stats_obj = yyjson_obj_iter_get_val(key);
			if (!yyjson_is_obj(stats_obj)) {
				continue;
			}

			JsonKeyStats ks;

			auto type_val = yyjson_obj_get(stats_obj, "type");
			if (yyjson_is_str(type_val)) {
				ks.type = JsonKeyStats::StringToType(yyjson_get_str(type_val));
			}

			auto min_val = yyjson_obj_get(stats_obj, "min");
			if (yyjson_is_str(min_val)) {
				ks.min_value = yyjson_get_str(min_val);
			}

			auto max_val = yyjson_obj_get(stats_obj, "max");
			if (yyjson_is_str(max_val)) {
				ks.max_value = yyjson_get_str(max_val);
			}

			auto null_count_val = yyjson_obj_get(stats_obj, "null_count");
			if (yyjson_is_uint(null_count_val)) {
				ks.null_count = yyjson_get_uint(null_count_val);
			}

			auto value_count_val = yyjson_obj_get(stats_obj, "value_count");
			if (yyjson_is_uint(value_count_val)) {
				ks.value_count = yyjson_get_uint(value_count_val);
			}

			key_stats[key_str] = std::move(ks);
		}
	}

	// Read total_count
	auto total_count_val = yyjson_obj_get(root, "total_count");
	if (yyjson_is_uint(total_count_val)) {
		total_count = yyjson_get_uint(total_count_val);
	}

	yyjson_doc_free(doc);
}

} // namespace duckdb
