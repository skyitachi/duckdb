//
// Created by Shiping Yao on 2023/6/6.
//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"

#ifndef DUCKDB_PHYSICAL_VECTOR_INDEX_SCAN_HPP
#define DUCKDB_PHYSICAL_VECTOR_INDEX_SCAN_HPP

namespace duckdb {

class PhysicalVectorIndexScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::VECTOR_INDEX_SCAN;

  PhysicalVectorIndexScan(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t limit, idx_t estimated_cardinality,
	                        optional_ptr<TableCatalogEntry> table):
	      PhysicalOperator(PhysicalOperatorType::VECTOR_INDEX_SCAN, std::move(types), estimated_cardinality),
	      orders(std::move(orders)), limit(limit), table(table) {};

  vector<BoundOrderByNode> orders;
  idx_t limit;

  optional_ptr<TableCatalogEntry> table;

public:
	string GetName() const override;
	string ParamsToString() const override;
	bool IsSource() const override {
		return true;
	}

	bool Equals(const PhysicalOperator &other) const override;

	// sink
public:
	bool IsSink() const override {
		return true;
	}
  SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                      DataChunk &input) const override;
};
} // namespace duckdb
#endif // DUCKDB_PHYSICAL_VECTOR_INDEX_SCAN_HPP
