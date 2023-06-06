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

class PhysicalVectorIndexScan: public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::VECTOR_INDEX_SCAN;

public:
  string GetName() const override;
  string ParamsToString() const override;

  bool Equals(const PhysicalOperator& other) const override;
};
}
#endif // DUCKDB_PHYSICAL_VECTOR_INDEX_SCAN_HPP
