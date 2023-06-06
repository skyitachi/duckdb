//
// Created by Shiping Yao on 2023/6/6.
//

#include "duckdb/execution/operator/scan/physical_vector_index_scan.hpp"

namespace duckdb {

string PhysicalVectorIndexScan::GetName() const {
	return "vector_index_scan";
}

string PhysicalVectorIndexScan::ParamsToString() const {
	return "not implement";
}
bool PhysicalVectorIndexScan::Equals(const PhysicalOperator &other) const {
  return PhysicalOperator::Equals(other);
}

}
