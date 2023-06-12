//
// Created by Shiping Yao on 2023/6/11.
//

#include "duckdb/execution/index/vector/ivfflat.hpp"
#include <memory>

#include <faiss/MetricType.h>

namespace duckdb {

static faiss::MetricType opToMetricType(string opclass) {
	if (opclass == "vector_ip_ops") {
		return faiss::METRIC_INNER_PRODUCT;
	} else if (opclass == "vector_l2_ops") {
		return faiss::METRIC_L2;
	}
}

IvfflatIndex::IvfflatIndex(AttachedDatabase &db, IndexType type, TableIOManager &tableIoManager,
                           const vector<column_t> &columnIds, const vector<unique_ptr<Expression>> &unboundExpressions,
                           IndexConstraintType constraintType, bool trackMemory, int dimension, int nlists,
                           std::string opclz)
    : Index(db,type,tableIoManager,columnIds,unboundExpressions,constraintType,trackMemory) {
	quantizer = make_uniq<faiss::IndexFlatL2>(dimension);
	index = make_uniq<faiss::IndexIVFFlat>(quantizer.get(), dimension, nlists);
}


}

