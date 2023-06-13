//
// Created by Shiping Yao on 2023/6/11.
//

#include "duckdb/execution/index/vector/ivfflat.hpp"
#include <memory>

#include <faiss/MetricType.h>

namespace duckdb {

static faiss::MetricType opToMetricType(OpClassType opclass) {
	if (opclass == OpClassType::Vector_IP_OPS) {
		return faiss::METRIC_INNER_PRODUCT;
	} else if (opclass == OpClassType::Vector_L2_OPS) {
		return faiss::METRIC_L2;
	}
}

IvfflatIndex::IvfflatIndex(AttachedDatabase &db, IndexType type, TableIOManager &tableIoManager,
                           const vector<column_t> &columnIds, const vector<unique_ptr<Expression>> &unboundExpressions,
                           IndexConstraintType constraintType, bool trackMemory, int dimension, int nlists,
                           OpClassType opclz)
    : Index(db,type,tableIoManager,columnIds,unboundExpressions,constraintType,trackMemory) {
	quantizer = make_uniq<faiss::IndexFlatL2>(dimension);
	auto metric_type = opToMetricType(opclz);
	index = make_uniq<faiss::IndexIVFFlat>(quantizer.get(), dimension, nlists, metric_type);

}


}

