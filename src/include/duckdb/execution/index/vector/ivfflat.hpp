//
// Created by Shiping Yao on 2023/6/11.
//

#ifndef DUCKDB_IVFFLAT_H
#define DUCKDB_IVFFLAT_H
#pragma once

#include "duckdb/storage/index.hpp"
#include <faiss/index_io.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>

namespace duckdb {
class IvfflatIndex: public Index {
public:
	IvfflatIndex(AttachedDatabase &db, IndexType type, TableIOManager &tableIoManager,
	             const vector<column_t> &columnIds, const vector<unique_ptr<Expression>> &unboundExpressions,
	             IndexConstraintType constraintType, bool trackMemory, int dimension, int nlists, OpClassType opclz);
	unique_ptr<faiss::IndexFlatL2> quantizer;
  unique_ptr<faiss::IndexIVFFlat> index;
};
}
#endif // DUCKDB_IVFFLAT_H
