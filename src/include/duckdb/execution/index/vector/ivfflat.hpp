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
  int dimension;

public:
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
	                                                           ExpressionType expressionType) override;

	bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	     vector<row_t> &result_ids) override;

	PreservedError Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) override;
  //! Insert a chunk of entries into the index
  PreservedError Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) override;

  //! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
  //! index must also be locked during the merge
  bool MergeIndexes(IndexLock &state, Index &other_index) override;
  string ToString() override;
};
}
#endif // DUCKDB_IVFFLAT_H
