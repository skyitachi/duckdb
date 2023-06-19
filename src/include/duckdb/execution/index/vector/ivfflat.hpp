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
	IvfflatIndex(AttachedDatabase &db, TableIOManager &tableIoManager,
	             const vector<column_t> &columnIds, const vector<unique_ptr<Expression>> &unboundExpressions,
	             IndexConstraintType constraintType, bool trackMemory, int dimension, int nlists, OpClassType opclz,
	             faiss::IndexFlatL2* quantizer_ptr = nullptr);
  faiss::IndexIVFFlat* index;
  int dimension;
  int nlist;
  faiss::MetricType metric_type;
  faiss::IndexFlatL2* quantizer;
  bool created;

  void initialize(faiss::IndexFlatL2* quantizer_ptr);
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

  unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, const Value &low_value,
	                                                     ExpressionType low_expression_type,
	                                                     const Value &high_value,
	                                                     ExpressionType high_expression_type) override;


  void VerifyAppend(DataChunk &chunk) override;
  //! Verify that data can be appended to the index without a constraint violation using the conflict manager
  void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;

  void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;

  void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) override;
  //! Obtains a lock and calls Delete while holding that lock
  void Delete(DataChunk &entries, Vector &row_identifiers) override;

  void Verify() override;

  void IncreaseAndVerifyMemorySize(idx_t old_memory_size) override;


  BlockPointer Serialize(MetaBlockWriter &writer) override;
};
}
#endif // DUCKDB_IVFFLAT_H
