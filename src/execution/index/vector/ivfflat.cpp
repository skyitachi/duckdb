//
// Created by Shiping Yao on 2023/6/11.
//

#include <memory>

#include "fmt/format.h"
#include "duckdb/execution/index/vector/ivfflat.hpp"
#include "duckdb/storage/table/scan_state.hpp"


#include <faiss/MetricType.h>
#include <faiss/IndexShardsIVF.h>

#include <iostream>
namespace duckdb {


struct IvfflatIndexScanState : public IndexScanState {

	//! Scan predicates (single predicate scan or range scan)
	Value values[2];
	//! Expressions of the scan predicates
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs
	vector<row_t> result_ids;
};



static faiss::MetricType opToMetricType(OpClassType opclass) {
	if (opclass == OpClassType::Vector_IP_OPS) {
		return faiss::METRIC_INNER_PRODUCT;
	} else if (opclass == OpClassType::Vector_L2_OPS) {
		return faiss::METRIC_L2;
	}
	return faiss::METRIC_INNER_PRODUCT;
}

IvfflatIndex::IvfflatIndex(AttachedDatabase &db, TableIOManager &tableIoManager,
                           const vector<column_t> &columnIds, const vector<unique_ptr<Expression>> &unboundExpressions,
                           IndexConstraintType constraintType, bool trackMemory, int dim, int nlists,
                           OpClassType opclz, faiss::IndexFlatL2 *quantizer_ptr)
    : Index(db, IndexType::IVFFLAT,tableIoManager,columnIds,unboundExpressions,constraintType,trackMemory) {
	dimension = dim;
	// quantizer 需要全局的，这样才可以merge
	nlist = nlists;
	metric_type = opToMetricType(opclz);
	quantizer = nullptr;
	if (quantizer_ptr != nullptr) {
    std::cout << "create real IndexIVFFlat index initial dimension :" << dimension << std::endl;

    index = new faiss::IndexIVFFlat(quantizer_ptr, dimension, nlists, metric_type);
	  created = true;
	  quantizer = quantizer_ptr;
	}
}

void IvfflatIndex::initialize(faiss::IndexFlatL2 *quantizer_ptr) {
	if (quantizer_ptr != nullptr) {
    index = new faiss::IndexIVFFlat(quantizer_ptr, dimension, nlist, metric_type);
    created = true;
	  quantizer = quantizer_ptr;
	}
}


unique_ptr<IndexScanState> IvfflatIndex::InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                         ExpressionType expression_type) {
  auto result = make_uniq<IvfflatIndexScanState>();
  result->values[0] = value;
  result->expressions[0] = expression_type;
  return std::move(result);
}

bool IvfflatIndex::Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
          vector<row_t> &result_ids) {
	return false;
}

PreservedError IvfflatIndex::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	// entries 是全部的column??
    DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(appended_data, expression_result);
	return Insert(lock, expression_result, row_identifiers);
}

PreservedError IvfflatIndex::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	auto old_memory_size = memory_size;
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	int v_size = 4;
	// TODO: support Physical::FLOAT and Physical::DOUBLE
	//　如何回收内存
	auto vector_data_ptr = reinterpret_cast<float *>(arena_allocator.AllocateAligned(input.size() * dimension * v_size));

	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);


	// TODO: need unwrap list data
	UnifiedVectorFormat input_data;
	input.data[0].ToUnifiedFormat(input.size(), input_data);
	auto list_entries = (list_entry_t*)input_data.data;

	auto real_data_vector = ListVector::GetEntry(input.data[0]);
	UnifiedVectorFormat real_data;
	real_data_vector.ToUnifiedFormat(input.size(), real_data);


	int values_count = 0;
	for(idx_t i = 0; i < input.size(); i++) {
		// NOTE: 这里为什么一定需要sel
		auto list_index = input_data.sel->get_index(i);
    auto entry = list_entries[list_index];
	  auto data_ptr = (float*)real_data.data;
	  for (int j = 0; j < entry.length; j++) {
		  auto offset = entry.offset + j;
		  auto index = real_data.sel->get_index(offset);
		  std::cout << "data_index: " << index << std::endl;
		  memcpy(vector_data_ptr + values_count, data_ptr + index, v_size);
		  values_count += 1;
	  }
	}
	using faiss_idx_t = int64_t;
	// TODO: failed here
	for (idx_t i = 0; i < input.size(); i++) {
	  int c = values_count / input.size();
	  for (int j = 0; j < c; j++) {
		  std::cout << " " << vector_data_ptr[c * i + j];
	  }
	  std::cout << std::endl;
	}
	index->train(input.size(), vector_data_ptr);
	index->add_with_ids(input.size(), vector_data_ptr, (faiss_idx_t*)row_identifiers);
//  std::cout << "create real ivfflat index here trained" <<  <<  std::endl;
//	arena_allocator.AllocateAligned()
  return PreservedError();
}

bool IvfflatIndex::MergeIndexes(IndexLock &state, Index &other_index) {
  // NOTE: try_to use merge_from
  auto& other = other_index.Cast<IvfflatIndex>();
  D_ASSERT(quantizer == other.quantizer);
  std::cout << "quantizer total: " << quantizer->ntotal << " other: " << other.quantizer->ntotal << std::endl;
  index->merge_from(*other.index, 0);

  return true;
}

string IvfflatIndex::ToString() {
  return duckdb_fmt::format("ivfflat(dimension={:d})", dimension);
}

unique_ptr<IndexScanState> IvfflatIndex::InitializeScanTwoPredicates(Transaction &transaction, const Value &low_value,
                                                       ExpressionType low_expression_type,
                                                       const Value &high_value,
                                                       ExpressionType high_expression_type) {
  return nullptr;
}

void IvfflatIndex::VerifyAppend(DataChunk &chunk) {

}
//! Verify that data can be appended to the index without a constraint violation using the conflict manager
void IvfflatIndex::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {

}

void IvfflatIndex::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {

}

void IvfflatIndex::Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) {
  // TODO
//  index->remove_ids();

}

//! Obtains a lock and calls Delete while holding that lock
void IvfflatIndex::Delete(DataChunk &entries, Vector &row_identifiers) {
    //  index->remove_ids();

}

void IvfflatIndex::Verify() {

}

void IvfflatIndex::IncreaseAndVerifyMemorySize(idx_t old_memory_size) {

}

BlockPointer IvfflatIndex::Serialize(MetaBlockWriter &writer) {
  return writer.GetBlockPointer();

}
}

