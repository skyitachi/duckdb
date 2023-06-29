//
// Created by Shiping Yao on 2023/6/6.
//

#include "duckdb/execution/operator/scan/physical_vector_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/vector/ivfflat.hpp"

#include <iostream>
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

class VectorScanGlobalSinkState : public GlobalSinkState {
public:
  VectorScanGlobalSinkState(ClientContext &context, optional_ptr<TableCatalogEntry> table) {

    D_ASSERT(table);
    auto &duck_table = table->Cast<DuckTableEntry>();
    data_table = &duck_table.GetStorage();
  }
  optional_ptr<DataTable> data_table;
  mutex lock;
  bool scanned;

  void Sink(DataChunk& input) {
    std::unique_lock<std::mutex> lk(lock);
    D_ASSERT(data_table);
	  for(auto& index: data_table->info->indexes.Indexes()) {
		  if (index->type != IndexType::IVFFLAT) {
			  continue;
		  }
      auto &ivf = index->Cast<IvfflatIndex>();
	    std::cout << "successfull find ivfflat index" << std::endl;
	  }

  }

};


SinkResultType PhysicalVectorIndexScan::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                    DataChunk &input) const {
  std::cout << "in the physical vector index scan " << std::endl;
  D_ASSERT(table);
  auto& index_scan_global_state = state.Cast<VectorScanGlobalSinkState>();
  index_scan_global_state.Sink(input);
  return SinkResultType::FINISHED;
};

}
