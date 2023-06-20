#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <faiss/IndexFlat.h>
#include <thread>

namespace duckdb {

PhysicalCreateIndex::PhysicalCreateIndex(LogicalOperator &op, TableCatalogEntry &table_p,
                                         const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                         vector<unique_ptr<Expression>> unbound_expressions,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)),
      unbound_expressions(std::move(unbound_expressions)) {
	D_ASSERT(table_p.IsDuckTable());
	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<Index> global_index;
	unique_ptr<faiss::IndexFlatL2> global_quantizer;
};

class CreateIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<Index> local_index;
	ArenaAllocator arena_allocator;
	vector<Key> keys;
	DataChunk key_chunk;
	vector<column_t> key_column_ids;
};

unique_ptr<GlobalSinkState> PhysicalCreateIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateIndexGlobalSinkState>();

	// create the global index
	switch (info->index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		// NOTE: 需要索引列，table信息
		state->global_index = make_uniq<ART>(storage_ids, TableIOManager::Get(storage), unbound_expressions,
		                                     info->constraint_type, storage.db, true);
		break;
	}
	case IndexType::IVFFLAT: {
		auto &storage = table.GetStorage();
		int d = info->options["d"];
		int nlists = info->options["oplists"];
		OpClassType opclz = OpClassType::Vector_IP_OPS;
		//		for (auto &expr : info->expressions) {
		//			//      std::cout << "parsed expr: " << expr->ToString() << std::endl;
		//			opclz = expr->opclass_type;
		//		}
		state->global_quantizer = make_uniq<faiss::IndexFlatL2>(d);
		state->global_index =
		    make_uniq<IvfflatIndex>(storage.db, TableIOManager::Get(storage), storage_ids, unbound_expressions,
		                            info->constraint_type, true, d, nlists, opclz, state->global_quantizer.get());
		//
		//		auto &storage = table.GetStorage();
		//		state->global_index = make_uniq<IvfflatIndex>(storage_ids, TableIOManager::Get(storage),
		// unbound_expressions, info->constraint_type, storage.db, true);
		std::cout << "create global ivfflat index: dimension: " << d << ", nlists: " << nlists
		          << ", opclass_type: " << int(opclz) << std::endl;
		auto &ivf = state->global_index->Cast<IvfflatIndex>();
		printf("state global index %p \n", ivf.index);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	return (std::move(state));
}

// TODO: 这里是如何保证是thread local state的
unique_ptr<LocalSinkState> PhysicalCreateIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateIndexLocalSinkState>(context.client);

	// create the local index
	switch (info->index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		state->local_index = make_uniq<ART>(storage_ids, TableIOManager::Get(storage), unbound_expressions,
		                                    info->constraint_type, storage.db, false);
		state->keys = vector<Key>(STANDARD_VECTOR_SIZE);
		state->key_chunk.Initialize(Allocator::Get(context.client), state->local_index->logical_types);

		for (idx_t i = 0; i < state->key_chunk.ColumnCount(); i++) {
			state->key_column_ids.push_back(i);
		}
		break;
	}
	case IndexType::IVFFLAT: {
		std::cout << "GetLocalLinkSinkState need to create IVFFLAT index" << std::endl;
		auto &storage = table.GetStorage();
		int d = info->options["d"];
		int nlists = info->options["oplists"];
		OpClassType opclz;
		for (auto &expr : info->expressions) {
			//		  std::cout << "parsed expr: " << expr->ToString() << std::endl;
			//			opclz = expr->opclass_type;
			opclz = OpClassType::Vector_IP_OPS;
		}
		std::cout << "local index: dimension: " << d << ", nlists: " << nlists << ", opclass_type: " << int(opclz)
		          << std::endl;
		state->local_index =
		    make_uniq<IvfflatIndex>(storage.db, TableIOManager::Get(storage), storage_ids, unbound_expressions,
		                            info->constraint_type, true, d, nlists, opclz);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}
	return std::move(state);
}

SinkResultType PhysicalCreateIndex::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                         DataChunk &input) const {

	D_ASSERT(input.ColumnCount() >= 2);
	auto &lstate = lstate_p.Cast<CreateIndexLocalSinkState>();
	auto &row_identifiers = input.data[input.ColumnCount() - 1];

	auto &gstate = gstate_p.Cast<CreateIndexGlobalSinkState>();

	// generate the keys for the given input
	lstate.key_chunk.ReferenceColumns(input, lstate.key_column_ids);
	lstate.arena_allocator.Reset();

	auto &storage = table.GetStorage();
	unique_ptr<Index> idx;
	if (lstate.local_index->type == IndexType::ART) {
		ART::GenerateKeys(lstate.arena_allocator, lstate.key_chunk, lstate.keys);
		auto art = make_uniq<ART>(lstate.local_index->column_ids, lstate.local_index->table_io_manager,
		                          lstate.local_index->unbound_expressions, lstate.local_index->constraint_type,
		                          storage.db, false);

		if (!art->ConstructFromSorted(lstate.key_chunk.size(), lstate.keys, row_identifiers)) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}

		idx = std::move(art);
	} else if (lstate.local_index->type == IndexType::IVFFLAT) {
		auto &ivfflat = lstate.local_index->Cast<IvfflatIndex>();
		int d = info->options["d"];
		int nlists = info->options["oplists"];
		OpClassType opclz;
		for (auto &expr : info->expressions) {
			//			std::cout << "parsed expr: " << expr->ToString() << std::endl;
			//			opclz = expr->opclass_type;
			opclz = OpClassType::Vector_IP_OPS;
		}
		std::cout << "global sink: dimension: " << d << ", nlists: " << nlists << ", opclass_type: " << int(opclz)
		          << std::endl;
		idx = make_uniq<IvfflatIndex>(storage.db, lstate.local_index->table_io_manager, lstate.local_index->column_ids,
		                              lstate.local_index->unbound_expressions, lstate.local_index->constraint_type,
		                              false, d, nlists, opclz, gstate.global_quantizer.get());
		idx->Append(input, row_identifiers);
		ivfflat.initialize(gstate.global_quantizer.get());
	}

	if (!lstate.local_index) {
		std::cout << "local index is nil" << std::endl;
	} else {
		// merge into the local ART
		if (!lstate.local_index->MergeIndexes(*idx)) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalCreateIndex::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                  LocalSinkState &lstate_p) const {

	auto &gstate = gstate_p.Cast<CreateIndexGlobalSinkState>();
	auto &lstate = lstate_p.Cast<CreateIndexLocalSinkState>();

	std::cout << "in the combine index " << std::endl;

	// merge the local index into the global index
	if (!gstate.global_index->MergeIndexes(*lstate.local_index)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {

	// here, we just set the resulting global index as the newly created index of the table

	auto &state = gstate_p.Cast<CreateIndexGlobalSinkState>();
	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	state.global_index->Verify();
	if (state.global_index->track_memory) {
		state.global_index->buffer_manager.IncreaseUsedMemory(state.global_index->memory_size);
	}

	auto &schema = *table.schema;
	auto index_entry = (DuckIndexEntry *)schema.CreateIndex(context, info.get(), &table);
	if (!index_entry) {
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}

	index_entry->index = state.global_index.get();
	index_entry->info = storage.info;
	for (auto &parsed_expr : info->parsed_expressions) {
		index_entry->parsed_expressions.push_back(parsed_expr->Copy());
	}

	// NOTE: 这里都是可以成功完成搜索的
	storage.info->indexes.AddIndex(std::move(state.global_index));
	std::thread th([&]{
    for (auto &idx : storage.info->indexes.Indexes()) {
      //		auto &index = *idx;
      //		auto &ivf = index.Cast<IvfflatIndex>();
      auto &ivf = idx->Cast<IvfflatIndex>();
      int k = 1;
      int64_t *I = new int64_t[10];
      float *D = new float[10];
      float *xq = new float[3];
      xq[0] = 0;
      xq[1] = 0.1;
      xq[2] = 0;

      ivf.index->search(1, xq, k, D, I);
	    printf("index pointer in physical_create_index: %p\n", ivf.index);
      std::cout << "search in finalize ok with new thread: " << std::this_thread::get_id() << std::endl;
    }
	});
	th.join();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

void PhysicalCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	// NOP
}

} // namespace duckdb
