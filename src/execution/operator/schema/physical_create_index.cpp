#include "duckdb/execution/operator/schema/physical_create_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"


#include <faiss/IndexFlat.h>
#include <thread>
#include <mutex>

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
	create_ivfflat_index();
}

void PhysicalCreateIndex::create_ivfflat_index() {
	if (info->index_type == IndexType::IVFFLAT) {
		int d = info->options["d"];
		int nlists = info->options["oplists"];
    OpClassType opclz = OpClassType::Vector_IP_OPS;
		auto& storage = table.GetStorage();
	  faiss::IndexFlatL2* quantizer = new faiss::IndexFlatL2(d);
    g_index = make_uniq<IvfflatIndex>(storage.db, TableIOManager::Get(storage), storage_ids, unbound_expressions,
                            info->constraint_type, true, d, nlists, opclz, quantizer);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<Index> global_index;
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
		D_ASSERT(g_index && g_index->type == IndexType::IVFFLAT);
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
		D_ASSERT(g_index && g_index->type == IndexType::IVFFLAT);
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

	if (!lstate.local_index && g_index && g_index->type == IndexType::IVFFLAT) {
    // 使用这种方式加锁
    IndexLock lock_state;
    g_index->InitializeLock(lock_state);
    g_index->Append(lock_state, input, row_identifiers);
	} else if (lstate.local_index->type == IndexType::ART) {
		ART::GenerateKeys(lstate.arena_allocator, lstate.key_chunk, lstate.keys);
		auto art = make_uniq<ART>(lstate.local_index->column_ids, lstate.local_index->table_io_manager,
		                          lstate.local_index->unbound_expressions, lstate.local_index->constraint_type,
		                          storage.db, false);

		if (!art->ConstructFromSorted(lstate.key_chunk.size(), lstate.keys, row_identifiers)) {
			throw ConstraintException("Data contains duplicates on indexed column(s)");
		}

		idx = std::move(art);
	}

	if (lstate.local_index != nullptr && lstate.local_index->type != IndexType::IVFFLAT) {
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

	if (gstate.global_index == nullptr) {
		return;
	}
	// merge the local index into the global index
	if (!gstate.global_index->MergeIndexes(*lstate.local_index)) {
		throw ConstraintException("Data contains duplicates on indexed column(s)");
	}
}

SinkFinalizeType PhysicalCreateIndex::finalize_ivfflat_index(ClientContext &context) const {
  D_ASSERT(g_index && g_index->type == IndexType::IVFFLAT);
  g_index->Verify();

  if (g_index->track_memory) {
		g_index->buffer_manager.IncreaseUsedMemory(g_index->memory_size);
  }

  auto &schema = *table.schema;
  auto index_entry = (DuckIndexEntry *)schema.CreateIndex(context, info.get(), &table);
  if (!index_entry) {
    // index already exists, but error ignored because of IF NOT EXISTS
    return SinkFinalizeType::READY;
  }

  auto &storage = table.GetStorage();
  index_entry->index = g_index.get();
  index_entry->info = storage.info;
  for (auto &parsed_expr : info->parsed_expressions) {
    index_entry->parsed_expressions.push_back(parsed_expr->Copy());
  }

  // 已经finalize了如何感知
  unique_ptr<Index> p_index;
  p_index.reset(g_index.get());
  // NOTE: 这里都是可以成功完成搜索的
  storage.info->indexes.AddIndex(std::move(p_index));
  return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalCreateIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               GlobalSinkState &gstate_p) const {

	// here, we just set the resulting global index as the newly created index of the table

	auto &state = gstate_p.Cast<CreateIndexGlobalSinkState>();
	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	if (g_index && g_index->type == IndexType::IVFFLAT) {
		auto result = finalize_ivfflat_index(context);
		return static_cast<SinkFinalizeType>(result);
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
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

void PhysicalCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                  LocalSourceState &lstate) const {
	// NOP
}

PhysicalCreateIndex::~PhysicalCreateIndex() noexcept {
	if (g_index) {
		g_index.release();
	}
}

} // namespace duckdb
