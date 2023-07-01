//
// Created by Shiping Yao on 2023/6/6.
//

#include "duckdb/execution/operator/scan/physical_vector_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/vector/ivfflat.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

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

class VectorScanGlobalSourceState : public GlobalSourceState {
public:
	VectorScanGlobalSourceState(ClientContext &context, optional_ptr<TableCatalogEntry> table, idx_t limit,
	                            const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &sel_expr)
	    : context(context), limit(limit), orders(orders), select_expressions(sel_expr) {

		D_ASSERT(table);
		D_ASSERT(orders.size() == 1);
		auto &duck_table = table->Cast<DuckTableEntry>();
		data_table = &duck_table.GetStorage();
		scanned = false;
	}
	ClientContext &context;
	idx_t limit;
	const vector<BoundOrderByNode> &orders;
	optional_ptr<DataTable> data_table;
	const vector<unique_ptr<Expression>> &select_expressions;

	mutex lock;
	bool scanned;

	void GetData(DataChunk &result) {
		std::unique_lock<std::mutex> lk(lock);
		D_ASSERT(data_table);
		D_ASSERT(result.data.size() == 1);
		for (auto &index : data_table->info->indexes.Indexes()) {
			if (index->type != IndexType::IVFFLAT) {
				continue;
			}
			auto &ivf = index->Cast<IvfflatIndex>();
			D_ASSERT(select_expressions.size() == 1);
			if (select_expressions[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
				auto &bound_func_expr = select_expressions[0]->Cast<BoundFunctionExpression>();
				D_ASSERT(bound_func_expr.children.size() == 2);
				D_ASSERT(bound_func_expr.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CAST);
				auto &bound_cast_expr = bound_func_expr.children[1]->Cast<BoundCastExpression>();
				ExpressionExecutor expr_executor(context, bound_cast_expr);
				DataChunk chunk;
				vector<LogicalType> return_types = {LogicalType::LIST(LogicalType::FLOAT)};
				chunk.Initialize(context, return_types);
				// NOTE: 终于获取到参数了
				expr_executor.Execute(chunk);

				auto *qv = get_data_ptr(chunk);
				int64_t *I = new int64_t[10];
				float *D = new float[10];
				ivf.index->search(1, qv, limit, D, I);

				auto result_vector = result.data[0];
				idx_t i = 0;
				for(; i < limit; i++) {
					if (I[i] == -1) {
						break;
					}
					result_vector.SetValue(i, Value::FLOAT(D[i]));
				}
				delete[] I;
				delete[] D;
				result.SetCardinality(i);
			}
		}
		scanned = true;
	}

  bool Ended() {
		std::unique_lock<mutex> lk(lock);
		return scanned;
  }

private:
	float *get_data_ptr(DataChunk &input) {
		D_ASSERT(input.data.size() == 1);
		Vector &lists = input.data[0];
		D_ASSERT(lists.GetType() == LogicalType::LIST(LogicalType::FLOAT));
		auto real_data_vector = ListVector::GetEntry(lists);
		UnifiedVectorFormat real_data;
		real_data_vector.ToUnifiedFormat(input.size(), real_data);
		return (float *)real_data.data;
	}
};

unique_ptr<GlobalSourceState> PhysicalVectorIndexScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VectorScanGlobalSourceState>(context, table, limit, orders, select_expressions);
};

void PhysicalVectorIndexScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	auto &vector_scan_global_state = gstate.Cast<VectorScanGlobalSourceState>();
	if (!vector_scan_global_state.Ended()) {
    vector_scan_global_state.GetData(chunk);
	}
	// 如何判断已经GetData完毕
};

} // namespace duckdb
