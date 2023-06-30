//
// Created by Shiping Yao on 2023/6/6.
//

#include "duckdb/execution/operator/scan/physical_vector_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/vector/ivfflat.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
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
	                            const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>>& sel_expr)
	    : context(context), limit(limit), orders(orders), select_expressions(sel_expr) {

		D_ASSERT(table);
		D_ASSERT(orders.size() == 1);
		auto &duck_table = table->Cast<DuckTableEntry>();
		data_table = &duck_table.GetStorage();
	}
	ClientContext& context;
	idx_t limit;
	const vector<BoundOrderByNode>& orders;
	optional_ptr<DataTable> data_table;
	const vector<unique_ptr<Expression>>& select_expressions;


	mutex lock;
	bool scanned;

	void GetData() {
		std::unique_lock<std::mutex> lk(lock);
		D_ASSERT(data_table);
		for (auto &index : data_table->info->indexes.Indexes()) {
			if (index->type != IndexType::IVFFLAT) {
				continue;
			}
			auto &ivf = index->Cast<IvfflatIndex>();
			D_ASSERT(select_expressions.size() == 1);
			if (select_expressions[0]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
        auto& bound_func_expr = select_expressions[0]->Cast<BoundFunctionExpression>();
        D_ASSERT(bound_func_expr.children.size() == 2);
        D_ASSERT(bound_func_expr.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CAST);
		    auto& bound_cast_expr = bound_func_expr.children[1]->Cast<BoundCastExpression>();
			  ExpressionExecutor expr_executor(context, bound_cast_expr);
			  DataChunk chunk;
			  vector<LogicalType> return_types = {LogicalType::LIST(LogicalType::FLOAT)};
			  chunk.Initialize(context, return_types);
			  chunk.data[0];
			  // NOTE: 终于获取到参数了


        std::cout << "before expr executor: " << chunk.size() << std::endl;
			  expr_executor.Execute(chunk);
			  std::cout << "after expr executor: " << chunk.size() << std::endl;

        auto* qv = get_data_ptr(chunk);
        int64_t *I = new int64_t[10];
        float *D = new float[10];
        ivf.index->search(1, qv, limit, D, I);
        std::cout << "search in physical_vector_index search D[0] = " << D[0] << ", I[0] = " << I[0] << std::endl;
        std::cout << "search in physical_vector_index search D[1] = " << D[1] << ", I[1] = " << I[1] << std::endl;
		    // NOTE: 如何将结果输出到output中

			}
			// TODO: fetch params
		}
	}

private:
	float* get_data_ptr(DataChunk& input) {
		D_ASSERT(input.data.size() == 1);
    Vector& lists = input.data[0];
    D_ASSERT(lists.GetType() == LogicalType::LIST(LogicalType::FLOAT));
    auto real_data_vector = ListVector::GetEntry(lists);
    UnifiedVectorFormat real_data;
    real_data_vector.ToUnifiedFormat(input.size(), real_data);
	  return (float*) real_data.data;
	}
};

unique_ptr<GlobalSourceState> PhysicalVectorIndexScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VectorScanGlobalSourceState>(context, table, limit, orders, select_expressions);
};

void PhysicalVectorIndexScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	std::cout << "in the PhysicalVectorIndexScan GetData operator" << std::endl;
	auto &vector_scan_global_state = gstate.Cast<VectorScanGlobalSourceState>();
	vector_scan_global_state.GetData();
};

} // namespace duckdb
