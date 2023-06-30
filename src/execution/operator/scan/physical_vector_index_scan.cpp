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
	                            const vector<BoundOrderByNode> &orders)
	    : limit(limit), orders(orders) {

		D_ASSERT(table);
		D_ASSERT(orders.size() == 1);
		auto &duck_table = table->Cast<DuckTableEntry>();
		data_table = &duck_table.GetStorage();
	}
	idx_t limit;
	const vector<BoundOrderByNode>& orders;
	optional_ptr<DataTable> data_table;

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
			std::cout << "successfull find ivfflat index" << std::endl;
			std::cout << "has parameter: " << orders[0].expression->HasParameter() << std::endl;
			std::cout << "expression: " << orders[0].expression->GetName() << std::endl;
			if (orders[0].expression->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
				auto& cast_expression = orders[0].expression->Cast<BoundCastExpression>();
				if (cast_expression.child->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
					auto& bound_func_expr = cast_expression.child->Cast<BoundFunctionExpression>();
					std::cout << "bound func name: " << bound_func_expr.GetName() << std::endl;
					D_ASSERT(bound_func_expr.children.size() == 2);
					D_ASSERT(bound_func_expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_REF);
					auto& bound_ref_expr = bound_func_expr.children[0]->Cast<BoundReferenceExpression>();
					std::cout << "bound ref expr params: " << bound_ref_expr.HasParameter() << std::endl;
					std::cout << "bound ref expr scalar: " << bound_ref_expr.IsScalar() << std::endl;
					std::cout << "bound ref expr name: " << bound_ref_expr.GetName() << std::endl;
					std::cout << "bound ref expr aggregate: " << bound_ref_expr.IsAggregate() << std::endl;

					D_ASSERT(bound_func_expr.children[1]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT);
					auto& bound_const_expr = bound_func_expr.children[1]->Cast<BoundConstantExpression>();

					std::cout << "params :" << bound_const_expr.value.ToString() << std::endl;
					std::cout << "const expr name: " << bound_const_expr.GetName() << std::endl;
				}
			}
			// TODO: fetch params
		}
	}
};

unique_ptr<GlobalSourceState> PhysicalVectorIndexScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<VectorScanGlobalSourceState>(context, table, limit, orders);
};

void PhysicalVectorIndexScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	std::cout << "in the PhysicalVectorIndexScan GetData operator" << std::endl;
	auto &vector_scan_global_state = gstate.Cast<VectorScanGlobalSourceState>();
	vector_scan_global_state.GetData();
};

} // namespace duckdb
