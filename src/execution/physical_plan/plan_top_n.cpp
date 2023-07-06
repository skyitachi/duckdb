#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/operator/scan/physical_vector_index_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	D_ASSERT(op.orders.size() > 0);
//	auto expression_name = op.orders[0].expression->GetName();
//	auto fn_idx = expression_name.find("list_distance");
//	if (fn_idx != -1) {
//		  auto vector_index_scan = make_uniq<PhysicalVectorIndexScan>(
//		      op.types, std::move(op.orders), (idx_t)op.limit, op.estimated_cardinality, op.table);
//		  // NOTE: no need to push back plan
//		  // 如果这里作为source的情况下，那么physicalprojection 中绑定函数的相关操作需要自己实现
////		  vector_index_scan->children.push_back(std::move(plan));
//		  vector_index_scan->select_expressions = std::move(op.select_expressions);
//		  return std::move(vector_index_scan);
//	}
	auto top_n =
	    make_uniq<PhysicalTopN>(op.types, std::move(op.orders), (idx_t)op.limit, op.offset, op.estimated_cardinality);
	top_n->children.push_back(std::move(plan));
	return std::move(top_n);
}

} // namespace duckdb
