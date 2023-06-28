#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTopN &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	// TODO: 这里可以改成想要的自定义的physical operator
	for(auto &order_node: op.orders) {
		  std::cout << "order_node: " << order_node.ToString() << std::endl;
		  std::cout << "order expression aggregate: " << order_node.expression->IsAggregate() << std::endl;
		  std::cout << "expression scalar: " << order_node.expression->IsScalar()  << std::endl;
		  std::cout << "expression name: " << order_node.expression->GetName() << std::endl;
		  std::cout << "expression type: " << ExpressionTypeToString(order_node.expression->GetExpressionType()) << std::endl;
		  std::cout << "expression class: " << ExpressionClassToString(order_node.expression->GetExpressionClass()) << std::endl;
	}

	auto expression_name = op.orders[0].expression->GetName();



	auto top_n =
	    make_uniq<PhysicalTopN>(op.types, std::move(op.orders), (idx_t)op.limit, op.offset, op.estimated_cardinality);
	top_n->children.push_back(std::move(plan));
	return std::move(top_n);
}

} // namespace duckdb
