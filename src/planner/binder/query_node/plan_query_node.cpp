#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

#include <iostream>
namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = (BoundDistinctModifier &)*mod;
			auto distinct = make_uniq<LogicalDistinct>(std::move(bound.target_distincts), bound.distinct_type);
			distinct->AddChild(std::move(root));
			root = std::move(distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &bound = (BoundOrderModifier &)*mod;
			if (root->type == LogicalOperatorType::LOGICAL_DISTINCT) {
				auto &distinct = root->Cast<LogicalDistinct>();
				if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
					auto order_by = make_uniq<BoundOrderModifier>();
					for (auto &order_node : bound.orders) {
						order_by->orders.push_back(order_node.Copy());
					}
					distinct.order_by = std::move(order_by);
				}
			}
			auto &select_node = node.Cast<BoundSelectNode>();
      bool found_vector_scan = false;
	    TableCatalogEntry* table_ptr = nullptr;
			if (select_node.from_table->type == TableReferenceType::BASE_TABLE) {
        auto &table_ref = select_node.from_table->Cast<BoundBaseTableRef>();
		    table_ptr = &table_ref.table;
        for (auto &order_node : bound.orders) {
          auto expression_name = order_node.expression->GetName();
          auto fn_idx = expression_name.find("list_distance");
          if (fn_idx != -1) {
            found_vector_scan = true;
            break;
          }
        }
        if (root->type == LogicalOperatorType::LOGICAL_PROJECTION) {
          auto& proj = root->Cast<LogicalProjection>();
          proj.is_vector_scan = found_vector_scan;
        }

			}

			auto order = make_uniq<LogicalOrder>(std::move(bound.orders), table_ptr);
			order->is_vector_scan = found_vector_scan;

			// IMPORTANT: need copy projection expressions
			for (auto &expression : root->expressions) {
				order->select_expressions.emplace_back(expression->Copy());
			}

			order->AddChild(std::move(root));
			root = std::move(order);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &bound = (BoundLimitModifier &)*mod;
			auto limit = make_uniq<LogicalLimit>(bound.limit_val, bound.offset_val, std::move(bound.limit),
			                                     std::move(bound.offset));
			if (root->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
				auto &order = root->Cast<LogicalOrder>();
				order.limit = limit->limit_val;
			}
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &bound = (BoundLimitPercentModifier &)*mod;
			auto limit = make_uniq<LogicalLimitPercent>(bound.limit_percent, bound.offset_val, std::move(bound.limit),
			                                            std::move(bound.offset));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		default:
			throw BinderException("Unimplemented modifier type!");
		}
	}
	return root;
}

} // namespace duckdb
