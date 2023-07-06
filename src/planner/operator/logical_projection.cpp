#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <iostream>
namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
	for(auto &expr: expressions) {
		if (expr->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &bound_expr = expr->Cast<BoundFunctionExpression>();
			for(auto& child_expr: bound_expr.children) {
				std::cout << "logical_projection found child expr: " << child_expr->GetName() << std::endl;
			}
		}
//		std::cout << "projection expr: " << expr->GetName() << std::endl;
	}

}

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list,
                                     TableCatalogEntry* table_ptr)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)),
      table_index(table_index), table(table_ptr) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

void LogicalProjection::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteSerializableList<Expression>(expressions);
}

unique_ptr<LogicalOperator> LogicalProjection::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return make_uniq<LogicalProjection>(table_index, std::move(expressions));
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb
