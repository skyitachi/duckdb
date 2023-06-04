//
// Created by skyitachi on 23-6-3.
//
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

#include <iostream>

#include <numeric>
namespace duckdb {

static void ListDistanceFunction(DataChunk& args, ExpressionState& state, Vector& result) {
    D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	std::cout << "rows: " << count << std::endl;

	Vector& lhs = args.data[0];
	Vector& rhs = args.data[1];

	UnifiedVectorFormat lhs_data;
	UnifiedVectorFormat rhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);
	rhs.ToUnifiedFormat(count, rhs_data);
	auto lhs_entries = (list_entry_t *)lhs_data.data;
	auto rhs_entries = (list_entry_t *)rhs_data.data;

	// 相当于rows
	auto lhs_list_size = ListVector::GetListSize(lhs);
	auto rhs_list_size = ListVector::GetListSize(rhs);

	auto &lhs_child = ListVector::GetEntry(lhs);
	auto &rhs_child = ListVector::GetEntry(rhs);
	UnifiedVectorFormat lhs_child_data;
	UnifiedVectorFormat rhs_child_data;
	lhs_child.ToUnifiedFormat(lhs_list_size, lhs_child_data);
	rhs_child.ToUnifiedFormat(rhs_list_size, rhs_child_data);

//	D_ASSERT(lhs_list_size == rhs_list_size);
	std::cout << "lhs_list_size: " << lhs_list_size << ", rhs_list_size: " << rhs_list_size << std::endl;

  switch (lhs_child.GetType().InternalType()) {
  case PhysicalType::INT32:
	  std::cout << "physical int32" << std::endl;
	  break;
  case PhysicalType::UINT8:
	  break;
  case PhysicalType::INT8:
	  break;
  case PhysicalType::UINT16:
	  break;
  case PhysicalType::INT16:
	  break;
  case PhysicalType::UINT32:
    std::cout << "physical uint32" << std::endl;
	  break;
  case PhysicalType::UINT64:
    std::cout << "physical uint64" << std::endl;
	  break;
  case PhysicalType::INT64:
    std::cout << "physical int64" << std::endl;
	  break;
  case PhysicalType::FLOAT:
	  break;
  case PhysicalType::DOUBLE:
	  break;
  case PhysicalType::INTERVAL:
	  break;
  case PhysicalType::LIST:
	  break;
  case PhysicalType::STRUCT:
	  break;
  case PhysicalType::VARCHAR:
	  break;
  case PhysicalType::INT128:
	  break;
  case PhysicalType::UNKNOWN:
	  break;
  case PhysicalType::BIT:
	  break;
  case PhysicalType::INVALID:
	  break;
  case PhysicalType::BOOL:
	  break;
  }

  result.SetVectorType(VectorType::FLAT_VECTOR);
	// set result vector type
	auto result_entries = FlatVector::GetData<int32_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	idx_t offset = 0;
	for(idx_t i = 0; i < count; i++) {
		auto lhs_list_index = lhs_data.sel->get_index(i);
		auto rhs_list_index = rhs_data.sel->get_index(i);
		std::cout << "lhs_list_index: " << lhs_list_index << " , rhs_list_index: " << rhs_list_index << std::endl;

		if (!lhs_data.validity.RowIsValid(lhs_list_index) && !rhs_data.validity.RowIsValid(rhs_list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		if (lhs_data.validity.RowIsValid(lhs_list_index) && rhs_data.validity.RowIsValid(rhs_list_index)) {
			const auto& lhs_entry = lhs_entries[lhs_list_index];
			const auto& rhs_entry = rhs_entries[rhs_list_index];
			std::vector<int32_t> l_values;
			std::vector<int32_t> r_values;
//			rhs_child_data.data[lhs_entry.offset]
//			for(int j = 0; j < lhs_list_size; j++) {
//			}

      auto l_child_format = (int32_t *) lhs_child_data.data;
	    auto r_child_format = (int32_t *) rhs_child_data.data;

			for (int j = 0; j < lhs_entry.length; j++) {
        auto child_offset = lhs_entry.offset + j;
        auto child_index = lhs_child_data.sel->get_index(child_offset);
		    l_values.push_back(l_child_format[child_index]);
        std::cout << "child_index: " << child_index << ", child data: " << l_child_format[child_index] << std::endl;
			}

      for (int j = 0; j < rhs_entry.length; j++) {
        auto child_offset = rhs_entry.offset + j;
        auto child_index = rhs_child_data.sel->get_index(child_offset);
		    r_values.push_back(r_child_format[child_index]);
        std::cout << "child_index: " << child_index << ", child data: " << r_child_format[child_index] << std::endl;
      }

	    auto dis = std::inner_product(l_values.begin(), l_values.end(), r_values.begin(), 0);
//			int32_t * lhs_start = (int32_t* )lhs_child_data.data[lhs_list_index];
			std::cout << "list entry value offset: " << lhs_entry.offset << ", length: " << lhs_entry.length << std::endl;
//			for(int j = 0; j < lhs_list_size; j++) {
//				std::cout << "value: " << lhs_start[j] << std::endl;
//			}
			result_entries[i] = dis;
		}
	}
}

static unique_ptr<FunctionData> ListDistanceBind(ClientContext& context, ScalarFunction& bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
    D_ASSERT(bound_function.arguments.size() == 2);
	auto &lhs = arguments[0]->return_type;
	auto &rhs = arguments[1]->return_type;

	D_ASSERT(lhs.id() == LogicalTypeId::LIST);
	D_ASSERT(rhs.id() == LogicalTypeId::LIST);

	LogicalType child_type = LogicalType::SQLNULL;
	for (const auto& argument: arguments) {
		child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
	}
	std::cout << "child_type: " << child_type.ToString() << std::endl;

	auto list_type = LogicalType::LIST(child_type);

	bound_function.arguments[0] = list_type;
	bound_function.arguments[1] = list_type;

	bound_function.return_type = LogicalType::INTEGER;

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListDistanceFunStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 2);

	auto &left_stats = child_stats[0];
	auto &right_stats = child_stats[1];

	auto stats = left_stats.ToUnique();
	stats->Merge(right_stats);

	return stats;
}



ScalarFunction ListDistanceFun::GetFunction() {

	auto fn = ScalarFunction({LogicalType::LIST(LogicalType::INTEGER), LogicalType::LIST(LogicalType::INTEGER)},
	                         LogicalType::INTEGER, ListDistanceFunction, ListDistanceBind, nullptr,
	                         ListDistanceFunStats);

	fn.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	return fn;
}

void ListDistanceFun::RegisterFunction(duckdb::BuiltinFunctions &set) {
	set.AddFunction({"list_distance", "list_dis"}, GetFunction());
}

}
