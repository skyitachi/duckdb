#include <vector>
#include "duckdb.hpp"

using namespace duckdb;

bool bigger_than_four(int value) {
	return value > 4;
}


// scalar function
template<typename TYPE>
static void udf_vectorized(DataChunk &args, ExpressionState &state, Vector &result) {
	// set the result vector type
	result.SetVectorType(VectorType::FLAT_VECTOR);
	// get a raw array from the result
	auto result_data = FlatVector::GetData<TYPE>(result);

	// get the solely input vector
	auto &input = args.data[0];
	// now get an orrified vector
//	FlatVector vdata;
  UnifiedVectorFormat vdata;
  input.ToUnifiedFormat(args.size(), vdata);
//	input.Orrify(args.size(), vdata);

	// get a raw array from the orrified input
//	auto input_data = (TYPE *)vdata.GetData();

	auto input_data = FlatVector::GetData<TYPE>(input);

	// handling the data
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		// 判断validity
		if (vdata.validity.RowIsValid(idx)) {
      result_data[i] = input_data[idx];
		}
	}
}

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (1), (2), (3), (999)");
	auto result = con.Query("SELECT * FROM integers");
	result->Print();

	std::vector<LogicalType> args = {};
	LogicalType return_type{LogicalTypeId::INTEGER};
//	con.CreateAggregateFunction("my_min", args, return_type);
	// 可以参考CreateScalarFunction 封装的用法
	con.CreateVectorizedFunction<int, int>("udf_vectorized_int", udf_vectorized<int>);

	con.CreateScalarFunction<bool, int>("bigger_than_four", &bigger_than_four);

	con.Query("SELECT udf_vectorized_int(i) FROM integers")->Print();

	con.Query("SELECT bigger_than_four(i) FROM integers")->Print();
}
