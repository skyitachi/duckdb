#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/udf_function.hpp"

#include <iostream>
#include <vector>

using namespace duckdb;

bool bigger_than_four(int value) {
	return value > 4;
}

// scalar function
template <typename TYPE>
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

template <class T>
struct my_sum_t {
	T sum;
};

class MySumAggr {
public:
	template <class STATE>
	static void Initialize(STATE* state) {
		std::cout << "my sum initialize " << std::endl;
		state->sum = 0;
	}
	static bool IgnoreNull() {
		return true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		std::cout << "in the my_sum operation: " << input[idx] << std::endl;
		state->sum += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		state->sum += input[0] * count;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->sum += source.sum;
	}

	// NOTE: important
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		// pass
		std::cout << "in the my_sum finalize" << std::endl;
		target[idx] = state->sum;
	}
};

template <class T>
struct my_list_sum_t {
	T sum;
};

class MyListSumAggr {
public:
	template <class STATE>
	static void Initialize(STATE* state) {
		std::cout << "my list sum initialize " << std::endl;
		state->sum = 0;
	}
	static bool IgnoreNull() {
		return true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		std::cout << "in the my_sum operation: " << input[idx] << std::endl;
//		auto list_input = LogicalType::LIST(LogicalType::INTEGER)* input;
		state->sum += input[idx];
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
								  idx_t count) {
//		state->sum += input[0] * count;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		target->sum += source.sum;
	}

	// NOTE: important
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		// pass
		std::cout << "in the my_sum finalize" << std::endl;
		target[idx] = state->sum;
	}

};


void vector_demo() {
	//	auto cap = STANDARD_VECTOR_SIZE;
	auto cap = 8;
	auto vec = make_uniq<Vector>(LogicalType::INTEGER, cap);
	vec->SetVectorType(VectorType::FLAT_VECTOR);

	//	vec->SetValue(0, NULL);
	// set value
	for (int i = 0; i < cap; i++) {
		if (i % 2 == 1) {
			vec->SetValue(i, i * 2);
		} else {
			FlatVector::SetNull(*vec, i, true);
		}
	}

	for (int i = 0; i < cap; i++) {
		auto v = vec->GetValue(i);
		if (v.IsNull()) {
			std::cout << i << " index value is null" << std::endl;
		} else {
			std::cout << v << std::endl;
		}
	}

	std::cout << vec->ToString() << std::endl;
	// get vector size and capacity

	//	UnifiedVectorFormat data;
	//	vec->ToUnifiedFormat()
}

// 复合类型的custom aggr

int main() {
	DuckDB db(nullptr);

	Connection con(db);

//	con.Query("CREATE TABLE floats(i FLOAT)");
//	con.Query("INSERT INTO floats VALUES (1), (2), (3), (999)")->Print();
//	con.Query("create index idx_i on integers (i)")->Print();
//	auto result = con.Query("SELECT * FROM integers");
//	result->Print();
//
//	std::vector<LogicalType> args = {};
//	LogicalType return_type {LogicalTypeId::INTEGER};
//	//	con.CreateAggregateFunction("my_min", args, return_type);
//	// 可以参考CreateScalarFunction 封装的用法
//	con.CreateVectorizedFunction<int, int>("udf_vectorized_int", udf_vectorized<int>);
//
//	con.CreateScalarFunction<bool, int>("bigger_than_four", &bigger_than_four);
//	con.CreateAggregateFunction<MySumAggr, my_sum_t<int>, int, int>("my_sum", LogicalType::INTEGER,
//	                                                                LogicalType::INTEGER);
//
//	con.Query("SELECT udf_vectorized_int(i) FROM integers")->Print();
//
//	con.Query("SELECT bigger_than_four(i) FROM integers")->Print();

//	auto fn = UDFWrapper::CreateScalarFunction("bigger_than_four", &bigger_than_four);

//	vector_demo();

//	con.Query("select my_sum(i) from integers")->Print();

	con.Query("create table list_table(embedding FLOAT[], id INTEGER)");

//	con.Query("insert into list_table VALUES ([1.1, 2.2, 3.3], 1)");
	con.Query("copy list_table from 'embedding.json'")->Print();

//	con.Query("select * from list_table")->Print();

//	con.CreateAggregateFunction<MyListSumAggr, my_list_sum_t<int>, int, int>("my_list_sum", LogicalType::INTEGER,
//																	LogicalType::LIST(LogicalType::INTEGER));

//	con.Query("select list_count(int_list), list_avg(int_list) from list_table")->Print();
//
//  con.Query("CREATE INDEX idx_v ON list_table USING ivfflat(embedding vector_l2_ops) WITH (oplists = 1, d = 3)")->Print();

//  con.Query("explain select embedding, list_min(embedding) as score from list_table order by score limit 10")->Print();
//  con.Query("select list_concat(float_list, [1.0, 1.0, 3.0]) from list_table")->Print();

	con.Query("explain select embedding, list_distance(embedding, [2.0, 1.2, 2.0]) as score from list_table order by score limit 10")->Print();
  // min_distance aggregation
//  con.Query("select min_distance(float_list) from list_table")->Print();
//  con.Query("select min_distance(float_list, 3) from list_table")->Print();

//  con.Query("select list_concat(int_list, [1, 2, 3]) from list_table")->Print();
//
//	con.Query("select min(list_distance(int_list, [1, 2, 3])) from list_table")->Print();
//
//	con.Query("CREATE INDEX ON list_table ivfflat(int_list) USING ivfflat (vector_cosine_ops) WITH (oplists = 100)")->Print();
}
