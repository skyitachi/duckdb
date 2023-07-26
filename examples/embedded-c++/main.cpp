#include "duckdb.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/udf_function.hpp"

#include <iostream>
#include <numeric>
#include <typeindex>
#include <typeinfo>
#include <vector>

using namespace duckdb;

bool bigger_than_four(int value) {
	return value > 4;
}

static void list_distance(DataChunk &args, ExpressionState &state, Vector &result) {
	std::cout << "in the vectorized list_distance" << std::endl;
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &lhs = args.data[0];
	Vector &rhs = args.data[1];

	UnifiedVectorFormat lhs_data;
	UnifiedVectorFormat rhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);
	rhs.ToUnifiedFormat(count, rhs_data);

	auto lhs_entries = ListVector::GetData(lhs);
	auto rhs_entries = ListVector::GetData(rhs);

	auto lhs_list_size = ListVector::GetListSize(lhs);
	auto rhs_list_size = ListVector::GetListSize(rhs);

	auto &lhs_child = ListVector::GetEntry(lhs);
	auto &rhs_child = ListVector::GetEntry(rhs);
	UnifiedVectorFormat lhs_child_data;
	UnifiedVectorFormat rhs_child_data;
	lhs_child.ToUnifiedFormat(lhs_list_size, lhs_child_data);
	rhs_child.ToUnifiedFormat(rhs_list_size, rhs_child_data);
	// TODO: 这里需要对类型做统一的换算
	std::cout << lhs.GetType().ToString() << " rhs type: " << rhs.GetType().ToString() << " physical type: " << int(rhs_child.GetType().InternalType()) << std::endl;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	// set result vector type
	auto result_entries = FlatVector::GetData<float>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto lhs_list_index = lhs_data.sel->get_index(i);
		auto rhs_list_index = rhs_data.sel->get_index(i);

		if (!lhs_data.validity.RowIsValid(lhs_list_index) && !rhs_data.validity.RowIsValid(rhs_list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		if (lhs_data.validity.RowIsValid(lhs_list_index) && rhs_data.validity.RowIsValid(rhs_list_index)) {
			const auto &lhs_entry = lhs_entries[lhs_list_index];
			const auto &rhs_entry = rhs_entries[rhs_list_index];
			std::vector<float> l_values;
			std::vector<float> r_values;

			auto l_child_format = (float *)lhs_child_data.data;
			auto r_child_format = (float *)rhs_child_data.data;

			for (int j = 0; j < lhs_entry.length; j++) {
				auto child_offset = lhs_entry.offset + j;
				auto child_index = lhs_child_data.sel->get_index(child_offset);
				l_values.push_back(l_child_format[child_index]);
			}

			for (int j = 0; j < rhs_entry.length; j++) {
				auto child_offset = rhs_entry.offset + j;
				auto child_index = rhs_child_data.sel->get_index(child_offset);
				r_values.push_back(r_child_format[child_index]);
			}
			auto dis = std::inner_product(l_values.begin(), l_values.end(), r_values.begin(), 0.0);
			result_entries[i] = dis;
		}
	}
}

void parse(Vector &input, list_entry_t *parent_entries, int parent_count, int count) {
	std::cout << "parse vector type: " << VectorTypeToString(input.GetVectorType()) << std::endl;
	if (input.GetType().id() != LogicalTypeId::LIST) {
		for (int i = 0; i < parent_count; i++) {
			std::cout << "data[i].offset = " << parent_entries[i].offset << ", length = " << parent_entries[i].length
			          << std::endl;
		}
		return;
	}
	auto *entries = ListVector::GetData(input);
	auto &data = ListVector::GetEntry(input);

	// 这里的count有问题
	auto sz = ListVector::GetListSize(input);
	std::cout << "count = " << sz << std::endl;
	if (count != -1) {
		parent_count = count;
	}
	count = sz;
	parse(data, entries, parent_count, count);
}
// scalar function
template <typename TYPE>
static void udf_vectorized(DataChunk &args, ExpressionState &state, Vector &result) {
	// set the result vector type
	result.SetVectorType(VectorType::FLAT_VECTOR);
	// get a raw array from the result
	// nested vector
	D_ASSERT(args.ColumnCount() == 1);
	std::cout << "args.size = " << args.size() << std::endl;

	// get the solely input vector
	auto &input = args.data[0];
	parse(input, nullptr, -1, -1);
	D_ASSERT(input.GetType().id() == LogicalTypeId::LIST);

	auto *entries = ListVector::GetData(input);
	std::cout << entries[0].offset << ", " << entries->length << std::endl;

	auto &data = ListVector::GetEntry(input);
	std::cout << "child vector type: " << data.GetType().ToString() << std::endl;

	// now get an orrified vector
	//	FlatVector vdata;
	//  auto result_data = FlatVector::GetData<TYPE>(result);
	//	UnifiedVectorFormat vdata;
	//	input.ToUnifiedFormat(args.size(), vdata);
	//	//	input.Orrify(args.size(), vdata);
	//
	//	// get a raw array from the orrified input
	//	//	auto input_data = (TYPE *)vdata.GetData();
	//
	//	auto input_data = FlatVector::GetData<TYPE>(input);
	//
	//	// handling the data
	//	for (idx_t i = 0; i < args.size(); i++) {
	//		auto idx = vdata.sel->get_index(i);
	//		// 判断validity
	//		if (vdata.validity.RowIsValid(idx)) {
	//			result_data[i] = input_data[idx];
	//		}
	//	}
}

template <class T>
struct my_sum_t {
	T sum;
};

struct my_sum_state {
	void *data;
	LogicalType type;
};

struct my_sum_input_state {};

struct my_sum_output_state {};

class MySumAggr {
public:
	template <class STATE>
	static void Initialize(STATE *state) {
		std::cout << "my sum initialize " << std::endl;
		state->sum = 0;
	}
	static bool IgnoreNull() {
		return true;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &data, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		//		std::cout << "in the my_sum operation, input_type: " << std::type_index(typeid(INPUT_TYPE)).name() <<
		//std::endl;
		auto entries = (list_entry_t *)input;
		if (entries[idx].data_ptr != nullptr) {
			auto data_ptr = (float *)entries[idx].data_ptr;
			for (idx_t i = 0; i < entries[idx].length; i++) {
				state->sum += data_ptr[entries[idx].offset + i];
			}
		}
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

template <class T>
struct my_list_sum_t {
	T sum;
};

class MyListSumAggr {
public:
	template <class STATE>
	static void Initialize(STATE *state) {
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

void list_value_demo() {
	auto iv = Value::INTEGER(1);
	auto iv1 = Value::INTEGER(2);
	auto iv2 = Value::INTEGER(3);
	auto li_v = Value::LIST(std::vector<Value> {iv, iv1, iv2});
	//	std::vector<Value> values;
	auto values = ListValue::GetChildren(li_v);
	for (auto &v : values) {
		std::cout << "v: " << v << std::endl;
	}
}

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
	//	con.CreateVectorizedFunction<float, list_entry_t>("udf_vectorized_int", udf_vectorized<float>);
	//	con.Query("select udf_vectorized_int([[1], [2, 3], [1,2,3]])")->Print();

	con.CreateVectorizedFunction<float, list_entry_t, list_entry_t>("my_list_distance", list_distance);
	//
		con.CreateScalarFunction<bool, int>("bigger_than_four", &bigger_than_four);
	//	con.CreateAggregateFunction<MySumAggr, my_sum_t<int>, int, int>("my_sum", LogicalType::INTEGER,
	//	                                                                LogicalType::INTEGER);

	//	con.CreateAggregateFunction<MySumAggr, my_sum_t<float>, float, list_entry_t>
	//	    ("my_list_sum", LogicalType::FLOAT, LogicalType::LIST(LogicalType::FLOAT));

	//  con.CreateAggregateFunction<MySumAggr, my_sum_t<float>, float, list_data_t<float>>
	//      ("my_list_sum2", LogicalType::FLOAT, LogicalType::LIST(LogicalType::FLOAT));
	//
	//	con.Query("SELECT udf_vectorized_int(i) FROM integers")->Print();
	//
	//	con.Query("SELECT bigger_than_four(i) FROM integers")->Print();

	//	auto fn = UDFWrapper::CreateScalarFunction("bigger_than_four", &bigger_than_four);

	vector_demo();
	list_value_demo();

	//	con.Query("select my_sum(i) from integers")->Print();

	con.Query("create table list_table(embedding FLOAT[], id INTEGER, c INTEGER)");

	con.Query("copy list_table from 'embedding.json'")->Print();

	con.Query("create INDEX idx_id on list_table(id)")->Print();

//	con.Query("select id, list_distance(embedding, [2.0, 1.2, 2.0]) from list_table where id % 2 == 0 limit 3")
//	    ->Print();
//	con.Query("select * from list_table where bigger_than_four(id)")->Print();

//	con.Query("select id, list_distance(embedding, [2.0, 1.2, 2.0]) from list_table where id % 2 == 0 limit 3")
//	    ->Print();

  con.Query("select id, my_list_distance(embedding, [2.0, 1.2, 2.0]) as score, embedding from list_table order by score limit 3")
      ->Print();

	con.Query("select id, list_distance(embedding, [2.0, 1.2, 2.0]) as score, embedding from list_table order by score limit 3")
	    ->Print();

	//  con.Query("select id, embedding from list_table order by c limit 3")->Print();
	//
	//  con.Query("select id, (id + 1) as score from list_table order by score limit 3")->Print();

	//  con.Query("select id, list_distance(embedding, [2.0, 1.2, 2.0]) as score, embedding from list_table order by id
	//  desc limit 3")->Print();
	//
	//  con.Query("select id, list_distance(embedding, [2.0, 1.2, 2.0]) as score, embedding from list_table order by
	//  score desc limit 3")->Print();

	//  con.Query("select id, embedding, my_list_distance(embedding, [2.0, 1.2, 2.0]) from list_table order by id desc
	//  limit 3")->Print();
	//
	//	con.Query("select my_list_distance(embedding, [1.0, 1.0, 1.0]) from list_table limit 10")->Print();

	//	con.Query("select my_sum(c) from list_table")->Print();

	//	con.Query("select count(*) from list_table where id < 20000 and id > 19900")->Print();

	//	con.Query("CREATE INDEX idx_v ON list_table USING ivfflat(embedding vector_ip_ops) WITH (oplists = 1, d =
	//3)")->Print();
	//
	//  con.Query("select id, embedding, list_distance(embedding, [2.0, 1.2, 2.0]) as score from list_table order by
	//  score limit 3")->Print();
	//
	//  con.Query("select id, embedding, list_distance(embedding, [2.0, 1.2, 2.0]) as score from list_table where id <
	//  100 and id > 90 order by score limit 3")->Print();

	// NOTE: DataChunk output为什么是Dictionary Vector
	//	con.Query("select * from list_table where c < 10")->Print();

	//	con.Query("select * from list_table")->Print();

	//	con.CreateAggregateFunction<MyListSumAggr, my_list_sum_t<int>, int, int>("my_list_sum", LogicalType::INTEGER,
	//																	LogicalType::LIST(LogicalType::INTEGER));

	//	con.Query("select list_count(int_list), list_avg(int_list) from list_table")->Print();
	//
	//  con.Query("CREATE INDEX idx_v ON list_table USING ivfflat(embedding vector_ip_ops) WITH (oplists = 1, d =
	//  3)")->Print();

	//  con.Query("select embedding, list_min(embedding) as score from list_table order by score limit 10")->Print();
	//  con.Query("select list_concat(float_list, [1.0, 1.0, 3.0]) from list_table")->Print();

	//  con.Query("select embedding, list_distance(embedding, [2.0, 1.2, 2.0]) as score from list_table order by score
	//  limit 3")->Print();

	//	con.Query("select embedding, list_distance(embedding, [2.0, 1.2, 2.0]) as score from list_table where id < 10
	//order by score limit 3")->Print(); 	con.Query("checkpoint(demo)")->Print();
	// min_distance aggregation
	//  con.Query("select min_distance(float_list) from list_table")->Print();
	//  con.Query("select min_distance(float_list, 3) from list_table")->Print();

	//  con.Query("select list_concat(int_list, [1, 2, 3]) from list_table")->Print();
	//
	//	con.Query("select min(list_distance(int_list, [1, 2, 3])) from list_table")->Print();
	//
	//	con.Query("CREATE INDEX ON list_table ivfflat(int_list) USING ivfflat (vector_cosine_ops) WITH (oplists =
	//100)")->Print();
}
