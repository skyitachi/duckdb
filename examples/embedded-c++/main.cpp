#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db("/Users/shiping.yao/lab/duckdb/cmake-build-debug/lineitem");

	Connection con(db);

	// perfect join
	auto result = con.Query("select * from a join b on c_a2 = c_b2");

	result->Print();

}
