#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3), (4), (5), (1)");
	auto result = con.Query("SELECT * FROM integers order by i limit 2");
	result->Print();
}
