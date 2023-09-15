#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");

	con.Query("create index idx_i on integers(i)");
	auto result = con.Query("SELECT * FROM integers");
  result->Print();

	con.Query("select * from integers where i < 9;")->Print();
}
