#include "duckdb.hpp"

#include <execinfo.h>
#include <stdio.h>

using namespace duckdb;

void debug_backtrace() {
	void* callstack[256];
	int i, frames = backtrace(callstack, 256);
	char** strs = backtrace_symbols(callstack, frames);
	for (i = 0; i < frames; ++i) {
		printf("%s\n", strs[i]);
	}
	free(strs);

}

int main() {
	DBConfig config{};
	DuckDB db(nullptr);

	Connection con(db);

//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	con.Query("INSERT INTO integers VALUES (5)");
	con.Query("CREATE TABLE actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");
	con.Query("CREATE TABLE film_actor(actor_id INTEGER, film_id INTEGER)");
	con.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
	con.Query("INSERT INTO film_actor VALUES (1, 1), (2, 2), (3, 3)");
//	{
//		auto result = con.Query("select * from actor");
//		result->Print();
//	}
//	{
//		auto result = con.Query("select * from film_actor");
//		result->Print();
//	}
	auto sql = "select actor.actor_id, film_id, first_name from actor join film_actor on film_actor.actor_id=actor.actor_id";

	auto plan = con.ExtractPlan(sql);
	plan ->Print();

//	auto result = con.Query("select actor.actor_id, film_id, first_name from actor join film_actor on film_actor.actor_id=actor.actor_id");
//
//	result->Print();
}
