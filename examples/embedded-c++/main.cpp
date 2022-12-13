#include "duckdb.hpp"

#include <execinfo.h>
#include <stdio.h>
#include <iostream>

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

void datachunk_example(Connection& conn) {
	DataChunk dataChunk;
	auto& context = conn.context;
	std::vector<LogicalType> types = {LogicalType::SMALLINT, LogicalType::BIGINT};
	dataChunk.Initialize(*context.get(), types.begin(), types.end(), 10);
	std::cout << "dataChunk size: " << dataChunk.size() << " column count: " << dataChunk.ColumnCount() << std::endl;
	Vector v1(LogicalType::SMALLINT);
	Vector v2(LogicalType::BIGINT);
	v1.SetVectorType(VectorType::FLAT_VECTOR);
	v2.SetVectorType(VectorType::FLAT_VECTOR);
	for (int i = 0; i < 10; i++) {
		Value v(LogicalType::SMALLINT);
		v = i;
		dataChunk.SetValue(0, i, v);
	}
	for (int i = 0; i < 10; i++) {
		Value v(LogicalType::BIGINT);
		v = i * 10;
		dataChunk.SetValue(1, i, v);
	}

	dataChunk.SetCardinality(10);

	std::cout << "after setting dataChunk size: " << dataChunk.size() << " column count: " << dataChunk.ColumnCount() << std::endl;
	dataChunk.Print();

	DataChunk chunk2;
	chunk2.Move(dataChunk);

	std::cout << "chunk2: " << chunk2.ColumnCount() << std::endl;
	std::cout << "after move: " << dataChunk.ColumnCount() << std::endl;

	DataChunk chunk3;
	chunk3.Initialize(*context.get(), types.begin(), types.end(), 10);
	chunk2.Copy(chunk3);
	std::cout << "copy chunk3: " << chunk3.ColumnCount() << std::endl;
	std::cout << "after copy: " << chunk2.ColumnCount() << std::endl;
	chunk3.Print();
	{
		// slice example
		sel_t* selects = new sel_t[]{3, 6};
		std::unique_ptr<sel_t[]> select_ptr;
		select_ptr.reset(selects);
		SelectionVector selectionVector(select_ptr.get());
		selectionVector.Print(2);
		DataChunk slice_chunk;
		slice_chunk.Initialize(*context.get(), types.begin(), types.end(), 10);
//		slice_chunk.Copy(chunk3);
		chunk3.Copy(slice_chunk);
//		slice_chunk.SetCardinality(2);
		slice_chunk.Slice(selectionVector, 2);
		std::cout << "slice chunk size: " << slice_chunk.size() << std::endl;
		slice_chunk.Print();
		slice_chunk.data[0].Flatten(10);
	}


}

void count_example(Connection& conn) {
	conn.Query("CREATE TABLE actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");
	conn.Query("CREATE TABLE film_actor(actor_id INTEGER, film_id INTEGER)");
	conn.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
	conn.Query("INSERT INTO film_actor VALUES (1, 1), (2, 2), (3, 3)");
	auto sql = "select count(*) from film_actor";
	auto plan = conn.ExtractPlan(sql);

	plan->Print();

	conn.Query(sql);

}

int main() {
	DBConfig config{};
	DuckDB db(nullptr);

	Connection con(db);

	count_example(con);
//	datachunk_example(con);
//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	con.Query("INSERT INTO integers VALUES (5)");
//	con.Query("CREATE TABLE actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");
//	con.Query("CREATE TABLE film_actor(actor_id INTEGER, film_id INTEGER)");
//	con.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
//	con.Query("INSERT INTO film_actor VALUES (1, 1), (2, 2), (3, 3)");
//	{
//		auto result = con.Query("select * from actor");
//		result->Print();
//	}
//	{
//		auto result = con.Query("select * from film_actor");
//		result->Print();
//	}
//	auto sql = "select actor.actor_id, film_id, first_name from actor join film_actor on film_actor.actor_id=actor.actor_id";
//
//	auto plan = con.ExtractPlan(sql);
//	plan ->Print();

//	auto result = con.Query("select actor.actor_id, film_id, first_name from actor join film_actor on film_actor.actor_id=actor.actor_id");
//
//	result->Print();
}
