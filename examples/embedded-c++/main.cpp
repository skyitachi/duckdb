#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint_manager.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"

#include <chrono>
#include <execinfo.h>
#include <iostream>
#include <stdio.h>
#include <thread>

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

void bulk_load() {
	DBConfig config{};
	DuckDB db("bulk_load_example", &config);
	Connection con(db);

	LocalFileSystem fs;
	if (fs.FileExists("bulk_load_example")) {
		fs.RemoveFile("bulk_load_example");
	}

	con.Query("CREATE TABLE lineitem AS SELECT * FROM read_parquet('lineitem.parquet')");
//	auto result = con.ExtractPlan("CREATE TABLE lineitem AS SELECT * FROM read_parquet('lineitem.parquet')");
//	result->Print();
}

void bulk_load_rollback() {
  LocalFileSystem fs;
  if (fs.FileExists("bulk_load_example")) {
    fs.RemoveFile("bulk_load_example");
  }

	DBConfig config{};
	DuckDB db("bulk_load_example", &config);


	Connection conn(db);
	conn.BeginTransaction();
	conn.SetAutoCommit(false);
	conn.Query("CREATE TABLE lineitem AS SELECT * FROM read_parquet('lineitem.parquet')");

  std::this_thread::sleep_for(std::chrono::minutes(2));
	conn.Rollback();
}

void small_bulk_load() {
	DBConfig config{};
	DuckDB db("small_bulk_load_example", &config);
	Connection con(db);

	con.Query("CREATE TABLE lineitem AS SELECT * FROM read_parquet('lineitem_10000.parquet')");
}

void scan_example() {
	DBConfig config{};
	DuckDB db("scan_example", &config);
	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");
	con.Query("INSERT INTO integers VALUES (5)");
	con.Query("CREATE TABLE actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");
	con.Query("CREATE TABLE film_actor(actor_id INTEGER, film_id INTEGER)");
	con.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
	con.Query("INSERT INTO film_actor VALUES (1, 1), (2, 2), (3, 3)");

	con.Query("select * from file_actor");

}

void prepare_example(Connection& conn) {
	auto prepared = conn.Prepare("select count(*) from film_actor");
//	conn.Query(prepared);
}

void pending_query_example(Connection& conn) {
	auto statements = conn.ExtractStatements("select count(*) from film_actor;");
	std::cout << statements.size() << std::endl;

	if (statements.size() > 0) {
		std::cout << "sql statement: " << statements[0]->ToString() << std::endl;
		auto result = conn.Query(std::move(statements[0]));
		result->Print();
	}

}

void persistent_example() {
	DBConfig config{};
	DuckDB db("persistent_example", &config);
	Connection conn(db);

	conn.Query("CREATE TABLE IF NOT EXISTS actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");

	conn.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
}

void lineitem_example() {
	DBConfig config{};
	DuckDB db("lineitem", &config);

	Connection conn(db);

	auto start = std::chrono::steady_clock::now();
	auto result = conn.Query("select count(l_orderkey) from lineitem");

	auto end = std::chrono::steady_clock::now();

	result->Print();

	std::cout << "time consumes(ms): " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << std::endl;

}

void transaction_example() {
	DBConfig config{};
	DuckDB db("transaction_example", &config);

	// prepare data;
	Connection conn(db);

	conn.Query("CREATE TABLE IF NOT EXISTS ball(ball_id INTEGER, color VARCHAR)");
	conn.Query("INSERT INTO ball VALUES (1, 'black'), (2, 'white')");

	std::thread t1([&]{
		Connection conn(db);
		conn.BeginTransaction();
    conn.SetAutoCommit(false);
		conn.Query("update ball set color = 'black' where color = 'white'");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    conn.Commit();
	});

	std::thread t2([&]{
	  Connection conn(db);
	  std::this_thread::sleep_for(std::chrono::seconds(1));
	  conn.BeginTransaction();
	  conn.SetAutoCommit(false);
	  conn.Query("update ball set color = 'white' where color = 'black'");
	  conn.Commit();
	});

	t1.join();
	t2.join();

	conn.BeginTransaction();
	conn.SetAutoCommit(false);
	auto result = conn.Query("select * from ball");
	std::cout << "main thread result: " << std::endl << result->ToString();
	conn.Commit();


}

void storage_example() {
	DBConfig config{};
	DuckDB db("lineitem", &config);
//	auto& blockManager = BlockManager::GetBlockManager(*db.instance);
	SingleFileBlockManager blockManager{*db.instance, "lineitem", true, false, false};
	// schema meta_block
	auto meta_block_id = blockManager.GetMetaBlock();
	auto free_block_id = blockManager.GetFreeBlockId();
	std::cout << "meta_block_id: " << meta_block_id << ", free_block_id: " << free_block_id << std::endl;
	std::cout << "is_root_block: " << blockManager.IsRootBlock(meta_block_id) << std::endl;
	std::cout << "total_blocks: " << blockManager.TotalBlocks() << std::endl;
	auto block = blockManager.RegisterBlock(meta_block_id);
	auto handle = blockManager.buffer_manager.Pin(block);
	auto next_block_id = Load<block_id_t>(handle.Ptr());
	std::cout << "next_block_id: " << next_block_id << std::endl;
	MetaBlockReader reader(blockManager, meta_block_id);
	auto schema_count = reader.Read<uint32_t>();
	std::cout << "schema_count: " << schema_count << std::endl;

//	SingleFileCheckpointReader cp_reader(blockManager);
}


// ww conflict
void update_transaction_example() {
	DBConfig config{};
	DuckDB db("transaction_example", &config);

	// prepare data;
	Connection conn(db);

	conn.Query("CREATE TABLE IF NOT EXISTS ball(ball_id INTEGER, color VARCHAR)");
	conn.Query("INSERT INTO ball VALUES (1, 'black'), (2, 'white')");

	std::thread t1([&]{
	  Connection conn(db);
	  conn.BeginTransaction();
	  conn.SetAutoCommit(false);
	  auto result = conn.Query("update ball set color = 'yellow' where ball_id = 1");
	  std::this_thread::sleep_for(std::chrono::seconds(2));
	  conn.Commit();
	  std::cout << "t1 results: " << result->ToString();
	});

	std::thread t2([&]{
	  Connection conn(db);
	  std::this_thread::sleep_for(std::chrono::seconds(1));
	  conn.BeginTransaction();
	  conn.SetAutoCommit(false);
	  auto result = conn.Query("update ball set color = 'white' where ball_id = 1");
	  conn.Commit();
	  std::cout << "t2 results: " << result->ToString();
	});

	t1.join();
	t2.join();
}

// rw conflict
void rw_transaction_example() {
	DBConfig config{};
	DuckDB db("transaction_example", &config);

	// prepare data;
	Connection conn(db);

	conn.Query("CREATE TABLE IF NOT EXISTS ball(ball_id INTEGER, color VARCHAR)");
	conn.Query("INSERT INTO ball VALUES (1, 'black'), (2, 'white')");

	std::thread t1([&]{
	  Connection conn(db);
	  conn.BeginTransaction();
	  conn.SetAutoCommit(false);
	  std::this_thread::sleep_for(std::chrono::seconds(2));
	  auto result = conn.Query("select * from ball where ball_id = 1");
	  conn.Commit();
	  std::cout << "t1 results: " << result->ToString();
	});

	std::thread t2([&]{
	  Connection conn(db);
	  std::this_thread::sleep_for(std::chrono::seconds(1));
	  conn.BeginTransaction();
	  conn.SetAutoCommit(false);
	  conn.Query("update ball set color = 'white' where ball_id = 1");
	  auto result = conn.Query("select * from ball where ball_id = 1");
	  conn.Commit();
	  std::cout << "t2 results: " << result->ToString();
	});

	t1.join();
	t2.join();

	LocalFileSystem fs;
	fs.RemoveFile("transaction_example");

}

int main() {
//	bulk_load_rollback();
//	bulk_load();
//	scan_example();
//	rw_transaction_example();
//	update_transaction_example();
//	transaction_example();
//	storage_example();
//	persistent_example();
	lineitem_example();
//	DBConfig config{};
//	DuckDB db(nullptr);
//
//	Connection con(db);
//
//	// prepare data
//	con.Query("CREATE TABLE integers(i INTEGER)");
//	con.Query("INSERT INTO integers VALUES (3)");
//	con.Query("INSERT INTO integers VALUES (5)");
//	con.Query("CREATE TABLE actor(actor_id INTEGER, first_name VARCHAR, last_name VARCHAR)");
//	con.Query("CREATE TABLE film_actor(actor_id INTEGER, film_id INTEGER)");
//	con.Query("INSERT INTO actor VALUES (1, 'PENELOPE', 'GUINESS'), (2, 'NICK', 'WAHLBERG')");
//	con.Query("INSERT INTO film_actor VALUES (1, 1), (2, 2), (3, 3)");
//
//	//	datachunk_example(con);
//	pending_query_example(con);
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
