//
// Created by Shiping Yao on 2022/12/5.
//

#ifndef DUCKDB_DEBUG_UTIL_H
#define DUCKDB_DEBUG_UTIL_H
#include <string>
namespace duckdb {

extern std::string MyBacktrace(int skip = 1);
}
#endif // DUCKDB_DEBUG_UTIL_H
