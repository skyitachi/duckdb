//
// Created by Shiping Yao on 2023/2/26.
//
#pragma once
#include "duckdb.hpp"

#ifndef DUCKDB_HDFS_EXTENSION_H
#define DUCKDB_HDFS_EXTENSION_H

namespace duckdb {
class HDFSExtension: public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};
}

#endif // DUCKDB_HDFS_EXTENSION_H
