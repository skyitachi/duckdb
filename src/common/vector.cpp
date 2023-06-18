#include "duckdb/common/exception.hpp"
#include <iostream>
namespace duckdb {

void AssertIndexInBounds(idx_t index, idx_t size) {
	if (index < size) {
		return;
	}
	std::cout << "just add breakpoints" << std::endl;
	throw InternalException("Attempted to access index %ld within vector of size %ld", index, size);
}

} // namespace duckdb
