add_library_unity(
        duckdb_vector_index_execution
        OBJECT
        ivfflat.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_vector_index_execution>
        PARENT_SCOPE)
