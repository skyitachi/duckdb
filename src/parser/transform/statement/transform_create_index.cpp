#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

#include <iostream>
namespace duckdb {

static IndexType StringToIndexType(const string &str) {
	string upper_str = StringUtil::Upper(str);
	if (upper_str == "INVALID") {
		return IndexType::INVALID;
	} else if (upper_str == "ART") {
		return IndexType::ART;
	} else if (upper_str == "IVFFLAT") {
		return IndexType::IVFFLAT;
	}
	return IndexType::INVALID;
}

static OpClassType StringToOpClassType(const string& str) {
	string lower_str = StringUtil::Lower(str);
	if (lower_str == "vector_cosine_ops") {
		return OpClassType::Vector_Cosine_OPS;
	} else if (lower_str == "vector_ip_ops") {
		return OpClassType::Vector_IP_OPS;
	} else if (lower_str == "vector_l2_ops") {
		return OpClassType::Vector_L2_OPS;
	} else if (lower_str == "vector_l1_ops") {
		return OpClassType::Vector_L1_OPS;
	}
	return OpClassType::INVALID;
}

vector<unique_ptr<ParsedExpression>> Transformer::TransformIndexParameters(duckdb_libpgquery::PGList *list,
                                                                           const string &relation_name) {
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto cell = list->head; cell != nullptr; cell = cell->next) {
		auto index_element = (duckdb_libpgquery::PGIndexElem *)cell->data.ptr_value;
		if (index_element->collation) {
			throw NotImplementedException("Index with collation not supported yet!");
		}
		OpClassType op_type = OpClassType::Vector_IP_OPS;
    auto opclass_list = index_element->opclass;
	  if (opclass_list) {
      std::string opclass;
      for (auto opclass_cell = opclass_list->head; opclass_cell != nullptr; opclass_cell = opclass_cell->next) {
        auto def_elem = (duckdb_libpgquery::PGDefElem*) opclass_cell->data.ptr_value;
        if (def_elem->type == duckdb_libpgquery::T_PGString) {
          opclass = def_elem->defnamespace;
          op_type = StringToOpClassType(opclass);
          if (op_type == OpClassType::INVALID) {
            throw NotImplementedException("invalid opclass type");
          }
        }
      }
	  }
		if (index_element->name) {
			// create a column reference expression
			expressions.push_back(make_uniq<ColumnRefExpression>(index_element->name, relation_name, op_type));
		} else {
			// parse the index expression
			D_ASSERT(index_element->expr);
			expressions.push_back(TransformExpression(index_element->expr));
		}
	}
	return expressions;
}

unique_ptr<CreateStatement> Transformer::TransformCreateIndex(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGIndexStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateIndexInfo>();
	if (stmt->unique) {
		info->constraint_type = IndexConstraintType::UNIQUE;
	} else {
		info->constraint_type = IndexConstraintType::NONE;
	}

	info->on_conflict = TransformOnConflict(stmt->onconflict);

	if (stmt->options) {
		// parse options
		auto list = stmt->options;
		for (auto cell = list->head; cell != nullptr; cell = cell->next) {
			auto def_elem = (duckdb_libpgquery::PGDefElem *)cell->data.ptr_value;
			std::string option_name = def_elem->defname;

			if (def_elem->arg != nullptr) {
				switch (def_elem->arg->type) {
				case duckdb_libpgquery::T_PGInteger: {
				  auto pg_value = (duckdb_libpgquery::PGValue *)def_elem->arg;
				  info->options.insert(std::make_pair(option_name, (int)pg_value->val.ival));
				  break;
				}
        default:
            throw NotImplementedException("options only support T_PGInteger");
				}
			}
		}
	}

	info->expressions = TransformIndexParameters(stmt->indexParams, stmt->relation->relname);

	info->index_type = StringToIndexType(string(stmt->accessMethod));
	auto tableref = make_uniq<BaseTableRef>();
	tableref->table_name = stmt->relation->relname;
	if (stmt->relation->schemaname) {
		tableref->schema_name = stmt->relation->schemaname;
	}
	info->table = std::move(tableref);
	if (stmt->idxname) {
		info->index_name = stmt->idxname;
	} else {
		throw NotImplementedException("Index without a name not supported yet!");
	}
	for (auto &expr : info->expressions) {
		info->parsed_expressions.emplace_back(expr->Copy());
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
