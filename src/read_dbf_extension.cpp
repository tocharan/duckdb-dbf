#define DUCKDB_EXTENSION_MAIN

#include "read_dbf_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "dbf_reader.hpp"

namespace duckdb {

struct ReadDbfBindData : public TableFunctionData {
	ReadDbfBindData(std::string file_path) : file_path(std::move(file_path)) {
	}
	std::string file_path;
};

struct ReadDbfGlobalState : public GlobalTableFunctionState {
	ReadDbfGlobalState(ClientContext &context, const std::string &file_path) : reader(context, file_path) {
		reader.Open();
	}
	DbfReader reader;
};

static LogicalType DbfToLogicalType(const DbfField &field) {
	switch (field.type) {
	case DbfType::CHARACTER:
		return LogicalType::VARCHAR;
	case DbfType::NUMERIC:
	case DbfType::FLOAT:
		if (field.decimal_count > 0) {
			return LogicalType::DOUBLE;
		} else {
			return LogicalType::BIGINT;
		}
	case DbfType::LOGICAL:
		return LogicalType::BOOLEAN;
	case DbfType::DATE:
		return LogicalType::DATE;
	case DbfType::INTEGER:
		return LogicalType::INTEGER;
	default:
		return LogicalType::VARCHAR;
	}
}

static unique_ptr<FunctionData> ReadDbfBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto file_path = input.inputs[0].GetValue<string>();
	auto bind_data = make_uniq<ReadDbfBindData>(file_path);

	DbfReader reader(context, file_path);
	reader.Open();

	for (auto &field : reader.GetFields()) {
		names.push_back(field.name);
		return_types.push_back(DbfToLogicalType(field));
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> ReadDbfInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadDbfBindData>();
	return make_uniq<ReadDbfGlobalState>(context, bind_data.file_path);
}

static void ReadDbfFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ReadDbfGlobalState>();

	idx_t output_idx = 0;
	while (output_idx < STANDARD_VECTOR_SIZE) {
		if (!state.reader.ReadNextRecord(output, output_idx)) {
			break;
		}
	}
	output.SetCardinality(output_idx);
}

void ReadDbfExtension::Load(ExtensionLoader &loader) {
	TableFunction read_dbf_func("read_dbf", {LogicalType::VARCHAR}, ReadDbfFunction, ReadDbfBind, ReadDbfInit);
	loader.RegisterFunction(read_dbf_func);
}

std::string ReadDbfExtension::Name() {
	return "read_dbf";
}

std::string ReadDbfExtension::Version() const {
#ifdef EXT_VERSION_READ_DBF
	return EXT_VERSION_READ_DBF;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(read_dbf, loader) {
    duckdb::ReadDbfExtension ext;
    ext.Load(loader);	
}
}
