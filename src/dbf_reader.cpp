#include "dbf_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/helper.hpp"
#include <cstring>

namespace duckdb {

DbfReader::DbfReader(ClientContext &context, const std::string &file_path) : context(context), file_path(file_path) {
}

DbfReader::~DbfReader() {
}

void DbfReader::Open() {
	auto &fs = FileSystem::GetFileSystem(context);
	handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	ReadHeader();
}

void DbfReader::ReadHeader() {
	handle->Read(&header, 32);
	// Fields start at offset 32
	idx_t field_pos = 32;
	while (field_pos < header.m_uFirstRecordOffset - 1) {
		DbfRecord field_buf;
		handle->Read(&field_buf, 32);

		if (field_buf.m_archName[0] == 0x0D || field_buf.m_archName[0] == 0x1A) { // End of field descriptors
			break;
		}

		DbfField field;
		char name[12] = {0};
		std::memcpy(name, field_buf.m_archName, 11);
		field.name = std::string(name);
		field.type = static_cast<DbfType>(field_buf.chFieldType);
		field.length = field_buf.m_uLength;
		field.decimal_count = field_buf.m_uDecimalPlaces;

		fields.push_back(field);
		field_pos += 32;
	}
}

bool DbfReader::ReadNextRecord(DataChunk &output, idx_t &output_idx) {
	if (current_record >= header.m_uNumRecords) {
		return false;
	}

	// Seek to record position
	handle->Seek(header.m_uFirstRecordOffset + current_record * header.m_uRecordSize);

	uint8_t delete_flag;
	handle->Read(&delete_flag, 1);

	// '*' means deleted, ' ' means active
	bool is_deleted = (delete_flag == '*');

	std::vector<uint8_t> record_data(header.m_uRecordSize - 1);
	handle->Read(record_data.data(), header.m_uRecordSize - 1);

	if (is_deleted) {
		current_record++;
		return ReadNextRecord(output, output_idx);
	}

	idx_t offset = 0;
	for (idx_t col_idx = 0; col_idx < fields.size(); col_idx++) {
		auto &field = fields[col_idx];

		if (field.type == DbfType::INTEGER) {
			if (offset + 4 <= record_data.size()) {
				int32_t val = Load<int32_t>(record_data.data() + offset);
				output.data[col_idx].SetValue(output_idx, Value::INTEGER(val));
			} else {
				output.data[col_idx].SetValue(output_idx, Value(nullptr));
			}
			offset += 4;
			continue;
		}
		if (field.type == DbfType::DOUBLE) {
			if (offset + 8 <= record_data.size()) {
				double val = Load<double>(record_data.data() + offset);
				output.data[col_idx].SetValue(output_idx, Value::DOUBLE(val));
			} else {
				output.data[col_idx].SetValue(output_idx, Value(nullptr));
			}
			offset += 8;
			continue;
		}
		std::string raw_val(reinterpret_cast<char *>(record_data.data() + offset), field.length);
		offset += field.length;

		// Trim whitespace
		auto first = raw_val.find_first_not_of(' ');
		if (std::string::npos == first) {
			output.data[col_idx].SetValue(output_idx, Value(nullptr));
			continue;
		}
		auto last = raw_val.find_last_not_of(' ');
		std::string val = raw_val.substr(first, (last - first + 1));

		switch (field.type) {
		case DbfType::CHARACTER:
			output.data[col_idx].SetValue(output_idx, Value(val));
			break;
		case DbfType::NUMERIC:
		case DbfType::FLOAT:
			try {
				if (field.decimal_count > 0) {
					output.data[col_idx].SetValue(output_idx, Value::DOUBLE(std::stod(val)));
				} else {
					output.data[col_idx].SetValue(output_idx, Value::BIGINT(std::stoll(val)));
				}
			} catch (...) {
				output.data[col_idx].SetValue(output_idx, Value(nullptr));
			}
			break;
		case DbfType::LOGICAL:
			if (val == "T") {
				output.data[col_idx].SetValue(output_idx, Value::BOOLEAN(true));
			} else if (val == "F") {
				output.data[col_idx].SetValue(output_idx, Value::BOOLEAN(false));
			} else {
				output.data[col_idx].SetValue(output_idx, Value(nullptr));
			}
			break;
		case DbfType::DATE:
			// DBF Date is YYYYMMDD
			if (val.length() == 8) {
				try {
					int year = std::stoi(val.substr(0, 4));
					int month = std::stoi(val.substr(4, 2));
					int day = std::stoi(val.substr(6, 2));
					output.data[col_idx].SetValue(output_idx, Value::DATE(year, month, day));
				} catch (...) {
					output.data[col_idx].SetValue(output_idx, Value(nullptr));
				}
			} else {
				output.data[col_idx].SetValue(output_idx, Value(nullptr));
			}
			break;
		default:
			// to implement
			// TIMESTAMP, MEMO, BINARY
			output.data[col_idx].SetValue(output_idx, Value(nullptr));
			break;
		}
	}

	current_record++;
	output_idx++;
	return true;
}

} // namespace duckdb
