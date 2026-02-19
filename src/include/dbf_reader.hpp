#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include <vector>
#include <string>

namespace duckdb {
    struct DbfHeader
    {
        uint8_t m_iType;
        char m_arcLastUpdate[3];
        uint32_t m_uNumRecords;
        uint16_t m_uFirstRecordOffset;
        uint16_t m_uRecordSize;
        char m_uReserved[15];
        uint8_t m_fFlags;
        uint8_t m_uCodePageMark;
        char m_uReserved2[2];
    };

    #pragma pack(push)
    #pragma pack(1)
    struct DbfRecord
    {
        char m_archName[11];
        char chFieldType;
        uint32_t m_uDisplacement;
        uint8_t m_uLength;
        uint8_t m_uDecimalPlaces;
        uint8_t m_fFlags;
        uint32_t m_uNextValue;
        uint8_t m_uStepValue;
        char m_uReserved[8];
    };
    #pragma pack(pop)

    enum class DbfType : uint8_t {
        CHARACTER = 'C',
        NUMERIC = 'N',
        LOGICAL = 'L', // 1 byte
        DATE = 'D', // 8 bytes
        FLOAT = 'F', 
        INTEGER = 'I', // 4 bytes
        MEMO = 'M',
        TIMESTAMP = '@', // 8 bytes first 4 date and next 4 time        
        DOUBLE = 'O', // 8 bytes
        BINARY = 'B',
        INVALID = 0
    };

    struct DbfField {
        std::string name;
        DbfType type;
        uint8_t length;
        uint8_t decimal_count;
    };

class DbfReader {
public:
    DbfReader(ClientContext &context, const std::string &file_path);
    ~DbfReader();

    void Open();
    
    const std::vector<DbfField>& GetFields() const { return fields; }
    uint32_t GetRecordCount() const { return header.m_uNumRecords; }
    uint16_t GetRecordLength() const { return header.m_uRecordSize; }
    uint16_t GetHeaderLength() const { return header.m_uFirstRecordOffset; }

    bool ReadNextRecord(DataChunk &output, idx_t &output_idx);

private:
    ClientContext &context;
    std::string file_path;
    unique_ptr<FileHandle> handle;
    DbfHeader header;    
    std::vector<DbfField> fields;
    idx_t current_record = 0;

    void ReadHeader();
};

} // namespace duckdb
