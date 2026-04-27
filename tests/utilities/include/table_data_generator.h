/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef TABLE_DATA_GENERATOR_H
#define TABLE_DATA_GENERATOR_H

#include <climits>
#include <cfloat>
#include <vector>

#include "table_data_generator_interface.h"
#include "table_operation_struct.h"
#include "tuple/dstore_tuple_struct.h"
#include "tuple/dstore_memheap_tuple.h"
#include "catalog/dstore_catalog_struct.h"
#include "index/dstore_index_interface.h"

using namespace DSTORE;

struct ColDef {
public:
    Oid type;
    bool canBeNull;
    uint64_t len;
    int64_t minValue;
    int64_t maxValue;
    bool compare(ColDef item)
    {
        if (type == item.type &&
            canBeNull == (item.canBeNull > 0 ? true : false) &&
            minValue == item.minValue &&
            maxValue == item.maxValue &&
            len == item.len) {
                return true;
        }
        return false;
    }
    ColDef &operator=(const ColDef& colDefObj)
    {
        if (this == &colDefObj) {
            return *this;
        }
        type = colDefObj.type;
        canBeNull = colDefObj.canBeNull;
        minValue = colDefObj.minValue;
        maxValue = colDefObj.maxValue;
        len = colDefObj.len;
        return *this;
    }
};

struct TestTuple {
    Datum* values;
    bool* isNulls;
};
enum RETSTATUS {
    SUCCESS,
    PARAM_ERROR,
    CONDITION_CONFLICT
};

static const uint32_t defaultColNum = 15;
static const uint32_t defaultRowNum = 3;
class TableDataGenerator : public TableDataGeneratorInterface
{
private:
    /* data */
    TableInfo m_tableInfo;
    ColDef* m_colDefs = nullptr;
    uint32_t m_colNum = 0;
    uint32_t m_rowNum = 0;
    uint32_t m_initRowNum = 0;
    bool m_created = false;
    bool m_colDefsAllocated = false;
    TupleDesc m_tupleDesc = nullptr;
    TestTuple *m_testTuples = nullptr;
public:
    TableDataGenerator(uint32_t colNum = 0, uint32_t rowNum = 0);
    TableDataGenerator(TupleDesc tupleDesc, ColDef* colDefs, uint32_t colNum);
    TableDataGenerator(ColDef *colDefs, uint32_t colNum);
    TableDataGenerator(const char* tableName, ColDef *colDefs, uint32_t colNum);
    ~TableDataGenerator() override;
    
    bool Create(Oid colTypes[], uint32_t colNum = defaultColNum) override;
    bool Describe(uint32_t colNum, Rule rule, int64_t value = 0) override;
    bool GenerateData(uint32_t rowNum = defaultRowNum) override;
    TestTuple *GenerateDataAndGetTestTuples(uint32_t rowNum = defaultRowNum);
    TestTuple *GenerateDataAndGetTestTuples(uint32_t rowNum, ColumnDesc *columns, uint32_t colNum);
    ColDef* FindColDefInGlobalTable(Oid type);

    RETSTATUS InsertRow(Datum *values, bool *isNull, uint32_t colNum);
    void Reset();
    uint32_t GetColNum() {return m_colNum;}
    uint32_t GetRowNum() {return m_rowNum;}
    ColDef *GetColDefs() {return m_colDefs;}
    RETSTATUS GetRow(uint32_t rowIndex, TestTuple &row);
    TestTuple *GetTestTuples() {return m_testTuples;}
    HeapTuple *GetHeapTuple(Relation relation, uint32_t rowIndex);
    IndexTuple *GetIndexTuple(Relation relation, uint32_t rowIndex);

    void GenerateColDefByType(uint32_t colIndex);

    void GenerationTableInfo();
    void GenerationIndexTableInfo(const TableInfo &heapDesc);
    inline TableInfo GetTableInfo() {
        return m_tableInfo;
    }

    static char *GenerateIndexName(const char *tableName, const uint32_t *colIndex, uint8_t colNum);
    static void GenerateColName(int colIndex, char *colName);

private:
    bool p_isVarLenType(Oid type);
    Datum p_copyVarChar(Datum value);
    void p_generateRow(uint32_t rowIndex);
    Datum p_generateColValue(uint32_t colIndex);
    bool p_generateColName(uint32_t colIndex, char *nameData, uint32_t len);
    ColDef p_generateColDefByType(Oid colType);//, uint32_t colIndex);
    int64_t p_getMinVal(Oid type, int64_t value);
    int64_t p_getMaxVal(Oid type, int64_t value);
    int64_t p_getLimitedVal(Oid type, int64_t value, bool isMinLimited = true);
    TupleDesc p_generateHeapTupleDesc(Relation relation);
    void *p_generateRandomDatumPtr(Oid typeOid, uint64_t len);
};

#endif
