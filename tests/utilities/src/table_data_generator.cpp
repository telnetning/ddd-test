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

#include <climits>
#include <cstdint>
#include "table_data_generator.h"
#include "heap/dstore_heap_interface.h"

#include "catalog/dstore_typecache.h"
#include "framework/dstore_instance.h"
#include "catalog/dstore_fake_type.h"
#include "common/dstore_common_utils.h"
const int TYPENUM = 15;
ColDef colDefGlobalTable[TYPENUM] = {
    {BOOLOID, true, 0, 0, 1},
    {INT2OID, true, 0, SHRT_MIN, SHRT_MAX},
    {INT4OID, true, 0, INT_MIN, INT_MAX},
    {INT8OID, true, 0, LLONG_MIN, LLONG_MAX},
    {CHAROID, true, 0, CHAR_MIN, CHAR_MAX},
    {OIDOID, true, 0, 0, UINT_MAX},
    {TIMESTAMPOID, true, 0, 0, LLONG_MAX},
    {TIMESTAMPTZOID, true, 0, 0, LLONG_MAX},
    {FLOAT4OID, true, 0, (int64_t)FLT_MIN, (int64_t)FLT_MAX},
    {FLOAT8OID, true, 0, (int64_t)DBL_MIN, (int64_t)DBL_MAX},
    {VARCHAROID, true, 0, 0, 0},
    {TEXTOID, true, 0, 0, 0},
    {OIDVECTOROID, true, 0, 0, 0},
    {INT2VECTOROID, true, 0, 0, 0},
    {ANYARRAYOID, true, 0, 0, 0}
};


TableDataGenerator::TableDataGenerator(uint32_t colNum, uint32_t rowNum) :
    m_colNum(colNum),
    m_rowNum(rowNum)
{
    m_tableInfo.Initialize();
}
TableDataGenerator::TableDataGenerator(TupleDesc tupleDesc, ColDef *colDefs, uint32_t colNum) :
    m_colDefs(colDefs),
    m_colNum(colNum),
    m_tupleDesc(tupleDesc)
{
    m_created = true;
    if (colDefs != nullptr && colNum != 0) {
        m_colDefs = (ColDef *)DstorePalloc(colNum * sizeof(ColDef));
        StorageAssert(m_colDefs != nullptr);
        int rc = memcpy_s(m_colDefs, colNum * sizeof(ColDef), colDefs, colNum * sizeof(ColDef));
        storage_securec_check(rc, "\0", "\0");       
    }
    m_tableInfo.Initialize();
}
TableDataGenerator::TableDataGenerator(ColDef *colDefs, uint32_t colNum)
    : m_colDefs(nullptr), m_colNum(colNum), m_tupleDesc(nullptr)
{
    m_created = true;
    if (colDefs != nullptr && colNum != 0) {
        m_colDefs = (ColDef *)DstorePalloc(colNum * sizeof(ColDef));
        StorageAssert(m_colDefs != nullptr);
        int rc = memcpy_s(m_colDefs, colNum * sizeof(ColDef), colDefs, colNum * sizeof(ColDef));
        storage_securec_check(rc, "\0", "\0");      
    }
    m_tableInfo.Initialize();
}
TableDataGenerator::TableDataGenerator(const char* tableName, ColDef *colDefs, uint32_t colNum)
{
    m_tableInfo.Initialize();
    if(m_tableInfo.relation.name == nullptr) {
        m_tableInfo.relation.name = (char *)DstorePalloc(NAME_MAX_LEN);
        StorageAssert(m_tableInfo.relation.name != nullptr);
    }
    errno_t rc = memcpy_s(m_tableInfo.relation.name, strlen(tableName) + 1, tableName, strlen(tableName) + 1);
    storage_securec_check(rc, "\0", "\0");
    m_tableInfo.relation.attrNum = colNum;
    if (colDefs != nullptr && colNum != 0) {
        m_colDefs = (ColDef *)DstorePalloc(colNum * sizeof(ColDef));
        StorageAssert(m_colDefs != nullptr);

        int rc = memcpy_s(m_colDefs, colNum * sizeof(ColDef), colDefs, colNum * sizeof(ColDef));
        storage_securec_check(rc, "\0", "\0");

        m_tableInfo.colDesc = (ColumnDesc *)DstorePalloc(colNum * sizeof(ColumnDesc));
        StorageAssert(m_tableInfo.colDesc != nullptr);

        for (uint32_t i = 0; i < colNum; i++) {
            GenerateColDefByType(i);
            m_tableInfo.colDesc[i].type = m_colDefs[i].type;
            m_tableInfo.colDesc[i].canBeNull = m_colDefs[i].canBeNull;
            m_tableInfo.colDesc[i].len = m_colDefs[i].len;
        }
    }
}
TableDataGenerator::~TableDataGenerator()
{
    Reset();
    if (m_colDefsAllocated && m_colDefs != nullptr) {
        delete [] m_colDefs;
    }
    if (m_tableInfo.relation.name != nullptr) {
        DstorePfreeExt(m_tableInfo.relation.name);
    }

    if (m_tableInfo.colDesc != nullptr) {
        DstorePfreeExt(m_tableInfo.colDesc);
        DstorePfreeExt(m_colDefs);
    }
}

void TableDataGenerator::Reset()
{
    if (m_testTuples == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < m_rowNum; i++) {
        if (m_testTuples[i].values == nullptr) {
            continue;
        }
        for (uint32_t j = 0; j < m_colNum; j++) {
            if (!p_isVarLenType(m_colDefs[j].type)) {
                continue;
            }
            char *tmpPtr = DatumGetPointer(m_testTuples[i].values[j]);
            if (tmpPtr != nullptr) {
                free(tmpPtr);
                tmpPtr = nullptr;
            }
        }
        delete [] m_testTuples[i].values;
        m_testTuples[i].values = nullptr;
        if (m_testTuples[i].isNulls != nullptr) {
            delete [] m_testTuples[i].isNulls;
            m_testTuples[i].isNulls = nullptr;
        }
    }
    delete [] m_testTuples;
    m_testTuples = nullptr;
    m_rowNum = 0;
}

bool TableDataGenerator::Create(Oid colTypes[], uint32_t colNum)
{
    m_colNum = colNum;
    m_colDefs = new ColDef[m_colNum];
    if (m_colDefs == nullptr) {
        return false;
    }
    m_colDefsAllocated = true;
    for (uint32_t i = 0; i < m_colNum; i++) {
        m_colDefs[i] = p_generateColDefByType(colTypes[i]);
        if (m_colDefs[i].type == 0) {
            return false;
        }
    }
    m_created = true;
    return true;
}

RETSTATUS TableDataGenerator::InsertRow(Datum *values, bool *isNull, uint32_t colNum)
{
    if (values == nullptr || isNull == nullptr || colNum != m_colNum) {
        return PARAM_ERROR;
    }
    for (uint32_t i = 0; i < colNum; i++) {
        if (!m_colDefs[i].canBeNull && isNull[i]) {
            return CONDITION_CONFLICT;
        }
    }
    TestTuple tmpTestTuple;
    tmpTestTuple.values = new Datum[m_colNum];
    tmpTestTuple.isNulls = new bool[m_colNum];
    for (uint32_t i = 0; i < colNum; i++) {
        tmpTestTuple.isNulls[i] = isNull[i];
        if (isNull[i]) {
            tmpTestTuple.values[i] = 0;
        } else if (p_isVarLenType(m_colDefs[i].type)) {
            tmpTestTuple.values[i] = p_copyVarChar(values[i]);
        } else {
            tmpTestTuple.values[i]= values[i];
        }
    }
    //m_testTuples.push_back(tmpTestTuple);
    m_rowNum++;
    return SUCCESS;
}

RETSTATUS TableDataGenerator::GetRow(uint32_t rowIndex, TestTuple &row)
{
    if (rowIndex >= m_rowNum) {
        return PARAM_ERROR;
    }
    row = m_testTuples[rowIndex];
    return SUCCESS;
}

bool TableDataGenerator::GenerateData(uint32_t rowNum)
{
    if (!m_created || rowNum == 0) {
        return false;
    }
    m_rowNum += rowNum;
    m_testTuples = new TestTuple[m_rowNum];
    uint32_t colNum = m_colNum;
    if (m_tupleDesc) {
        colNum = m_tupleDesc->natts;
    }
    if (colNum == 0) {
        return false;
    }
    for (uint32_t i = 0; i < m_rowNum; i++) {
        m_testTuples[i].values = new Datum[colNum];
        m_testTuples[i].isNulls = new bool[colNum];
        p_generateRow(i);
    }
    return true;
}
TestTuple* TableDataGenerator::GenerateDataAndGetTestTuples(uint32_t rowNum, ColumnDesc *columns, uint32_t colNum)
{
    if (colNum == 0 || columns == nullptr) {
        return nullptr;
    }
    m_created = true;
    m_colNum = colNum;
    m_colDefs = (ColDef *)DstorePalloc(colNum * sizeof(ColDef));
    for(uint32_t i = 0; i < colNum; ++i) {
        m_colDefs[i].type = columns[i].type;
        if (columns[i].len <= 1) {
            m_colDefs[i].len = 0;
        } else {
            m_colDefs[i].len = columns[i].len;
        }
        m_colDefs[i].canBeNull = columns[i].canBeNull;
        ColDef* colDef = FindColDefInGlobalTable(columns[i].type);
        StorageAssert(colDef != nullptr);
        m_colDefs[i].minValue = colDef->minValue;
        m_colDefs[i].maxValue = colDef->maxValue;
    }

    if (GenerateData(rowNum)) {
        return GetTestTuples();
    }
    return nullptr;
}

ColDef* TableDataGenerator::FindColDefInGlobalTable(Oid type)
{
    for(int i = 0; i < TYPENUM; ++i) {
        if(colDefGlobalTable[i].type == type) {
            return &colDefGlobalTable[i];
        }
    }
    return nullptr;
}

TestTuple *TableDataGenerator::GenerateDataAndGetTestTuples(uint32_t rowNum)
{
    if(GenerateData(rowNum)){
        return GetTestTuples();
    }
    return nullptr;
}

HeapTuple *TableDataGenerator::GetHeapTuple(Relation relation, uint32_t rowIndex)
{
    if (rowIndex >= m_rowNum || m_testTuples == nullptr) {
        return nullptr;
    }
    HeapTuple *heapTuple = TupleInterface::FormHeapTuple(relation->attr, m_testTuples[rowIndex].values,
                                                         m_testTuples[rowIndex].isNulls, nullptr);
    return heapTuple;
}

IndexTuple *TableDataGenerator::GetIndexTuple(Relation relation, uint32_t rowIndex)
{
    if (rowIndex >= m_rowNum || m_testTuples == nullptr) {
        return nullptr;
    }
    TupleDesc desc = p_generateHeapTupleDesc(relation);
    IndexTuple *indexTuple =
        TupleInterface::FormIndexTuple(desc, m_testTuples[rowIndex].values, m_testTuples[rowIndex].isNulls);
    return indexTuple;
}
bool TableDataGenerator::p_isVarLenType(Oid type)
{
    if (type == VARCHAROID || type == TEXTOID ||
        type == OIDVECTOROID || type == INT2VECTOROID ||
        type == ANYARRAYOID) {
        return true;
    }
    return false;
}
Datum TableDataGenerator::p_copyVarChar(Datum value)
{
    char *tmpPtr = DatumGetCString(value);
    uint64_t len = strlen(tmpPtr) + 1;
    text *textPtr = (text*) malloc(sizeof(varlena) + (len + 1) * sizeof(char));
    DstoreSetVarSize(textPtr, VARHDRSZ + (len + 1)* sizeof(char));
    for (uint64_t i = 0; i < len; i++) {
        textPtr->vl_dat[i] = tmpPtr[i];
    }
    textPtr->vl_dat[len] = '\0';
    return PointerGetDatum(textPtr);
}

void TableDataGenerator::p_generateRow(uint32_t rowIndex)
{
    for (uint32_t i = 0; i < m_colNum; i++) {
        m_testTuples[rowIndex].isNulls[i] = false;
        m_testTuples[rowIndex].values[i] = p_generateColValue(i);
    }
}

Datum TableDataGenerator::p_generateColValue(uint32_t colIndex)
{
    Oid type = m_colDefs[colIndex].type;
    uint32_t seed = time(NULL);
    int64_t random = rand_r(&seed);
    int64_t value = random;
    int64_t minVal = m_colDefs[colIndex].minValue;
    int64_t maxVal = m_colDefs[colIndex].maxValue;
    if (value > maxVal || value < minVal) {
        value = (random % (maxVal - minVal + 1)) + minVal;
    }
    switch(type) {
        case CHAROID:
            return Int32GetDatum((int32_t) ((uint8_t) random % 128));
        case INT2OID:
            return Int16GetDatum(value);
        case INT4OID:
        case BOOLOID:
            return Int32GetDatum(value);
        case INT8OID:
            return Int64GetDatum(value);
        case OIDOID:
            return ObjectIdGetDatum(value);
        case TIMESTAMPOID:
            return TimestampGetDatum(value);
        case TIMESTAMPTZOID:
            return TimestampTzGetDatum(value);
        case FLOAT4OID:
            return Float32GetDatum(value);
        case FLOAT8OID:
            return Float64GetDatum(value);
        case VARCHAROID:
        case TEXTOID:
        case INT2VECTOROID:
        case OIDVECTOROID:
        case ANYARRAYOID: {
            uint64_t len = m_colDefs[colIndex].len;
            if (len == 0) {
                uint64_t arrayLen = 16;
                len = random % (arrayLen - 1) + 1;
            }
            return PointerGetDatum(p_generateRandomDatumPtr(type, len));
        }
        default:
            StorageAssert(0);
    }
    return Datum(0);
}

bool TableDataGenerator::p_generateColName(uint32_t colIndex, char *nameData, uint32_t len)
{
    int rc = sprintf_s(nameData, len, "%s_%d", nameData, colIndex);
    if (rc == -1) {
        return false;
    }
    return true;
}

ColDef TableDataGenerator::p_generateColDefByType(Oid colType)
{
    ColDef res;
    res.type = 0;
    for (int i = 0; i < TYPENUM; i++) {
        if (colType == colDefGlobalTable[i].type) {
            res = colDefGlobalTable[i];
            break;
        }
    }
    return res;
}

bool TableDataGenerator::Describe(uint32_t colIndex, Rule rule, int64_t value)
{
    if (m_colDefs == nullptr || colIndex > m_colNum) {
        return false;
    }
    Oid type = m_colDefs[colIndex].type;
    switch(rule) {
        case NotNull:
            m_colDefs[colIndex].canBeNull = false;
            break;
        case SetMinVal:
            m_colDefs[colIndex].minValue = p_getLimitedVal(type, value, true);
            break;
        case SetMaxVal:
            m_colDefs[colIndex].maxValue = p_getLimitedVal(type, value, false);
            break;
        case SetLen:
            m_colDefs[colIndex].len = value;
    }
    return true;
}

int64_t TableDataGenerator::p_getLimitedVal(Oid type, int64_t value, bool isMinLimited)
{
    ColDef tmp;
    int64_t minValue, maxValue;
    for (int i = 0; i < TYPENUM; i++) {
        if (type == colDefGlobalTable[i].type) {
            tmp = colDefGlobalTable[i];
            break;
        }
    }
    minValue = tmp.minValue;
    maxValue = tmp.maxValue;
    if (value < minValue || value > maxValue) {
        if (isMinLimited) {
            return minValue;
        }
        return maxValue;
    }
    return value;
}

TupleDesc TableDataGenerator::p_generateHeapTupleDesc(Relation relation)
{
    Size descSize = MAXALIGN((uint32_t)(sizeof(TupleDescData) + m_colNum * sizeof(Form_pg_attribute)));
    Size attrsSize = m_colNum * MAXALIGN((uint32_t)sizeof(FormData_pg_attribute));
    char *offset = (char *)DstorePalloc0(descSize + attrsSize);
    TupleDesc desc = (TupleDesc)offset;

    desc->natts = m_colNum;
    desc->tdisredistable = false;
    desc->attrs = (Form_pg_attribute *)(offset + sizeof(TupleDescData));
    desc->initdefvals = nullptr;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = false;
    desc->tdrefcount = -1;
    desc->tdhasuids = false;

    offset += descSize;
    if (relation != nullptr) {
        relation->attr = desc;
    }
    return desc;
}

void *TableDataGenerator::p_generateRandomDatumPtr(Oid typeOid, uint64_t len)
{
    switch (typeOid) {
        case VARCHAROID:
        case TEXTOID: {
            text *textPtr = (text*) malloc(sizeof(varlena) + (len + 1) * sizeof(char));
            DstoreSetVarSize(textPtr, VARHDRSZ + (len + 1)* sizeof(char));
            uint32_t seed = time(NULL);
            for (uint64_t i = 0; i < len; i++) {
                textPtr->vl_dat[i] = ('A' + rand_r(&seed) % 26);
            }
            textPtr->vl_dat[len] = '\0';
            return textPtr;
        }
        case OIDVECTOROID: {
            uint64_t size = sizeof(OidVector) + len * sizeof(Oid);
            OidVector *oidvectorPtr = (OidVector *) malloc(size);
            DstoreSetVarSize(oidvectorPtr, size);
            oidvectorPtr->elemtype = OIDOID;
            oidvectorPtr->dim1 = len;
            oidvectorPtr->ndim = 1;
            oidvectorPtr->dataoffset = 0;
            for (uint64_t i = 0; i < len; i++) {
                oidvectorPtr->values[i] = (uint32_t)rand();
            }
            return oidvectorPtr;
        }
        case INT2VECTOROID: {
            uint64_t size = sizeof(int2vector) + len * sizeof(int16);
            int2vector *int2vectorPtr = (int2vector *) malloc(size);
            DstoreSetVarSize(int2vectorPtr, size);
            int2vectorPtr->elemtype = INT2OID;
            int2vectorPtr->dim1 = len;
            int2vectorPtr->ndim = 1;
            int2vectorPtr->dataoffset = 0;
            for (uint64_t i = 0; i < len; i++) {
                int2vectorPtr->values[i] = Int16GetDatum(rand());
            }
            return int2vectorPtr;
        }
        case ANYARRAYOID: {
            /* Create a new int array with room for "num" elements */
            ArrayType* arrayTypeP = NULL;
            uint64_t nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(int) * len;
            arrayTypeP = (ArrayType*)malloc(nbytes);

            DstoreSetVarSize(arrayTypeP, nbytes);
            ARR_NDIM(arrayTypeP) = 1;
            arrayTypeP->dataoffset = 0; /* marker for no null bitmap */
            ARR_ELEMTYPE(arrayTypeP) = INT4OID;
            ARR_DIMS(arrayTypeP)[0] = len;
            ARR_LBOUND(arrayTypeP)[0] = 1;
            for (uint64_t i = 0; i < len; i++) {
                ARR_DATA_PTR(arrayTypeP)[i] = rand();
            }
            return arrayTypeP;
        }
        default:
            Datum *tmpPtr= (Datum *)malloc(VARHDRSZ);
            errno_t rc = memset_s(tmpPtr, VARHDRSZ, 0, VARHDRSZ);
            storage_securec_check(rc, "\0", "\0");
            return tmpPtr;
    }
}
void TableDataGenerator::GenerateColDefByType(uint32_t colIndex)
{
    if (m_colDefs != nullptr) {
        Oid colType = m_colDefs[colIndex].type;
        if (colType == INT4OID) {
            m_colDefs[colIndex].minValue = INT_MIN;
            m_colDefs[colIndex].maxValue = INT_MAX;
        } else if (colType == CHAROID) {
            m_colDefs[colIndex].minValue = 0;
            m_colDefs[colIndex].maxValue = 128;
        } else if (colType == BOOLOID) {
            m_colDefs[colIndex].minValue = 0;
            m_colDefs[colIndex].maxValue = 1;
        } else if (colType == TEXTOID) {
        }
    }
}
void TableDataGenerator::GenerationTableInfo()
{
    m_tableInfo.relation.persistenceLevel = SYS_RELPERSISTENCE_PERMANENT;
    m_tableInfo.relation.partHeapOid = DSTORE_INVALID_OID;
    m_tableInfo.relation.parentId = DSTORE_INVALID_OID;
    m_tableInfo.relation.partType = SYS_PARTTYPE_NON_PARTITIONED_RELATION;
    m_tableInfo.relation.relKind = SYS_RELKIND_RELATION;

    TypeCache typeCache;
    for (int i = 0; i < m_tableInfo.relation.attrNum; i++) {
        typeCache = g_storageInstance->GetCacheHashMgr()->GetTypeCacheFromTypeOid(m_tableInfo.colDesc[i].type);
        GenerateColName(i, typeCache.name);
        errno_t rc = memcpy_s(m_tableInfo.colDesc[i].name, NAME_MAX_LEN, typeCache.name, NAME_MAX_LEN);
        storage_securec_check(rc, "\0", "\0");

        m_tableInfo.colDesc[i].canBeNull = false;
        m_tableInfo.colDesc[i].len = typeCache.attlen;
        m_tableInfo.colDesc[i].storageType = 'p';
        m_tableInfo.colDesc[i].isByVal = typeCache.attbyval;
        m_tableInfo.colDesc[i].align = typeCache.attalign;
        m_tableInfo.colDesc[i].isHaveDefVal = false;
    }
}
void TableDataGenerator::GenerationIndexTableInfo(const TableInfo &heapTable)
{
    m_tableInfo.Initialize();
    m_tableInfo.relation.name =
        GenerateIndexName(heapTable.relation.name, heapTable.indexDesc->indexCol, heapTable.indexDesc->indexAttrNum);

    m_tableInfo.relation.persistenceLevel = SYS_RELPERSISTENCE_PERMANENT;
    m_tableInfo.relation.partHeapOid = DSTORE_INVALID_OID;
    m_tableInfo.relation.parentId = DSTORE_INVALID_OID;
    m_tableInfo.relation.partType = SYS_PARTTYPE_NON_PARTITIONED_RELATION;
    m_tableInfo.relation.relKind = SYS_RELKIND_INDEX;
    m_tableInfo.relation.attrNum = heapTable.indexDesc->indexAttrNum;
    m_tableInfo.indexDesc = heapTable.indexDesc;
    StorageAssert(heapTable.indexDesc != nullptr);

    if (heapTable.indexDesc->indexAttrNum != 0) {
        m_tableInfo.colDesc = (ColumnDesc *)DstorePalloc(heapTable.indexDesc->indexAttrNum * sizeof(ColumnDesc));
        for (uint32_t i = 0; i < heapTable.indexDesc->indexAttrNum; i++) {
            m_tableInfo.colDesc[i] = heapTable.colDesc[heapTable.indexDesc->indexCol[i]];
        }
    }
}

char *TableDataGenerator::GenerateIndexName(const char *tableName, const uint32_t *colIndex, uint8_t colNum)
{
    char *indexName = (char *)DstorePalloc0(NAME_MAX_LEN);
    char colName[NAME_MAX_LEN];
    int rc = memcpy_s(indexName, NAME_MAX_LEN, tableName, strlen(tableName) + 1);
    storage_securec_check(rc, "\0", "\0");
    if (colNum != 0) {
        rc = strcat_s(indexName, NAME_MAX_LEN, "_");
        storage_securec_check(rc, "\0", "\0");
    }
    for (uint8_t i = 0; i < colNum; i++) {
        GenerateColName(colIndex[i], colName);
        rc = strcat_s(indexName, NAME_MAX_LEN, colName);
        storage_securec_check(rc, "\0", "\0");
    }
    return indexName;
}
void TableDataGenerator::GenerateColName(int colIndex, char *colName)
{
    int rc = sprintf_s(colName, NAME_MAX_LEN, "column_%d", colIndex);
    storage_securec_check_ss(rc);
}