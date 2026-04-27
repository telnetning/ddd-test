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
 *
 * Description: CloudNativeDatabase UTTableOperation(ut table Operate class)
 */

#include "ut_tablehandler/ut_table_operation.h"
#include "framework/dstore_instance_interface.h"
#include "table_operation.h"
#include "table_handler.h"
#include "table_data_generator.h"
#include "transaction/dstore_transaction_interface.h"
#include "common/datatype/dstore_varlena_utils.h"
namespace DSTORE {

UTTableOperation::UTTableOperation(StorageInstanceInterface* instance)
    : m_instance(instance),m_tableOperation(new TableOperation(instance))
{}

UTTableOperation::~UTTableOperation()
{}

void UTTableOperation::Initialize()
{
    simulator = new StorageTableContext(m_instance, 1);
}

void UTTableOperation::Destroy() noexcept
{
    if (simulator != nullptr) {
        simulator->Destory();
        delete simulator;
        simulator = nullptr;
    }
    m_instance = nullptr;
    if(m_tableOperation != nullptr) {
        delete m_tableOperation;
        m_tableOperation = nullptr;
    }
}

int UTTableOperation::CreateTable(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    return m_tableOperation->CreateTable(tableInfo);
}
int UTTableOperation::CreateAllTable()
{
    int ret = 0;
    for (TableNameType i = TABLE_1; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        TableInfo tableInfo = TABLE_CACHE[i];
        ret = m_tableOperation->CreateTable(tableInfo);
        if (ret != 0) {
            break;
        }
    }
    return ret;
}

StorageRelation UTTableOperation::GetTable(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return nullptr;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    StorageRelation relation = m_tableOperation->GetTable(tableInfo.relation.name);
    return relation;
}

void UTTableOperation::DestroyRelation(StorageRelation relation)
{
    m_tableOperation->DestroyRelation(relation);
}

int UTTableOperation::RecoveryAllTable()
{
    DstoreTableHandler tableHandler(m_instance);
    for (TableNameType i = TABLE_1; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        TableInfo tableInfo = TABLE_CACHE[i];
        int ret = m_tableOperation->RecoveryTable(tableInfo.relation.name);
        StorageAssert(ret == 0);
    }
    return 0;
}
int UTTableOperation::CheckTable(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    return m_tableOperation->CheckTable(tableInfo);
}
int UTTableOperation::Insert(const TableInfo &tableInfo, Datum *values, bool *isNulls,
                           const char *indexName /* = nullptr */)
{
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    int ret = m_tableOperation->Insert(tableInfo, values, isNulls, indexName);
    if (ret != 0) {
        TransactionInterface::AbortTrx();
    } else {
        TransactionInterface::CommitTrxCommand();
    }
    return ret;
}
int UTTableOperation::Insert(TableNameType type, uint32 rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    StorageAssert(rowNum != 0);
    TableInfo tableInfo = TABLE_CACHE[type];
    TableDataGenerator dataGenerator;
    TestTuple *testTuples =
        dataGenerator.GenerateDataAndGetTestTuples(rowNum, tableInfo.colDesc, tableInfo.relation.attrNum);

    int ret = 0;
    for (uint32 rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < tableInfo.indexDesc->indexAttrNum; ++colIdx) {
            testTuples[rowIdx].values[tableInfo.indexDesc->indexCol[colIdx]] =
                Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }
        ret = Insert(tableInfo, testTuples[rowIdx].values, testTuples[rowIdx].isNulls);
        if (ret != 0) {
            break;
        }
    }
    dataGenerator.Reset();
    return ret;
}

int UTTableOperation::CreateIndex(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();

    TableDataGenerator dataGenerator;
    dataGenerator.GenerationIndexTableInfo(tableInfo);
    TableInfo indexTableInfo = dataGenerator.GetTableInfo();
    int ret = m_tableOperation->CreateIndex(tableInfo.relation.name, indexTableInfo);
    if (ret != 0) {
        TransactionInterface::AbortTrx();
    } else {
        TransactionInterface::CommitTrxCommand();
    }
    return ret;
}

int UTTableOperation::CreateAllIndex()
{
    int ret = 0;
    for (TableNameType i = TABLE_1; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        ret = CreateIndex(i);
        if (ret != 0) {
            break;
        }
    }
    return ret;
}

int UTTableOperation::RecoveryAllIndex()
{
    int ret = 0;
    DstoreTableHandler tableHandler(m_instance);
    for (TableNameType i = TABLE_1; i < TABLE_MAX_CNT; i = TableNameType(i + 1)) {
        TableInfo tableInfo = TABLE_CACHE[i];
        char *indexName = TableDataGenerator::GenerateIndexName(tableInfo.relation.name, tableInfo.indexDesc->indexCol,
                                                                tableInfo.indexDesc->indexAttrNum);

        int ret = m_tableOperation->RecoveryTable(indexName);
        DstorePfreeExt(indexName);
        if (ret != 0) {
            break;
        }
    }
    return ret;
}

StorageRelation UTTableOperation::GetIndex(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return nullptr;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    char *indexName = TableDataGenerator::GenerateIndexName(tableInfo.relation.name, tableInfo.indexDesc->indexCol,
                                                            tableInfo.indexDesc->indexAttrNum);

    StorageRelation relation = m_tableOperation->GetIndex(indexName);
    DstorePfreeExt(indexName);
    return relation;
}

int UTTableOperation::CheckIndex(TableNameType type)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    TableDataGenerator generator;
    generator.GenerationIndexTableInfo(tableInfo);
    TableInfo indexTableInfo = generator.GetTableInfo();
    return m_tableOperation->CheckIndex(tableInfo.relation.name, indexTableInfo);
}
HeapTuple *UTTableOperation::Scan(const TableInfo &tableInfo, const char *indexName, Datum *indexValues)
{
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    HeapTuple *heapTuple = m_tableOperation->Scan(tableInfo, indexName, indexValues);
    if (heapTuple == nullptr) {
        TransactionInterface::AbortTrx();
    } else {
        TransactionInterface::CommitTrxCommand();
    }
    return heapTuple;
}

int UTTableOperation::Scan(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT || rowNum <= 0) {
        return 1;
    }
    int ret = 0;
    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;
    char *indexName =
        TableDataGenerator::GenerateIndexName(tableInfo.relation.name, tableInfo.indexDesc->indexCol, indexAttrNum);

    Datum indexValues[rowNum][indexAttrNum];
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }
        HeapTuple *heapTuple = Scan(tableInfo, indexName, indexValues[rowIdx]);
        if (heapTuple == nullptr) {
            ret = 1;
            break;
        }
        DstorePfreeExt(heapTuple);
    }
    DstorePfreeExt(indexName);
    return ret;
}
int UTTableOperation::InsertAndScan(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }

    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;
    char *indexName =
        TableDataGenerator::GenerateIndexName(tableInfo.relation.name, tableInfo.indexDesc->indexCol, indexAttrNum);

    TableDataGenerator dataGenerator;
    TestTuple *testTuples =
        dataGenerator.GenerateDataAndGetTestTuples(rowNum, tableInfo.colDesc, tableInfo.relation.attrNum);

    Datum indexValues[rowNum][indexAttrNum];
    
    int ret = 0;
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            testTuples[rowIdx].values[tableInfo.indexDesc->indexCol[colIdx]] =
                Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);

            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }
        ret = Insert(tableInfo, testTuples[rowIdx].values, testTuples[rowIdx].isNulls, indexName);

        StorageAssert(ret == 0);
        HeapTuple *heapTuple = Scan(tableInfo, indexName, indexValues[rowIdx]);
        if (heapTuple == nullptr) {
            ret = 2;
            break;
        }

        CheckData(tableInfo.relation.name, heapTuple, testTuples[rowIdx].values, testTuples[rowIdx].isNulls);
        DstorePfreeExt(heapTuple);
    }
    DstorePfreeExt(indexName);
    dataGenerator.Reset();
    return ret;
}
int UTTableOperation::Delete(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;

    char *tableName = tableInfo.relation.name;
    char *indexName = TableDataGenerator::GenerateIndexName(tableName, tableInfo.indexDesc->indexCol, indexAttrNum);

    int ret = 0;
    Datum indexValues[rowNum][indexAttrNum];
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }
        ret = Delete(tableInfo, indexName, indexValues[rowIdx]);
        if (ret != 0) {
           break;
        }
    }

    DstorePfreeExt(indexName);
    return ret;
}

int UTTableOperation::DeleteAndCheck(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;

    char *tableName = tableInfo.relation.name;
    char *indexName = TableDataGenerator::GenerateIndexName(tableName, tableInfo.indexDesc->indexCol, indexAttrNum);

    int ret = 0;
    Datum indexValues[rowNum][indexAttrNum];
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }

        ret = Delete(tableInfo, indexName, indexValues[rowIdx]);
        if (ret != 0) {
           break;
        }
        HeapTuple *heapTuple = Scan(tableInfo, indexName, indexValues[rowIdx]);
        StorageAssert(heapTuple == nullptr);
    }

    DstorePfreeExt(indexName);
    return ret;
}
int UTTableOperation::Delete(const TableInfo &tableInfo, const char *indexName, Datum *indexValues)
{
    char *tableName = tableInfo.relation.name;
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    int ret = m_tableOperation->Delete(tableInfo, indexName, indexValues);
    if (ret == 0) {
        TransactionInterface::CommitTrxCommand();
    } else {
        TransactionInterface::AbortTrx();
    }
    return ret;
}

int UTTableOperation::Update(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;
    char *tableName = tableInfo.relation.name;
    char *indexName = TableDataGenerator::GenerateIndexName(tableName, tableInfo.indexDesc->indexCol, indexAttrNum);

    TableDataGenerator dataGenerator;
    TestTuple *testTuples =
        dataGenerator.GenerateDataAndGetTestTuples(rowNum, tableInfo.colDesc, tableInfo.relation.attrNum);

    int ret = 0;
    Datum indexValues[rowNum][indexAttrNum];
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            testTuples[rowIdx].values[tableInfo.indexDesc->indexCol[colIdx]] =
                Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);

            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }

        ret = Update(tableInfo, indexName, testTuples[rowIdx].values, testTuples[rowIdx].isNulls, indexValues[rowIdx]);
        if (ret != 0) {
            break;
        }
    }
    DstorePfreeExt(indexName);
    dataGenerator.Reset();
    return ret;
}

int UTTableOperation::UpdateAndCheck(TableNameType type, int rowNum)
{
    if (type < TABLE_1 || type >= TABLE_MAX_CNT) {
        return 1;
    }
    TableInfo tableInfo = TABLE_CACHE[type];
    int indexAttrNum = tableInfo.indexDesc->indexAttrNum;
    char *tableName = tableInfo.relation.name;
    char *indexName = TableDataGenerator::GenerateIndexName(tableName, tableInfo.indexDesc->indexCol, indexAttrNum);

    TableDataGenerator dataGenerator;
    TestTuple *testTuples =
        dataGenerator.GenerateDataAndGetTestTuples(rowNum, tableInfo.colDesc, tableInfo.relation.attrNum);

    int ret = 0;
    Datum indexValues[rowNum][indexAttrNum];
    for (int rowIdx = 0; rowIdx < rowNum; ++rowIdx) {
        for (uint8 colIdx = 0; colIdx < indexAttrNum; ++colIdx) {
            testTuples[rowIdx].values[tableInfo.indexDesc->indexCol[colIdx]] =
                Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);

            indexValues[rowIdx][colIdx] = Int32GetDatum((rowIdx * rowNum) + tableInfo.indexDesc->indexCol[colIdx]);
        }

        ret = Update(tableInfo, indexName, testTuples[rowIdx].values, testTuples[rowIdx].isNulls, indexValues[rowIdx]);
        if (ret != 0) {
            break;
        }

        HeapTuple *heapTuple = Scan(tableInfo, indexName, indexValues[rowIdx]);
        StorageAssert(heapTuple != nullptr);

        CheckData(tableInfo.relation.name, heapTuple, testTuples[rowIdx].values, testTuples[rowIdx].isNulls);
        DstorePfreeExt(heapTuple);
    }
    DstorePfreeExt(indexName);
    dataGenerator.Reset();
    return ret;
}

int UTTableOperation::Update(const TableInfo &tableInfo, const char *indexName, Datum *values, bool *isNulls,
                             Datum *indexValues)
{
    HeapTuple *heapTuple = Scan(tableInfo, indexName, indexValues);
    if(heapTuple == nullptr) {
        return -1;
    }
    StorageAssert(heapTuple != nullptr);
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    int ret = m_tableOperation->Update(tableInfo, indexName, heapTuple->GetCtid(), values, isNulls);
    DstorePfreeExt(heapTuple);
    if (ret == 0) {
        TransactionInterface::CommitTrxCommand();
    } else {
        TransactionInterface::AbortTrx();
    }

    return ret;
}

void UTTableOperation::CheckData(const char* tableName, HeapTuple *heapTuple, Datum *originalValues, bool *originalIsNulls)
{
    StorageRelation relation = m_tableOperation->GetTable(tableName);
    StorageAssert(relation != nullptr);
    Datum values[relation->attr->natts];
    bool isNulls[relation->attr->natts];

    heapTuple->DeformTuple(relation->attr, values, isNulls);
    
    for (int colIdx = 0; colIdx < relation->attr->natts; ++colIdx) {
        Datum testData = originalValues[colIdx];
        Datum value = values[colIdx];
        StorageAssert(isNulls[colIdx] == originalIsNulls[colIdx]);
        if (relation->attr->attrs[colIdx]->attlen != -1) {
            StorageAssert(value == testData);
        } else if (relation->attr->attrs[colIdx]->atttypid == TEXTOID ||
                   relation->attr->attrs[colIdx]->atttypid == VARCHAROID) {
            text *textPtr = (text *)DatumGetPointer(value);
            text *testTextPtr = (text *)DatumGetPointer(testData);

            int result = TextCmp(textPtr, testTextPtr, DEFAULT_COLLATION_OID);
            StorageAssert(result == 0);
        }
    }
    m_tableOperation->DestroyRelation(relation);
}
}