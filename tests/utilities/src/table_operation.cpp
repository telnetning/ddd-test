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
 * Description: CloudNativeDatabase TableOperation(table Operation class)
 */

#include "table_operation.h"
#include "table_handler.h"
#include "common/dstore_datatype.h"

TableOperation::TableOperation(StorageInstanceInterface *instance) : m_storageInstance(instance)

{
}

TableOperation::~TableOperation()
{}

int TableOperation::CreateTable(const TableInfo &tableInfo)
{
    DstoreTableHandler tableHandler(m_storageInstance);
    return tableHandler.CreateTable(tableInfo);
}

int TableOperation::CheckTable(const TableInfo& tableInfo)
{
    int ret = 1;
    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableInfo.relation.name, nullptr);
    if (tableHandler != nullptr) {
        ret = tableHandler->CheckTable(tableInfo);
        delete tableHandler;
    }
    return ret;
}
int TableOperation::RecoveryTable(const char* tableName)
{
    if (tableName == nullptr || strlen(tableName) == 0) {
        return -1;
    }
    DstoreTableHandler tableHandler(m_storageInstance);
    return tableHandler.RecoveryTable(tableName);
}

StorageRelation TableOperation::GetTable(const char *tableName)
{
    if (tableName == nullptr || strlen(tableName) == 0) {
        return nullptr;
    }
    DstoreTableHandler tableHandler(m_storageInstance);
    StorageRelation relation = tableHandler.GetTable(tableName);
    return relation;
}
void TableOperation::DestroyRelation(StorageRelation relation)
{
    DstoreTableHandler tableHandler(m_storageInstance);
    tableHandler.DestroyRelation(relation);
}
int TableOperation::Insert(const TableInfo &tableInfo, Datum *values, bool *isNulls,
                           const char *indexName /* = nullptr */)
{
    char *tableName = tableInfo.relation.name;
    if (tableName == nullptr || strlen(tableName) == 0 || values == nullptr || isNulls == nullptr) {
        return 1;
    }
    int ret = 0;
    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexName);
    if (tableHandler != nullptr) {
        ret = tableHandler->Insert(values, isNulls, tableInfo.indexDesc->indexCol);
        delete tableHandler;
    }
    return ret;
}

int TableOperation::Delete(const TableInfo &tableInfo, const char *indexName, Datum *indexValues)
{   
    char *tableName = tableInfo.relation.name;
    if (tableName == nullptr || strlen(tableName) == 0 || indexName == nullptr || strlen(indexName) == 0 ||
        tableInfo.indexDesc->indexAttrNum == 0 || indexValues == nullptr) {
        return 1;
    }

    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexName);
    int ret = 2;
    if (tableHandler != nullptr) {
        ret = tableHandler->Delete(tableInfo.indexDesc->indexCol, indexValues, tableInfo.indexDesc->indexAttrNum);
        delete tableHandler;
    }
    return ret;
}
int TableOperation::Update(const TableInfo &tableInfo, const char *indexName, ItemPointerData* oldTupleCtid,
                           Datum *values, bool *isNulls)
{
    char *tableName = tableInfo.relation.name;
    if (tableName == nullptr || strlen(tableName) == 0 || indexName == nullptr || strlen(indexName) == 0 ||
        tableInfo.indexDesc->indexAttrNum == 0 || values == nullptr || isNulls == nullptr) {
        return 1;
    }

    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexName);
    int ret = 2;
    if (tableHandler != nullptr) {
        ret = tableHandler->Update(oldTupleCtid, tableInfo.indexDesc->indexCol, values, isNulls);
        delete tableHandler;
    }
    return ret;
}

HeapTuple *TableOperation::Scan(const TableInfo &tableInfo, const char *indexName, Datum *indexValues)
{
    char *tableName = tableInfo.relation.name;
    if (tableName == nullptr || strlen(tableName) == 0 || indexName == nullptr || strlen(indexName) == 0 ||
        tableInfo.indexDesc->indexAttrNum == 0 || indexValues == nullptr) {
        return nullptr;
    }
    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexName);

    HeapTuple *tuple = nullptr;
    if (tableHandler != nullptr) {
        tableHandler->Scan(tableInfo.indexDesc->indexCol, indexValues, &tuple, tableInfo.indexDesc->indexAttrNum);
        delete tableHandler;
    }
    return tuple;
}

int TableOperation::CreateIndex(const char *tableName, const TableInfo &indexTableInfo)
{
    if (tableName == nullptr || strlen(tableName) == 0) {
        return 1;
    }
    int ret = 2;
    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, nullptr);
    if (tableHandler != nullptr) {
        ret = tableHandler->CreateIndex(indexTableInfo);
        delete tableHandler;
    }
    return ret;
}

int TableOperation::CheckIndex(const char *tableName, const TableInfo &indexTableInfo)
{
    if (tableName == nullptr || strlen(tableName) == 0) {
        return 1;
    }
    int ret = 2;
    DstoreTableHandler *tableHandler = simulator->GetTableHandler(tableName, indexTableInfo.relation.name);
    if (tableHandler != nullptr) {
        ret = tableHandler->CheckIndex(indexTableInfo);
        delete tableHandler;
    }
    return ret;
}

StorageRelation TableOperation::GetIndex(const char *indexName)
{
    StorageAssert(indexName != nullptr);
    StorageAssert(strlen(indexName) != 0);
    DstoreTableHandler tableHandler(m_storageInstance);
    StorageRelation relation = tableHandler.GetIndex(indexName);
    return relation;
}