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

#ifndef TABLE_OPERATION_H
#define TABLE_OPERATION_H
#include "table_operation_interface.h"
#include "framework/dstore_instance_interface.h"

using namespace DSTORE;

class TableOperation : public TableOperationInterface {
public:
    TableOperation(StorageInstanceInterface *instance);
    virtual ~TableOperation();

    int CreateTable(const TableInfo& tableInfo);
    int CheckTable(const TableInfo& tableInfo);

    int RecoveryTable(const char* tableName);
    StorageRelation GetTable(const char* tableName);
    void DestroyRelation(StorageRelation relation);

    int Insert(const TableInfo &tableInfo, Datum *values, bool *isNulls, const char *indexName = nullptr);

    int Delete(const TableInfo &tableInfo, const char *indexName, Datum *indexValues);
    int Update(const TableInfo &tableInfo, const char *indexName, ItemPointerData *oldTupleCtid, Datum *values,
               bool *isNulls);

    HeapTuple *Scan(const TableInfo &tableInfo, const char *indexName, Datum *indexValues);
    
    int CreateIndex(const char *tableName, const TableInfo &indexTableInfo);
    int CheckIndex(const char *tableName, const TableInfo &indexTableInfo);
    StorageRelation GetIndex(const char *indexName);

private:
    StorageInstanceInterface* m_storageInstance;
};

#endif