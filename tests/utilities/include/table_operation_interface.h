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

 * Description: CloudNativeDatabase TableOperationInterface(table Operation interface)
 */

#ifndef TABLE_OPERATION_INTERFACE_H
#define TABLE_OPERATION_INTERFACE_H
#include "catalog/dstore_catalog_struct.h"
#include "page/dstore_itemptr.h"
#include "table_operation_struct.h"
#include "tuple/dstore_memheap_tuple.h"
#include "systable/dstore_relation.h"
#include <cstdint>

using namespace DSTORE;

class TableOperationInterface {
public:
    virtual ~TableOperationInterface() = default;
    virtual int CreateTable(const TableInfo& tableInfo) = 0;
    virtual int CheckTable(const TableInfo& tableInfo) = 0;
    virtual StorageRelation GetTable(const char* tableName) = 0;
    virtual void DestroyRelation(StorageRelation relation) = 0;
    virtual int RecoveryTable(const char* tableName) = 0;

    virtual int Insert(const TableInfo &tableInfo, Datum *values, bool *isNulls, const char *indexName = nullptr) = 0;
    virtual int Delete(const TableInfo &tableInfo, const char *indexName, Datum *indexValues) = 0;
    virtual int Update(const TableInfo &tableInfo, const char *indexName, ItemPointerData *oldTupleCtid, Datum *values,
                       bool *isNulls) = 0;
    virtual HeapTuple *Scan(const TableInfo &tableInfo, const char *indexName, Datum *indexValues) = 0;

    virtual int CreateIndex(const char *tableName, const TableInfo &indexTableInfo) = 0;
    virtual int CheckIndex(const char *tableName, const TableInfo& indexTableInfo) = 0;
    virtual StorageRelation GetIndex(const char *indexName) = 0;
};

#endif