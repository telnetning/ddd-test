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

#ifndef DSTORE_UT_TABLE_OPERATION_H
#define DSTORE_UT_TABLE_OPERATION_H

#include "common/memory/dstore_mctx.h"
#include "ut_tablehandler/ut_table.h"

class TableOperationInterface;

namespace DSTORE {
class StorageInstanceInterface;

class UTTableOperation : public BaseObject {
public:
    explicit UTTableOperation(StorageInstanceInterface* instance);
    ~UTTableOperation();

    void Initialize();
    void Destroy() noexcept;

    int CreateTable(TableNameType type);
    int CreateAllTable();
    
    StorageRelation GetTable(TableNameType type);
    void DestroyRelation(StorageRelation relation);

    int RecoveryAllTable();
    int CheckTable(TableNameType type);

    int Insert(TableNameType type, uint32 rowNum);

    int CreateIndex(TableNameType type);
    int CreateAllIndex();
    int RecoveryAllIndex();

    StorageRelation GetIndex(TableNameType type);
    int CheckIndex(TableNameType type);

    HeapTuple *Scan(const TableInfo &tableInfo, const char *indexName, Datum *indexValues);
    int Scan(TableNameType type, int rowNum);
    int InsertAndScan(TableNameType type, int rowNum);
    
    int Delete(TableNameType type, int rowNum);
    int DeleteAndCheck(TableNameType type, int rowNum);
    int Update(TableNameType type, int rowNum);
    int UpdateAndCheck(TableNameType type, int rowNum);
    
protected:
    StorageInstanceInterface *m_instance;
    TableOperationInterface* m_tableOperation;

    int Insert(const TableInfo &tableInfo, Datum *values, bool *isNulls, const char *indexName = nullptr);
    int Delete(const TableInfo &tableInfo, const char *indexName, Datum *indexValues);
    int Update(const TableInfo &tableInfo, const char *indexName, Datum *values, bool *isNulls, Datum *indexValues);
    void CheckData(const char *tableName, HeapTuple *heapTuple, Datum *originalValues, bool *originalIsNulls);
};

} /* namespace DSTORE */

#endif /* DSTORE_UT_TABLE_OPERATION_H */
