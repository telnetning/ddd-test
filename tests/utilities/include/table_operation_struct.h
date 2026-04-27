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

 * Description: CloudNativeDatabase table_operation_struct.h(Some structures related to table operations)
 */

#ifndef TABLE_OPERATION_STRUCT_H
#define TABLE_OPERATION_STRUCT_H
#include <cstdint>
#include "common/dstore_common_utils.h"

using namespace DSTORE;
struct ColumnDesc {
    Oid type;
    char name[NAME_MAX_LEN];
    int16_t len;
    bool canBeNull;
    bool isByVal;                   /* 是否是值传递 true：为值传递， false：为引用传递*/
    bool isHaveDefVal;              /* 该属性是否有默认值，即在pg_attrdef 系统表中是否有该属性的默认值 */
    char align;
    char storageType;
};

struct IndexDesc {
    uint8_t indexAttrNum;
    uint32_t indexCol[4];
    bool isUnique;
};


struct RelationDesc {
    char* name;
    int attrNum;
    Oid relOid;
    Oid tablespaceOid;      /* 表空间 */
    char relKind;           /* 表类型
                             * r：表示普通表。
                             * i：表示索引。
                             * I：表示分区表GLOBAL索引。
                             * S：表示序列。
                             * L：表示Large序列。
                             * v：表示视图。           
                             * c：表示复合类型。
                             * t：表示TOAST表。
                             * f：表示外表。
                             * m：表示物化视图。
                             */

    char partType;          /* 表或者索引是否具有分区表的性质。
                             * p：表示带有分区表性质。
                             * n：表示没有分区表特性。
                             * v：表示该表为HDFS的Value分区表。
                             * s：表示该表为二级分区表。
                             */
    char persistenceLevel;  /* rel need persistence move to class data info */
    Oid partHeapOid;        /* partition index's partition oid */

    Oid parentId;           /* if this is construct by partitionGetRelation,
                             * this is Partition Oid,else this is DSTORE_INVALID_OID 
                             */
};

struct TableInfo {
    RelationDesc      relation;
    ColumnDesc       *colDesc;
    IndexDesc        *indexDesc;
    void Initialize()
    {
        colDesc = nullptr;
        indexDesc = nullptr;
        relation.relOid = DSTORE_INVALID_OID;
        relation.name = nullptr;
    }
};

#endif