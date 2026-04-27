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
 * Description: CloudNativeDatabase ut_tale(ut table metadata)
 */

#ifndef UT_TABLE_H
#define UT_TABLE_H

#include <cstdio>
#include <cstdint>
#include "common/dstore_datatype.h"
#include "table_operation_interface.h"
#include "catalog/dstore_fake_type.h"
#include "catalog/dstore_fake_class.h"

namespace DSTORE {

#define GetTableName(tableNameType) (char*)#tableNameType
#define GetColDesc(tableNameType) tableNameType##_COL_DESC
#define GetIndexDesc(tableNameType) tableNameType##_INDEX_DESC
#define GetTableColMax(tableNameType) tableNameType##_COL_MAX

enum TableNameType : uint8_t {
    TABLE_1,
    TABLE_2,
    TABLE_3,
    TABLE_4,
    TABLE_5,
    TABLE_MAX_CNT,
};

/*
 * 1. Table Layout 
 */
enum Table1ColNameType : uint8_t {
    TABLE1_ID,  /* Primary Key, unique warehouse id */
    TABLE1_COL2, 
    TABLE1_COL3,
    TABLE1_COL4,
    TABLE1_COL5,
    GetTableColMax(TABLE_1)
};

static UNUSE_PARAM ColumnDesc GetColDesc(TABLE_1)[GetTableColMax(TABLE_1)] = {
    { INT4OID,      "column_1",      sizeof(int32),   false,   true,   false,     'i',    'p'},
    { FLOAT8OID,    "column_2",      sizeof(float64),  true,    true,   false,     'd',    'p'},
    { FLOAT8OID,    "column_3",      sizeof(float64),  true,    true,   false,     'd',    'p'},
    { TEXTOID,      "column_4",        -1,            true,    false,  false,     'c',    'p'},
    { TEXTOID,      "column_5",        -1,            true,    false,  false,     'c',    'p'},
};

static UNUSE_PARAM IndexDesc GetIndexDesc(TABLE_1)[] = {
    {1, {TABLE1_ID, 0, 0, 0}, true},
};

/*
 * 2. Table2 Layout
 */
enum Table2ColNameType : uint8_t {
    TABLE2_ID,  /* 10 are populated per warehouse */
    TABLE2_TABLE1_ID,
    TABLE2_COL3,
    TABLE2_COL4,
    TABLE2_COL5,
    TABLE2_COL6,
    TABLE2_COL7,
    TABLE2_COL8,
    GetTableColMax(TABLE_2)
};

static UNUSE_PARAM ColumnDesc GetColDesc(TABLE_2)[GetTableColMax(TABLE_2)] = {
    { INT4OID,      "column_1",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_2",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { FLOAT8OID,    "column_3",      sizeof(float64),  true,     true,   false,     'd',    'p'},
    { FLOAT8OID,    "column_4",      sizeof(float64),  true,     true,   false,     'd',    'p'},
    { VARCHAROID,   "column_5",        -1,            true,     false,  false,     'c',    'p'},
    { VARCHAROID,   "column_6",        -1,            true,     false,  false,     'c',    'p'},
    { CHAROID,      "column_7",       sizeof(char),   true,     true,   false,     'c',    'p'},
    { CHAROID,      "column_8",       sizeof(char),   true,     true,   false,     'c',    'p'},
};

static UNUSE_PARAM IndexDesc GetIndexDesc(TABLE_2)[] = {
    {1, {TABLE2_ID, 0, 0}, true},
};

/*
 * 3. Table3 Layout
 */
enum Table3ColNameType : uint8_t {
    TABLE3_ID = 0,
    TABLE3_TABLE1_ID,
    TABLE3_COL3,
    TABLE3_COL4,
    TABLE3_COL5,
    TABLE3_COL6,
    TABLE3_COL7,
    TABLE3_COL8,
    TABLE3_COL9,
    GetTableColMax(TABLE_3)
};

static UNUSE_PARAM ColumnDesc GetColDesc(TABLE_3)[GetTableColMax(TABLE_3)] = {
    { INT4OID,      "column_1",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_2",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_3",      sizeof(int32),   true,     true,   false,     'i',    'p'},
    { INT4OID,      "column_4",      sizeof(int32),   true,     true,   false,     'i',    'p'},
    { VARCHAROID,   "column_5",           -1,         true,     false,  false,     'c',    'p'},
    { CHAROID,      "column_6",      sizeof(char),    true,     true,   false,     'c',    'p'},
    { CHAROID,      "column_7",      sizeof(char),    true,     true,   false,     'c',    'p'},
    { CHAROID,      "column_8",      sizeof(char),    true,     true,   false,     'c',    'p'},
    { BOOLOID,      "column_9",      sizeof(bool),    true,     true,   false,     's',    'p'},
};


static UNUSE_PARAM IndexDesc GetIndexDesc(TABLE_3)[] = {
    {2, {TABLE3_TABLE1_ID, TABLE3_ID, 0, 0}, true},
};

/*
 * 4. Table4 Layout
 */
enum Table4ColNameType : uint8_t {
    TABLE4_ID = 0,
    TABLE4_TABLE1_ID,
    TABLE4_TABLE2_ID,
    TABLE4_TABLE3_ID,
    TABLE4_COL5,
    GetTableColMax(TABLE_4)
};

static UNUSE_PARAM ColumnDesc GetColDesc(TABLE_4)[GetTableColMax(TABLE_4)] = {
    { INT4OID,      "column_1",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_2",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_3",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_4",      sizeof(int32),   true,     true,   false,     'i',    'p'},
    { TIMESTAMPOID, "column_5",      sizeof(int64),   true,     true,   false,      'd',    'p'},
};


static UNUSE_PARAM IndexDesc GetIndexDesc(TABLE_4)[] = {
    {3, {TABLE4_TABLE1_ID, TABLE4_TABLE2_ID, TABLE4_ID, 0}, true},
};

/*
 * 5. Table5 Layout
 */
enum Table5ColNameType : uint8_t {
    TABLE5_ID = 0,
    TABLE5_TABLE1_ID,
    TABLE5_TABLE2_ID,
    TABLE5_TABLE3_ID,
    TABLE5_COL6,
    TABLE5_COL7,
    GetTableColMax(TABLE_5)
};

static UNUSE_PARAM ColumnDesc GetColDesc(TABLE_5)[GetTableColMax(TABLE_5)] = {
    { INT4OID,      "column_1",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_2",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_3",      sizeof(int32),   false,    true,   false,     'i',    'p'},
    { INT4OID,      "column_4",      sizeof(int32),   true,     true,   false,     'i',    'p'},
    { TIMESTAMPOID, "column_5",      sizeof(int64),   true,     true,   false,     'd',    'p'},
    { CHAROID,      "column_6",      sizeof(char),    true,     true,   false,     'c',    'p'},
};

static UNUSE_PARAM IndexDesc GetIndexDesc(TABLE_5)[] = {
    {4, {TABLE5_TABLE1_ID, TABLE5_TABLE2_ID, TABLE5_TABLE3_ID, TABLE5_ID}, true},
};

static UNUSE_PARAM TableInfo TABLE_CACHE[TABLE_MAX_CNT] = {
    {
        {
            (char*)GetTableName(TABLE_1),
            GetTableColMax(TABLE_1),
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID,
            SYS_RELKIND_RELATION,
            SYS_PARTTYPE_NON_PARTITIONED_RELATION,
            SYS_RELPERSISTENCE_PERMANENT,
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID
        },
        GetColDesc(TABLE_1),
        GetIndexDesc(TABLE_1)
    },
    {
        {
            (char*)GetTableName(TABLE_2),
            GetTableColMax(TABLE_2),
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID,
            SYS_RELKIND_RELATION,
            SYS_PARTTYPE_NON_PARTITIONED_RELATION,
            SYS_RELPERSISTENCE_PERMANENT,
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID
        },
        GetColDesc(TABLE_2),
        GetIndexDesc(TABLE_2)
    },
    {
        {
            (char*)GetTableName(TABLE_3),
            GetTableColMax(TABLE_3),
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID,
            SYS_RELKIND_RELATION,
            SYS_PARTTYPE_NON_PARTITIONED_RELATION,
            SYS_RELPERSISTENCE_PERMANENT,
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID
        },
        GetColDesc(TABLE_3),
        GetIndexDesc(TABLE_3)
    },
    {
        {
            (char*)GetTableName(TABLE_4),
            GetTableColMax(TABLE_4),
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID,
            SYS_RELKIND_RELATION,
            SYS_PARTTYPE_NON_PARTITIONED_RELATION,
            SYS_RELPERSISTENCE_PERMANENT,
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID
        },
        GetColDesc(TABLE_4),
        GetIndexDesc(TABLE_4)
    },
    {
        {
            (char*)GetTableName(TABLE_5),
            GetTableColMax(TABLE_5),
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID,
            SYS_RELKIND_RELATION,
            SYS_PARTTYPE_NON_PARTITIONED_RELATION,
            SYS_RELPERSISTENCE_PERMANENT,
            DSTORE_INVALID_OID,
            DSTORE_INVALID_OID
        },
        GetColDesc(TABLE_5),
        GetIndexDesc(TABLE_5)
    }
};
}
#endif