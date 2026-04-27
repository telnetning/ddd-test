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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_typecache.h
 *
 * Description:
 * typecache public header file, introduces typecache table and visit interface
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#ifndef DSTORE_DSTORE_TYPECACHE_H
#define DSTORE_DSTORE_TYPECACHE_H


#include "securec.h"
#include "dstore_fake_type.h"
#include "common/datatype/dstore_compare_utils.h"
#include "index/dstore_btree_scan.h"
#include "catalog/dstore_function.h"
#include "common/datatype/dstore_array_utils.h"
#include "common/datatype/dstore_inet_utils.h"
#include "common/datatype/dstore_output_utils.h"
#include "logical_replication/dstore_logical_types.h"

namespace DSTORE {
/* For taskId */
constexpr uint64 INVALID_INVALIDATION_TASK_ID = 0;
constexpr uint64 FIRST_INVALIDATION_TASK_ID = 1;
struct FuncCache {
    Oid fnOid;
    Oid leftTypeOid;
    Oid rightTypeOid;
    uint16 funcStrategy;
    PGFunction fnAddr;         /* callback function */
};
const FuncCache INVALID_FUNC_CACHE =
    FuncCache{DSTORE_INVALID_OID, DSTORE_INVALID_OID, DSTORE_INVALID_OID, SCAN_ORDER_INVALID, nullptr};

/* B-tree comparison functions from pg_proc.dat */
static const FuncCache FUNC_CACHE_TABLE[] = {
    /* compare */
    {350,    INT2OID,        INT2OID,        MAINTAIN_ORDER,          BtreeInt2Cmp},
    {351,    INT4OID,        INT4OID,        MAINTAIN_ORDER,          BtreeInt4Cmp},
    {842,    INT8OID,        INT8OID,        MAINTAIN_ORDER,          BtreeInt8Cmp},
    {354,    FLOAT4OID,      FLOAT4OID,      MAINTAIN_ORDER,          BtreeFloat4Cmp},
    {355,    FLOAT8OID,      FLOAT8OID,      MAINTAIN_ORDER,          BtreeFloat8Cmp},
    {356,    OIDOID,         OIDOID,         MAINTAIN_ORDER,          BtreeOidCmp},
    {404,    OIDVECTOROID,   OIDVECTOROID,   MAINTAIN_ORDER,          BtreeOidvectorCmp},
    {404000, INT2VECTOROID,  INT2VECTOROID,  MAINTAIN_ORDER,          BtreeInt2vectorCmp},
    {358,    CHAROID,        CHAROID,        MAINTAIN_ORDER,          BtreeCharCmp},
    {359,    NAMEOID,        NAMEOID,        MAINTAIN_ORDER,          BtreeNameCmp},
    {359,    CSTRINGOID,     CSTRINGOID,     MAINTAIN_ORDER,          BtreeNameCmp},
    {360,    TEXTOID,        TEXTOID,        MAINTAIN_ORDER,          BtreeTextCmp},
    {360,    VARCHAROID,     VARCHAROID,     MAINTAIN_ORDER,          BtreeTextCmp},
    {377,    CASHOID,        CASHOID,        MAINTAIN_ORDER,          BtreeCashCmp},
    {1092,   DATEOID,        DATEOID,        MAINTAIN_ORDER,          BtreeDateCmp},
    {1107,   TIMEOID,        TIMEOID,        MAINTAIN_ORDER,          BtreeTimeCmp},
    {1314,   TIMESTAMPTZOID, TIMESTAMPTZOID, MAINTAIN_ORDER,          BtreeTimestamptzCmp},
    {1315,   INTERVALOID,    INTERVALOID,    MAINTAIN_ORDER,          BtreeIntervalCmp},
    {1358,   TIMETZOID,      TIMETZOID,      MAINTAIN_ORDER,          BtreeTimetzCmp},
    {836,    MACADDROID,     MACADDROID,     MAINTAIN_ORDER,          BtreeMacaddrCmp},
//        {4119, MACADDR8OID, }
    {926,    INETOID,        INETOID,        MAINTAIN_ORDER,          BtreeInetCmp},
    {1693,   BOOLOID,        BOOLOID,        MAINTAIN_ORDER,          BtreeBoolCmp},
    {2045,   TIMESTAMPOID,   TIMESTAMPOID,   MAINTAIN_ORDER,          BtreeTimeStampCmp},
    {2166,   TEXTOID,        TEXTOID,        MAINTAIN_ORDER,          BtreeTextPatternCmp},
    {2188,   INT4OID,        INT8OID,        MAINTAIN_ORDER,          BtreeInt48Cmp},
    {2189,   INT8OID,        INT4OID,        MAINTAIN_ORDER,          BtreeInt84Cmp},
    {2190,   INT2OID,        INT4OID,        MAINTAIN_ORDER,          BtreeInt24Cmp},
    {2191,   INT4OID,        INT2OID,        MAINTAIN_ORDER,          BtreeInt42Cmp},
    {2192,   INT2OID,        INT8OID,        MAINTAIN_ORDER,          BtreeInt28Cmp},
    {2193,   INT8OID,        INT2OID,        MAINTAIN_ORDER,          BtreeInt82Cmp},
    {2194,   FLOAT4OID,      FLOAT8OID,      MAINTAIN_ORDER,          BtreeFloat48Cmp},
    {2195,   FLOAT8OID,      FLOAT4OID,      MAINTAIN_ORDER,          BtreeFloat84Cmp},
    {2344,   DATEOID,        TIMESTAMPOID,   MAINTAIN_ORDER,          BtreeDateTimestampCmp},
    {2370,   TIMESTAMPOID,   DATEOID,        MAINTAIN_ORDER,          BtreeTimestampDateCmp},
    {2960,   UUIDOID,        UUIDOID,        MAINTAIN_ORDER,          BtreeUUIDCmp},
    {4835,   DATEAOID,       DATEAOID,       MAINTAIN_ORDER,          BtreeDateaCmp},
//        {3251, PG_LSNOID,}

    /* sort support */
    {3129,   INT2OID,        INT2OID,        SORT_SUPPORT,            Int2SortSupport},
    {3130,   INT4OID,        INT4OID,        SORT_SUPPORT,            Int4SortSupport},
    {3131,   INT8OID,        INT8OID,        SORT_SUPPORT,            Int8SortSupport},
    {3132,   FLOAT4OID,      FLOAT4OID,      SORT_SUPPORT,            Float4SortSupport},
    {3133,   FLOAT8OID,      FLOAT8OID,      SORT_SUPPORT,            Float8SortSupport},
    {3134,   OIDOID,         OIDOID,         SORT_SUPPORT,            OidSortSupport},
    {3135,   NAMEOID,        NAMEOID,        SORT_SUPPORT,            NameSortSupport},
    {3136,   DATEOID,        DATEOID,        SORT_SUPPORT,            DateSortSupport},
    {3137,   TIMESTAMPOID,   TIMESTAMPOID,   SORT_SUPPORT,            TimestampSortSupport},
    {3137,   DATEAOID,       DATEAOID,       SORT_SUPPORT,            TimestampSortSupport},

    /* comparator for btree scan */
    {64,     INT2OID,        INT2OID,        SCAN_ORDER_LESS,         Int2Lt},
    {148,    INT2OID,        INT2OID,        SCAN_ORDER_LESSEQUAL,    Int2Le},
    {63,     INT2OID,        INT2OID,        SCAN_ORDER_EQUAL,        Int2Eq},
    {151,    INT2OID,        INT2OID,        SCAN_ORDER_GREATEREQUAL, Int2Ge},
    {146,    INT2OID,        INT2OID,        SCAN_ORDER_GREATER,      Int2Gt},

    {66,     INT4OID,        INT4OID,        SCAN_ORDER_LESS,         Int4Lt},
    {149,    INT4OID,        INT4OID,        SCAN_ORDER_LESSEQUAL,    Int4Le},
    {65,     INT4OID,        INT4OID,        SCAN_ORDER_EQUAL,        Int4Eq},
    {150,    INT4OID,        INT4OID,        SCAN_ORDER_GREATEREQUAL, Int4Ge},
    {147,    INT4OID,        INT4OID,        SCAN_ORDER_GREATER,      Int4Gt},

    {469,    INT8OID,        INT8OID,        SCAN_ORDER_LESS,         Int8Lt},
    {471,    INT8OID,        INT8OID,        SCAN_ORDER_LESSEQUAL,    Int8Le},
    {467,    INT8OID,        INT8OID,        SCAN_ORDER_EQUAL,        Int8Eq},
    {472,    INT8OID,        INT8OID,        SCAN_ORDER_GREATEREQUAL, Int8Ge},
    {470,    INT8OID,        INT8OID,        SCAN_ORDER_GREATER,      Int8Gt},

    {289,    FLOAT4OID,      FLOAT4OID,      SCAN_ORDER_LESS,         Float4Lt},
    {290,    FLOAT4OID,      FLOAT4OID,      SCAN_ORDER_LESSEQUAL,    Float4Le},
    {287,    FLOAT4OID,      FLOAT4OID,      SCAN_ORDER_EQUAL,        Float4Eq},
    {292,    FLOAT4OID,      FLOAT4OID,      SCAN_ORDER_GREATEREQUAL, Float4Ge},
    {291,    FLOAT4OID,      FLOAT4OID,      SCAN_ORDER_GREATER,      Float4Gt},

    {295,    FLOAT8OID,      FLOAT8OID,      SCAN_ORDER_LESS,         Float8Lt},
    {296,    FLOAT8OID,      FLOAT8OID,      SCAN_ORDER_LESSEQUAL,    Float8Le},
    {293,    FLOAT8OID,      FLOAT8OID,      SCAN_ORDER_EQUAL,        Float8Eq},
    {298,    FLOAT8OID,      FLOAT8OID,      SCAN_ORDER_GREATEREQUAL, Float8Ge},
    {297,    FLOAT8OID,      FLOAT8OID,      SCAN_ORDER_GREATER,      Float8Gt},

    {740,    TEXTOID,        TEXTOID,        SCAN_ORDER_LESS,         TextLt},
    {741,    TEXTOID,        TEXTOID,        SCAN_ORDER_LESSEQUAL,    TextLe},
    {67,     TEXTOID,        TEXTOID,        SCAN_ORDER_EQUAL,        TextEq},
    {743,    TEXTOID,        TEXTOID,        SCAN_ORDER_GREATEREQUAL, TextGe},
    {742,    TEXTOID,        TEXTOID,        SCAN_ORDER_GREATER,      TextGt},

    {60,     BOOLOID,        BOOLOID,        SCAN_ORDER_EQUAL,         BoolEq},
    {61,     CHAROID,        CHAROID,        SCAN_ORDER_EQUAL,         CharEq},
    {62,     NAMEOID,        NAMEOID,        SCAN_ORDER_EQUAL,         NameEq},
    {184,    OIDOID,         OIDOID,         SCAN_ORDER_EQUAL,         OidEq},

    /* for logical decoding */
    {1243,   BOOLOID,        BOOLOID,        LOGICAL_DECODE_OUTPUT,    BoolOut},
    {201,    FLOAT4OID,      FLOAT4OID,      LOGICAL_DECODE_OUTPUT,    Float4Out},
    {215,    FLOAT8OID,      FLOAT8OID,      LOGICAL_DECODE_OUTPUT,    Float8Out},
    {39,     INT2OID,        INT2OID,        LOGICAL_DECODE_OUTPUT,    Int2Out},
    {43,     INT4OID,        INT4OID,        LOGICAL_DECODE_OUTPUT,    Int4Out},
    {461,    INT8OID,        INT8OID,        LOGICAL_DECODE_OUTPUT,    Int8Out},
    {1047,   VARCHAROID,     VARCHAROID,     LOGICAL_DECODE_OUTPUT,    VarcharOut},
};

struct TypeCache {
    Oid type;
    char name[NAME_DATA_LEN];
    /*
     * For a fixed-size type, typlen is the number of bytes we use to
     * represent a value of this type, e.g. 4 for an int32.	But for a
     * variable-length type, typlen is negative.  We use -1 to indicate a
     * "varlena" type (one that has a length word), -2 to indicate a
     * null-terminated C string.
     */
    int16 attlen;
    bool attbyval;
    char attalign;
    Oid typoutput;
    
    TypeCache() {}

    TypeCache(Oid type_in, const char* name_in, int16 attlen_in, bool attbyval_in, char attalign_in, Oid typoutput_in)
        : type(type_in), attlen(attlen_in), attbyval(attbyval_in), attalign(attalign_in), typoutput(typoutput_in)
    {
        errno_t rc = strcpy_s(this->name, NAME_DATA_LEN, name_in);
        storage_securec_check(rc, "", "");
    }

    TypeCache(const TypeCache& other)
    {
        type = other.type;
        errno_t rc = strcpy_s(name, NAME_DATA_LEN, other.name);
        storage_securec_check(rc, "", "");
        attlen = other.attlen;
        attbyval = other.attbyval;
        attalign = other.attalign;
        typoutput = other.typoutput;
    }

    bool operator==(const TypeCache &typeDef) const
    {
        return type == typeDef.type && strcmp(name, typeDef.name) == 0 && attlen == typeDef.attlen &&
               attbyval == typeDef.attbyval && attalign == typeDef.attalign && typoutput == typeDef.typoutput;
    }

    TypeCache &operator=(const TypeCache &typeDef)
    {
        type = typeDef.type;
        int rc = memcpy_s(name, NAME_DATA_LEN, typeDef.name, NAME_DATA_LEN);
        storage_securec_check(rc, "\0", "\0");
        attlen = typeDef.attlen;
        attbyval = typeDef.attbyval;
        attalign = typeDef.attalign;
        typoutput = typeDef.typoutput;
        return *this;
    }
};
const TypeCache INVALID_TYPE_CACHE = TypeCache{DSTORE_INVALID_OID, "invalid_type", 0, false, '0', DSTORE_INVALID_OID};

static const TypeCache TYPE_CACHE_TABLE[] = {
    [0] = {BOOLOID,          "column_bool",         sizeof(bool),     true,  'c',       1243},
    [1] = {INT2OID,          "column_int16",        sizeof(int16),    true,  's',       39},
    [2] = {INT4OID,          "column_int32",        sizeof(int32),    true,  'i',       43},
    [3] = {INT8OID,          "column_int64",        sizeof(int64),    true,  'd',       461},
    [4] = {CHAROID,          "column_char",         sizeof(char),     true,  'c',       33},
    [5] = {NAMEOID,          "column_name",         sizeof(DstoreNameData), false, 'c', 35},
    [6] = {OIDOID,           "column_oid",          sizeof(Oid),      true,  'i',       1799},
    [7] = {TIMESTAMPOID,     "column_timestamp",    sizeof(int64),    true,  'd',       1313},
    [8] = {TIMESTAMPTZOID,   "column_timestamptz",  sizeof(int64),    true,  'd',       1151},
    [9] = {FLOAT4OID,        "column_float32",      sizeof(float32),  true,  'i',       201},
    [10] = {FLOAT8OID,       "column_float64",      sizeof(float64),  true,  'd',       215},
    [11] = {TEXTOID,         "column_text",         -1,               false, 'i',       47},
    [12] = {VARCHAROID,      "column_varchar",      -1,               false, 'i',       1047},
    [13] = {CSTRINGOID,      "column_cstring",      -2,               false, 'c',       2293},
    [14] = {DATEOID,         "column_date",         sizeof(DateADT),  true,  'i',       1085},
    [15] = {CASHOID,         "column_money",        sizeof(Cash),     true,  'd',       887},
    [16] = {TIMEOID,         "column_time",         sizeof(TimeADT),  true,  'd',       1144},
    [17] = {MACADDROID,      "column_macaddr",      sizeof(MacAddr),  false, 'i',       437},
    [18] = {INETOID,         "column_inet",         -1,               false, 'i',       911},
    [19] = {BLOBOID,         "column_blob",         -1,               false, 'c',       DSTORE_INVALID_OID},
    [20] = {CLOBOID,         "column_clob",         -1,               false, 'c',       DSTORE_INVALID_OID},
    [21] = {BPCHAROID,       "column_bpchar",       -1,               false, 'i',       1045},
    [22] = {NUMERICOID,      "column_numeric",      -1,               false, 'i',       1702},
    [23] = {OIDVECTOROID,    "column_oidvector",    -1,               false, 'i',       55},
    [24] = {INT2VECTOROID,   "column_int2vector",   -1,               false, 'i',       41},
    [25] = {INT1ARRAYOID,    "column_int1array",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [26] = {INT2ARRAYOID,    "column_int2array",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [27] = {INT4ARRAYOID,    "column_int4array",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [28] = {INT8ARRAYOID,    "column_int8array",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [29] = {BOOLARRAYOID,    "column_boolarray",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [30] = {CHARARRAYOID,    "column_chararray",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [31] = {NAMEARRAYOID,    "column_namearray",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [32] = {BPCHARARRAYOID,  "column_bpchararray",  -1,               false, 'i',       DSTORE_INVALID_OID},
    [33] = {TEXTARRAYOID,    "column_textarray",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [34] = {VARCHARARRAYOID, "column_varchararray", -1,               false, 'i',       DSTORE_INVALID_OID},
    [35] = {FLOAT4ARRAYOID,  "column_flot4array",   -1,               false, 'd',       DSTORE_INVALID_OID},
    [36] = {FLOAT8ARRAYOID,  "column_flot8array",   -1,               false, 'd',       DSTORE_INVALID_OID},
    [37] = {INETARRAYOID,    "column_inetarray",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [38] = {INTERVALOID,     "column_interval",     16,               false, 'd',       247},
    [39] = {INT1OID,         "column_int1",         sizeof(int8),     true,  'c',       DSTORE_INVALID_OID},
    [40] = {REGPROCOID,      "column_regproc",      4,                true,  'i',       45},
    [41] = {CASHARRAYOID,    "column_casharray",    -1,               false, 'd',       DSTORE_INVALID_OID},
    [42] = {ARRAYINTERVALOID, "column_arrayinterval", -1,             false, 'd',       DSTORE_INVALID_OID},
    [43] = {CSTRINGARRAYOID, "column_cstringarray", -1,               false, 'c',       DSTORE_INVALID_OID},
    [44] = {TIMESTAMPARRAYOID, "column_timestamparray", -1,           false, 'd',       DSTORE_INVALID_OID},
    [45] = {DATEARRAYOID,    "column_int1array",    -1,               false, 'i',       DSTORE_INVALID_OID},
    [46] = {TIMEARRAYOID,    "column_timearray",    -1,               false, 'd',       DSTORE_INVALID_OID},
    [47] = {TIMESTAMPTZARRAYOID, "column_timestamptzarray", -1,       false, 'd',       DSTORE_INVALID_OID},
    [48] = {ARRAYNUMERICOID, "column_arraynumeric", -1,               false, 'i',       DSTORE_INVALID_OID},
    [49] = {DATEAOID,        "column_datea",        sizeof(int64),    true,  'd',       8792}
};

#ifdef UT
/* eliminate *ARRAY & CSTRING */
static const int TYPE_CACHE_NUM = 20 + 1; /* until CLOBOID */
#endif

struct TypeCacheTypeOidEntry {
    Oid typeOid;
    TypeCache typeCache;
};

struct FuncCacheFnOidEntry {
    Oid fnOid;
    FuncCache funcCache;
};

struct ArgTypes {
    Oid leftType;
    Oid rightType;
    uint16 proc;
};

struct FuncCacheArgTypeEntry {
    ArgTypes argTypes;
    FuncCache funcCache;
};

class CacheHashManager : public BaseObject {
public:
    CacheHashManager() {}
    ~CacheHashManager() {}

    void Init(DstoreMemoryContext context);
    void Destory();

    Oid GetFnOidFromArgType(Oid leftType, Oid rightType, uint16 proc);

    FuncCache GetFuncCacheFromFnOid(Oid functionOid);

    inline FuncCache GetFuncCacheFromArgType(Oid leftType, Oid rightType, uint16 proc)
    {
        return GetFuncCacheFromFnOid(GetFnOidFromArgType(leftType, rightType, proc));
    }
    Datum GetNormalizedDatum(Datum datum, Oid typeOid);
    Oid GetSubType(Oid typeOid);
    TypeCache GetTypeCacheFromTypeOid(Oid typeOid);

    void GenerateScanKey(Oid typeOid, Datum arg, StrategyNumber strategy, ScanKey scanKey, AttrNumber attrNumber);
    void CacheGetFuncCb(IndexGetFuncCb *cb);
    GetProcFuncCb GetIndexProcFuncCb();
    GetOpfamilyProcFuncCb GetIndexOpfProcCb();
    GetOpfamilyStratFuncCb GetIndexOpfStratCb();
    PGFunction GPICheckPartVisible();
    IndexCommonCb GetIndexCommonCb();

private:
    HTAB        *m_funcOidHash;
    HTAB        *m_funcTypeHash;
    HTAB        *m_typeOidHash;
    /* cache callback for index to get comparison functions from sql engine */
    IndexGetFuncCb m_indexCb;
};

}
#endif // DSTORE_STORAGE_TYPECACHE_H
