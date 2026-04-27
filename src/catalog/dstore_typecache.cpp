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
 * dstore_typecache.cpp
 *
 * IDENTIFICATION
 *        src/catalog/dstore_typecache.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "catalog/dstore_typecache.h"

namespace DSTORE {
static const int DEFAULT_HASH_PARTITION = 16;

void CacheHashManager::Init(DstoreMemoryContext context)
{
    int hash_flags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_SHRCTX);

    HASHCTL oidInfo;
    oidInfo.keysize = sizeof(Oid);
    oidInfo.entrysize = sizeof(FuncCacheFnOidEntry);
    oidInfo.hash = oid_hash;
    oidInfo.hcxt = context;
    oidInfo.num_partitions = DEFAULT_HASH_PARTITION;
    m_funcOidHash =
        hash_create("skFuncOidHashTable", sizeof(FUNC_CACHE_TABLE) / sizeof(FuncCache), &oidInfo, hash_flags);

    HASHCTL tagInfo;
    tagInfo.keysize = sizeof(Oid) * 2 + sizeof(uint16) * 1;
    tagInfo.entrysize = sizeof(FuncCacheArgTypeEntry);
    tagInfo.hash = tag_hash;
    tagInfo.hcxt = context;
    tagInfo.num_partitions = DEFAULT_HASH_PARTITION;
    m_funcTypeHash =
        hash_create("skFuncTypeHashTable", sizeof(FUNC_CACHE_TABLE) / sizeof(FuncCache), &tagInfo, hash_flags);

    FuncCacheFnOidEntry *oidEntry = nullptr;
    FuncCacheArgTypeEntry *tagEntry = nullptr;
    bool found = false;
    for (uint i = 0; i < sizeof(FUNC_CACHE_TABLE) / sizeof(FuncCache); i++) {
        oidEntry = (FuncCacheFnOidEntry *)hash_search(m_funcOidHash, &FUNC_CACHE_TABLE[i].fnOid,
                                                      HASH_ENTER, &found);
        oidEntry->fnOid = FUNC_CACHE_TABLE[i].fnOid;
        oidEntry->funcCache = FUNC_CACHE_TABLE[i];

        ArgTypes argTypes = {FUNC_CACHE_TABLE[i].leftTypeOid,
                             FUNC_CACHE_TABLE[i].rightTypeOid,
                             FUNC_CACHE_TABLE[i].funcStrategy};
        tagEntry = (FuncCacheArgTypeEntry *)hash_search(m_funcTypeHash, &argTypes, HASH_ENTER, &found);
        tagEntry->argTypes = argTypes;
        tagEntry->funcCache = FUNC_CACHE_TABLE[i];
    }

    HASHCTL typeInfo;
    typeInfo.keysize = sizeof(Oid);
    typeInfo.entrysize = sizeof(TypeCacheTypeOidEntry);
    typeInfo.hash = oid_hash;
    typeInfo.hcxt = context;
    typeInfo.num_partitions = DEFAULT_HASH_PARTITION;
    m_typeOidHash =
        hash_create("datumTypeHashTable", sizeof(TYPE_CACHE_TABLE) / sizeof(TypeCache), &typeInfo, hash_flags);

    TypeCacheTypeOidEntry *typeEntry = nullptr;
    found = false;
    for (uint i = 0; i < sizeof(TYPE_CACHE_TABLE) / sizeof(TypeCache); i++) {
        typeEntry = (TypeCacheTypeOidEntry *)hash_search(m_typeOidHash, &TYPE_CACHE_TABLE[i].type,
                                                         HASH_ENTER, &found);
        typeEntry->typeOid = TYPE_CACHE_TABLE[i].type;
        typeEntry->typeCache = TYPE_CACHE_TABLE[i];
    }

    m_indexCb = {nullptr};
}


void CacheHashManager::Destory()
{
    hash_destroy(m_funcTypeHash);
    hash_destroy(m_funcOidHash);
    hash_destroy(m_typeOidHash);
}

Oid CacheHashManager::GetFnOidFromArgType(Oid leftType, Oid rightType, uint16 proc)
{
    bool found = false;
    struct {
        Oid left_type;
        Oid right_type;
        uint16 proc;
    } tag = {leftType, rightType, proc};
    FuncCacheArgTypeEntry *entry =
        static_cast<FuncCacheArgTypeEntry *>(hash_search(m_funcTypeHash, &tag, HASH_FIND, &found));
    if (unlikely(!found)) {
        return DSTORE_INVALID_OID;
    }
    StorageAssert(entry != nullptr);
    return entry->funcCache.fnOid;
}

FuncCache CacheHashManager::GetFuncCacheFromFnOid(Oid functionOid)
{
    bool found = false;
    FuncCacheFnOidEntry *entry =
        static_cast<FuncCacheFnOidEntry *>(hash_search(m_funcOidHash, &functionOid, HASH_FIND, &found));
    if (!found || entry == nullptr) {
        return INVALID_FUNC_CACHE;
    }
    return entry->funcCache;
}

TypeCache CacheHashManager::GetTypeCacheFromTypeOid(Oid typeOid)
{
    bool found = false;
    TypeCacheTypeOidEntry *entry =
        static_cast<TypeCacheTypeOidEntry *>(hash_search(m_typeOidHash, &typeOid, HASH_FIND, &found));
    if (!found || entry == nullptr) {
        return INVALID_TYPE_CACHE;
    }
    return entry->typeCache;
}

Datum CacheHashManager::GetNormalizedDatum(Datum datum, Oid typeOid)
{
    switch (typeOid) {
        case BOOLOID:
            return BoolGetDatum(DatumGetBool(datum));
        case INT2OID:
            return Int16GetDatum(DatumGetInt16(datum));
        case INT4OID:
        case OIDOID:
            return Int32GetDatum(DatumGetInt32(datum));
        case INT8OID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return Int64GetDatum(DatumGetInt64(datum));
        case CHAROID:
            return CharGetDatum(DatumGetChar(datum));
        case FLOAT4OID:
            return Float32GetDatum(DatumGetFloat32(datum));
        case FLOAT8OID:
            return Float64GetDatum(DatumGetFloat64(datum));
        case NAMEOID:
        case OIDVECTOROID:
        case INT2VECTOROID:
        case TEXTOID:
        case VARCHAROID:
        case ANYARRAYOID:
        default:
            return PointerGetDatum(datum);
    }
}


Oid CacheHashManager::GetSubType(Oid typeOid)
{
    switch (typeOid) {
        case OIDVECTOROID:
            return OIDOID;
        case INT2VECTOROID:
            return INT2OID;
        default:
            return DSTORE_INVALID_OID;
    }
}

void CacheHashManager::GenerateScanKey(Oid typeOid, Datum arg, StrategyNumber strategy, ScanKey scanKey,
                                       AttrNumber attrNumber)
{
    scanKey->skArgument = GetNormalizedDatum(arg, typeOid);
    scanKey->skStrategy = strategy;
    scanKey->skSubtype = GetSubType(typeOid);

    FuncCache funcCache = GetFuncCacheFromArgType(typeOid, typeOid, strategy);
    scanKey->skFunc.fnAddr = funcCache.fnAddr;
    scanKey->skFunc.fnOid = funcCache.fnOid;
    scanKey->skFunc.fnNargs = 2;
    scanKey->skFunc.fnStrict = true;
    scanKey->skFunc.fnRetset = false;
    scanKey->skFunc.fnMcxt = g_dstoreCurrentMemoryContext;
    scanKey->skCollation = DEFAULT_COLLATION_OID;
    scanKey->skFlags = SCAN_KEY_ROW_MEMBER;
    scanKey->skAttno = attrNumber;
}

void CacheHashManager::CacheGetFuncCb(IndexGetFuncCb *cb)
{
    if (cb == nullptr) {
        return;
    }

    m_indexCb = *cb;
}

GetProcFuncCb CacheHashManager::GetIndexProcFuncCb()
{
    return m_indexCb.procFuncCb;
}

GetOpfamilyProcFuncCb CacheHashManager::GetIndexOpfProcCb()
{
    return m_indexCb.procOpfamilyProcFuncCb;
}

GetOpfamilyStratFuncCb CacheHashManager::GetIndexOpfStratCb()
{
    return m_indexCb.procOpfamilyStratFuncCb;
}

PGFunction CacheHashManager::GPICheckPartVisible()
{
    return m_indexCb.partitionIsVisible;
}

IndexCommonCb CacheHashManager::GetIndexCommonCb()
{
    return m_indexCb.commonCb;
}

}