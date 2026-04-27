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
 * dstore_decode_dict.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_decode_dict.h"
#include "wal/dstore_wal.h"
#include "tuple/dstore_memheap_tuple.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
bool IsLogicalDecodeDictNeeded(Oid tableId)
{
    if (!WalLogicalActive()) {
        return false;
    }
    for (Oid oid : DECODE_DICT_COLLECT_SYSTABLE_OID) {
        if (tableId == oid) {
            return true;
        }
    }
    return false;
}

DecodeDict::DecodeDict(PdbId pdbId, DstoreMemoryContext mctx)
    : m_pdbId(pdbId),
      m_memoryContext(mctx),
      m_decodeDictFile(nullptr),
      m_tableInfo(nullptr)
{
    LWLockInitialize(&m_bufferLock, LWLOCK_GROUP_DECODE_DICT_BUF);
}

DecodeDict::~DecodeDict()
{
    m_memoryContext = nullptr;
    m_decodeDictFile = nullptr;
    m_tableInfo = nullptr;
}

RetStatus DecodeDict::Init()
{
    AutoMemCxtSwitch autoSwitch{m_memoryContext};

    m_decodeDictFile = DstoreNew(m_memoryContext) DecodeDictFile(m_pdbId);
    if (STORAGE_VAR_NULL(m_decodeDictFile)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_decodeDictFile->Init())) {
        return DSTORE_FAIL;
    }

    LoadDecodeTableInfoFromDisk();
    return DSTORE_SUCC;
}

void DecodeDict::Destroy()
{
    if (m_decodeDictFile != nullptr) {
        m_decodeDictFile->Destroy();
        delete m_decodeDictFile;
        m_decodeDictFile = nullptr;
    }

    if (m_tableInfo != nullptr) {
        hash_destroy(m_tableInfo);
        m_tableInfo = nullptr;
    }
    DstoreMemoryContextDelete(m_memoryContext);
}

void DecodeDict::LoadDecodeTableInfoFromDisk()
{
    DstoreLWLockAcquire(&m_bufferLock, LW_EXCLUSIVE);
    StorageAssert(m_tableInfo == nullptr);
    /* now, load from disk */
    AutoMemCxtSwitch autoSwitch{m_memoryContext};
    HASHCTL info;
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(DecodeTableInfoEntry);
    info.hcxt = m_memoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT | HASH_SHRCTX);
    m_tableInfo = hash_create("decodeTableInfoHash", DEFAULT_TABLES_PER_DECODE_DICT, &info, hashFlags);
    StorageAssert(m_decodeDictFile != nullptr && m_tableInfo != nullptr);
    m_decodeDictFile->LoadToHTAB(m_tableInfo);
    LWLockRelease(&m_bufferLock);
}

RetStatus DecodeDict::SynchronizeCatalog(CatalogInfo *rawCatalog)
{
    /* sanity check */
    StorageAssert(rawCatalog->tableOid != DSTORE_INVALID_OID && rawCatalog->csn != INVALID_CSN);

    /* step1. find if it already have this tableInfo */
    DstoreLWLockAcquire(&m_bufferLock, LW_EXCLUSIVE);
    DecodeTableInfoPos* pos = nullptr;
    DecodeTableInfoEntry* ent = FindDecodeTableInfoPos(rawCatalog->tableOid, rawCatalog->csn, false, pos);
    UNUSED_VARIABLE(ent);
    if (pos != nullptr) {
        /* already have */
        LWLockRelease(&m_bufferLock);
        return DSTORE_SUCC;
    }
    AutoMemCxtSwitch autoSwitch{m_memoryContext};
    /* step2. convert raw catalog info to tableInfo */
    DecodeTableInfo *tableInfo = rawCatalog->ConvertToDecodeTableInfo();
    /* step3. create table info */
    if (STORAGE_FUNC_FAIL(CreateDecodeTableInfo(tableInfo))) {
        LWLockRelease(&m_bufferLock);
        return DSTORE_FAIL;
    }
    LWLockRelease(&m_bufferLock);
    return DSTORE_SUCC;
}

RetStatus DecodeDict::CollectDecodeDictChange(Oid tableOid, CommitSeqNo dictChangeCsn)
{
    StorageAssert(tableOid != DSTORE_INVALID_OID && dictChangeCsn != INVALID_CSN);
    AutoMemCxtSwitch autoSwitch{m_memoryContext};
    DstoreLWLockAcquire(&m_bufferLock, LW_EXCLUSIVE);
    /* first create a fake tableInfo, like a new-version tag and be waited fill-up from logical decoding */
    DecodeTableInfo *tableInfo = static_cast<DecodeTableInfo *>(DstorePalloc0(sizeof(DecodeTableInfo)));
    if (STORAGE_VAR_NULL(tableInfo)) {
        return DSTORE_FAIL;
    }
    tableInfo->Init(tableOid, dictChangeCsn, DecodeTableInfoStatus::EXIST_BUT_UNCOLLECTED);
    tableInfo->InitDesc(0, false);
    if (STORAGE_FUNC_FAIL(CreateDecodeTableInfo(tableInfo))) {
        LWLockRelease(&m_bufferLock);
        return DSTORE_FAIL;
    }
    LWLockRelease(&m_bufferLock);
    return DSTORE_SUCC;
}

RetStatus DecodeDict::CreateDecodeTableInfo(DecodeTableInfo *tableInfo)
{
    StorageAssert(tableInfo != nullptr);
    /* step1. create in disk */
    BlockNumber newBlock;
    DecodeTableInfoDiskData *newItem = DecodeTableInfo::ConvertToItem(tableInfo);
    if (STORAGE_FUNC_FAIL(m_decodeDictFile->AddDecodeTableInfoItem(newItem, newBlock))) {
        DstorePfree(newItem);
        return DSTORE_FAIL;
    }

    /* step2. create in hash table, keep csn order */
    DecodeTableInfoPos *newPos;
    DecodeTableInfoEntry *ent = FindDecodeTableInfoPos(tableInfo->tableOid, tableInfo->csn, true, newPos);
    StorageAssert(newPos == nullptr);
    StorageAssert(ent == nullptr);
    newPos = static_cast<DecodeTableInfoPos *>(DstorePalloc0(sizeof(DecodeTableInfoPos)));
    newPos->Init(tableInfo, newBlock);
    bool add = false;
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &ent->head) {
        DecodeTableInfoPos *cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
        if (cur->tableInfo->csn > tableInfo->csn) { /* get the max version tableInfo */
            DListInsertBefore(&cur->posNode, &newPos->posNode);
            add = true;
        }
    }
    if (!add) {
        DListPushTail(&ent->head, &newPos->posNode);
    }
    ent->versionNum += 1;
    DstorePfree(newItem);
    return DSTORE_SUCC;
}

DecodeTableInfo* DecodeDict::GetVisibleDecodeTableInfo(Oid tableOid, CommitSeqNo snapshotCsn)
{
    StorageAssert(m_tableInfo != nullptr);
    DstoreLWLockAcquire(&m_bufferLock, LW_SHARED);
    bool found;
    DecodeTableInfoEntry *ent = static_cast<DecodeTableInfoEntry *>(
        hash_search(m_tableInfo, static_cast<void *>(&tableOid), HASH_FIND, &found));
    if (!found) {
        ErrLog(DSTORE_WARNING, MODULE_LOGICAL_REPLICATION,
            ErrMsg("tableOid not found in deocode dict. tableOid: %u snapshotCsn: %lu", tableOid, snapshotCsn));
        return nullptr;
    }
    DecodeTableInfo *res = nullptr;
    dlist_iter iter;
    dlist_foreach(iter, &ent->head) {
        DecodeTableInfoPos *cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
        if (cur->tableInfo->csn < snapshotCsn) { /* get the max version tableInfo */
            res = cur->tableInfo;
            StorageAssert(res != nullptr);
        } else {
            break;
        }
    }
    if (unlikely(res == nullptr)) {
        StringInfoData buf;
        if (unlikely(!buf.init())) {
            ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("StringInfoData alloc memory failed."));
            return nullptr;
        }
        GetALLDecodeTableInfoVersion(tableOid, &buf);
        ErrLog(DSTORE_WARNING, MODULE_LOGICAL_REPLICATION,
            ErrMsg("table info not found in deocode dict. tableOid: %u, versionNum %d, snapshotCsn: %lu, %s",
                tableOid, ent->versionNum, snapshotCsn, buf.data));
        DstorePfreeExt(buf.data);
    }
    LWLockRelease(&m_bufferLock);
    return res;
}

RetStatus DecodeDict::UpdateDecodeDictChangeFromWal(DecodeTableInfo *tableInfo)
{
    StorageAssert(m_tableInfo != nullptr);
    StorageAssert(tableInfo->status == DecodeTableInfoStatus::DELETE ||
                  tableInfo->status == DecodeTableInfoStatus::COLLECTED);

    DstoreLWLockAcquire(&m_bufferLock, LW_EXCLUSIVE);
    AutoMemCxtSwitch autoSwitch{m_memoryContext};

    /* step1. check if other logical-decode task have already filled it up */
    DecodeTableInfoPos* orignalPos;
    UNUSED_VARIABLE(FindDecodeTableInfoPos(tableInfo->tableOid, tableInfo->csn, true, orignalPos));
    StorageAssert(orignalPos != nullptr);
    if (orignalPos->tableInfo->status == DecodeTableInfoStatus::COLLECTED) {
        LWLockRelease(&m_bufferLock);
        return DSTORE_SUCC;
    }

    /* step2. write to disk */
    BlockNumber newBlock;
    DecodeTableInfoDiskData *newItem = DecodeTableInfo::ConvertToItem(tableInfo);
    if (STORAGE_FUNC_FAIL(m_decodeDictFile->UpdateDecodeTableInfoItem(orignalPos->block,
        orignalPos->tableInfo->tableOid, orignalPos->tableInfo->csn, newItem, newBlock))) {
        LWLockRelease(&m_bufferLock);
        DstorePfree(newItem);
        return DSTORE_FAIL;
    }
    DstorePfree(newItem);

    /* step3. update hash table, orignalPos must be reload */
    UNUSED_VARIABLE(FindDecodeTableInfoPos(tableInfo->tableOid, tableInfo->csn, true, orignalPos));
    StorageAssert(orignalPos != nullptr);
    DstorePfreeExt(orignalPos->tableInfo); /* release the old tableInfo memory */
    DecodeTableInfo *newTableInfo = tableInfo->Copy(); /* copy new table info to our memory */
    orignalPos->tableInfo = newTableInfo; /* replace old one */
    orignalPos->block = newBlock; /* replace old one */
    LWLockRelease(&m_bufferLock);
    return DSTORE_SUCC;
}

void DecodeDict::GetInfoFromSysRelationTuple(const HeapTuple &sysRelTup, DecodeRelationData &relationData) const
{
    Form_pg_class tableDef = static_cast<Form_pg_class>(static_cast<void *>(sysRelTup.GetValues()));
    relationData.tableOid = sysRelTup.GetOid();
    relationData.relHasOids = tableDef->relhasoids;
    relationData.nspId = tableDef->relnamespace;
    relationData.natts = tableDef->relnatts;
    errno_t rc = memcpy_s(relationData.relName.data, NAME_DATA_LEN, tableDef->relname.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
}

DecodeTableInfoEntry* DecodeDict::FindDecodeTableInfoPos(Oid tableOid, CommitSeqNo ddlCsn,
    bool forUpdate, DecodeTableInfoPos* &outPos)
{
    /* caller shoule be lock hash table outside!!!!! */
    bool found;
    outPos = nullptr;
    DecodeTableInfoEntry *ent =  static_cast<DecodeTableInfoEntry *>(
        hash_search(m_tableInfo, static_cast<void *>(&tableOid), forUpdate ? HASH_ENTER : HASH_FIND, &found));
    StorageAssert(ent != nullptr);
    StorageReleasePanic(ent == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("FindDecodeTableInfoPos ent is null."));
    if (!found) {
        if (forUpdate) {
            ent->Init(tableOid);
        }
        return ent;
    }
    DecodeTableInfoPos *cur = nullptr;
    if (forUpdate) {
        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &ent->head) {
            cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
            if (cur->tableInfo->csn == ddlCsn) {
                outPos = cur;
                break;
            }
        }
    } else {
        dlist_iter iter;
        dlist_foreach(iter, &ent->head) {
            cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
            if (cur->tableInfo->csn == ddlCsn) {
                outPos = cur;
                break;
            }
        }
    }
    return ent;
}

void DecodeDict::GetALLDecodeTableInfoVersion(Oid tableOid, StringInfo buf)
{
    bool found;
    DecodeTableInfoEntry *ent = static_cast<DecodeTableInfoEntry *>(
        hash_search(m_tableInfo, static_cast<void *>(&tableOid), HASH_FIND, &found));
    if (!found) {
        buf->AppendString("No version");
        return;
    }
    StorageAssert(ent->versionNum != 0);
    buf->append("%d versions: ", ent->versionNum);
    dlist_iter iter;
    dlist_foreach(iter, &ent->head) {
        DecodeTableInfoPos *cur = dlist_container(DecodeTableInfoPos, posNode, iter.cur);
        buf->append("%lu ", cur->tableInfo->csn);
    }
}
#endif

}