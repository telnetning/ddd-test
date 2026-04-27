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
 * dstore_parallel_logical_decode_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include <thread>
#include "port/dstore_port.h"
#include "framework/dstore_instance.h"
#include "logical_replication/dstore_decode_dict.h"
#include "logical_replication/dstore_wal_dispatcher.h"
#include "logical_replication/dstore_parallel_decode_worker.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
ParallelDecodeWorker::ParallelDecodeWorker(const ParallelDecodeWorkerInitParam &initParam, const PdbId pdbId)
    : m_privateDecodeMctx(initParam.decodeMctx),
      m_pdbId(pdbId),
      m_workerId(initParam.workerId),
      m_isRunningFlag(false),
      m_needStopFlag(false),
      m_workerThrd(nullptr),
      m_trxChangesQueue(nullptr),
      m_trxLogicalLogQueue(nullptr),
      m_walDispatcher(nullptr),
      m_decodeDict(initParam.decodeDict),
      m_decodeOptions(initParam.decodeOptions),
      m_decodePlugin(initParam.decodePlugin)
{}

RetStatus ParallelDecodeWorker::Init()
{
    AutoMemCxtSwitch autoSwitch{m_privateDecodeMctx};
    StorageAssert(m_trxChangesQueue == nullptr);
    StorageAssert(m_trxLogicalLogQueue == nullptr);

    m_trxChangesQueue = static_cast<LogicalQueue *>(DstorePalloc0(sizeof(LogicalQueue)));
    if (m_trxChangesQueue == nullptr) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_trxChangesQueue->Init())) {
        storage_set_error(DECODE_ERROR_CONTEXT_CREATE_ERROR);
        return DSTORE_FAIL;
    }

    m_trxLogicalLogQueue = static_cast<LogicalQueue *>(DstorePalloc0(sizeof(LogicalQueue)));
    if (m_trxLogicalLogQueue == nullptr) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_trxLogicalLogQueue->Init())) {
        storage_set_error(DECODE_ERROR_CONTEXT_CREATE_ERROR);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void ParallelDecodeWorker::Destroy()
{
    /* release resources */
    if (m_trxChangesQueue != nullptr) {
        m_trxChangesQueue->Destroy();
    }
    if (m_trxLogicalLogQueue != nullptr) {
        m_trxLogicalLogQueue->Destroy();
    }
    DstoreMemoryContextDelete(m_privateDecodeMctx);
    m_privateDecodeMctx = nullptr;
}

void ParallelDecodeWorker::Run()
{
    if (IsRunning()) {
        return;
    }
    StorageAssert(m_workerThrd == nullptr);
    m_isRunningFlag.store(true);
    m_workerThrd = new std::thread(&ParallelDecodeWorker::WorkerMain, this, m_pdbId);
}

void ParallelDecodeWorker::QueueTrx(TrxChangeCtx *trx)
{
    int tryCnt = 0;
    while (unlikely(!m_trxChangesQueue->Put(trx))) {
        if (m_needStopFlag.load(std::memory_order_acquire)) {
            break;
        }
        if (++tryCnt >= MAX_TRY_OP_QUEUE) {
            GaussUsleep(QUEUE_WAIT_TIME);
            tryCnt = 0;
        }
    }
}

/* pop from m_trxChangesQueue and decoded to m_trxLogicalLogQueue */
void ParallelDecodeWorker::WorkerMain(PdbId pdbId)
{
    (void)pthread_setname_np(pthread_self(), "LogicDecodeWrk");
    InitSignalMask();
    (void)g_storageInstance->CreateThreadAndRegister(pdbId, false, "LogicDecodeWrk", true,
                                                     ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
        ErrMsg("parallel decode worker %d begin. pid: %lu", m_workerId, thrd->GetCore()->pid));
    AutoMemCxtSwitch autoSwitch{m_privateDecodeMctx};
    int tryCnt = 0;
    void *curTrx = nullptr;
    while (true) {
        curTrx = nullptr;
        /* step1: fetch */
        while (unlikely((curTrx = m_trxChangesQueue->Pop()) == nullptr)) {
            if (++tryCnt < MAX_TRY_OP_QUEUE) {
                continue;
            }
            tryCnt = 0;
            if (m_needStopFlag.load(std::memory_order_acquire)) {
                goto Exit;
            }
            /* check waldispatcher status */
            if (m_walDispatcher->IsRunning()) {
                ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("wal dispatcher alive, decode worker keep wait"));
                GaussUsleep(QUEUE_WAIT_TIME);
            } else {
                /* no wal to read and to decode, exit self. */
                ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("wal dispatcher stop, decode worker stop too"));
                goto Exit;
            }
        }
        StorageAssert(curTrx != nullptr);
        /* step2: decode */
        TrxLogicalLog *trxOut = static_cast<TrxLogicalLog *>(DstorePalloc0(sizeof(TrxLogicalLog)));
        trxOut->workerId = m_workerId;
        trxOut->Init();
        if (unlikely(static_cast<TrxChangeCtx *>(curTrx)->flags.IsFakeTrxChange())) {
            trxOut->commitCsn = static_cast<TrxChangeCtx *>(curTrx)->commitCsn;
            trxOut->restartDecodingPlsn = static_cast<TrxChangeCtx *>(curTrx)->restartDecodingPlsn;
            ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
                ErrMsg("decode fake trx logical log with next csn %lu", trxOut->commitCsn));
        } else {
            /* decode curTrx to trxOut */
            if (STORAGE_FUNC_FAIL(DecodeTrx(trxOut, static_cast<TrxChangeCtx *>(curTrx)))) {
                ErrLog(DSTORE_WARNING, MODULE_LOGICAL_REPLICATION, ErrMsg("logical decode failed"));
                goto Exit;
            }
        }
        /* step3: push */
        tryCnt = 0;
        while (unlikely(!m_trxLogicalLogQueue->Put(trxOut))) {
            if (++tryCnt < MAX_TRY_OP_QUEUE) {
                continue;
            }
            tryCnt = 0;
            GaussUsleep(QUEUE_WAIT_TIME);
            if (m_needStopFlag.load(std::memory_order_acquire)) {
                goto Exit;
            }
        }
        m_walDispatcher->FreeTrxChangeCtx(static_cast<TrxChangeCtx *>(curTrx));
    }
Exit:
    m_isRunningFlag.store(false);
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
           ErrMsg("parallel decode worker %d end. pid: %lu", m_workerId, thrd->GetCore()->pid));
    Report();
    g_storageInstance->UnregisterThread();
}

/*
 * this function will:
 * 1. decode input trx.
 * 2. collect new catalog change in this trx caused by ddl if it has.
 * 3. update decode dict to keep tableInfo fresh.
 */
RetStatus ParallelDecodeWorker::DecodeTrx(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange)
{
    if (trxChange->flags.HasCatalogChanges()) {
        StorageAssert(0);
        return DecodeTrxWithDDLInternal(trxOut, trxChange);
    } else {
        return DecodeTrxWithoutDDLInternal(trxOut, trxChange);
    }
}

/* normal condition, trx without ddl */
RetStatus ParallelDecodeWorker::DecodeTrxWithoutDDLInternal(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange)
{
    dlist_iter iter;
    if (unlikely(m_decodePlugin->DecodeBegin(trxOut, trxChange, m_decodeOptions) == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    dlist_foreach(iter, &trxChange->changes) {
        RowChange *rowChange = dlist_container(RowChange, node, iter.cur);
        /* step1: get the correspond tableInfo from decodeDict */
        DecodeTableInfo *tableInfo = GetTableInfoFromDecodeDict(rowChange);
        if (unlikely(tableInfo == nullptr)) {
            return DSTORE_FAIL;
        }
        /* step2: use decode dict to decode. */
        if (unlikely(m_decodePlugin->DecodeChange(trxOut, rowChange, tableInfo, m_decodeOptions) == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }
    }
    if (unlikely(m_decodePlugin->DecodeCommit(trxOut, trxChange, m_decodeOptions) == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    /* This is used for plsn advance */
    trxOut->restartDecodingPlsn  = trxChange->restartDecodingPlsn;
    return DSTORE_SUCC;
}

/* trx with ddl */
RetStatus ParallelDecodeWorker::DecodeTrxWithDDLInternal(TrxLogicalLog *trxOut, TrxChangeCtx *trxChange)
{
    /* step1: collect catalog tuple changes of each ddl into internal tableInfo */
    CollectTableInfoFromTuples(trxChange);
    /* step2: convert each internal tableInfo to internalDecodetableInfo(DecodeTableInfo + cid) and ready to decode */
    ConvertTrxInternalTblInfoToDecodeTableInfo(trxChange);
    StorageAssert(DListIsEmpty(&trxChange->internalTableInfos));
    /*
     * step3: Init decodetableinfo hash, map(tableOid) to DecodeTableInfo.
     * ddls on the same table will influence the visibility of internal DecodeTableInfo,
     * such as, In the process of decoding from begin to end in this trx,
     * we make this hashtable always keep the newest decodeTableInfo relative to current cid,
     * meanwhile drop the old one when it update,
     * cause the old one will never influence other trxs outside.
     * More efficiently, at last we can use all newest DecodeTableInfo to update decodedict, i.e, let other trx see.
     */
    InitDecodeTableInfoHash(trxChange);
    dlist_iter iter;
    if (unlikely(m_decodePlugin->DecodeBegin(trxOut, trxChange, m_decodeOptions) == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    dlist_foreach(iter, &trxChange->changes) {
        RowChange *rowChange = dlist_container(RowChange, node, iter.cur);
        /* 1. if rowChange is DDL change(row): try to update decodetableInfo HASH and drop old one;
         * 2. if rowChange is DML change(row): get correct DecodetableInfo version based on hashtable! */
        if (rowChange->IsCatalogChange()) {
            UpdateDecodeTableInfoHash(trxChange, rowChange);
            continue;
        }
        /* Decode this rowchange.
         * first try find correspond tableInfo internal,
         * if not found, means ddl in this trx not change this row's tableInfo, so get tableinfo from decodedict */
        bool found;
        TrxInternalDecodeTblInfoEnt *ent = static_cast<TrxInternalDecodeTblInfoEnt *>(
            hash_search(trxChange->internalDecodeTableInfoHash, static_cast<void *>(&rowChange->data.tuple.tableOid),
            HASH_FIND, &found));
        DecodeTableInfo *tableInfo;
        if (!found) {
            /* this trx had not changed this tableOid's tableinfo, so get from decodedict */
            tableInfo = GetTableInfoFromDecodeDict(rowChange);
        } else {
            TrxInternalDecodeTblInfo *decodeInfo =
                dlist_container(TrxInternalDecodeTblInfo, tableInfoNode, ent->tableInfoNode);
            tableInfo = decodeInfo->tableInfo;
        }
        if (unlikely(m_decodePlugin->DecodeChange(trxOut, rowChange, tableInfo, m_decodeOptions) == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }
    }
    if (unlikely(m_decodePlugin->DecodeCommit(trxOut, trxChange, m_decodeOptions) == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    /* step3: update/fresh decode dict cause there is ddl operation in this trx */
    UpdateDecodeDict(trxChange);
    return DSTORE_SUCC;
}

DecodeTableInfo* ParallelDecodeWorker::GetTableInfoFromDecodeDict(RowChange *rowChange) const
{
    Oid tableOid = rowChange->data.tuple.tableOid;
    CommitSeqNo snapshotCsn = rowChange->data.tuple.snapshotCsn;
    DecodeTableInfo *tableInfo = nullptr;
    int tryCnt = 0;
    while (likely((tableInfo = m_decodeDict->GetVisibleDecodeTableInfo(tableOid, snapshotCsn)) != nullptr)) {
        if (unlikely(tableInfo->status == DecodeTableInfoStatus::EXIST_BUT_UNCOLLECTED)) {
            tryCnt += 1;
            ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
                ErrMsg("parallel decode worker %lu is waiting for decode dict fresh."
                    "tableOid: %u, snapshotCsn: %lu, try get times: %d", thrd->GetCore()->pid,
                    rowChange->data.tuple.tableOid, rowChange->data.tuple.snapshotCsn, tryCnt));
            GaussUsleep(1000000L);
        } else {
            StorageAssert(tableInfo->status == DecodeTableInfoStatus::COLLECTED);
            return tableInfo;
        }
    }
    return tableInfo;
}

void ParallelDecodeWorker::CollectTableInfoFromTuples(TrxChangeCtx *trxChange)
{
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &trxChange->catalogTupleChanges) {
        RowChange *rowChange = dlist_container(RowChange, data.catalogTuple.catalogNode, iter.cur);
        if (rowChange->data.catalogTuple.tableOid == PG_NAMESPACE_OID) {
            BuildNameSpaceInfo(trxChange, rowChange);
            continue;
        } else {
            BuildRelationInfo(trxChange, rowChange);
        }
        DListDelete(iter.cur);
    }
}

void ParallelDecodeWorker::ConvertTrxInternalTblInfoToDecodeTableInfo(TrxChangeCtx *trxChange)
{
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &trxChange->internalTableInfos) {
        TrxInternalTblInfo *baseInfo = dlist_container(TrxInternalTblInfo, tableInfoNode, iter.cur);
        /* convert TrxInternalTblInfo to internalDecodeTableInfo (DecodeTableInfo + cid) */
        TrxInternalDecodeTblInfo *intlDecodeTableInfo =
            static_cast<TrxInternalDecodeTblInfo *>(DstorePalloc0(sizeof(TrxInternalDecodeTblInfo)));
        if (unlikely(intlDecodeTableInfo == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate intlDecodeTableInfo OOM."));
        }
        intlDecodeTableInfo->cid = baseInfo->cid;
        intlDecodeTableInfo->tableInfo = baseInfo->ConvertToDecodeTableInfo();
        DListPushTail(&trxChange->trxInternalDecodeTblInfos, &intlDecodeTableInfo->tableInfoNode);
        DListDelete(iter.cur);
        DstorePfree(baseInfo);
    }
}


void ParallelDecodeWorker::BuildNameSpaceInfo(TrxChangeCtx *trxChange, RowChange *rowChange)
{
    Oid nspId;
    DstoreNameData nspName;
    GetNspInfo(rowChange, nspId, nspName);
    TrxInternalNsp *info = FindNewestTrxInternalNsp(trxChange, nspId, INVALID_CID);
    if (info != nullptr && rowChange->data.catalogTuple.cid == info->cid) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
            ErrMsg("Namespace tuple changed several times with the same conmanId. Oid: %u nspName: %s cid: %u",
                   info->nspId, info->nspName.data, info->cid));
        return;
    }
    if (info == nullptr || rowChange->data.catalogTuple.cid > info->cid) {
        info = static_cast<TrxInternalNsp *>(DstorePalloc(sizeof(TrxInternalNsp)));
        if (unlikely(info == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate info OOM."));
        }
        info->nspId = nspId;
        info->nspName = nspName;
        info->cid = rowChange->data.catalogTuple.cid;
        info->isDelete = (rowChange->type == RowChangeType::CATALOG_DELETE);
        DListPushTail(&trxChange->internalNameSpaceInfos, &info->nspNode);
        trxChange->ddlCounts += 1;
    }
}

/* parse each tuple to build TrxInternalTblInfo */
void ParallelDecodeWorker::BuildRelationInfo(TrxChangeCtx *trxChange, RowChange *rowChange)
{
    Oid ddlTableOid = GetDDLTableOid(rowChange);
    TrxInternalTblInfo *lastInternalTableInfo = FindNewestTrxInternalTblInfo(trxChange, ddlTableOid);
    TrxInternalTblInfo *curInternalTableInfo;
    if (lastInternalTableInfo == nullptr || lastInternalTableInfo->cid < rowChange->data.catalogTuple.cid) {
        curInternalTableInfo = static_cast<TrxInternalTblInfo *>(DstorePalloc(sizeof(TrxInternalTblInfo)));
        if (unlikely(curInternalTableInfo == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate space OOM"));
        }
        if (lastInternalTableInfo == nullptr) {
            /* update based on decode dict */
            DecodeTableInfo *decodeTableInfo = GetTableInfoFromDecodeDict(rowChange);
            ConvertDecodeTableInfoToTrxInternalTableInfo(decodeTableInfo, curInternalTableInfo);
        } else {
            /* update based on last table info */
            lastInternalTableInfo->CopyData(curInternalTableInfo);
        }
        curInternalTableInfo->cid = rowChange->data.catalogTuple.cid;
        CollectRelationChangeToTrxInternalTblInfo(rowChange, curInternalTableInfo);
        DListPushTail(&trxChange->internalTableInfos, &curInternalTableInfo->tableInfoNode);
        trxChange->ddlCounts += 1;
    } else {
        StorageAssert(lastInternalTableInfo->cid == rowChange->data.catalogTuple.cid);
        CollectRelationChangeToTrxInternalTblInfo(rowChange, lastInternalTableInfo);
    }
}

void ParallelDecodeWorker::ConvertDecodeTableInfoToTrxInternalTableInfo(DecodeTableInfo *decodeTableInfo,
                                                                        TrxInternalTblInfo *trxInternalTableInfo) const
{
    StorageAssert(decodeTableInfo != nullptr);
    StorageAssert(trxInternalTableInfo != nullptr);
    trxInternalTableInfo->tableOid = decodeTableInfo->tableOid;
    errno_t rc = memcpy_s(trxInternalTableInfo->relName.data,
                          NAME_DATA_LEN, decodeTableInfo->relName.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    trxInternalTableInfo->nspId = decodeTableInfo->nspId;
    rc = memcpy_s(trxInternalTableInfo->nspName.data, NAME_DATA_LEN, decodeTableInfo->nspName.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    trxInternalTableInfo->isDelete = (decodeTableInfo->status == DecodeTableInfoStatus::DELETE);
    trxInternalTableInfo->natts = decodeTableInfo->fakeDescData.natts;
    DListInit(&trxInternalTableInfo->attrs);

    for (int i = 0; i < decodeTableInfo->fakeDescData.natts; i++) {
        TrxInternalAttr *copyAttr = static_cast<TrxInternalAttr *>(DstorePalloc(sizeof(TrxInternalAttr)));
        rc = memcpy_s(&copyAttr->attr, ATTRIBUTE_FIXED_SIZE,
                      decodeTableInfo->fakeDescData.attrs[i], ATTRIBUTE_FIXED_SIZE);
        storage_securec_check(rc, "", "");
        DListPushTail(&trxInternalTableInfo->attrs, &copyAttr->attrNode);
    }
}

void ParallelDecodeWorker::CollectRelationChangeToTrxInternalTblInfo(RowChange *rowChange,
                                                                     TrxInternalTblInfo *internalTableInfo)
{
    StorageAssert(rowChange->data.catalogTuple.cid == internalTableInfo->cid);
    StorageAssert(GetDDLTableOid(rowChange) == internalTableInfo->tableOid);
    if (rowChange->data.catalogTuple.tableOid == SYSTABLE_RELATION_OID) {
        CollectSysRelChange(rowChange, internalTableInfo);
    } else {
        /* collect pg_attribute change */
        CollectSysAttrChange(rowChange, internalTableInfo);
    }
}

void ParallelDecodeWorker::CollectSysRelChange(RowChange *rowChange, TrxInternalTblInfo *internalTableInfo)
{
    /* collect pg_class change */
    StorageAssert(!internalTableInfo->isDelete);
    if (rowChange->type == RowChangeType::CATALOG_DELETE) {
        internalTableInfo->isDelete = true;
        return;
    } else if (rowChange->type == RowChangeType::CATALOG_INSERT) {
        /* we already have parse this tuple in FindCreateTableOpAndAllocMem */
        return;
    } else {
        DecodeRelationData relationData;
        m_decodeDict->GetInfoFromSysRelationTuple(rowChange->data.catalogTuple.newTuple->memTup, relationData);
        StorageAssert(internalTableInfo->tableOid == relationData.tableOid);
        internalTableInfo->UpdateRelationInfo(&relationData);
    }
}

void ParallelDecodeWorker::CollectSysAttrChange(RowChange *rowChange, TrxInternalTblInfo *internalTableInfo) const
{
    if (internalTableInfo->isDelete) {
        return;
    }
    Form_pg_attribute oldAttr = nullptr;
    Form_pg_attribute newAttr = nullptr;
    if (rowChange->data.tuple.oldTuple != nullptr) {
        oldAttr = static_cast<Form_pg_attribute>(static_cast<void *>(
            rowChange->data.catalogTuple.oldTuple->memTup.GetValues()));
        StorageAssert(rowChange->type == RowChangeType::CATALOG_DELETE ||
                      rowChange->type == RowChangeType::CATALOG_UPDATE);
        bool exist = internalTableInfo->DeleteAttr(oldAttr->attnum);
        StorageAssert(exist);
        UNUSED_VARIABLE(exist);
    }
    if (rowChange->data.tuple.newTuple != nullptr) {
        newAttr = static_cast<Form_pg_attribute>(static_cast<void *>(
            rowChange->data.catalogTuple.newTuple->memTup.GetValues()));
        StorageAssert(rowChange->type == RowChangeType::CATALOG_UPDATE ||
                      rowChange->type == RowChangeType::CATALOG_INSERT);
        TrxInternalAttr *newAttrInfo =  static_cast<TrxInternalAttr *>(DstorePalloc(sizeof(TrxInternalAttr)));
        if (unlikely(newAttrInfo == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate newAttrInfo OOM."));
        }
        errno_t rc = memcpy_s(&newAttrInfo->attr, MAXALIGN(ATTRIBUTE_FIXED_SIZE),
                              static_cast<void *>(newAttr), MAXALIGN(ATTRIBUTE_FIXED_SIZE));
        storage_securec_check(rc, "\0", "\0");
        DListPushTail(&internalTableInfo->attrs, &newAttrInfo->attrNode);
    }
}

void ParallelDecodeWorker::InitDecodeTableInfoHash(TrxChangeCtx *trxChange)
{
    HASHCTL info;
    info.keysize = sizeof(Oid);
    info.entrysize = sizeof(TrxInternalDecodeTblInfoEnt);
    info.hcxt = m_privateDecodeMctx;
    int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    trxChange->internalDecodeTableInfoHash =
        hash_create("TrxInternalTableInfo", trxChange->ddlCounts, &info, hashFlags);
}

/* update internal tableinfo hash, and drop the old one, the old one must exist */
void ParallelDecodeWorker::UpdateDecodeTableInfoHash(TrxChangeCtx *trxChange, RowChange *rowChange) const
{
    if (rowChange->data.tuple.tableOid == PG_NAMESPACE_OID) {
        Oid nspId;
        DstoreNameData nspName;
        GetNspInfo(rowChange, nspId, nspName);
        SetNspInfoVisible(trxChange, nspId, nspName, rowChange->data.catalogTuple.cid);
        return;
    }
    Oid ddlTableOid = GetDDLTableOid(rowChange);
    bool found;
    TrxInternalDecodeTblInfoEnt *ent = static_cast<TrxInternalDecodeTblInfoEnt *>(
        hash_search(trxChange->internalDecodeTableInfoHash, static_cast<void *>(&ddlTableOid), HASH_ENTER, &found));
    StorageAssert(ent != nullptr);
    if (found) {
        TrxInternalDecodeTblInfo *decodeTable =
            dlist_container(TrxInternalDecodeTblInfo, tableInfoNode, ent->tableInfoNode);
        if (decodeTable->cid == rowChange->data.catalogTuple.cid) {
            /* this rowChange belongs to the newest DecodeTableInfo, nothing to do */
            return;
        }
        StorageAssert(decodeTable->cid < rowChange->data.catalogTuple.cid);
        /* delete old DecodetableInfo version, cause it won't be used anymore */
        DListDelete(ent->tableInfoNode);
        DstorePfree(decodeTable);
    }
    /* insert new DecodetableInfo to keep internalDecodeTableInfoHash fresh */
    dlist_iter iter;
    dlist_foreach(iter, &trxChange->trxInternalDecodeTblInfos) {
        TrxInternalDecodeTblInfo *newInfo = dlist_container(TrxInternalDecodeTblInfo, tableInfoNode, iter.cur);
        if (newInfo->cid == rowChange->data.catalogTuple.cid) {
            StorageAssert(newInfo->tableInfo->tableOid == ddlTableOid);
            ent->tableInfoNode = &newInfo->tableInfoNode;
            return;
        }
    }
    /* should not happened. */
    ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("update fialed. internal error"));
    return;
}

void ParallelDecodeWorker::SetNspInfoVisible(TrxChangeCtx *trxChange,
                                             const Oid nspId, const DstoreNameData nspName, const CommandId cid) const
{
    /* step1. drop old namespace info, cause it won't be used anymore */
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &trxChange->internalNameSpaceInfos) {
        TrxInternalNsp *info = dlist_container(TrxInternalNsp, nspNode, iter.cur);
        StorageAssert(info->cid <= cid);
        if (info->nspId == nspId && info->cid < cid) {
            DListDelete(iter.cur);
            DstorePfree(info);
            break;
        }
        if (info->cid >= cid) {
            break;
        }
    }
    /* step2. update nspname of decodetableInfo generated by this trx after cid */
    dlist_iter decodeTableIter;
    dlist_reverse_foreach(decodeTableIter, &trxChange->trxInternalDecodeTblInfos) {
        TrxInternalDecodeTblInfo *info = dlist_container(TrxInternalDecodeTblInfo, tableInfoNode, decodeTableIter.cur);
        StorageAssert(info->cid >= cid);
        if (info->tableInfo->nspId == nspId && info->cid > cid) {
            /* visible */
            errno_t rt = memcpy_s(info->tableInfo->nspName.data, NAME_DATA_LEN, nspName.data, NAME_DATA_LEN);
            storage_securec_check(rt, "\0", "\0");
        }
        if (info->cid < cid) {
            break;
        }
    }
}

TrxInternalTblInfo* ParallelDecodeWorker::FindNewestTrxInternalTblInfo(TrxChangeCtx *trxChange,
                                                                       Oid tableOid, CommandId cid) const
{
    dlist_iter iter;
    dlist_reverse_foreach(iter, &trxChange->internalTableInfos) {
        TrxInternalTblInfo *internalTableInfo = dlist_container(TrxInternalTblInfo, tableInfoNode, iter.cur);
        if (internalTableInfo->tableOid == tableOid) {
            if (cid == INVALID_CID || internalTableInfo->cid == cid) {
                return internalTableInfo;
            }
        }
    }
    return nullptr;
}

TrxInternalNsp* ParallelDecodeWorker::FindNewestTrxInternalNsp(TrxChangeCtx *trxChange,
                                                               Oid nspId, CommandId cid) const
{
    dlist_iter iter;
    dlist_reverse_foreach(iter, &trxChange->internalNameSpaceInfos) {
        TrxInternalNsp *info = dlist_container(TrxInternalNsp, nspNode, iter.cur);
        if (info->nspId == nspId) {
            if (cid == INVALID_CID || info->cid == cid) {
                return info;
            }
        }
    }
    return nullptr;
}

void ParallelDecodeWorker::UpdateDecodeDict(TrxChangeCtx *trxChange)
{
    /* now, trxChange->internalDecodeTableInfos and internalNameSpaceInfos are the newest ddls of this trx block,
     * older ones has been droped when decode, here to update decode dict and release source */
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &trxChange->trxInternalDecodeTblInfos) {
        TrxInternalDecodeTblInfo *decodeInfo = dlist_container(TrxInternalDecodeTblInfo, tableInfoNode, iter.cur);
        decodeInfo->tableInfo->csn = trxChange->commitCsn;
        if (STORAGE_FUNC_FAIL(m_decodeDict->UpdateDecodeDictChangeFromWal(decodeInfo->tableInfo))) {
            ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("parallel worker update decode dict failed"));
            return;
        }
        DListDelete(iter.cur);
        DstorePfree(decodeInfo);
    }
    hash_destroy(trxChange->internalDecodeTableInfoHash);
    trxChange->internalDecodeTableInfoHash = nullptr;
}

Oid ParallelDecodeWorker::GetDDLTableOid(RowChange *rowChange) const
{
    StorageAssert(rowChange->data.catalogTuple.tableOid == SYSTABLE_RELATION_OID ||
                  rowChange->data.catalogTuple.tableOid == SYSTABLE_ATTRIBUTE_OID);
    HeapTuple *destTuple;
    if (rowChange->data.tuple.newTuple != nullptr) {
        destTuple = &(rowChange->data.catalogTuple.newTuple->memTup);
    } else {
        destTuple = &(rowChange->data.catalogTuple.oldTuple->memTup);
    }
    if (rowChange->data.catalogTuple.tableOid == SYSTABLE_RELATION_OID) {
        StorageAssert(destTuple->GetOid() != DSTORE_INVALID_OID);
        return destTuple->GetOid();
    } else {
        Form_pg_attribute attr = static_cast<Form_pg_attribute>(static_cast<void *>(destTuple->GetValues()));
        return attr->attrelid;
    }
}

void ParallelDecodeWorker::GetNspInfo(RowChange *rowChange, Oid &nspId, DstoreNameData &nspName) const
{
    StorageAssert(rowChange->data.catalogTuple.tableOid == PG_NAMESPACE_OID);
    HeapTuple* destTuple;
    if (rowChange->data.tuple.newTuple != nullptr) {
        destTuple = &rowChange->data.catalogTuple.newTuple->memTup;
    } else {
        destTuple = &rowChange->data.catalogTuple.oldTuple->memTup;
    }
    nspId = destTuple->GetOid();
    StorageAssert(nspId != DSTORE_INVALID_OID);

    Form_pg_namespace newNsp = static_cast<Form_pg_namespace>(static_cast<void *>(destTuple->GetValues()));
    errno_t rc = memcpy_s(nspName.data, NAME_DATA_LEN, newNsp->nspname.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
}

void ParallelDecodeWorker::Stop() noexcept
{
    if (m_isRunningFlag) {
        m_needStopFlag.store(true, std::memory_order_release);
    }

    if (m_workerThrd != nullptr) {
        m_workerThrd->join();
        delete m_workerThrd;
        m_workerThrd = nullptr;
    }
}

void ParallelDecodeWorker::Report() const
{
    ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION,
        ErrMsg("parallel decode worker %lu : trxChangeQueue max usage %u, trxLogicalLogQueue max usage %u.",
               thrd->GetCore()->pid, m_trxChangesQueue->GetMaxUsage(), m_trxLogicalLogQueue->GetMaxUsage()));
}

TrxLogicalLog* ParallelDecodeWorker::PopTrxLogicalLog()
{
    void *log = nullptr;
    int tryCnt = 0;
    while ((log = m_trxLogicalLogQueue->Pop()) == nullptr) {
        if (++tryCnt >= MAX_TRY_OP_QUEUE) {
            if (!m_isRunningFlag.load()) {
                return nullptr;
            }
            GaussUsleep(QUEUE_WAIT_TIME);
            tryCnt = 0;
        }
    }
    return static_cast<TrxLogicalLog *>(log);
}
#endif

}