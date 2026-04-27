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
 * dstore_logical_replication_mgr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_logical_replication_mgr.h"
#include "transaction/dstore_csn_mgr.h"
#include "wal/dstore_wal_write_context.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
LogicalReplicaMgr::LogicalReplicaMgr(PdbId pdbId, DstoreMemoryContext memoryContext)
    : m_pdbId(pdbId),
      m_memoryContext(memoryContext),
      m_decodeDict(nullptr),
      m_slotsPlsnMin(INVALID_END_PLSN),
      m_slotsCatalogCsnMin(INVALID_CSN),
      m_slotsDecodeDictCsnMin(INVALID_CSN),
      m_logicalSlotArray(),
      m_logicalDecodedCtx()
{
    LWLockInitialize(&m_slotCtlLock, LWLOCK_GROUP_LOGICAL_REPLICA_MGR_SLOT_CTL);
    LWLockInitialize(&m_slotAllocLock, LWLOCK_GROUP_LOGICAL_REPLICA_MGR_SLOT_ALLOC);
}

LogicalReplicaMgr::~LogicalReplicaMgr()
{
    m_memoryContext = nullptr;
    m_decodeDict = nullptr;
}

RetStatus LogicalReplicaMgr::Init()
{
    /* step1: init slots resources. try to load persistent data and init slot, if found one */
    if (STORAGE_FUNC_FAIL(LoadLogicalReplicationSlotsFromDisk())) {
        return DSTORE_FAIL;
    }

    /* step2: init decode context. */
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        m_logicalDecodedCtx[i] = nullptr;
    }

    /* step3: init and load decode dict. */
    DstoreMemoryContext decodeDictMctx = DstoreAllocSetContextCreate(m_memoryContext,
        "DecodeDict", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    m_decodeDict = DstoreNew(m_memoryContext) DecodeDict(m_pdbId, decodeDictMctx);
    StorageReleasePanic(m_decodeDict == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("Out of memory"));
    if (STORAGE_FUNC_FAIL(m_decodeDict->Init())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void LogicalReplicaMgr::Destroy()
{
    /* step1. destroy decode context and destroy replication slot */
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        if (m_logicalDecodedCtx[i] != nullptr) {
            m_logicalDecodedCtx[i]->Stop();
            m_logicalDecodedCtx[i]->Destroy();
            delete m_logicalDecodedCtx[i];
            m_logicalDecodedCtx[i] = nullptr;
        }

        if (m_logicalSlotArray[i].IsInUse()) {
            m_logicalSlotArray[i].Destroy();
        }
    }

    /* step2. destroy decodedict */
    if (m_decodeDict != nullptr) {
        m_decodeDict->Destroy();
        delete m_decodeDict;
        m_decodeDict = nullptr;
    }
}

RetStatus LogicalReplicaMgr::LoadLogicalReplicationSlotsFromDisk()
{
    WalStreamManager *walStreamManager =
        g_storageInstance->GetPdb(m_pdbId)->GetWalMgr()->GetWalStreamManager();
    uint32 walStreamsCount = walStreamManager->GetTotalWalStreamsCount();
    if (walStreamsCount == 0) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION, ErrMsg("no walstream in this node"));
        return DSTORE_SUCC;
    }
    dlist_mutable_iter iter = {};
    WalStreamNode *walStreamNode = nullptr;
    WalStream *walStream = nullptr;
    walStreamNode = walStreamManager->GetNextWalStream(&iter, WalStream::IsWalStreamForWrite);
    if (walStreamNode != nullptr) {
        walStream = walStreamNode->walStream;
    }
    if (walStream == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_LOGICAL_REPLICATION, ErrMsg("no writing walstream in this node"));
        return DSTORE_SUCC;
    }
    WalId targetWalId = walStream->GetWalId();
    StorageAssert(targetWalId != INVALID_WAL_ID);
    auto repSlotInfo = static_cast<ControlLogicalReplicationSlotPageItemData **>(
        DstorePalloc(MAX_LOGICAL_SLOT_NUM * sizeof(ControlLogicalReplicationSlotPageItemData *)));
    if (STORAGE_VAR_NULL(repSlotInfo)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    ControlFile *controlFile = storagePdb->GetControlFile();
    int slotCountNum = 0;
    if (controlFile->GetAllLogicalReplicationSlotBaseOnWalId(targetWalId, repSlotInfo, slotCountNum)) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
               ErrMsg("failed to get all logical replication slot info from disk"));
        DstorePfreeExt(repSlotInfo);
        return DSTORE_FAIL;
    }
    StorageAssert(slotCountNum <= MAX_LOGICAL_SLOT_NUM);
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("get %d logical replication slots from disk", slotCountNum));
    /* restored slots num is limited by slotCountNum */
    for (int i = 0; i < slotCountNum; i++) {
        m_logicalSlotArray[i].RestoreFromDisk(*repSlotInfo[i]);
        m_logicalSlotArray[i].SetSlotId(i);
    }
    DstorePfreeExt(repSlotInfo);
    FlushSlotsRelatedLimit();
    return DSTORE_SUCC;
}

/*
 * Create a new replication slot and mark it as used by this backend.
 */
RetStatus LogicalReplicaMgr::CreateLogicalReplicationSlot(char *slotName, char *plugin,
    const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    DstoreLWLockAcquire(&m_slotAllocLock, LW_EXCLUSIVE);
    if (STORAGE_FUNC_FAIL(DoCreateLogicalReplicationSlot(slotName, plugin,
        thrd->m_walWriterContext->GetWalId(), syncCatalogCallBack))) {
        LWLockRelease(&m_slotAllocLock);
        return DSTORE_FAIL;
    }
    LWLockRelease(&m_slotAllocLock);
    return DSTORE_SUCC;
}

RetStatus LogicalReplicaMgr::DoCreateLogicalReplicationSlot(char *slotName, char *plugin, WalId walId,
    const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    /* Step 1: Get proper slotId. */
    LogicalReplicationSlot *targetSlot = nullptr;
    int slotId = INVALID_SLOT_ID;
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        LogicalReplicationSlot *cur = &m_logicalSlotArray[i];
        if (cur->IsInUse() && cur->IsNameEqual(slotName)) {
            LWLockRelease(&m_slotCtlLock);
            ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
                   ErrMsg("replication slot \"%s\" already exists", slotName));
            storage_set_error(DECODE_ERROR_DUPLICATE_SLOT, slotName);
            return DSTORE_FAIL;
        }
        if (targetSlot == nullptr && !cur->IsInUse()) {
            slotId = i;
            targetSlot = cur;
        }
    }

    /* If all slots are in use, we're out of luck. */
    if (targetSlot == nullptr) {
        LWLockRelease(&m_slotCtlLock);
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
               ErrMsg("all replication slots are in use. Free one or increase MAX_LOGICAL_SLOT_NUM."));
        storage_set_error(DECODE_ERROR_SLOT_EXCEED_MAX_NUM);
        return DSTORE_FAIL;
    }
    LWLockRelease(&m_slotCtlLock);

    /* Step 2: init slot and serialize to disk */
    DstoreLWLockAcquire(&m_slotCtlLock, LW_EXCLUSIVE);
    StorageAssert(!targetSlot->IsInUse());
    targetSlot->Init(m_pdbId, slotName, plugin, walId, slotId);
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalStream *stream = walStreamMgr->GetWalStream(walId);
    WalPlsn stuckPlsn = stream->GetMaxAppendedPlsn();
    stream->WaitTargetPlsnPersist(stuckPlsn);
    CommitSeqNo stuckCsn;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(stuckCsn, false))) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Get next csn failed."));
        return DSTORE_FAIL;
    }
    targetSlot->AdvanceRestartPlsn(stuckPlsn);
    targetSlot->AdvanceDecodeDictCsnMin(stuckCsn);
    targetSlot->SetCatalogCsnMin(stuckCsn);
    targetSlot->SetUse(true);
    targetSlot->SetActive(true);
    targetSlot->SerializeToDisk(true);
    LWLockRelease(&m_slotCtlLock);

    /* stuck wal recycle and recycleCsn forward as soon as possible. */
    FlushSlotsRelatedLimit();

    /*
     * step 3: find start point of logical decoding for this slot, this process can do without control lock, cause we
     * set targetSlot as active, other threads which aim to acquire this slot would get nothing.
     */
    RetStatus ret = targetSlot->FindStartPoint(syncCatalogCallBack);
    if (ret == DSTORE_SUCC) {
        /* We can now mark the slot inUse, and that makes it our slot. */
        targetSlot->SerializeToDisk(false);
    } else {
        /* anything wrong in the process of finding start point would clear the resource */
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
            ErrMsg("create replication slot failed in finding startpoint."));
        DoDropLogicalReplicationSlot(targetSlot);
        return DSTORE_FAIL;
    }

    /* Step 4: keep recycleCsn forward and release this slot */
    DstoreLWLockAcquire(&m_slotCtlLock, LW_EXCLUSIVE);
    if (syncCatalogCallBack != nullptr) {
        targetSlot->SetCatalogCsnMin(INVALID_CSN);
    }
    targetSlot->SetActive(false);
    LWLockRelease(&m_slotCtlLock);

    FlushSlotsRelatedLimit();
    return DSTORE_SUCC;
}

/*
 * Drop replication slot with given name
 * @return SUCC if and only if found and dropped
 */
RetStatus LogicalReplicaMgr::DropLogicalReplicationSlot(char *slotName)
{
    LogicalReplicationSlot *slot = LogicalReplicaMgr::AcquireLogicalReplicationSlot(slotName);
    if (slot == nullptr) {
        return DSTORE_FAIL;
    }
    DstoreLWLockAcquire(&m_slotAllocLock, LW_EXCLUSIVE);
    DoDropLogicalReplicationSlot(slot);
    FlushSlotsRelatedLimit();
    LWLockRelease(&m_slotAllocLock);
    return DSTORE_SUCC;
}

void LogicalReplicaMgr::DoDropLogicalReplicationSlot(LogicalReplicationSlot *slot)
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_EXCLUSIVE);
    /* first delete persistent data and then clean memory */
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    ControlFile *controlFile = storagePdb->GetControlFile();
    bool isExist;
    if (STORAGE_FUNC_FAIL(controlFile->DeleteLogicalReplicationSlot(slot->GetName(), slot->GetWalId(), isExist))) {
        LWLockRelease(&m_slotCtlLock);
        StorageAssert(0);
    }
    slot->Reset();
    LWLockRelease(&m_slotCtlLock);
}

LogicalReplicationSlot* LogicalReplicaMgr::AcquireLogicalReplicationSlot(char *slotName)
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        if (m_logicalSlotArray[i].IsInUse() && m_logicalSlotArray[i].IsNameEqual(slotName)) {
            if (m_logicalSlotArray[i].IsActive()) {
                storage_set_error(DECODE_ERROR_SLOT_IS_ACTIVE);
                LWLockRelease(&m_slotCtlLock);
                return nullptr;
            }
            m_logicalSlotArray[i].SetActive(true);
            LWLockRelease(&m_slotCtlLock);
            return &m_logicalSlotArray[i];
        }
    }
    LWLockRelease(&m_slotCtlLock);
    storage_set_error(DECODE_ERROR_SLOT_NOT_FOUND, slotName);
    return nullptr;
}

/**
 * now we just mark slot as no active, not destroy
 * @param slot
 */
void LogicalReplicaMgr::ReleaseLogicalReplicationSlot(LogicalReplicationSlot *slot)
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    slot->SetActive(false);
    LWLockRelease(&m_slotCtlLock);
}

RetStatus LogicalReplicaMgr::AdvanceLogicalReplicationSlot(LogicalReplicationSlot* slot, CommitSeqNo uptoCSN,
                                                           bool needCheck)
{
    if (unlikely(slot == nullptr || !slot->IsInUse() || !slot->IsActive())) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("wrong slot, unkown"));
        return DSTORE_FAIL;
    }
    if (uptoCSN == INVALID_CSN || uptoCSN < slot->GetConfirmCsn()) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION,
               ErrMsg("uptoCsn %lu less than confirmcsn %lu", uptoCSN, slot->GetConfirmCsn()));
        return DSTORE_FAIL;
    }
    if (needCheck) {
        CommitSeqNo nextCsn = INVALID_CSN;
        if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false))) {
            ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("Get next csn failed."));
            return DSTORE_FAIL;
        }
        uptoCSN = uptoCSN < nextCsn ? uptoCSN : (nextCsn - 1);
    }
    slot->AdvanceConfirmCsn(uptoCSN);
    slot->SerializeToDisk(false);
    return DSTORE_SUCC;
}

void LogicalReplicaMgr::ReportLogicalReplicationSlot(char *name, StringInfo slotInfo)
{
    DstoreLWLockAcquire(&m_slotAllocLock, LW_SHARED);
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        LogicalReplicationSlot *cur = &m_logicalSlotArray[i];
        if (cur->IsInUse() && (name == nullptr || cur->IsNameEqual(name))) {
            slotInfo->append("%s\t confirmCsn: %lu\t restartPlsn: %lu\t nodeId: %d\n",
                cur->GetName(), cur->GetConfirmCsn(), cur->GetRestartPlsn(), g_storageInstance->GetGuc()->selfNodeId);
        }
    }
    LWLockRelease(&m_slotAllocLock);
}

void LogicalReplicaMgr::FlushDependentMinCatalogCsn()
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    CommitSeqNo aggCatalogCsn = INVALID_CSN;
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        if (!m_logicalSlotArray[i].IsInUse()) {
            continue;
        }
        /* count slots with spinlock held */
        CommitSeqNo catalogCsn = m_logicalSlotArray[i].GetCatalogCsnMin();
        /* check the catalog catalogCsn */
        if (catalogCsn != INVALID_CSN && (aggCatalogCsn == INVALID_CSN || catalogCsn < aggCatalogCsn)) {
            aggCatalogCsn = catalogCsn;
        }
    }
    m_slotsCatalogCsnMin = aggCatalogCsn;
    /* Todo: use m_slotsCatalogCsnMin to stuck recycleCsn */

    LWLockRelease(&m_slotCtlLock);
}

void LogicalReplicaMgr::FlushDependentMinDecodeDictCsn()
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    CommitSeqNo aggDictCsn = INVALID_CSN;

    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        if (!m_logicalSlotArray[i].IsInUse()) {
            continue;
        }
        /* count slots with spinlock held */
        CommitSeqNo dictCsn = m_logicalSlotArray[i].GetDecodeDictCsnMin();
        /* check the decode dict restartCsn */
        if (dictCsn != INVALID_CSN && (aggDictCsn == INVALID_CSN || dictCsn < aggDictCsn))
            aggDictCsn = dictCsn;
    }
    m_slotsDecodeDictCsnMin = aggDictCsn;
    /* Todo: use m_slotsDecodeDictCsnMin to stuck decode dict recycle */

    LWLockRelease(&m_slotCtlLock);
}

/*
 * Compute the oldest WAL PLSN required by *logical* decoding slots.
 *
 * Returns INVALID_WAL_REC_PTR if logical decoding is disabled or no logicals
 * slots exist.
 *
 */
void LogicalReplicaMgr::FlushDependentMinPlsn()
{
    DstoreLWLockAcquire(&m_slotCtlLock, LW_SHARED);
    WalPlsn aggPlsn = INVALID_END_PLSN;
    for (int i = 0; i < MAX_LOGICAL_SLOT_NUM; i++) {
        if (!m_logicalSlotArray[i].IsInUse()) {
            continue;
        }
        /* count slots with spinlock held */
        WalPlsn restartPlsn = m_logicalSlotArray[i].GetRestartPlsn();
        if (restartPlsn != INVALID_CSN && (aggPlsn == INVALID_END_PLSN || restartPlsn < aggPlsn)) {
            aggPlsn = restartPlsn;
        }
    }
    m_slotsPlsnMin = aggPlsn;
    /* Todo: use m_slotsPlsnMin to stuck wal recycle */

    LWLockRelease(&m_slotCtlLock);
}

void LogicalReplicaMgr::FlushSlotsRelatedLimit()
{
    FlushDependentMinCatalogCsn();
    FlushDependentMinDecodeDictCsn();
    FlushDependentMinPlsn();
}

LogicalDecodeHandler* LogicalReplicaMgr::CreateLogicalDecodeHandler(LogicalReplicationSlot *logicalSlot,
                                                                    DecodeOptions *decodeOptions)
{
    int slotId = logicalSlot->GetSlotId();
    StorageAssert(slotId < MAX_LOGICAL_SLOT_NUM && slotId >= 0);
    StorageAssert(m_logicalDecodedCtx[slotId] == nullptr);
    StorageAssert(logicalSlot->IsActive()); /* caller should have aquired this slot and then start decode. */
    StorageAssert(logicalSlot->GetRestartPlsn() != INVALID_END_PLSN);
    StorageAssert(logicalSlot->GetConfirmCsn() != INVALID_CSN);

    m_logicalDecodedCtx[slotId] = DstoreNew(m_memoryContext)
        LogicalDecodeHandler(logicalSlot, decodeOptions, m_decodeDict, m_memoryContext, m_pdbId);
    if (STORAGE_VAR_NULL(m_logicalDecodedCtx[slotId])) {
        storage_set_error(DECODE_ERROR_DECODE_HANDLER_NO_MEM);
        return nullptr;
    }

    RetStatus rt = m_logicalDecodedCtx[slotId]->Init();
    if (rt == DSTORE_FAIL) {
        m_logicalDecodedCtx[slotId]->Destroy();
        delete m_logicalDecodedCtx[slotId];
        m_logicalDecodedCtx[slotId] = nullptr;
        storage_set_error(DECODE_ERROR_DECODE_CTX_INIT_FAIL);
        return nullptr;
    }
    return m_logicalDecodedCtx[slotId];
}

void LogicalReplicaMgr::DeleteLogicalDecodeHandler(LogicalDecodeHandler* decodeContext)
{
    StorageAssert(decodeContext != nullptr);
    m_logicalDecodedCtx[decodeContext->GetDecodeSlotId()] = nullptr;
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
}

void LogicalReplicaMgr::StartUpLogicalDecode(LogicalDecodeHandler* decodeContext)
{
    StorageAssert(decodeContext != nullptr);
    decodeContext->StartUp();
}

void LogicalReplicaMgr::StopLogicalDecode(LogicalDecodeHandler* decodeContext)
{
    StorageAssert(decodeContext != nullptr);
    decodeContext->Stop();
}

RetStatus LogicalReplicaMgr::SyncCatalogToDecodeDict(CatalogInfo *rawCatalog)
{
    StorageAssert(rawCatalog != nullptr);
    /* decode dict object do the real work */
    return m_decodeDict->SynchronizeCatalog(rawCatalog);
}
#endif
}
