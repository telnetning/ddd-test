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
 * dstore_wal_parallel_redo_worker.cpp
 *	  Implementation of Wal playback workers in parallel.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/wal/dstore_wal_parallel_redo_worker.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "port/dstore_port.h"
#include "framework/dstore_instance.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "wal/dstore_wal_recovery.h"
#include "wal/dstore_wal_parallel_redo_worker.h"

namespace DSTORE {
constexpr uint32 MAX_REDO_THREAD_NAME_LEN = 32;
constexpr uint32 MAX_REDO_QUE_SLEEP_SHORT = 200; /* 200 us */
static constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 5000;
static constexpr uint32 WAIT_COUNT_FOR_REPORT_DDL_REDO = 500000;

ParallelRedoWorker::ParallelRedoWorker(const ParallelRedoWorkerInitParam &initParam)
    :m_workerId(initParam.workerId),
    m_pdbId(initParam.pdbId),
    m_walId(initParam.walId),
    m_maxDispatchedPlsn(0),
    m_maxPlsnSyncer(0),
    m_maxRecoveryPlsn(0),
    m_maxRedoPlsn(0),
    m_ctrlFlag(initParam.flag),
    m_isRunningFlag(false),
    m_capacity(initParam.capacity),
    m_func(initParam.func),
    m_redoQueue(nullptr),
    m_memoryContext(initParam.memoryContext),
    m_workerThrd(nullptr),
    m_canStopFlag(false),
    m_finishFlag(false),
    m_freeList(nullptr),
    m_walRecovery(initParam.walRecovery)
{
}

ParallelRedoWorker::~ParallelRedoWorker() noexcept
{
    if (m_workerThrd != nullptr) {
        m_workerThrd->join();
        delete m_workerThrd;
        m_workerThrd = nullptr;
    }
    m_freeList->Destory();
    DstorePfreeExt(m_freeList);
    m_redoQueue->Destroy();
    DstorePfreeExt(m_redoQueue);
    m_func = nullptr;
    m_memoryContext = nullptr;
    m_walRecovery = nullptr;
}

RetStatus ParallelRedoWorker::Init()
{
    StorageAssert(m_redoQueue == nullptr);
    m_redoQueue = static_cast<BlockSpscQueue *>(DstorePalloc0(sizeof(BlockSpscQueue)));
    if (m_redoQueue == nullptr) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu new queue error!", m_pdbId, m_walId, m_workerId));
        return DSTORE_FAIL;
    }

    if (m_redoQueue->Init(m_capacity, sizeof(RedoWalRecordEntry), m_func) != DSTORE_SUCC) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu Init queue error!", m_pdbId, m_walId, m_workerId));
        return DSTORE_FAIL;
    }

    StorageAssert(m_freeList == nullptr);
    m_freeList = static_cast<WalRecordBufFreeEntryList *>(DstorePalloc0(sizeof(WalRecordBufFreeEntryList)));
    if (m_freeList == nullptr) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu new WalRecordBufFreeEntryList error!",
            m_pdbId, m_walId, m_workerId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

uint32 ParallelRedoWorker::GetFreeSpace()
{
    return m_redoQueue->GetFreeSpace();
}

void ParallelRedoWorker::AppendWalRecord(RedoWalRecordEntry *record, uint32 num, const char *functionName,
                                         int lineNumber)
{
    StorageAssert(record->recordEndPlsn >= m_maxDispatchedPlsn);
    if (unlikely(num > MAX_REDO_QUE_BATCH_NUM)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL,
               ErrMsg("[PDB:%u WAL:%lu]WalRecovery AppendWalRecord get invalid batch num %u.", m_pdbId, m_walId, num));
    }

    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu AppendWalRecord num:%d, endPlsn:%lu, func:%s, line:%d",
                  m_pdbId, m_walId, m_workerId, num, record[num - 1].recordEndPlsn, functionName, lineNumber));
    m_maxDispatchedPlsn = record[num - 1].recordEndPlsn;

    constexpr uint64 reportStep = 1000 * 1000 * 60; /* 1min */
    uint64 retryCounter = 0;
    while (STORAGE_FUNC_FAIL(m_redoQueue->PutNIfCould<RedoWalRecordEntry *>(record, num))) {
        if (unlikely(++retryCounter % reportStep == 0)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu with maxRedoPlsn %lu AppendWalRecord num:%d, "
                          "endPlsn:%lu, func:%s, line:%d retry %lu times.",
                          m_pdbId, m_walId, m_workerId, m_maxRedoPlsn, num, record[num - 1].recordEndPlsn, functionName,
                          lineNumber, retryCounter));
        }
        GaussUsleep(SLEEP_TIME);
    }
}

void ParallelRedoWorker::Run(const char* workerName)
{
    if (m_isRunningFlag) {
        return;
    }
    StorageAssert(m_workerThrd == nullptr);
    m_workerThrd = new std::thread(&ParallelRedoWorker::WorkerMain, this, workerName);
    m_isRunningFlag = true;
}

inline void ParallelRedoWorker::InitWorkerThreadName(const char *threadType, uint16 workerId)
{
    char threadName[MAX_REDO_THREAD_NAME_LEN];
    int result = sprintf_s(threadName, sizeof(threadName), "%s_%hu", threadType, workerId);
    if (result < 0) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu init threadName error!", m_pdbId, m_walId, m_workerId));
        g_storageInstance->UnregisterThread();
        return;
    }
    (void)pthread_setname_np(pthread_self(), &(threadName[0]));
}

inline void PrepareBuffForDecompress(BuffForDecompress &buffInfoForDecompress)
{
    buffInfoForDecompress.buffer = static_cast<char *>(DstorePalloc0(INIT_TEMP_BUFF_SIZE));
    if (buffInfoForDecompress.buffer == nullptr) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("ParallelRedoWorker new buffForDecompress error!"));
        return;
    }
    buffInfoForDecompress.bufferSize = INIT_TEMP_BUFF_SIZE;
}

void ParallelRedoWorker::WorkerMain(const char* workerName)
{
    DstoreSetMemoryOutOfControl();
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "redoWorker", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    WalUtils::HandleWalThreadCpuBind(workerName);

    InitWorkerThreadName(workerName, m_workerId);

    BuffForDecompress buffInfoForDecompress;
    PrepareBuffForDecompress(buffInfoForDecompress);
    RedoWalRecordEntry *recordEntry;
    uint32 getNum = 0;

    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]%s %hu started.", m_pdbId, m_walId, workerName, m_workerId));
    uint64 waitCount = 0;
    while (true) {
        if (m_canStopFlag.load(std::memory_order_relaxed) && m_redoQueue->IsEmpty() && CheckWaitListIsEmpty()) {
            UpdateMaxRedoPlsn(m_maxPlsnSyncer);
            m_finishFlag.store(true, std::memory_order_relaxed);
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]%s %hu stopping", m_pdbId, m_walId, workerName, m_workerId));
            break;
        }
        if (m_redoQueue->GetN<RedoWalRecordEntry *>(MAX_REDO_QUE_BATCH_NUM, &recordEntry, &getNum) != DSTORE_SUCC) {
            g_storageInstance->GetStat()->m_reportWaitStatus(
                static_cast<uint32_t>(GsStatWaitState::STATE_WAL_PROC_NOREDO_RECORD));
            ProcNoRedoWalRecordComing(&buffInfoForDecompress, waitCount);
            g_storageInstance->GetStat()->m_reportWaitStatus(
                static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
            if (++waitCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]%s %hu wait %lu times for record coming, "
                    "maxDispatchedPlsn:%lu, maxPlsnSyncer:%lu, maxRedoPlsn:%lu, maxRecoveryPlsn:%lu", m_pdbId, m_walId,
                    workerName, m_workerId, waitCount, m_maxDispatchedPlsn, m_maxPlsnSyncer, m_maxRedoPlsn,
                    m_maxRecoveryPlsn));
            }
            continue;
        }
        waitCount = 0;
        g_storageInstance->GetStat()->m_reportWaitStatus(
            static_cast<uint32_t>(GsStatWaitState::STATE_WAL_GET_REDO_RECORD));
        ProcAllGetRedoWalRecordEntrys(recordEntry, getNum, &buffInfoForDecompress);
        g_storageInstance->GetStat()->m_reportWaitStatus(
            static_cast<uint32_t>(GsStatWaitState::STATE_WAIT_UNDEFINED));
        if (unlikely(m_redoQueue->PopN(getNum) != DSTORE_SUCC)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]%s %hu m_redoQueue popN error!",
                m_pdbId, m_walId, workerName, m_workerId));
            break;
        }
    }
    DstorePfree(buffInfoForDecompress.buffer);
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]%s %hu queue max usage %u",
        m_pdbId, m_walId, workerName, m_workerId, m_redoQueue->GetMaxUsage()));
    WalUtils::HandleWalThreadCpuUnbind("WalRedoWorker");
    g_storageInstance->UnregisterThread();
    DstoreSetMemoryInControl();
}

void ParallelRedoWorker::WaitRedoFinish()
{
    m_canStopFlag.store(true);
    while (true) {
        if (m_finishFlag.load()) {
            break;
        }
    }
}

bool ParallelRedoWorker::IsEmpty() const
{
    return m_redoQueue->IsEmpty();
}

uint64 ParallelRedoWorker::GetMaxDispatchedPlsn() const
{
    return m_maxDispatchedPlsn;
}

uint64 ParallelRedoWorker::GetMaxRedoFinishedPlsn()
{
    return GsAtomicReadU64(&m_maxRedoPlsn);
}

uint64 ParallelRedoWorker::GetMaxRecoveryPlsn()
{
    return GsAtomicReadU64(&m_maxRecoveryPlsn);
}

void ParallelRedoWorker::UpdateMaxRedoPlsn(uint64 plsn)
{
    if (plsn > m_maxRedoPlsn) {
        GsAtomicWriteU64(&m_maxRedoPlsn, plsn);
    }
}

bool ParallelDDLRedoWorker::CheckWaitListIsEmpty()
{
    for (int i = 0; i < MAX_TABLESPACE_ID + 1; ++i) {
        if (m_tbsRedoWaitingList[i] != nullptr) {
            return false;
        }
    }
    return true;
}

void ParallelRedoWorker::UpdateMaxRedoSyncerPlsn(uint64 plsn)
{
    if (plsn > m_maxPlsnSyncer) {
        GsAtomicWriteU64(&m_maxPlsnSyncer, plsn);
    }
}

void ParallelRedoWorker::UpdateMaxRecoveryPlsn(uint64 plsn)
{
    if (plsn > m_maxRecoveryPlsn) {
        GsAtomicWriteU64(&m_maxRecoveryPlsn, plsn);
    }
}

bool ParallelRedoWorker::CheckWaitListIsEmpty()
{
    return true;
}

bool ParallelRedoWorker::IsRedoQueueEmptyOrOnlyNullRecord()
{
    RedoWalRecordEntry *recordEntry = nullptr;
    uint32 getNum = 0;
    RetStatus ret = m_redoQueue->GetAll<RedoWalRecordEntry*>(&recordEntry, &getNum);
    if (ret == DSTORE_FAIL || getNum == 0 || m_walRecovery->IsAllNullRecord(recordEntry, getNum)) {
        return true;
    }
    return false;
}

void ParallelPageRedoWorker::ProcNoRedoWalRecordComing(UNUSE_PARAM BuffForDecompress *buffInfoForDecompress,
                                                       UNUSE_PARAM uint64 waitCount)
{
    UpdateMaxRedoPlsn(m_maxPlsnSyncer);
    GaussUsleep(MAX_REDO_QUE_SLEEP_SHORT);
}

void ParallelPageRedoWorker::ClearBatchRecoveryPara(WalBatchRecoveryPara *para)
{
    for (uint32 i = 0; i < para->batchNum; i++) {
        if (para->recordEntry[i].walRecordNeedFree) {
            m_freeList->FreeRedoWalRecordEntry(para->recordEntry[i].walRecordInfo.freeRedoEntry);
        }
    }
    para->pageId = INVALID_PAGE_ID;
    para->batchNum = 0;
    para->redoPos = 0;
}

inline void ParallelPageRedoWorker::ProcAllGetRedoWalRecordEntrys(
    RedoWalRecordEntry *recordEntry, uint32 getNum, BuffForDecompress *buffInfoForDecompress)
{
    RedoWalRecordEntry recordEntries[MAX_BATCH_RECORD_NUM];
    WalBatchRecoveryPara batchRecoveryPara{};
    batchRecoveryPara.Init();
    batchRecoveryPara.recordEntry = &recordEntries[0];
    const WalRecordForPageOnDisk *recordForPageOnDisk = nullptr;
    PageId curPageId;
    uint64 maxWalRecoveryPlsn = INVALID_PLSN;
    for (uint32 i = 0; i < getNum; i++) {
        if (recordEntry[i].recordEndPlsn != INVALID_PLSN && recordEntry[i].GetWalRecord() == nullptr) {
            UpdateMaxRedoSyncerPlsn(recordEntry[i].recordEndPlsn);
            continue;
        }
        if (recordEntry[i].recordEndPlsn != INVALID_PLSN &&
            recordEntry[i].GetWalRecord()->GetType() == WAL_BARRIER_CSN) {
            maxWalRecoveryPlsn =
                DstoreMax(maxWalRecoveryPlsn, WalUtils::GetWalGroupStartPlsn(recordEntry[i].recordEndPlsn,
                recordEntry[i].GetWalRecord()->GetSize(), m_walRecovery->GetWalFileSize()));
            m_walRecovery->ProcessBarrierRecord(static_cast<const WalBarrierCsn *>(recordEntry[i].GetWalRecord()),
                recordEntry[i].recordEndPlsn);
            continue;
        }
        recordForPageOnDisk =
            static_cast<const WalRecordForPageOnDisk *>(static_cast<const void *>(recordEntry[i].GetWalRecord()));
        curPageId = recordForPageOnDisk->GetCompressedPageId();
        uint64 curPrevFileVersion = recordForPageOnDisk->GetCompressedFilePreVersion();
        if ((curPageId != batchRecoveryPara.pageId && batchRecoveryPara.pageId.IsValid()) ||
            batchRecoveryPara.batchNum >= MAX_BATCH_RECORD_NUM - 1 ||
            curPrevFileVersion != batchRecoveryPara.prevFileVersion) {
            if (STORAGE_FUNC_FAIL(m_walRecovery->RedoBatch(&batchRecoveryPara, m_workerId, buffInfoForDecompress))) {
                storage_set_error(WAL_ERROR_INTERNAL_ERROR);
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu redo batch recordEntry error!",
                    m_pdbId, m_walId, m_workerId));
            }
            if (batchRecoveryPara.redoPos > 0) {
                UpdateMaxRedoPlsn(batchRecoveryPara.recordEntry[batchRecoveryPara.redoPos - 1].recordEndPlsn);
            }
            UpdateMaxRecoveryPlsn(maxWalRecoveryPlsn);
            ClearBatchRecoveryPara(&batchRecoveryPara);
        }
        batchRecoveryPara.AddWalRecord(&recordEntry[i], curPageId, curPrevFileVersion);
    }
    if (batchRecoveryPara.batchNum > 0 &&
        STORAGE_FUNC_FAIL(m_walRecovery->RedoBatch(&batchRecoveryPara, m_workerId, buffInfoForDecompress))) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu redo batch recordEntry error!",
            m_pdbId, m_walId, m_workerId));
    }
    if (batchRecoveryPara.redoPos > 0) {
        UpdateMaxRedoPlsn(batchRecoveryPara.recordEntry[batchRecoveryPara.redoPos - 1].recordEndPlsn);
    }
    UpdateMaxRecoveryPlsn(maxWalRecoveryPlsn);
    ClearBatchRecoveryPara(&batchRecoveryPara);
}

ParallelDDLRedoWorker::~ParallelDDLRedoWorker()
{
    for (uint32 i = 0; i < MAX_TABLESPACE_ID + 1; i++) {
        if (m_tbsRedoWaitingList[i] != nullptr) {
            DstorePfreeExt(m_tbsRedoWaitingList[i]->recordEntry);
            DstorePfreeExt(m_tbsRedoWaitingList[i]);
            m_tbsRedoWaitingList[i] = nullptr;
        }
    }
}

void ParallelDDLRedoWorker::ProcNoRedoWalRecordComing(BuffForDecompress *buffInfoForDecompress, uint64 waitCount)
{
    bool needSleep = true;
    /* when no WalRecord coming, try to batch redo WalRecord in waiting list */
    for (TablespaceId tbsId = 0; tbsId < MAX_TABLESPACE_ID + 1; tbsId++) {
        if (m_tbsRedoWaitingList[tbsId] != nullptr && m_tbsRedoWaitingList[tbsId]->batchNum > 0) {
            needSleep = false;
            WalBatchRecoveryPara *batchRecoveryPara = m_tbsRedoWaitingList[tbsId];
            bool printLog = (waitCount % WAIT_COUNT_FOR_REPORT_DDL_REDO == 0);
            if (STORAGE_FUNC_FAIL(
                    m_walRecovery->RedoBatch(batchRecoveryPara, m_workerId, buffInfoForDecompress, printLog))) {
                storage_set_error(WAL_ERROR_INTERNAL_ERROR);
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelRedoWorker %hu redo batch recordEntry error!",
                    m_pdbId, m_walId, m_workerId));
            }

            if (batchRecoveryPara->redoPos > 0) {
                UpdateMaxRedoPlsn(batchRecoveryPara->recordEntry[batchRecoveryPara->redoPos - 1].recordEndPlsn);
                ClearBatchRecoveryPara(tbsId);
            }
        }
    }
    if (needSleep) {
        UpdateMaxRedoPlsn(m_maxPlsnSyncer);
        GaussUsleep(MAX_REDO_QUE_SLEEP_SHORT);
    }
}

void ParallelDDLRedoWorker::ClearBatchRecoveryPara(TablespaceId tbsId)
{
    WalBatchRecoveryPara *para = m_tbsRedoWaitingList[tbsId];
    for (uint32 i = 0; i < para->redoPos; i++) {
        if (para->recordEntry[i].walRecordNeedFree) {
            m_freeList->FreeRedoWalRecordEntry(para->recordEntry[i].walRecordInfo.freeRedoEntry);
        }
    }
    if (para->redoPos == para->batchNum) {
        DstorePfreeExt(para->recordEntry);
        para->recordEntry = nullptr;
        para->redoPos = 0;
        para->batchNum = 0;
        para->entryCapacity = 0;
        DstorePfreeExt(m_tbsRedoWaitingList[tbsId]);
        m_tbsRedoWaitingList[tbsId] = nullptr;
    } else if (para->redoPos > 0) {
        for (uint32 i = para->redoPos; i < para->batchNum; i++) {
            para->recordEntry[i - para->redoPos] = para->recordEntry[i];
        }
        para->batchNum -= para->redoPos;
        para->redoPos = 0;
    }
}

RetStatus ParallelDDLRedoWorker::ExpandBatchRecoveryPara(TablespaceId tbsId)
{
    WalBatchRecoveryPara *batchRecoveryPara = m_tbsRedoWaitingList[tbsId];
    uint32 newCapacity = batchRecoveryPara->entryCapacity + MAX_BATCH_RECORD_NUM;
    if (unlikely(newCapacity > UINT32_MAX / sizeof(RedoWalRecordEntry))) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu expand redoEntry failed due to entryCapacity overflow, "
            "entryCapacity: %u", m_pdbId, m_walId, m_workerId, batchRecoveryPara->entryCapacity));
        return DSTORE_FAIL;
    }
    uint32 oldSize = sizeof(RedoWalRecordEntry) * batchRecoveryPara->entryCapacity;
    uint32 newSize = sizeof(RedoWalRecordEntry) * newCapacity;
    AutoMemCxtSwitch autoMemCxtSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    RedoWalRecordEntry *newEntry = static_cast<RedoWalRecordEntry *>(DstorePalloc(newSize));
    if (STORAGE_VAR_NULL(newEntry)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu expand redoEntry alloc memory for newEntry failed, "
            "newSize: %u", m_pdbId, m_walId, m_workerId, newSize));
        return DSTORE_FAIL;
    }
    errno_t rc = memcpy_s(newEntry, oldSize, batchRecoveryPara->recordEntry, oldSize);
    storage_securec_check(rc, "\0", "\0");
    DstorePfreeExt(batchRecoveryPara->recordEntry);
    batchRecoveryPara->recordEntry = newEntry;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu expand size of redoEntry from %u to %u for tablespace %u!",
        m_pdbId, m_walId, m_workerId, batchRecoveryPara->entryCapacity, newCapacity, tbsId));
    batchRecoveryPara->entryCapacity = newCapacity;
    return DSTORE_SUCC;
}

void ParallelDDLRedoWorker::BatchProc(const TablespaceId tbsId, BuffForDecompress *buffInfoForDecompress,
    uint64 maxrecoveryPlsn)
{
    WalBatchRecoveryPara *batchRecoveryPara = nullptr;
    if (m_ddlRedoType == DDLRedoType::DDLREDO_SYSTABLE) {
        batchRecoveryPara = m_systableRedo;
    } else {
        batchRecoveryPara = m_tbsRedoWaitingList[tbsId];
    }
    if (STORAGE_VAR_NULL(batchRecoveryPara)) {
        /* High-frequency logs for standby, modify to debug level later. */
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu get empty batch for tablespace %u",
            m_pdbId, m_walId, m_workerId, tbsId));
        return;
    }
    if (STORAGE_FUNC_FAIL(m_walRecovery->RedoBatch(batchRecoveryPara, m_workerId, buffInfoForDecompress))) {
        storage_set_error(WAL_ERROR_INTERNAL_ERROR);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu redo batch recordEntry error!",
            m_pdbId, m_walId, m_workerId));
    }

    if (batchRecoveryPara->redoPos > 0) {
        UpdateMaxRedoPlsn(batchRecoveryPara->recordEntry[batchRecoveryPara->redoPos - 1].recordEndPlsn);
        UpdateMaxRecoveryPlsn(maxrecoveryPlsn);
        if (m_ddlRedoType == DDLRedoType::DDLREDO_TABLESAPCE) {
            ClearBatchRecoveryPara(tbsId);
        }
    }
}

void ParallelDDLRedoWorker::ProcAllGetRedoWalRecordEntrys(
    RedoWalRecordEntry *recordEntry, uint32 getNum, BuffForDecompress *buffInfoForDecompress)
{
    const WalRecordTbsLogical *recordTbsLogical = nullptr;
    TablespaceId curTbsId = INVALID_TABLESPACE_ID;
    TablespaceId lastTbsId = INVALID_TABLESPACE_ID;
    AutoMemCxtSwitch autoMemCxtSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    uint64 maxWalRecoveryPlsn = INVALID_PLSN;
    /* divide WalRecords to batchs by tablespaceId, try to redo batch when tablespaceId changes or batch is full */
    /* if WalRecord NOT_REPLAYABLE, keep it in m_tbsRedoWaitingList, and try to redo them again when no record coming */
    for (uint32 i = 0; i < getNum; i++) {
        if (recordEntry[i].recordEndPlsn != INVALID_PLSN && recordEntry[i].GetWalRecord() == nullptr) {
            UpdateMaxRedoSyncerPlsn(recordEntry[i].recordEndPlsn);
            continue;
        }
        if (recordEntry[i].recordEndPlsn != INVALID_PLSN) {
            if (recordEntry[i].GetWalRecord()->GetType() == WAL_BARRIER_CSN) {
                maxWalRecoveryPlsn =
                    DstoreMax(maxWalRecoveryPlsn, WalUtils::GetWalGroupStartPlsn(recordEntry[i].recordEndPlsn,
                    recordEntry[i].GetWalRecord()->GetSize(), m_walRecovery->GetWalFileSize()));
                continue;
            }
            if (recordEntry[i].GetWalRecord()->GetType() == WAL_SYSTABLE_WRITE_BUILTIN_RELMAP) {
                m_systableRedo = static_cast<WalBatchRecoveryPara *>(DstorePalloc(sizeof(WalBatchRecoveryPara)));
                StorageReleasePanic(m_systableRedo == nullptr, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu alloc memory for WalBatchRecoveryPara failed",
                        m_pdbId, m_walId, m_workerId));
                m_systableRedo->Init();
                m_systableRedo->recordEntry =
                    static_cast<RedoWalRecordEntry*>(DstorePalloc(
                        sizeof(RedoWalRecordEntry) * m_systableRedo->entryCapacity));
                StorageReleasePanic(m_systableRedo->recordEntry == nullptr, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu alloc memory for RedoWalRecordEntry failed",
                        m_pdbId, m_walId, m_workerId));
                m_systableRedo->AddWalRecord(&recordEntry[i]);
                m_ddlRedoType = DDLRedoType::DDLREDO_SYSTABLE;
                BatchProc(INVALID_TABLESPACE_ID, buffInfoForDecompress, maxWalRecoveryPlsn);
                m_ddlRedoType = DDLRedoType::DDLREDO_TABLESAPCE;
                DstorePfreeExt(m_systableRedo->recordEntry);
                DstorePfreeExt(m_systableRedo);
                m_systableRedo = nullptr;
                continue;
            }
        }
        recordTbsLogical = static_cast<const WalRecordTbsLogical *>(recordEntry[i].GetWalRecord());
        curTbsId = recordTbsLogical->GetTablespaceId();
        if (curTbsId > MAX_TABLESPACE_ID) {
            ErrLog(DSTORE_PANIC, MODULE_WAL,
                ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu get invalid tablespaceId %u",
                m_pdbId, m_walId, m_workerId, curTbsId));
        }
        if (m_tbsRedoWaitingList[curTbsId] == nullptr) {
            m_tbsRedoWaitingList[curTbsId] =
                static_cast<WalBatchRecoveryPara *>(DstorePalloc(sizeof(WalBatchRecoveryPara)));
            if (STORAGE_VAR_NULL(m_tbsRedoWaitingList[curTbsId])) {
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu alloc memory for WalBatchRecoveryPara failed",
                    m_pdbId, m_walId, m_workerId));
            }
            m_tbsRedoWaitingList[curTbsId]->Init();
            m_tbsRedoWaitingList[curTbsId]->waitStartPlsn = m_walRecovery->GetRecoveryStartPlsn();
            m_tbsRedoWaitingList[curTbsId]->recordEntry =
                static_cast<RedoWalRecordEntry*>(DstorePalloc(
                    sizeof(RedoWalRecordEntry) * m_tbsRedoWaitingList[curTbsId]->entryCapacity));
            if (STORAGE_VAR_NULL(m_tbsRedoWaitingList[curTbsId]->recordEntry)) {
                ErrLog(DSTORE_PANIC, MODULE_WAL,
                    ErrMsg("[PDB:%u WAL:%lu]ParallelDDLRedoWorker %hu alloc memory for RedoWalRecordEntry failed",
                    m_pdbId, m_walId, m_workerId));
            }
        }

        if (m_tbsRedoWaitingList[lastTbsId] != nullptr && (curTbsId != lastTbsId ||
            m_tbsRedoWaitingList[lastTbsId]->batchNum >= m_tbsRedoWaitingList[lastTbsId]->entryCapacity - 1)) {
            BatchProc(lastTbsId, buffInfoForDecompress, maxWalRecoveryPlsn);
        }

        /* if reach entryCapacity, expand recordEntry capacity */
        if ((m_tbsRedoWaitingList[curTbsId]->batchNum == m_tbsRedoWaitingList[curTbsId]->entryCapacity) &&
            STORAGE_FUNC_FAIL(ExpandBatchRecoveryPara(curTbsId))) {
            /* if expand batch recovery para fail, try to redo it until success */
            while (m_tbsRedoWaitingList[curTbsId]->batchNum == m_tbsRedoWaitingList[curTbsId]->entryCapacity) {
                BatchProc(curTbsId, buffInfoForDecompress, maxWalRecoveryPlsn);
            }
        }
        m_tbsRedoWaitingList[curTbsId]->AddWalRecord(&recordEntry[i]);
        lastTbsId = curTbsId;
    }

    BatchProc(curTbsId, buffInfoForDecompress, maxWalRecoveryPlsn);
}

void ParallelDDLRedoWorker::UpdateMaxRedoPlsn(uint64 plsn)
{
    if (plsn > m_maxRedoPlsn) {
        /* for ddl wal record, some record may not be replayed */
        for (int i = 0; i < MAX_TABLESPACE_ID + 1; i++) {
            if (m_tbsRedoWaitingList[i] == nullptr) {
                continue;
            }
            plsn = plsn < m_tbsRedoWaitingList[i]->waitStartPlsn ? plsn : m_tbsRedoWaitingList[i]->waitStartPlsn;
        }
        
        if (plsn > m_maxRedoPlsn) {
            GsAtomicWriteU64(&m_maxRedoPlsn, plsn);
        }
    }
}

void WalRecordBufFreeEntryList::FreeRedoWalRecordEntry(WalRecordBufFreeEntry *entry)
{
    WalRecordBufFreeEntry *oldHead = addHead.load(std::memory_order_relaxed);
    do {
        entry->freeNext = oldHead;
    } while (!addHead.compare_exchange_weak(oldHead, entry,
        std::memory_order_release, std::memory_order_relaxed));
}

void WalRecordBufFreeEntryList::ReAlloc(WalRecordBufFreeEntry *newEntry, uint32 recordSize) const
{
    AutoMemCxtSwitch autoMemCxtSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    WalRecord *realloc = (WalRecord *)DstorePalloc0(recordSize);
    if (STORAGE_VAR_NULL(realloc)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecordBufFreeEntryList alloc memory failed"));
    }
    DstorePfreeExt(newEntry->walRecord);
    newEntry->walRecord = realloc;
    newEntry->walRecordSize = recordSize;
}

WalRecordBufFreeEntry *WalRecordBufFreeEntryList::AllocRedoWalRecord(uint32 recordSize)
{
    WalRecordBufFreeEntry *newEntry = nullptr;
    AutoMemCxtSwitch autoMemCxtSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    do {
        if (getHead != nullptr) {
            newEntry = getHead;
            getHead = newEntry->freeNext;
            break;
        } else {
            WalRecordBufFreeEntry *head = addHead.exchange(nullptr, std::memory_order_acq_rel);
            if (head != nullptr) {
                newEntry = head;
                getHead = newEntry->freeNext;
                break;
            } else if (curEntryNum < REDO_FREELIST_CAPACITY) {
                uint32 maxIndex = REDO_FREELIST_CAPACITY - 1;
                newEntry = &allocatedHead[maxIndex - curEntryNum];
                newEntry->walRecord = (WalRecord*)DstorePalloc0(recordSize);
                newEntry->walRecordSize = recordSize;
                ++curEntryNum;
                break;
            }
        }
    } while (newEntry == nullptr);
    if (STORAGE_VAR_NULL(newEntry->walRecord)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("AllocRedoWalRecord alloc memory failed"));
    }
    if (newEntry->walRecordSize < recordSize) {
        ReAlloc(newEntry, recordSize);
    }
    newEntry->freeNext = nullptr;

    return newEntry;
}

void WalRecordBufFreeEntryList::Destory() noexcept
{
    uint32 maxIndex = curEntryNum - 1;
    for (uint32 i = 0; i < curEntryNum; i++) {
        if (allocatedHead[maxIndex - i].walRecord != nullptr) {
            DstorePfreeExt(allocatedHead[maxIndex - i].walRecord);
        }
    }
}
}
