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
 * dstore_logical_decode_handler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_decode_handler.h"
#include "logical_replication/dstore_decode_plugin.h"
#include "logical_replication/dstore_decode2text_plugin.h"
#include "common/dstore_datatype.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
LogicalDecodeHandler::LogicalDecodeHandler(LogicalReplicationSlot *logicalSlot, DecodeOptions *decodeOptions,
                                           DecodeDict *decodeDict, DstoreMemoryContext mctx, PdbId pdbId)
    : m_logicalSlot(logicalSlot),
      m_decodeOptions(*decodeOptions),
      m_decodePlugin(nullptr),
      m_decodeDict(decodeDict),
      m_memoryContext(mctx),
      m_walDispatcher(nullptr),
      m_parallelDecodeWorker(),
      m_curFetchLogCursor(0),
      m_fetchedChanges(0),
      m_fetchedCsn(logicalSlot->GetConfirmCsn()),
      m_lastConfirmedCsn(INVALID_CSN),
      m_lastConfirmedPlsn(INVALID_END_PLSN), /* single node use */
      m_lastAdvanceTime(GetCurrentTimestampInSecond()),
      m_pdbId(pdbId)
{
    m_statusLock.Init();
}

LogicalDecodeHandler::~LogicalDecodeHandler()
{
    m_logicalSlot = nullptr;
    m_decodePlugin = nullptr;
    m_decodeDict = nullptr;
    m_memoryContext = nullptr;
    m_walDispatcher = nullptr;
}

RetStatus LogicalDecodeHandler::Init()
{
    /* step1. check and load output plugin */
    char* pluginName = m_logicalSlot->GetPluginName();
    UNUSED_VARIABLE(pluginName);
    /* for now, we just implement text decode plugin. */
    m_decodePlugin = DstoreNew(m_memoryContext) Decode2TextPlugin();
    if (unlikely(m_decodePlugin == nullptr)) {
        storage_set_error(DECODE_INFO_PLUGIN_LOAD_ERROR);
        return DSTORE_FAIL;
    }

    /* step2. initialize parallel workers */
    ParallelDecodeWorkerInitParam decodeParam = {INVALID_WORKER_ID, nullptr,
        &m_decodeOptions, m_decodePlugin, m_decodeDict};
    for (int i = 0; i < m_decodeOptions.parallelDecodeWorkerNum; i++) {
        decodeParam.workerId = i;
        decodeParam.decodeMctx = DstoreAllocSetContextCreate(m_memoryContext, "WorkerDecodeMctx",
                                                             ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
        if (STORAGE_VAR_NULL(decodeParam.decodeMctx)) {
            return DSTORE_FAIL;
        }
        m_parallelDecodeWorker[i] = DstoreNew(m_memoryContext) ParallelDecodeWorker(decodeParam, m_pdbId);
        if (STORAGE_VAR_NULL(m_parallelDecodeWorker[i]) || STORAGE_FUNC_FAIL(m_parallelDecodeWorker[i]->Init())) {
            return DSTORE_FAIL;
        }
    }

    /* step3. initialize walDispatcher */
    DstoreMemoryContext walMctx = DstoreAllocSetContextCreate(m_memoryContext, "walReaderAndSortBuffer",
        ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    m_walDispatcher = DstoreNew(m_memoryContext) WalDispatcher(walMctx, m_logicalSlot, &m_decodeOptions, m_pdbId);

    if (STORAGE_VAR_NULL(m_walDispatcher) ||
        STORAGE_FUNC_FAIL(m_walDispatcher->Init(m_parallelDecodeWorker, m_decodeOptions.parallelDecodeWorkerNum))) {
        return DSTORE_FAIL;
    }
    /*
     * add extra terminal condition in case of that decode process never reach user's given,
     * this will only be used on single node.
     */
    if (m_decodeOptions.uptoCSN != INVALID_CSN || m_decodeOptions.uptoNchanges != INVALID_NCHANGES) {
        StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
        StorageAssert(storagePdb != nullptr);
        WalStream *stream = storagePdb->GetWalMgr()->GetWalStreamManager()->GetWalStream(m_logicalSlot->GetWalId());
        StorageAssert(stream != nullptr);
        WalPlsn endPlsn = stream->GetMaxAppendedPlsn();
        stream->WaitTargetPlsnPersist(endPlsn);
        m_walDispatcher->SetEndPlsn(endPlsn);
    }
    for (int i = 0; i < m_decodeOptions.parallelDecodeWorkerNum; i++) {
        m_parallelDecodeWorker[i]->SetWalDispatcher(m_walDispatcher);
    }
    return DSTORE_SUCC;
}

void LogicalDecodeHandler::Destroy()
{
    if (m_walDispatcher != nullptr) {
        m_walDispatcher->Destroy();
        delete m_walDispatcher;
        m_walDispatcher = nullptr;
    }
    ClearParallelWorkers(m_decodeOptions.parallelDecodeWorkerNum);
    delete m_decodePlugin;
    return;
}

void LogicalDecodeHandler::ClearParallelWorkers(int clearNums)
{
    StorageAssert(clearNums <= m_decodeOptions.parallelDecodeWorkerNum);
    for (int i = 0; i < clearNums; i++) {
        if (m_parallelDecodeWorker[i] != nullptr) {
            m_parallelDecodeWorker[i]->Destroy();
            delete m_parallelDecodeWorker[i];
            m_parallelDecodeWorker[i] = nullptr;
        }
    }
}

void LogicalDecodeHandler::StartUp()
{
    m_statusLock.Acquire();
    /* step1: run waldispatcher */
    m_walDispatcher->Run();

    /* step2: run all parallel decode worker */
    for (int i = 0; i < m_decodeOptions.parallelDecodeWorkerNum; i++) {
        m_parallelDecodeWorker[i]->Run();
    }
    m_statusLock.Release();
}

void LogicalDecodeHandler::Stop()
{
    m_statusLock.Acquire();
    /* step1: stop all parallel decode worker */
    for (int i = 0; i < m_decodeOptions.parallelDecodeWorkerNum; i++) {
        m_parallelDecodeWorker[i]->Stop();
    }
    /* step2: stop waldispatcher */
    m_walDispatcher->Stop();

    /* step3: advance slot if needed */
    if (m_decodeOptions.advanceSlotFlag) {
        DoLogicalSlotAdvance();
    }
    m_statusLock.Release();
}

int LogicalDecodeHandler::GetDecodeSlotId() const
{
    return m_logicalSlot->GetSlotId();
}

bool LogicalDecodeHandler::IsStreamDecode()
{
    return (m_decodeOptions.uptoCSN == INVALID_CSN && m_decodeOptions.uptoNchanges == INVALID_NCHANGES);
}

bool LogicalDecodeHandler::DecodeToLimit()
{
    if (IsStreamDecode() || m_walDispatcher->GetEndPlsn() == INVALID_END_PLSN) {
        /* stream decode */
        return false;
    }
    if (m_decodeOptions.uptoCSN != INVALID_CSN && m_fetchedCsn > m_decodeOptions.uptoCSN) {
        return true;
    }
    if (m_decodeOptions.uptoNchanges != INVALID_NCHANGES && m_fetchedChanges >= m_decodeOptions.uptoNchanges) {
        return true;
    }
    return false;
}

TrxLogicalLog* LogicalDecodeHandler::GetNextTrxLogicalLog()
{
    TrxLogicalLog *nextLog = m_parallelDecodeWorker[m_curFetchLogCursor]->PopTrxLogicalLog();
    if (unlikely(nextLog == nullptr)) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
            ErrMsg("Fetch logical log to target end plsn. fetched csn: %lu; fetched changes: %d",
                m_fetchedCsn, m_fetchedChanges));
        return nullptr;
    }
    m_fetchedCsn = nextLog->commitCsn;
    if (DecodeToLimit()) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
            ErrMsg("Fetched logical log to user given limit. fetched csn: %lu; fetched changes: %d",
                m_fetchedCsn, m_fetchedChanges));
        return nullptr;
    }
    m_fetchedChanges += nextLog->nRows;
    m_curFetchLogCursor = (m_curFetchLogCursor + 1) % m_decodeOptions.parallelDecodeWorkerNum;
    return nextLog;
}

/*
 * advance slot is quite expensive operation.
 * In stream logical decode, do the real advance at a time interval.
 * In none-stream logical decode, only do the real advance at decode stop.
 */
void LogicalDecodeHandler::ConfirmTrxLogicalLog(TrxLogicalLog *trxLog)
{
    if (!m_decodeOptions.advanceSlotFlag) {
        return;
    }
    m_lastConfirmedCsn = trxLog->commitCsn;
    m_lastConfirmedPlsn = trxLog->restartDecodingPlsn;
    if (IsStreamDecode()) {
        TimestampTz curTime = GetCurrentTimestampInSecond();
        if (curTime - m_lastAdvanceTime > STREAM_DECODE_ADDVANCE_TIME_INTERVAL) {
            DoLogicalSlotAdvance();
            m_lastAdvanceTime = curTime;
        }
    }
}

void LogicalDecodeHandler::DoLogicalSlotAdvance()
{
    if (m_lastConfirmedCsn != INVALID_CSN && m_lastConfirmedPlsn != INVALID_END_PLSN) {
        m_logicalSlot->AdvanceAndSerialize(m_lastConfirmedCsn, m_lastConfirmedPlsn);
    }
}
#endif

}
