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
 * dstore_wal_read_buffer.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/wal/dstore_wal_read_buffer.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "port/dstore_port.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_bitmap_meta_page.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "wal/dstore_wal_reader.h"
#include "wal/dstore_wal_perf_unit.h"
#include "wal/dstore_wal_read_buffer.h"
#include "wal/dstore_wal_logstream.h"
#ifdef DSTORE_USE_NUMA
#include "numa.h"
#endif

namespace DSTORE {

static constexpr uint32 WAIT_BUFFER_LOADED_MICROSEC = 200;
static constexpr uint32 WAIT_READ_BUFFER_RECYCLED_MICROSEC = 200;
static constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 5000;
static constexpr uint32 ASYNC_READ_BATCH_NUM = 8;

WalReadBuffer::WalReadBuffer(DstoreMemoryContext memoryContext, WalStream *walStream, RedoMode redoMode)
    : m_walStream(walStream),
    m_readBuffer(nullptr),
    m_readBufferStartPlsn(0),
    m_recycledReadBufferNum(0),
    m_readBufferBlockStates(nullptr),
    m_loadWalToBufferWorker(nullptr),
    m_walStreamEndPlsn(INVALID_END_PLSN),
    m_stopLoadWorker(false),
    m_loadToBufferStarted(false),
    m_readCnt(0),
    m_memoryContext(memoryContext),
    m_redoMode(redoMode),
    m_standbyReadMaxPlsn(0)
{
    m_loadToBufferConf = {};
}

WalReadBuffer::~WalReadBuffer()
{
    FreeReadBuffer();
    m_loadWalToBufferWorker = nullptr;
    m_readBuffer = nullptr;
    m_readBufferBlockStates = nullptr;
    m_walStream = nullptr;
    m_memoryContext = nullptr;
}

RetStatus WalReadBuffer::StartLoadToBuffer(const WalFileLoadToBufferConf &conf)
{
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu] WalRecovery StartLoadToBuffer", m_walStream->GetPdbId(), m_walStream->GetWalId()));
    if (m_loadToBufferStarted) {
        return DSTORE_SUCC;
    }
#ifndef UT
    uint64 fileSize = m_walStream->GetWalFileSize();
    if (conf.readBufferBlockSize > fileSize || fileSize % conf.readBufferBlockSize != 0) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Invalid WalReadBufferLoadToBufferConf, StartLoadToBuffer fail."));
        return DSTORE_FAIL;
    }
#endif
    if (conf.readBufferBlockNum == 0 || conf.readBufferBlockSize <= WAL_FILE_HDR_SIZE ||
        conf.readBufferSize < WAL_FILE_HDR_SIZE + WAL_GROUP_MAX_SIZE) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Invalid WalReadBufferLoadToBufferConf, StartLoadToBuffer fail."));
        return DSTORE_FAIL;
    }

    m_loadToBufferConf = conf;
    if (unlikely(m_loadToBufferConf.readBufferBlockNum * m_loadToBufferConf.readBufferBlockSize !=
                 m_loadToBufferConf.readBufferSize)) {
        uint64 oldBufSize = m_loadToBufferConf.readBufferSize;
        m_loadToBufferConf.readBufferSize =
            m_loadToBufferConf.readBufferBlockNum * m_loadToBufferConf.readBufferBlockSize;
        ErrLog(DSTORE_LOG, MODULE_WAL,
               ErrMsg("readBufferSize (%lu) is not divisible by readBufferBlockSize (%lu), rounded to %lu",
                      m_loadToBufferConf.readBufferSize, m_loadToBufferConf.readBufferBlockSize, oldBufSize));
    }

    /* if out of memory reduce buffer size and retry allocate memory */
    while (m_readBuffer == nullptr) {
#ifdef DSTORE_USE_NUMA
        m_readBuffer = static_cast<uint8 *>(numa_alloc_onnode(m_loadToBufferConf.readBufferSize, 0));
#else
        m_readBuffer =
            static_cast<uint8 *>(DstoreMemoryContextAllocHugeSize(m_memoryContext, m_loadToBufferConf.readBufferSize));
#endif
        if (unlikely(m_readBuffer == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                ErrMsg("WalReadBuffer alloc buffer fail with buffer size %lu.", m_loadToBufferConf.readBufferSize));

            uint64 reduceSize = 2;
            uint64 newSize =
                (m_loadToBufferConf.readBufferSize / reduceSize) & (~(m_loadToBufferConf.readBufferBlockSize - 1));
            uint32 newBlockNum = newSize / m_loadToBufferConf.readBufferBlockSize;
            if (newBlockNum > 0 && newSize >= WAL_FILE_HDR_SIZE + WAL_GROUP_MAX_SIZE) {
                m_loadToBufferConf.readBufferSize = newSize;
                m_loadToBufferConf.readBufferBlockNum = newBlockNum;
            }
        }
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu] WalRecovery StartLoadToBuffer alloc readBuffer success, size %lu",
                  m_walStream->GetPdbId(), m_walStream->GetWalId(), m_loadToBufferConf.readBufferSize));

    while (m_readBufferBlockStates == nullptr) {
#ifdef DSTORE_USE_NUMA
        m_readBufferBlockStates = static_cast<WalFileLoadToBufferBlockState *>(
            numa_alloc_onnode(m_loadToBufferConf.readBufferBlockNum * sizeof(WalFileLoadToBufferBlockState), 0));
#else
        m_readBufferBlockStates = static_cast<WalFileLoadToBufferBlockState *>(DstoreMemoryContextAllocZero(
            m_memoryContext, m_loadToBufferConf.readBufferBlockNum * sizeof(WalFileLoadToBufferBlockState)));
#endif
        if (unlikely(m_readBufferBlockStates == nullptr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            ErrLog(DSTORE_ERROR, MODULE_WAL,
                ErrMsg("WalReadBuffer alloc buffer block states fail with buffer num %u.",
                m_loadToBufferConf.readBufferBlockNum));
        }
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u WAL:%lu] WalRecovery StartLoadToBuffer alloc readBufferBlockStates success, blockNum %u",
                  m_walStream->GetPdbId(), m_walStream->GetWalId(), m_loadToBufferConf.readBufferBlockNum));

    /* pre access pages for updating tlbs */
    if (!IsTemplate(m_walStream->GetPdbId())) {
        uint64 pageSize = static_cast<uint64>(sysconf(_SC_PAGESIZE));
        pageSize = pageSize > 0 ? pageSize : WAL_DIO_BLOCK_SIZE;
        for (uint64 i = 0; i < m_loadToBufferConf.readBufferSize / pageSize; i++) {
            m_readBuffer[i * pageSize] = 0;
        }
    }

    for (uint32 i = 0; i < m_loadToBufferConf.readBufferBlockNum; i++) {
        m_readBufferBlockStates[i].recycled = true;
        m_readBufferBlockStates[i].readCnt = &m_readCnt;
        m_readBufferBlockStates[i].loadWorkerNeedStop = &m_stopLoadWorker;
    }

    m_readBufferStartPlsn =
        m_loadToBufferConf.loadStartPlsn - m_loadToBufferConf.loadStartPlsn % m_loadToBufferConf.readBufferBlockSize;
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu] WalRecovery StartLoadToBuffer: loadStartPlsn %lu, readBufferBlockNum %u, "
               "readBufferBlockSize %lu, "
        "readBufferSize %lu, m_readBufferStartPlsn %lu",
        m_walStream->GetPdbId(), m_walStream->GetWalId(), m_loadToBufferConf.loadStartPlsn,
        m_loadToBufferConf.readBufferBlockNum, m_loadToBufferConf.readBufferBlockSize,
        m_loadToBufferConf.readBufferSize, m_readBufferStartPlsn));

    m_loadWalToBufferWorker = new std::thread(&WalReadBuffer::LoadWalToBufferWorkerMain, this, m_readBufferStartPlsn,
        m_walStream->GetPdbId());
    m_loadToBufferStarted = true;
    return DSTORE_SUCC;
}

void WalReadBuffer::FreeReadBuffer() noexcept
{
    if (!m_loadToBufferStarted) {
        return;
    }
    if (m_loadWalToBufferWorker != nullptr) {
        m_stopLoadWorker.store(true);
        m_loadWalToBufferWorker->join();
        delete m_loadWalToBufferWorker;
        m_loadWalToBufferWorker = nullptr;
        m_stopLoadWorker = false;
        m_walStreamEndPlsn = INVALID_END_PLSN;
    }

#ifdef DSTORE_USE_NUMA
    numa_free(m_readBufferBlockStates, m_loadToBufferConf.readBufferBlockNum * sizeof(WalFileLoadToBufferBlockState));
    numa_free(m_readBuffer, m_loadToBufferConf.readBufferSize);
#else
    DstorePfreeExt(m_readBufferBlockStates);
    DstorePfreeExt(m_readBuffer);
#endif
    m_loadToBufferStarted = false;
}

RetStatus WalReadBuffer::WaitBufferRecycled(uint32 readBufferBlockNo) const
{
    uint64 waitCount = 0;
    uint64 startTime = 0;
    const uint64 waitCountForReportWarning = 25000;
    if (m_walStream->IsCollectWalWriteIoStat()) {
       startTime = GetSystemTimeInMicrosecond();
    }
    while (!m_readBufferBlockStates[readBufferBlockNo].recycled.load(std::memory_order_acquire)) {
        if (m_stopLoadWorker.load(std::memory_order_relaxed)) {
            return DSTORE_FAIL;
        }
        GaussUsleep(WAIT_READ_BUFFER_RECYCLED_MICROSEC);
        if (++waitCount % waitCountForReportWarning == 0) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("WalRecovery WaitBufferRecycled wait %lu times for readBufferBlockNo %u.", waitCount,
                readBufferBlockNo));
        }
    }
    if (m_walStream->IsCollectWalReadIoStat() && startTime != 0) {
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().waitCount), 1);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().waitTime), GetSystemTimeInMicrosecond() - startTime);
    }

    return DSTORE_SUCC;
}

void ReadAsyncCallback(ErrorCode errorCode, int64_t successSize, void *asyncContext)
{
    StorageReleasePanic((errorCode != EOK || asyncContext == nullptr || successSize < 0), MODULE_WAL,
        ErrMsg("ReadAsyncCallback func param invalid, ErrorCode:%lld successSize:%ld.", errorCode, successSize));
    uint64 resultSize = static_cast<uint64>(successSize);
    RecoveryAsyncInfo *asyncInfo = static_cast<RecoveryAsyncInfo *>(asyncContext);
    StorageAssert(asyncInfo != nullptr);
    uint64 endPlsn = GsAtomicAddFetchU64(&asyncInfo->blockState->readEndPlsn, resultSize);
    /* If read size less than buffer block size, that means read error or reach the end of wal. */
    if (unlikely(resultSize < WAL_READ_BUFFER_BLOCK_SIZE)) {
        /* If this block end plsn less than wal stream end plsn which may be set by other pread async call back
         * thread either, mark wal stream end plsn to this block end plsn.
         * It need to use CAS here to avoid concurrent scene.
         */
        bool exchangeSuccess = false;
        uint64 walStreamEndPlsn = GsAtomicReadU64(asyncInfo->walStreamEndPlsn);
        while (endPlsn < walStreamEndPlsn && !exchangeSuccess) {
            exchangeSuccess = GsAtomicCompareExchangeU64(asyncInfo->walStreamEndPlsn, &walStreamEndPlsn, endPlsn);
        }
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("Wal stream %lu at pdb %u read end plsn %lu read size %lu little than expect size %lu, attempt to "
            "update wal stream end plsn to %lu which is %lu.",
            asyncInfo->walId, asyncInfo->pdbId, endPlsn, resultSize, WAL_READ_BUFFER_BLOCK_SIZE, endPlsn,
            walStreamEndPlsn));
    }
    (void)GsAtomicSubFetchU32(asyncInfo->blockState->readCnt, 1);
    DstorePfreeExt(asyncInfo);
}

void ReadAsyncCallbackPdbStandbyRedo(ErrorCode errorCode, int64_t successSize, void *asyncContext)
{
    StorageReleasePanic((errorCode != EOK || asyncContext == nullptr || successSize <= 0), MODULE_WAL,
        ErrMsg("ReadAsyncCallbackPdbStandbyRedo func param invalid, ErrorCode:%lld successSize:%ld.",
            errorCode, successSize));
    uint64 resultSize = static_cast<uint64>(successSize);
    PdbStandbyRedoAsyncInfo *asyncInfo = static_cast<PdbStandbyRedoAsyncInfo *>(asyncContext);
    WalFileLoadToBufferBlockState *blockState = asyncInfo->blockState;
    uint64 readStartPlsn = asyncInfo->readStartPlsn;
    uint64 readEndPlsn = readStartPlsn + resultSize;

    /* Loop here until readEndPlsn equals to our readStartPlsn */
    StorageAssert(GsAtomicReadU64(&blockState->readEndPlsn) <= readStartPlsn);
    while (readStartPlsn != GsCompareAndSwapU64(&blockState->readEndPlsn, readStartPlsn, readEndPlsn)) {
        GaussUsleep(10L);
    }
    (void) GsAtomicSubFetchU32(blockState->readCnt, 1);

    ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("LoadToBufferPageStore: successSize %lu at readPlsn %lu",
        resultSize, readStartPlsn));

    /* In PDB_STANDBY_REDO, callback has the duty to free passed-in asyncContext */
    DstorePfreeExt(asyncInfo);
}

uint64 WalReadBuffer::WaitForEnoughBytesToReadStandby(uint64 readPlsn, uint64 eachReadBytes)
{
    /* timeout is only for exiting, so it can be reasonably long. */
    uint64 availableBytes = 0;
    uint64 curMaxFlushedPlsn = INVALID_PLSN;
    if (GetStandbyEndPlsn() != 0 && GetStandbyEndPlsn() > readPlsn + eachReadBytes) {
        return eachReadBytes;
    }
    /* Update read result here */
    curMaxFlushedPlsn = m_walStream->GetStandbyMaxFlushedPlsn();
    SetStandbyEndPlsn(curMaxFlushedPlsn);
    availableBytes = (curMaxFlushedPlsn < readPlsn ? 0 : DstoreMin(curMaxFlushedPlsn - readPlsn, eachReadBytes));
    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("WaitForEnoughBytesToReadPdbStandby returning %lu maxFlushedPlsn: %lu",
            availableBytes, GetStandbyEndPlsn()));
    return availableBytes;
}

uint64 WalReadBuffer::WaitForEnoughBytesToRead(uint64 readPlsn, uint64 eachReadBytes)
{
    uint64 availableBytes = 0;
    if (m_redoMode == RedoMode::PDB_STANDBY_REDO) {
        availableBytes = WaitForEnoughBytesToReadStandby(readPlsn, eachReadBytes);
    } else {
        /* For standard recovery, no need to wait and we could read as many bytes as possible */
        availableBytes = eachReadBytes;
    }
    return availableBytes;
}

void WalReadBuffer::PrepareAsyncIoContext(AsyncIoContext &asyncIoContext, const LoadToBufferContext &context)
{
    if (m_redoMode != RedoMode::PDB_STANDBY_REDO) {
        asyncIoContext.callback = ReadAsyncCallback;
        RecoveryAsyncInfo *asyncInfo =
            static_cast<RecoveryAsyncInfo *>(DstorePalloc(sizeof(RecoveryAsyncInfo)));
        while (STORAGE_VAR_NULL(asyncInfo)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("RecoveryAsyncInfo palloc fail, retry it"));
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            asyncInfo = static_cast<RecoveryAsyncInfo *>(DstorePalloc(sizeof(RecoveryAsyncInfo)));
        }
        asyncInfo->blockState = &m_readBufferBlockStates[context.readBufferBlockNo];
        asyncInfo->walStreamEndPlsn = &m_walStreamEndPlsn;
        asyncInfo->walId = m_walStream->GetWalId();
        asyncInfo->pdbId = m_walStream->GetPdbId();
        asyncIoContext.asyncContext = static_cast<void *>(asyncInfo);
    } else {
        asyncIoContext.callback = ReadAsyncCallbackPdbStandbyRedo;
        PdbStandbyRedoAsyncInfo *asyncInfo =
            static_cast<PdbStandbyRedoAsyncInfo *>(DstorePalloc(sizeof(PdbStandbyRedoAsyncInfo)));
        while (STORAGE_VAR_NULL(asyncInfo)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("PdbStandbyRedoAsyncInfo palloc fail, retry it"));
            GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
            asyncInfo = static_cast<PdbStandbyRedoAsyncInfo *>(DstorePalloc(sizeof(PdbStandbyRedoAsyncInfo)));
        }
        asyncInfo->blockState = &m_readBufferBlockStates[context.readBufferBlockNo];
        asyncInfo->readStartPlsn = context.readPlsn;
        asyncIoContext.asyncContext = static_cast<void *>(asyncInfo);
    }
}

RetStatus WalReadBuffer::LoadToBuffer(const LoadToBufferContext &context)
{
    RetStatus loadStatus;
    if (USE_PAGE_STORE) {
        loadStatus = LoadToBufferPageStore(context);
    } else {
        loadStatus = LoadToBufferLocal(context);
    }
    if (STORAGE_FUNC_FAIL(loadStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg(
            "LoadToBuffer failed. readBufferBlockNo: %u, readOffset: %ld, eachReadBytes: %lu.",
            context.readBufferBlockNo, context.readOffset, *context.eachReadBytes));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/* Due to potentially high latency, we use asyncronous read in case of page store */
RetStatus WalReadBuffer::LoadToBufferPageStore(const LoadToBufferContext &context)
{
    StorageAssert(!context.dioRw);
    AutoMemCxtSwitch autoSwitch{m_memoryContext};
    AsyncIoContext asyncIoContext = {nullptr, nullptr};
    constexpr uint32 waitEnoughBytesUsec = 500;
    uint64 *eachReadBytes = context.eachReadBytes;
    uint32 readBufferBlockNo = context.readBufferBlockNo;
    PrepareAsyncIoContext(asyncIoContext, context);

    uint64 availableBytes = WaitForEnoughBytesToRead(context.readPlsn, *eachReadBytes);
    *eachReadBytes = Min(*eachReadBytes, availableBytes);
    /* If we have nothing to read, exit now */
    if (*eachReadBytes == 0) {
        if (m_redoMode == RedoMode::PDB_STANDBY_REDO) {
            return DSTORE_SUCC;
        } else {
            return DSTORE_FAIL;
        }
    }

    uint64 startTime = 0;
    if (m_walStream->IsCollectWalWriteIoStat()) {
       startTime = GetSystemTimeInMicrosecond();
    }

    (void)GsAtomicFetchAddU32(m_readBufferBlockStates[readBufferBlockNo].readCnt, 1);
    /* See ReadAsyncCallback() for how the read bytes are handled */
    RetStatus retStatus = context.walFile->PreadAsync(
        context.readBuffer, *eachReadBytes, context.readOffset, &asyncIoContext);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_PANIC,
            MODULE_WAL, ErrMsg("Wal read bytes worker pread async wal file failed."));
        (void)GsAtomicSubFetchU32(m_readBufferBlockStates[readBufferBlockNo].readCnt, 1);
        return DSTORE_FAIL;
    }

    /* for every batch, wait until all pending async reads to finish before proceeding. */
    uint64 waitCount = 0;
    while (GsAtomicReadU32(m_readBufferBlockStates[readBufferBlockNo].readCnt) == ASYNC_READ_BATCH_NUM) {
        /* the readcnt is decremented in the call back function ReadAsyncCallback() */
        uint64 walStreamEndPlsn = GsAtomicReadU64(&m_walStreamEndPlsn);
        uint64 blockEndPlsn = GsAtomicReadU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn);
        if (walStreamEndPlsn != INVALID_END_PLSN && walStreamEndPlsn <= blockEndPlsn) {
            ErrLog(walStreamEndPlsn == blockEndPlsn ? DSTORE_LOG : DSTORE_ERROR, MODULE_WAL,
                ErrMsg("Pdb %hhu walId:%lu at current block number:%u read plsn:%lu, block end plsn:%lu greater than "
                "or equal to wal stream end plsn:%lu, already reach to the end of wal.",
                m_walStream->GetPdbId(), m_walStream->GetWalId(), readBufferBlockNo, context.readPlsn, blockEndPlsn,
                walStreamEndPlsn));
            return DSTORE_FAIL;
        }
        if (m_stopLoadWorker.load(std::memory_order_acquire)) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("Pdb %hhu walId:%lu at plsn %lu stop wal load success.", m_walStream->GetPdbId(),
                m_walStream->GetWalId(), context.readPlsn));
            return DSTORE_FAIL;
        }
        GaussUsleep(waitEnoughBytesUsec);
        if (waitCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("LoadToBufferPageStore pdbId %u walId %lu wait %lu times for aync read finish, "
                       "readBufferBlockNo:%u, readPlsn:%lu, "
                "blockEndPlsn:%lu, walStreamEndPlsn:%lu.",
                m_walStream->GetPdbId(), m_walStream->GetWalId(), waitCount, readBufferBlockNo, context.readPlsn,
                blockEndPlsn, walStreamEndPlsn));
        }
        waitCount++;
    }

    if (m_walStream->IsCollectWalReadIoStat() && startTime != 0) {
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readCount), 1);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readLen), *eachReadBytes);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().actualReadLen), *eachReadBytes);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readTime), GetSystemTimeInMicrosecond() - startTime);
    }

    ErrLog(DSTORE_DEBUG1, MODULE_WAL,
        ErrMsg("Pdb %hhu walId %lu LoadToBufferPageStore async read %lu bytes at readPlsn %lu for readBufferBlock %u.",
            m_walStream->GetPdbId(), m_walStream->GetWalId(), *eachReadBytes, context.readPlsn, readBufferBlockNo));
    return DSTORE_SUCC;
}

RetStatus WalReadBuffer::DoLoadToBufferLocal(const LoadToBufferContext &context)
{
    uint64 *eachReadBytes = context.eachReadBytes;
    uint32 readBufferBlockNo = context.readBufferBlockNo;
    uint64 readPlsn = context.readPlsn;
    int64 resultSize = 0;
    RetStatus retStatus;
    uint64 startTime = 0;
    if (m_walStream->IsCollectWalWriteIoStat()) {
       startTime = GetSystemTimeInMicrosecond();
    }

    if (context.dioRw) {
        StorageAssert(context.dioReadAdaptor != nullptr);
        retStatus = context.dioReadAdaptor->Read(
            context.readBuffer, *eachReadBytes, context.walFile, context.readOffset, &resultSize);
    } else {
        retStatus = context.walFile->Read(context.readBuffer, *eachReadBytes, context.readOffset, &resultSize);
    }

    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_PANIC,
            MODULE_WAL, ErrMsg("Wal read bytes worker Read wal file failed."));
        return DSTORE_FAIL;
    }

    if (m_walStream->IsCollectWalReadIoStat() && startTime != 0) {
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readCount), 1);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readLen), *eachReadBytes);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().actualReadLen), resultSize);
        GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().readTime), GetSystemTimeInMicrosecond() - startTime);
    }

    uint64 tmpResultSize = static_cast<uint64>(resultSize);
    GsAtomicWriteU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn, readPlsn + tmpResultSize);

    /* Let the caller know how many bytes we really read this time */
    *context.eachReadBytes = tmpResultSize;
    return DSTORE_SUCC;
}

RetStatus WalReadBuffer::LoadToBufferLocal(const LoadToBufferContext &context)
{
    if (STORAGE_FUNC_FAIL(DoLoadToBufferLocal(context))) {
        return DSTORE_FAIL;
    }
    /* if the buffer is not full, it's the end of wal. */
    if (m_redoMode != RedoMode::PDB_STANDBY_REDO && (*context.eachReadBytes) < m_loadToBufferConf.readBufferBlockSize) {
        uint64 curReadEndPlsn = GsAtomicReadU64(&m_readBufferBlockStates[context.readBufferBlockNo].readEndPlsn);
        /* No need to use CAS here, because read is serials here, and current read end plsn must less than wal stream
         * end plsn. */
        (void)GsAtomicWriteU64(&m_walStreamEndPlsn, curReadEndPlsn);
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Wal buffer set pdb %u wal stream %lu end plsn at %lu, "
            "read len %lu not enough, reach to the end of wal.", m_walStream->GetPdbId(), m_walStream->GetWalId(),
            curReadEndPlsn, *context.eachReadBytes));
    }
    return DSTORE_SUCC;
}

void WalReadBuffer::LoadWalToBufferWorkerMain(uint64 readPlsn, PdbId pdbId)
{
    DstoreSetMemoryOutOfControl();
    LatencyStat::Timer timer(&WalPerfUnit::GetInstance().m_walRedoPreloadBufferBlock);

    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister(pdbId, false, "LoadWalToBufWrk", true,
                                                     ThreadMemoryLevel::THREADMEM_HIGH_PRIORITY);
    WalUtils::HandleWalThreadCpuBind("LoadWalToBufWrk");
    TimePoint start = std::chrono::high_resolution_clock::now();
    uint64 walFileSize = m_walStream->GetWalFileSize();
    int64 readOffset = static_cast<int64>(readPlsn % walFileSize);
    bool isPrimary = GetRedoMode() == RedoMode::PDB_STANDBY_REDO ? false : true;
    WalFile *readFile = m_walStream->GetWalFileManager()->GetWalFileByPlsn(readPlsn);
    if (STORAGE_VAR_NULL(readFile)) {
        if (isPrimary) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("WalRecovery LoadToBuffer reach end. find no wal file, readPlsn %lu.", readPlsn));
            goto READ_BYTES_END;
        } else {
            if (StandbyWaitWalFile(readPlsn)) {
                readFile = m_walStream->GetWalFileManager()->GetWalFileByPlsn(readPlsn);
                StorageAssert(readFile);
            } else {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                    ErrMsg("WalRecovery LoadToBuffer reach end. find no wal file, readPlsn %lu.", readPlsn));
                goto READ_BYTES_END;
            }
        }
    }
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("[PDB:%u WAL:%lu] LoadWalToBufferWorkerMain start", m_walStream->GetPdbId(), m_walStream->GetWalId()));

    LoadWalToBuffer(readFile, readPlsn, readOffset, start);

READ_BYTES_END:
    /* For read pagestore async, wait all read request returned */
    const uint32 waitReadFinishUs = 1000;
    while (GsAtomicReadU32(&m_readCnt) != 0) {
        GaussUsleep(waitReadFinishUs);
    }
    std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
    constexpr double byteToMbFactor = 1024 * 1024;
    /* if m_walStreamEndPlsn = INVALID_END_PLSN, it means all wal files are full and read block never failed before */
    /* make it equal to end read plsn */
    uint64 oldVal = INVALID_END_PLSN;
    if (GsAtomicCompareExchangeU64(&m_walStreamEndPlsn, &oldVal, readPlsn)) {
        ErrLog(DSTORE_LOG, MODULE_WAL,
            ErrMsg("Pdb %hhu Wal %lu update wal stream end plsn to %lu when read finish",
            m_walStream->GetPdbId(), m_walStream->GetWalId(), readPlsn));
    }
    uint64 walStreamEndPlsn = GsAtomicReadU64(&m_walStreamEndPlsn);
    ErrLog(DSTORE_LOG, MODULE_WAL,
        ErrMsg("Pdb %hhu Wal %lu load worker start readPlsn %lu end readPlsn %lu, end read fileOffset:%ld, "
               "spend time %f seconds avgSpeed %f MB/S",
               m_walStream->GetPdbId(), m_walStream->GetWalId(), m_readBufferStartPlsn, walStreamEndPlsn, readOffset,
               spendSecs.count(),
               (static_cast<double>(walStreamEndPlsn - m_readBufferStartPlsn) / byteToMbFactor) / spendSecs.count()));
    WalUtils::HandleWalThreadCpuUnbind("LoadWalToBufWrk");
    g_storageInstance->UnregisterThread();
    DstoreSetMemoryInControl();
}

bool WalReadBuffer::StandbyWaitWalFile(uint64 readPlsn)
{
    const uint32 waitWalFromPrimary = 10000;
    uint32 cnt = 0;
    const uint32 printTimes = 6000;
    bool isExist = false;
    do {
        isExist = m_walStream->GetWalFileManager()->CreateWalStreamFileObj(readPlsn,
            m_walStream->GetWalFileSize(), m_walStream->GetPdbId(), m_walStream->GetWalId());
        if (m_stopLoadWorker.load(std::memory_order_acquire)) {
            ErrLog(DSTORE_WARNING, MODULE_WAL,
                ErrMsg("WalRecovery LoadToBuffer reach end. find no wal file, readPlsn %lu.", readPlsn));
            return false;
        }
        GaussUsleep(waitWalFromPrimary);
        if ((++cnt) % printTimes == 0) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("[PDB:%hhu WAL:%lu]WalRecovery LoadToBuffer Wait %u times, readPlsn %lu.",
                m_walStream->GetPdbId(), m_walStream->GetWalId(), cnt, readPlsn));
        }
    } while (!isExist);
    return true;
}

void WalReadBuffer::LoadWalToBuffer(WalFile *readFile, uint64 &readPlsn, int64 &readOffset, TimePoint start)
{
    uint32 readBufferBlockNo = static_cast<uint32>(((readPlsn - m_readBufferStartPlsn) /
        m_loadToBufferConf.readBufferBlockSize) % m_loadToBufferConf.readBufferBlockNum);
    uint32 oldReadBufferBlockNo = readBufferBlockNo;
    uint64 walFileSize = m_walStream->GetWalFileSize();
    uint8 *readBuffer = m_readBuffer;
    uint64 eachReadBytes = m_loadToBufferConf.readBufferBlockSize;
    uint64 curBlockReadBytes = 0;
    bool dioRw = readFile->IsDioRw();
    WalDioReadAdaptor dioReadAdaptor{m_memoryContext, WAL_BLCKSZ};
    bool isPrimary = GetRedoMode() == RedoMode::PDB_STANDBY_REDO ? false : true;

    while (true) {
        TsAnnotateBenignRaceSized(readBuffer, eachReadBytes);
        if (STORAGE_FUNC_FAIL(WaitBufferRecycled(readBufferBlockNo))) {
            break;
        }
        /* set readEndPlsn to be the one we are reading if we just start reading this buffer block */
        if (curBlockReadBytes == 0) {
            GsAtomicWriteU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn, readPlsn);
        }
        m_readBufferBlockStates[readBufferBlockNo].recycled.store(false, std::memory_order_release);

        /* Record how many bytes we could still read in this wal buffer */
        uint64 maxReadBytes = m_loadToBufferConf.readBufferBlockSize - curBlockReadBytes;
        eachReadBytes = maxReadBytes;
        LoadToBufferContext loadToBufferContext = {readBufferBlockNo, readBuffer, readOffset, readFile, readPlsn,
            &eachReadBytes, dioRw, &dioReadAdaptor};

        if (STORAGE_FUNC_FAIL(LoadToBuffer(loadToBufferContext))) {
            break;
        }

        if ((m_redoMode != RedoMode::PDB_STANDBY_REDO && eachReadBytes < m_loadToBufferConf.readBufferBlockSize)) {
            ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("WalRecovery LoadToBuffer reach to the end of wal stream."
                "readBufferBlockNo: %u, readOffset: %ld, eachReadBytes: %lu, maxReadBytes: %lu.",
                readBufferBlockNo, readOffset, eachReadBytes, maxReadBytes));
            break;
        }

        readBuffer += eachReadBytes;
        readOffset += static_cast<int64>(eachReadBytes);
        readPlsn += eachReadBytes;
        if ((m_redoMode == RedoMode::PDB_STANDBY_REDO && eachReadBytes < maxReadBytes &&
            m_walStream->m_hasFinishedReceiving.load())) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("WalRecovery LoadToBuffer reach to the end of wal stream."
                "readBufferBlockNo: %u, readOffset: %ld, eachReadBytes: %lu, maxReadBytes: %lu.",
                readBufferBlockNo, readOffset, eachReadBytes, maxReadBytes));
            break;
        }

        curBlockReadBytes += eachReadBytes;
        oldReadBufferBlockNo = readBufferBlockNo;
        /* if we've load the current block to full, add 1 to readBufferBlockNo. Do nothing otherwise. */
        readBufferBlockNo = (readBufferBlockNo + (curBlockReadBytes / m_loadToBufferConf.readBufferBlockSize)) %
                            m_loadToBufferConf.readBufferBlockNum;
        if (oldReadBufferBlockNo == readBufferBlockNo) {
            /* buffer is not full yet, reset its recycle status to keep loading in next round */
            /* only standbypdb can reach here */
            m_readBufferBlockStates[readBufferBlockNo].recycled.store(true, std::memory_order_release);
        } else {
            /* current buffer is full and we will switch to next buffer. reset curBlockReadBytes here */
            StorageAssert(curBlockReadBytes == m_loadToBufferConf.readBufferBlockSize);
            curBlockReadBytes = 0;
        }
        if (static_cast<uint64>(readOffset) == walFileSize) {
            uint64 startTime = 0;
            if (m_walStream->IsCollectWalWriteIoStat()) {
                startTime = GetSystemTimeInMicrosecond();
            }
            readOffset = 0;
            WalFile *readFilePre = readFile;
            readFile = m_walStream->GetWalFileManager()->GetNextWalFile(readFile, false);
            if (readFile == nullptr) {
                if (isPrimary) {
                    ErrLog(DSTORE_WARNING, MODULE_WAL,
                        ErrMsg("WalRecovery LoadToBuffer reach end when switch. find no wal file, readPlsn %lu.",
                        readPlsn));
                    break;
                } else {
                    if (StandbyWaitWalFile(readPlsn)) {
                        readFile = readFilePre;
                        readFile = m_walStream->GetWalFileManager()->GetNextWalFile(readFile, false);
                        StorageAssert(readFile);
                    } else {
                        ErrLog(DSTORE_WARNING, MODULE_WAL,
                            ErrMsg("WalRecovery LoadToBuffer reach end when switch. find no wal file, readPlsn %lu.",
                            readPlsn));
                        break;
                    }
                }
            }
            StorageAssert(readFile->IsDioRw() == dioRw);
            if (m_walStream->IsCollectWalReadIoStat() && startTime != 0) {
                GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().waitCount), 1);
                GsAtomicFetchAddU64(&(m_walStream->GetWalReadIoStat().waitTime),
                    GetSystemTimeInMicrosecond() - startTime);
            }
        }
        if (readBuffer == m_readBuffer + m_loadToBufferConf.readBufferSize) {
            std::chrono::duration<double> spendSecs = std::chrono::high_resolution_clock::now() - start;
            ErrLog(DSTORE_LOG, MODULE_WAL,
                   ErrMsg("WalRecovery Wal load worker readPlsn:%lu, read fileOffset:%ld spend time %f seconds",
                       readPlsn, readOffset, spendSecs.count()));
            readBuffer = m_readBuffer;
        }
    }
}

RetStatus WalReadBuffer::ReadFromBuffer(uint64 plsn, uint64 readLen, uint64 *resultLen, uint8 **data)
{
    /* don't read anything prior to m_readBufferStartPlsn -- it should be covered by the last checkpoint */
    if (!m_loadToBufferStarted || plsn < m_readBufferStartPlsn) {
        *resultLen = 0;
        *data = nullptr;
        return DSTORE_SUCC;
    }
    uint32 readBufferBlockNo = static_cast<uint32>(((plsn - m_readBufferStartPlsn) /
                                                    m_loadToBufferConf.readBufferBlockSize) %
                                                   m_loadToBufferConf.readBufferBlockNum);
    uint64 readBlockOffset = plsn % m_loadToBufferConf.readBufferBlockSize;
    uint8 *bufferPtr = m_readBuffer + ((plsn - m_readBufferStartPlsn) % m_loadToBufferConf.readBufferSize);
    *data = bufferPtr;
    uint64 readEndPlsn;
    uint64 availableWalLen;
    *resultLen = 0;

    /* standby replay not need to wait a complete block, directly get readEndPlsn in current block. */
    if (m_redoMode == RedoMode::PDB_STANDBY_REDO) {
        readEndPlsn = GsAtomicReadU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn);
        /* when blocknum is max and restart from 0, if load data is slow, readEndPLsn may be not update */
        if (readEndPlsn >= plsn) {
            (*resultLen) = readEndPlsn - plsn;
        }
        return DSTORE_SUCC;
    }

    /* try to read one block at a time, until either readLen is satisfied or we reach the end of readBuffer */
    while (readLen > 0) {
        /* if we just reached the end of current buffer block, go to the next one */
        if (readBlockOffset == m_loadToBufferConf.readBufferBlockSize) {
            readBufferBlockNo = (readBufferBlockNo + 1U) % m_loadToBufferConf.readBufferBlockNum;
            readBlockOffset = 0;
        }

        readEndPlsn = GsAtomicReadU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn);
        if (readEndPlsn > plsn + m_loadToBufferConf.readBufferBlockSize) {
            return DSTORE_SUCC;
        }
        uint64 waitCount = 0;
        while (readEndPlsn <= plsn) {
            uint64 walStreamEndPlsn = GsAtomicReadU64(&m_walStreamEndPlsn);
            readEndPlsn = GsAtomicReadU64(&m_readBufferBlockStates[readBufferBlockNo].readEndPlsn);
            if (walStreamEndPlsn != INVALID_END_PLSN &&
                (walStreamEndPlsn <= readEndPlsn || walStreamEndPlsn == plsn) && readEndPlsn <= plsn) {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("Read from buffer reach to the end of wal at plsn %lu, when read need plsn %lu at buffer "
                           "block number %u.",
                    walStreamEndPlsn, plsn, readBufferBlockNo));
                return DSTORE_SUCC;
            }
            waitCount++;
            if (waitCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                ErrLog(DSTORE_LOG, MODULE_WAL,
                    ErrMsg("ReadFromBuffer wait %lu times for buffer BlockNo:%u, readPlsn:%lu, blockEndPlsn:%lu, "
                    "walStreamEndPlsn:%lu.", waitCount, readBufferBlockNo, plsn, readEndPlsn, walStreamEndPlsn));
            }
            GaussUsleep(WAIT_BUFFER_LOADED_MICROSEC);
        }

        availableWalLen = readEndPlsn - plsn;
        StorageAssert(availableWalLen <= m_loadToBufferConf.readBufferBlockSize);
        uint64 gotLen = Min(availableWalLen, readLen);
        readLen -= gotLen;
        bufferPtr += gotLen;
        readBlockOffset += gotLen;
        plsn += gotLen;
        (*resultLen) += gotLen;
        /* we reached the end of readBuffer. caller should try again in case of wrapping around */
        if (static_cast<uint64>(bufferPtr - m_readBuffer) == m_loadToBufferConf.readBufferSize) {
            ErrLog(DSTORE_LOG, MODULE_WAL,
                ErrMsg("read buffer is wrapping around. caller need to try again with remainder size"));
            return DSTORE_SUCC;
        }
    }
    return DSTORE_SUCC;
}

void WalReadBuffer::RecycleReadBuffer(uint64 endPlsn)
{
    if (!m_loadToBufferStarted || endPlsn <= m_readBufferStartPlsn + m_loadToBufferConf.readBufferBlockSize) {
        return;
    }
    uint32 readBufferBlockNo;
    uint64 curRecycledReadBufferNum = ((endPlsn - m_readBufferStartPlsn) / m_loadToBufferConf.readBufferBlockSize);
    for (; m_recycledReadBufferNum < curRecycledReadBufferNum; m_recycledReadBufferNum++) {
        readBufferBlockNo = static_cast<uint32>(m_recycledReadBufferNum % m_loadToBufferConf.readBufferBlockNum);
        m_readBufferBlockStates[readBufferBlockNo].recycled.store(true, std::memory_order_release);
    }
}

bool WalReadBuffer::ReadRangeReachBufferEnd(uint64 plsn, uint64 readLen) const
{
    return ((plsn - m_readBufferStartPlsn) % m_loadToBufferConf.readBufferSize) + readLen >
           m_loadToBufferConf.readBufferSize;
}

void WalReadBuffer::StopLoadWorker()
{
    /* Set hint here to stop load worker thread */
    m_walStream->m_hasFinishedReceiving.store(true);
    m_stopLoadWorker.store(true);

    /* load worker thread will be deleted by WalRecovery::Redo thread eventually */
}

WalRecord *WalReadBuffer::GetWalRecordForPageByPlsn(uint64 startPlsn, uint16 walRecordSize, const PageId pageId,
                                                    bool *needFree)
{
    uint64 fileSize = m_walStream->GetWalFileSize();
    *needFree = false;
    uint8 *walRecord = nullptr;
    uint64 readSize = walRecordSize;
    RetStatus retStatus;
    uint64 firstSegWalRecordSize = fileSize - startPlsn % fileSize;
    bool crossSeg = walRecordSize > firstSegWalRecordSize;
    if (crossSeg) {
        *needFree = true;
        StorageAssert(walRecordSize < fileSize - WAL_FILE_HDR_SIZE);
        readSize += WAL_FILE_HDR_SIZE;
        walRecord = static_cast<uint8 *>(DstorePalloc(readSize));
        if (STORAGE_VAR_NULL(walRecord)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("GetWalRecordForPageByPlsn palloc fail"));
        }
    }
    uint8 *bufferPtr = nullptr;
    uint64 resultLen = 0;
    retStatus = ReadFromBuffer(startPlsn, readSize, &resultLen, &bufferPtr);
    if (STORAGE_FUNC_FAIL(retStatus) || resultLen < readSize || bufferPtr == nullptr) {
        if (!*needFree) {
            walRecord = static_cast<uint8 *>(DstorePalloc(readSize));
            if (STORAGE_VAR_NULL(walRecord)) {
                ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("GetWalRecordForPageByPlsn palloc fail"));
            }
            *needFree = true;
        }
        retStatus = m_walStream->Read(startPlsn, walRecord, readSize, &resultLen);
        ErrLog(DSTORE_WARNING,
               MODULE_WAL, ErrMsg("GetWalRecordForPageByPlsn read from disk plsn:%lu", startPlsn));
        if (STORAGE_FUNC_FAIL(retStatus) || resultLen < readSize) {
            return nullptr;
        }
    } else {
        if (crossSeg) {
            errno_t rc = memcpy_s(walRecord, readSize, bufferPtr, resultLen);
            storage_securec_check(rc, "\0", "\0")
        } else {
            walRecord = bufferPtr;
        }
    }
    if (crossSeg) {
        errno_t rc = memmove_s(walRecord + firstSegWalRecordSize, walRecordSize - firstSegWalRecordSize,
                               walRecord + firstSegWalRecordSize + WAL_FILE_HDR_SIZE,
                               walRecordSize - firstSegWalRecordSize);
        storage_securec_check(rc, "\0", "\0")
    }

    WalRecord *tmpWalRecord = STATIC_CAST_PTR_TYPE(walRecord, WalRecord *);
    StorageReleasePanic(walRecordSize != tmpWalRecord->GetSize(), MODULE_WAL,
        ErrMsg("walRecordSize mismatch size recorded in wal, walRecordSize:%hu size in wal:%hu.",
        walRecordSize, tmpWalRecord->GetSize()));
    uint32 pageIndex = 0;
    if (tmpWalRecord->m_type == WAL_TBS_INIT_MULTIPLE_DATA_PAGES) {
        WalRecordTbsInitDataPages *initDataPagesRecord =
                STATIC_CAST_PTR_TYPE(tmpWalRecord, WalRecordTbsInitDataPages *);
        StorageAssert(pageId.m_fileId == initDataPagesRecord->firstDataPageId.m_fileId);
        pageIndex = pageId.m_blockId - initDataPagesRecord->firstDataPageId.m_blockId;
        WalRecordTbsInitOneDataPage *initOneDataPageRecord =
                static_cast<WalRecordTbsInitOneDataPage *>(DstorePalloc(sizeof(WalRecordTbsInitOneDataPage)));
        if (STORAGE_VAR_NULL(initOneDataPageRecord)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecordTbsInitOneDataPage palloc fail"));
        }
        initOneDataPageRecord->SetData(pageId, initDataPagesRecord->dataPageType,
                                       {initDataPagesRecord->firstFsmIndex.page,
                                        static_cast<uint16>(initDataPagesRecord->firstFsmIndex.slot +
                                                            pageIndex)},
                                       initDataPagesRecord->preWalPointer[pageIndex],
                                       initDataPagesRecord->filePreVersion);
        if (*needFree) {
            DstorePfreeExt(walRecord);
        }
        *needFree = true;
        tmpWalRecord = STATIC_CAST_PTR_TYPE(initOneDataPageRecord, WalRecord *);
    } else if (tmpWalRecord->m_type == WAL_TBS_INIT_BITMAP_PAGES) {
        WalRecordTbsInitBitmapPages *initBitmapPagesRecord =
                STATIC_CAST_PTR_TYPE(tmpWalRecord, WalRecordTbsInitBitmapPages *);
        StorageAssert(pageId.m_fileId == initBitmapPagesRecord->firstPageId.m_fileId);
        pageIndex = pageId.m_blockId - initBitmapPagesRecord->firstPageId.m_blockId;
        WalRecordTbsInitOneBitmapPage *initOneBitmapPageRecord =
                static_cast<WalRecordTbsInitOneBitmapPage *>(DstorePalloc(sizeof(WalRecordTbsInitOneBitmapPage)));
        if (STORAGE_VAR_NULL(initOneBitmapPageRecord)) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("WalRecordTbsInitOneBitmapPage palloc fail"));
        }
        initOneBitmapPageRecord->SetData(pageId, initBitmapPagesRecord->pageType,
                                         {initBitmapPagesRecord->firstDataPageId.m_fileId,
                                          initBitmapPagesRecord->firstDataPageId.m_blockId +
                                          pageIndex * DF_BITMAP_BIT_CNT *
                                                  static_cast<uint16>(initBitmapPagesRecord->extentSize)},
                                         initBitmapPagesRecord->preWalPointer[pageIndex],
                                         initBitmapPagesRecord->filePreVersion);
        if (*needFree) {
            DstorePfreeExt(walRecord);
        }
        *needFree = true;
        tmpWalRecord = STATIC_CAST_PTR_TYPE(initOneBitmapPageRecord, WalRecord *);
    }
    return tmpWalRecord;
}
}
