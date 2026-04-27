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
 * dstore_wal_read_buffer.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/wal/dstore_wal_read_buffer.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_WAL_READ_BUFFER_H
#define DSTORE_DSTORE_WAL_READ_BUFFER_H

#include <thread>
#include "common/dstore_datatype.h"
#include "wal/dstore_wal_file_manager.h"

namespace DSTORE {
#ifdef UT
constexpr uint64 WAL_READ_BUFFER_BLOCK_SIZE = 16 * 1024U;
#else
constexpr uint64 WAL_READ_BUFFER_BLOCK_SIZE = 512 * 1024U;
#endif

enum class RedoMode;
class WalStream;

struct WalFileLoadToBufferConf {
    uint64 loadStartPlsn;
    uint32 readBufferBlockNum;
    uint64 readBufferBlockSize;
    uint64 readBufferSize;
};

struct WalFileLoadToBufferBlockState {
    uint64 readEndPlsn;
    gs_atomic_uint32 *readCnt;
    std::atomic_bool recycled;
    std::atomic_bool *loadWorkerNeedStop;
} ALIGNED(DSTORE_CACHELINE_SIZE);

class WalDioReadAdaptor;
struct LoadToBufferContext {
    uint32 readBufferBlockNo;
    uint8 *readBuffer;
    int64 readOffset;
    WalFile *walFile;
    uint64 readPlsn;
    uint64 *eachReadBytes;
    bool dioRw;
    WalDioReadAdaptor *dioReadAdaptor;
};

struct RecoveryAsyncInfo {
    WalFileLoadToBufferBlockState *blockState;
    /* walStreamEndPlsn is wal stream end flag which will be set by some threads. */
    gs_atomic_uint64 *walStreamEndPlsn;
    PdbId pdbId;
    WalId walId;
};

struct PdbStandbyRedoAsyncInfo {
    WalFileLoadToBufferBlockState *blockState;
    uint64 readStartPlsn;
};

class WalReadBuffer : public BaseObject {
public:
    using TimePoint = std::chrono::time_point<std::chrono::system_clock, std::chrono::duration<double>>;
    WalReadBuffer(DstoreMemoryContext memoryContext, WalStream *walStream, RedoMode redoMode);

    virtual ~WalReadBuffer();
    DISALLOW_COPY_AND_MOVE(WalReadBuffer)

    /**
     * Load segment files to read buffer according to WalFileLoadToBufferConf
     *
     * @param conf : configure load start plsn, read buffer size, record read call back etc.
     * @return DSTORE_SUCC if load success
     */
    RetStatus StartLoadToBuffer(const WalFileLoadToBufferConf &conf);

    /**
     * Free read buffers used to cache segment files.
     */
    void FreeReadBuffer() noexcept;

    /**
      * Recycle ReadBuffer block
      *
      * @param endPlsn : recycle read buffer block before endPlsn.
      */
    void RecycleReadBuffer(uint64 endPlsn);

    bool StandbyWaitWalFile(uint64 readPlsn);

    /**
     * Read from read buffer. output a contiguous chunk of memory until:
     * 1) the desired length (readLen), 2) the end of readBuffer, or 3) the end of wal data.
     *
     * @param: plsn: read start plsn.
     * @param: readLen: target len to read.
     * @param: resultLen: output parameter, is actual read data len.
     * @param: data: output parameter, read buffer ptr correspond to plsn
     *
     * @return: OK if success, detail error info otherwise.
     */
    RetStatus ReadFromBuffer(uint64 plsn, uint64 readLen, uint64 *resultLen, uint8 **data);

    /**
     * Judge read from read buffer range reach buffer end.
     *
     * @param: plsn: read start plsn.
     * @param: readLen: target len to read.
     *
     * @return: true if reach buffer end, false if not.
     */
    bool ReadRangeReachBufferEnd(uint64 plsn, uint64 readLen) const;

    /**
     * Stop load worker thread if worker is running.
     */
    void StopLoadWorker();

    WalRecord *GetWalRecordForPageByPlsn(uint64 startPlsn, uint16 walRecordSize, const PageId pageId, bool *needFree);

    inline bool IsLoadWorkerStarted() const
    {
        return (m_loadWalToBufferWorker != nullptr);
    }

    inline RedoMode GetRedoMode() const
    {
        return m_redoMode;
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    uint8 *GetReadBufferPtr()
    {
        return m_readBuffer;
    }
#endif
    uint64 GetWalStreamEndPlsn()
    {
        return GsAtomicReadU64(&m_walStreamEndPlsn);
    }
    uint64 GetStandbyEndPlsn()
    {
        return m_standbyReadMaxPlsn;
    }
    void SetStandbyEndPlsn(uint64 maxPlsn)
    {
        m_standbyReadMaxPlsn = maxPlsn;
    }
#ifndef UT
private:
#endif

    RetStatus WaitBufferRecycled(uint32 readBufferBlockNo) const;

    void LoadWalToBufferWorkerMain(uint64 readPlsn, PdbId pdbId);
    virtual void PrepareAsyncIoContext(AsyncIoContext &asyncIoContext, const LoadToBufferContext &context);

    virtual void LoadWalToBuffer(WalFile *readFile, uint64 &readPlsn, int64 &readOffset, TimePoint start);
    RetStatus LoadToBufferPageStore(const LoadToBufferContext &context);
    RetStatus LoadToBuffer(const LoadToBufferContext &context);
    RetStatus DoLoadToBufferLocal(const LoadToBufferContext &context);
    virtual RetStatus LoadToBufferLocal(const LoadToBufferContext &context);
    virtual uint64 WaitForEnoughBytesToRead(uint64 readPlsn, uint64 eachReadBytes);
    uint64 WaitForEnoughBytesToReadStandby(uint64 readPlsn, uint64 eachReadBytes);

    WalStream *m_walStream;
    WalFileLoadToBufferConf m_loadToBufferConf;
    uint8 *m_readBuffer;
    uint64 m_readBufferStartPlsn;
    uint64 m_recycledReadBufferNum;
    WalFileLoadToBufferBlockState *m_readBufferBlockStates;
    std::thread *m_loadWalToBufferWorker;
    gs_atomic_uint64 m_walStreamEndPlsn;
    std::atomic_bool m_stopLoadWorker;
    bool m_loadToBufferStarted;
    gs_atomic_uint32 m_readCnt;
    DstoreMemoryContext m_memoryContext;
    RedoMode m_redoMode;
    uint64 m_standbyReadMaxPlsn;
};

}
#endif /* DSTORE_STORAGE_WAL_READ_BUFFER_H */
