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
 * dstore_logical_decode_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_DISPATCHER_H
#define DSTORE_WAL_DISPATCHER_H

#include <thread>

#include "wal/dstore_wal.h"
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "dstore_wal_sort_buffer.h"
#include "dstore_logical_types.h"
#include "dstore_logical_replication_slot.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
struct WalRecordBuf {
    WalPlsn groupBeginPlsn;
    WalPlsn groupEndPlsn;
    Xid xid;
    const WalRecord *record;
};

class WalDispatcher : public BaseObject {
public:
    WalDispatcher(DstoreMemoryContext mctx, LogicalReplicationSlot *logicalSlot, DecodeOptions *decodeOpt, PdbId pdbId);
    ~WalDispatcher() = default;
    RetStatus Init(ParallelDecodeWorker** workerArray, int parallelDecodeWorkerNum);
    void Destroy();

    /* startup wal reader thread, then re-order and sort wal in warsortbuffer */
    void Run();
    void Stop() noexcept;
    inline bool IsRunning()
    {
        return m_isRunningFlag.load();
    }
    bool NeedMoreWal() const;
    inline WalPlsn GetEndPlsn()
    {
        return m_endPlsn;
    }
    inline void SetEndPlsn(WalPlsn endPlsn)
    {
        m_endPlsn = endPlsn;
    }
    inline void FreeTrxChangeCtx(TrxChangeCtx *freeTrx)
    {
        m_walSortBuffer->FreeTrxChangeCtx(static_cast<TrxChangeCtx *>(freeTrx));
    }
private:
    void WorkerMain(PdbId pdbId);
    void ReleaseWalDecompressedBuf(const WalRecord *walrecord);
    void ProcessXid(WalRecordBuf *buf);
    void ProcessWalRecord(WalRecordBuf *buf);
    void ProcessHeapOp(WalRecordBuf *buf);
    void ProcessXactOp(WalRecordBuf *buf);
    void ProcessWalBarrierCsn(WalRecordBuf *buf);
    void ParseInsert(WalRecordBuf *buf);
    void ParseDelete(WalRecordBuf *buf);
    void ParseUpdate(WalRecordBuf *buf);
    void ParseCommit(WalRecordBuf *buf);
    void ParseAbort(WalRecordBuf *buf);
    bool IsNewWalFlushed(const WalPlsn lastPlsn);

    DstoreMemoryContext m_memoryContext;
    LogicalReplicationSlot *m_logicalSlot;
    std::atomic<bool> m_isRunningFlag;
    std::atomic_bool m_needStopFlag;
    std::thread *m_workerThrd;
    WalRecordReader *m_walReader;
    WalSortBuffer *m_walSortBuffer;
    WalPlsn m_endPlsn;  /* distribute logical decode will never use this as terminal condition */
    DecodeOptions *m_decodeOptions;
    PdbId m_pdbId;
};
#endif

}
#endif