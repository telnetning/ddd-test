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
 * dstore_parallel.h
 *    ParallelWorkControlBlock provides all functions to access work in parallel workload.
 *
 * IDENTIFICATION
 *        include/framework/dstore_parallel.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef DSTORE_PARALLEL_H
#define DSTORE_PARALLEL_H

#include <mutex>
#include <thread>

#include "common/memory/dstore_mctx.h"
#include "page/dstore_page_struct.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_parallel_interface.h"
#include "lock/dstore_lwlock.h"
#include "transaction/dstore_transaction_types.h"
#include "framework/dstore_thread.h"

namespace DSTORE {
class ParallelHeapScanWorkloadInfo;
class ParallelWorkloadInfoInterface : public BaseObject {
public:
    virtual ~ParallelWorkloadInfoInterface() = default;
    virtual PageId GetStartPage() = 0;
    virtual PageId GetNextPage() = 0;
    virtual PageId GetEndPage() = 0;
    virtual uint GetNumPagesLeft() = 0;
    virtual void Reset() = 0;
};

class WorkSourceInterface : public BaseObject {
public:
    virtual ~WorkSourceInterface() = default;
    /* Initialize the parallel worksource */
    virtual RetStatus Init() = 0;
    /* Get next batch of work */
    virtual ParallelWorkloadInfoInterface *GetWork() = 0;
    /* Get if the all works are done */
    virtual bool InProgress() = 0;
    /* Reset to initialized state to re-produce the batches */
    virtual RetStatus Reset() = 0;
    /* Return a Meta Page ID, this should uniquely identifies a work source */
    virtual PageId GetMetaPageId() = 0;
};

const uint16 MAX_WORKER_NUM = 128;
const uint64 INIT_EXT_ARRAY_SIZE = 20;

class ParallelSeqscanHistory : public BaseObject {
public:
    ParallelSeqscanHistory() : m_workloadArray(nullptr), m_arraySize(0), m_hisNum(0), m_scannedIdx(0) {}
    ~ParallelSeqscanHistory();
    ParallelHeapScanWorkloadInfo *m_workloadArray;
    int m_arraySize;
    int m_hisNum;
    int m_scannedIdx;
    void Reset();
    bool Done();
    RetStatus AddWorkload(ParallelHeapScanWorkloadInfo*);
    ParallelWorkloadInfoInterface *GetWork();
};

class ParallelWorkController : public BaseObject {
public:
    ParallelWorkController(int smpNum);
    virtual ~ParallelWorkController();
    DISALLOW_COPY_AND_MOVE(ParallelWorkController);
    RetStatus InitParallelHeapScan(PdbId pdbId, PageId segmentMetaPageId);
    RetStatus InitParallelIndexScan();
    RetStatus InitWorkload();
    ParallelWorkloadInfoInterface *GetWork(int smpId);
    bool IsInitialized();
    bool Done();
    void ResetWorkSource();
    void Rescan(int smpId)
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (m_seqscanHistory != nullptr) {
            m_seqscanHistory[smpId].Reset();
        }
    }
    void ClearHistory()
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (m_seqscanHistory != nullptr) {
            delete[] m_seqscanHistory;
            m_seqscanHistory = nullptr;
        }
    }
    inline PageId GetMetaPageId()
    {
        return m_workSource->GetMetaPageId();
    }
    inline Xid GetTransactionId()
    {
        return m_txid;
    }
    std::unique_lock<std::mutex> *LockController()
    {
        return new std::unique_lock<std::mutex>(m_mutex);
    }
    inline void UnlockController(std::unique_lock<std::mutex> * uniqueLock)
    {
        if (uniqueLock == nullptr) {
            return;
        }
        uniqueLock->unlock();
        delete uniqueLock;
    }

    ThreadContext *m_mainThread;
    bool m_workerCanAbort[MAX_WORKER_NUM] = {false};

private:
    std::mutex m_mutex;
    Xid m_txid = INVALID_XID;
    WorkSourceInterface *m_workSource;
    int m_smpNum = 1;
    /* In the future, we can proceed with refactoring
     * by defining a heapscan class and inheriting from
     * ParallelWorkController.
     */
    ParallelSeqscanHistory *m_seqscanHistory = nullptr;
};
} // namespace DSTORE


#endif /* DSTORE_PARALLEL_H */