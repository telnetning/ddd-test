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
 * dstore_btree_build_parallel.h
 *	  Definition of Btree build in parallel.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/index/dstore_btree_build_parallel.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_BUILD_PARALLEL_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_BUILD_PARALLEL_H

#include <thread>
#include <mutex>

#include "tuple/dstore_index_tuple.h"
#include "common/algorithm/dstore_tuplesort.h"
#include "index/dstore_btree.h"
#include "page/dstore_index_page.h"
#include "framework/dstore_parallel.h"

namespace DSTORE {
enum class WorkerState {
    NORMAL_STATE,
    INIT_SQL_THREAD_CONTEXT_FAIL_STATE,
    FINISH_MAIN_JOB_SUCC_STATE,
    FINISH_MAIN_JOB_FAIL_STATE
};

class ParallelBtreeBuildWorker : public BaseObject {
public:
    ParallelBtreeBuildWorker() = delete;
    /**
     * Each worker is for one worker thread, it must inherit the csn and cid from the active transaction of main thread
     * All worker threads will share a memory context
     */
    ParallelBtreeBuildWorker(TuplesortMgr *tupleSortMgr, Transaction *mainThrdTransaction,
                             IndexBuildInfo *indexBuildInfo, int workMem, ScanKey scanKey,
                             DstoreMemoryContext parallelCxt, TuplesortMgr *mainTupleSortMgr,
                             int workerId);
    DISALLOW_COPY_AND_MOVE(ParallelBtreeBuildWorker)

    void Destroy();

    /**
     * Start running scan worker threads
     *
     */
    RetStatus Run(PdbId pdbId, StorageRelation heapRel, Oid tableOid, ParallelWorkController *controller,
                  bool isLastPartition = true);

    /**
     * Start running build worker threads
     *
     */
    RetStatus BuildLpiParallelCrossPartition(PdbId pdbId, ParallelWorkController *controller);

    /**
     * Join the thread of the worker and get result
     */
    RetStatus GetWorkerResult();

    inline uint GetHeapTupleCount() const
    {
        return m_numHeapTuples;
    }

    inline uint GetIndexTupleCount() const
    {
        return m_numIndexTuples;
    }

    inline TuplesortMgr *GetTupleSortMgr()
    {
        return m_tupleSortMgr;
    }

    /* We will never clear m_threadId once set.
     * When needCurrThreadId == true, this function returns the real thread ID of the thread.
     *                                if the thread has already been destroied, then we'll return invalid pid.
     * When needCurrThreadId == false, this function returns the thread ID we're using or have ever used, ignoring
     *                                 whether the thread is still alive or not. */
    inline ThreadId GetWorkerThreadId(bool needCurrThreadId = false) const
    {
        ThreadId tid = m_threadId;
        if (needCurrThreadId && (m_thrd == nullptr || m_thrd->GetCore() == nullptr)) {
            tid = INVALID_THREAD_ID;
        }
        return tid;
    }

    inline void SetWorkerState(WorkerState workerState)
    {
        m_workerState = workerState;
    }

    inline WorkerState GetWorkerState()
    {
        return m_workerState;
    }

    inline void AbortBuild()
    {
        if (m_thrd != nullptr && m_thrd->GetCore() != nullptr) {
            m_thrd->SetInterruptPending();
        }
    }

private:
    void ScanWorkerMain(PdbId pdbId, StorageRelation heapRel, Oid tableOid, bool isLastPartition = true);
    void BuildWorkerMain(PdbId pdbId);
    IndexBuildInfo* ConstructIndexBuildInfoForOnePart(IndexBuildInfo* indexBuildInfo, int partIdx);
    void DestroyIndexBuildInfoForOnePart(IndexBuildInfo* indexBuildInfo, BtreeStorageMgr* oldBtreeSmgr);

    std::thread *m_workerThrd;
    RetStatus m_taskRes;
    TuplesortMgr *m_tupleSortMgr;
    TuplesortMgr *m_mainTupleSortMgr;
    ParallelWorkController *m_controller;
    Transaction *m_mainThrdTransaction;
    StorageRelation m_heapRel;
    uint m_numIndexTuples;
    uint m_numHeapTuples;
    IndexBuildInfo* m_indexBuildInfo;
    int m_workMem;
    ScanKey m_scanKey;
    DstoreMemoryContext m_parallelCxt; /* share memory context for worker threads */
    ThreadContext *m_thrd;
    ThreadId m_threadId;
    WorkerState m_workerState;
    int m_workerId;
};

const uint16 MAX_BTREE_WORKER_NUM = 128;

class ParallelBtreeBuild : public BaseObject {
public:
    ParallelBtreeBuild() = delete;
    ParallelBtreeBuild(int numThrds, int workMem, IndexBuildInfo *indexBuildInfo, ScanKey scanKey,
                       TuplesortMgr *mainTupleSortMgr);

    DISALLOW_COPY_AND_MOVE(ParallelBtreeBuild)

    void Destroy();

    /**
     * CollectTuplesFromTable
     *
     * 1. Scan table and collect heap tuples.
     * 2. Form index tuple by heap tuples according to index information
     * 3. Add index tuple to TuplesortMgr for later sorting
     */
    RetStatus CollectTuplesFromTable(StorageRelation heapRel, Oid tableOid, double &numHeapTuples,
                                     bool isLastPartition = true);

    RetStatus MergeWorkerResults();

    RetStatus BuildLpiParallelCrossPartition(PdbId pdbId);

    RetStatus Init();

    PdbId GetPdbId()
    {
        StorageReleasePanic(m_indexBuildInfo == nullptr || m_indexBuildInfo->heapRels == nullptr ||
            (*(m_indexBuildInfo->heapRels)) == nullptr, MODULE_INDEX, ErrMsg("heapRel is nullptr"));\
        return (*(m_indexBuildInfo->heapRels))->m_pdbId;
    }
private:
    void InitParallelCxt();
    /**
     * Delete all of workers
     */
    void ClearWorkers();
    int m_numThrds;
    int m_workMem;
    IndexBuildInfo *m_indexBuildInfo;
    ParallelBtreeBuildWorker *m_workers[MAX_BTREE_WORKER_NUM];
    BufMgrInterface *m_bufMgr;
    ScanKey m_scanKey;
    DstoreMemoryContext m_parallelCxt; /* share memory context for worker threads */
    TuplesortMgr *m_mainTupleSortMgr;
};

} // namespace DSTORE

#endif /* SRC_GAUSSKERNEL_INCLUDE_BTREE_BUILD_PARALLEL */
