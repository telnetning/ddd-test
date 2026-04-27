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
 * dstore_wal_parallel_redo_worker.h
 *	  Definition of Wal playback workers in parallel.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/wal/dstore_wal_parallel_redo_worker.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_WAL_PARALLEL_REDO_WORKER_H
#define DSTORE_WAL_PARALLEL_REDO_WORKER_H

#include <thread>

#include "dstore_wal_struct.h"
#include "dstore_wal_block_spsc_queue.h"

namespace DSTORE {
constexpr uint32 MAX_BATCH_RECORD_NUM = 100;
enum ParallelRedoWorkerFlag : uint8 {
    PARALLEL_REDO_WORKER_SUPPORT_READ = 0x1,
};
class WalRecovery;
struct ParallelRedoWorkerInitParam {
    uint16 workerId;
    PdbId pdbId;
    WalId walId;
    uint8 flag;
    uint32 capacity;
    SleepNotify func;
    DstoreMemoryContext memoryContext;
    WalRecovery *walRecovery;
};

struct WalRecordBufFreeEntry {
    WalRecordBufFreeEntry *freeNext;
    uint32 walRecordSize;
    WalRecord *walRecord;
};

struct RedoWalRecordEntry {
    uint64 recordEndPlsn;
    WalRecordRedoContext ctx;
    bool walRecordNeedFree = true;
    union {
        const WalRecord *walRecord;
        WalRecordBufFreeEntry *freeRedoEntry;
    } walRecordInfo;

    inline const WalRecord* GetWalRecord()
    {
        return walRecordNeedFree ? walRecordInfo.freeRedoEntry->walRecord : walRecordInfo.walRecord;
    }
    inline WalRecord* GetWalRecordInBufFreeEntry()
    {
        StorageAssert(walRecordInfo.freeRedoEntry != nullptr);
        return walRecordInfo.freeRedoEntry->walRecord;
    }
};

constexpr uint32 REDO_FREELIST_CAPACITY = 1024;
struct WalRecordBufFreeEntryList {
    /* Free memory of a WalRecord to the free list */
    void FreeRedoWalRecordEntry(WalRecordBufFreeEntry *entry);
    /* Get memory of a WalRecord from the free list */
    WalRecordBufFreeEntry *AllocRedoWalRecord(uint32 recordSize);
    /* Reclaim all node memory */
    void Destory() noexcept;
    void ReAlloc(WalRecordBufFreeEntry *newEntry, uint32 recordSize) const;
    std::atomic<WalRecordBufFreeEntry *> addHead;
    WalRecordBufFreeEntry *getHead;
    WalRecordBufFreeEntry allocatedHead[REDO_FREELIST_CAPACITY];
    uint32 curEntryNum;
}ALIGNED(DSTORE_CACHELINE_SIZE);

struct WalBatchRecoveryPara {
    uint32 batchNum;
    uint32 redoPos;
    PageId pageId;
    uint32 entryCapacity;
    RedoWalRecordEntry *recordEntry;
    uint64 prevFileVersion;
    uint64 waitStartPlsn;

    inline void Init()
    {
        batchNum = 0;
        redoPos = 0;
        pageId = INVALID_PAGE_ID;
        entryCapacity = MAX_BATCH_RECORD_NUM;
        recordEntry = nullptr;
        prevFileVersion = INVALID_FILE_VERSION;
        waitStartPlsn = UINT64_MAX;
    }

    inline void AddWalRecord(RedoWalRecordEntry *newRecordEntry, const PageId newPageId = INVALID_PAGE_ID,
                             const uint64 newPrevFileVersion = INVALID_FILE_VERSION)
    {
        pageId = newPageId;
        prevFileVersion = newPrevFileVersion;
        StorageAssert(recordEntry != nullptr);
        recordEntry[batchNum] = *newRecordEntry;
        batchNum++;
    }
};

class ParallelRedoWorker : public BaseObject {
public:
    explicit ParallelRedoWorker(const ParallelRedoWorkerInitParam &initParam);
    virtual ~ParallelRedoWorker() noexcept;
    DISALLOW_COPY_AND_MOVE(ParallelRedoWorker)

    /*
     * Initialize the worker queue
     *
     * @return: init results
     */
    RetStatus Init();

    /*
     * Insert a RedoWalRecordEntry into the playback queue
     *
     * @return: append results
     */
    void AppendWalRecord(RedoWalRecordEntry *record, uint32 num, const char *functionName, int lineNumber);

    /*
     * Start running worker threads and consuming queue
     *
     */
    void Run(const char* workerName);

    void WorkerMain(const char* workerName);

    virtual void ProcNoRedoWalRecordComing(UNUSE_PARAM BuffForDecompress *buffInfoForDecompress,
                                           UNUSE_PARAM uint64 waitCount) {}

    virtual void ProcAllGetRedoWalRecordEntrys(UNUSE_PARAM RedoWalRecordEntry *recordEntry, UNUSE_PARAM uint32 getNum,
        UNUSE_PARAM BuffForDecompress *buffInfoForDecompress) {}

    inline void InitWorkerThreadName(const char *threadType, uint16 workerId);
    /*
     * Wait for the playback queue to process all RedoWalRecordEntrys
     *
     */
    void WaitRedoFinish();

    /*
     * Returns whether the queue is empty
     *
     */
    bool IsEmpty() const;

    uint64 GetMaxDispatchedPlsn() const;

    /*
     * Gets the maximum PLSN that has been played back
     *
     */
    uint64 GetMaxRedoFinishedPlsn();

    uint64 GetMaxRecoveryPlsn();

    inline WalRecordBufFreeEntry *AllocRedoWalRecord(uint32 recordSize)
    {
        return m_freeList->AllocRedoWalRecord(recordSize);
    }

    virtual void UpdateMaxRedoPlsn(uint64 plsn);

    void UpdateMaxRedoSyncerPlsn(uint64 plsn);

    void UpdateMaxRecoveryPlsn(uint64 plsn);

    uint32 GetFreeSpace();

    virtual bool CheckWaitListIsEmpty();

    bool IsRedoQueueEmptyOrOnlyNullRecord();

protected:
    uint16 m_workerId;
    PdbId m_pdbId;
    WalId m_walId;
    uint64 m_maxDispatchedPlsn;
    uint64 m_maxPlsnSyncer;
    /* The m_maxRecoveryPlsn used for inserting dirty page queues of standby PDB. */
    uint64 m_maxRecoveryPlsn;
    gs_atomic_uint64 m_maxRedoPlsn;
    uint8 m_ctrlFlag;
    bool m_isRunningFlag;

    uint32 m_capacity;
    SleepNotify m_func;
    BlockSpscQueue *m_redoQueue;

    DstoreMemoryContext m_memoryContext;
    std::thread *m_workerThrd;
    std::atomic_bool m_canStopFlag;
    std::atomic_bool m_finishFlag;
    WalRecordBufFreeEntryList *m_freeList;
    WalRecovery *m_walRecovery;
};

class ParallelPageRedoWorker : public ParallelRedoWorker {
public:
    explicit ParallelPageRedoWorker(const ParallelRedoWorkerInitParam &initParam) : ParallelRedoWorker(initParam) {}
    ~ParallelPageRedoWorker() noexcept final {}
    DISALLOW_COPY_AND_MOVE(ParallelPageRedoWorker)

private:
    void ProcNoRedoWalRecordComing(UNUSE_PARAM BuffForDecompress *buffInfoForDecompress, UNUSE_PARAM uint64 waitCount);
    void ClearBatchRecoveryPara(WalBatchRecoveryPara *para);
    void ProcAllGetRedoWalRecordEntrys(
        RedoWalRecordEntry *recordEntry, uint32 getNum, BuffForDecompress *buffInfoForDecompress);
};

enum class DDLRedoType {
    DDLREDO_TABLESAPCE = 0,
    DDLREDO_SYSTABLE,
    DDLREDO_MAX
};

class ParallelDDLRedoWorker : public ParallelRedoWorker {
public:
    explicit ParallelDDLRedoWorker(const ParallelRedoWorkerInitParam &initParam)
        : ParallelRedoWorker(initParam), m_tbsRedoWaitingList{nullptr}, m_systableRedo(nullptr),
          m_ddlRedoType(DDLRedoType::DDLREDO_TABLESAPCE) {}
    ~ParallelDDLRedoWorker() noexcept final;
    DISALLOW_COPY_AND_MOVE(ParallelDDLRedoWorker)
    void UpdateMaxRedoPlsn(uint64 plsn) override;
    bool CheckWaitListIsEmpty() override;

private:
    void ProcNoRedoWalRecordComing(BuffForDecompress *buffInfoForDecompress, uint64 waitCount);
    void ClearBatchRecoveryPara(TablespaceId tbsId);
    RetStatus ExpandBatchRecoveryPara(TablespaceId tbsId);
    void BatchProc(const TablespaceId tbsId, BuffForDecompress *buffInfoForDecompress, uint64 maxrecoveryPlsn);
    void ProcAllGetRedoWalRecordEntrys(
        RedoWalRecordEntry *recordEntry, uint32 getNum, BuffForDecompress *buffInfoForDecompress);

    WalBatchRecoveryPara *m_tbsRedoWaitingList[MAX_TABLESPACE_ID + 1];
    WalBatchRecoveryPara *m_systableRedo;
    DDLRedoType m_ddlRedoType;
};

}
#endif