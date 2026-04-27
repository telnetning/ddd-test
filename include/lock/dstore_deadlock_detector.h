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
 */

#ifndef DSTORE_DEADLOCK_DETECTOR_H
#define DSTORE_DEADLOCK_DETECTOR_H

#include "lock/dstore_lock_wait_for_graph.h"

namespace DSTORE {

uint64 GetDeadLockDetectThreadCheckInterval();
#define DEADLOCK_DETECT_THREAD_CHECK_INTERVAL_US GetDeadLockDetectThreadCheckInterval()

#ifndef UT
constexpr uint64 DEADLOCK_DETECT_GLOBAL_CHECK_INTERVAL_US = 3 * 1000 * 1000;
constexpr uint64 DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US = 2 * 1000 * 1000;
#else
constexpr uint64 DEADLOCK_DETECT_GLOBAL_CHECK_INTERVAL_US = 1 * 1000 * 1000;
constexpr uint64 DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US = 800 * 1000;
#endif

constexpr uint32 DEADLOCK_DETECT_PRINT_WFG_MAX_VERTEX_NUM = 50;

class DeadlockThrdState : public BaseObject {
public:
    DeadlockThrdState() = default;
    ~DeadlockThrdState() = default;
    DISALLOW_COPY_AND_MOVE(DeadlockThrdState);

    void Initialize();
    void Destroy();

    bool IsDeadlock();

    void StartWaiting();
    void EndWaiting();

    uint64 GetWaitingStartTime(uint64 *transStartTime);

    RetStatus SetDeadlock(char *cycleInfo, const uint64 startWaitTime, const int32 cycleLength);
    char *CopyDeadlockReport(int *cycleLength);
    void ClearDeadlockReport();

private:
    /**
     * The start time of lock waiting. For deadlock detection, we only detect waiter who waited for a long time,
     * so that the num of waiters is reduced.
     */
    uint64 m_startWaitTime;
    /**
     * The start time of current transaction who requesting the lock, to pick the latest one for breaking deadlock.
     * If there is not any trans running, using startWaitTime to pick victim thread.
     */
    uint64 m_waitingTransStartTime;

    /* Current thread's deadlock report, it will be filled if local thrd is the deadlock victim. */
    char *m_deadlockReport;

    /* Whether current thread need to return deadlock error or not. */
    bool m_isDeadlock;
    int32 m_deadlockGraphVertexNum;
    LWLock m_statLock;
};

class LockWaitingThread : public BaseObject {
public:
    LockWaitingThread();
    virtual ~LockWaitingThread() = default;
    DISALLOW_COPY_AND_MOVE(LockWaitingThread);

    void SetThreadInfo(ThreadId threadId, uint32 threadCoreIndex);
    void SetWaitLockInfo(LockMode lockMode, uint64 startWaitTime, uint64 trxStartTime, uint32 waitLockNum);

    virtual bool IsSameRequester(LockWaitingThread *otherThread);
    virtual RetStatus Dump(StringInfo stringInfo);

    ThreadId m_threadId;
    uint32 m_threadCoreIndex;
    LockMode m_waitLockMode;
    uint64 m_startWaitTime;
    uint64 m_trxStartTime;
    uint32 m_waitLockNum;
    LockWaitingThread *m_softBlocker;
    LockWaitingThread *m_next;
};

class LockWaitingMap : public BaseObject {
public:
    LockWaitingMap();
    ~LockWaitingMap();
    DISALLOW_COPY_AND_MOVE(LockWaitingMap);

    RetStatus BuildMap(DstoreMemoryContext ctx);
    void DestroyMap();

    void LockMap();
    void UnlockMap();

    RetStatus AddLockWaitingThread(const LockTag &lockTag, LockWaitingThread *thread);

    uint32 GetTotalWaitingThreadNum();
    uint32 GetTotalWaitingLockNum();

    bool HasConflictWithWaitingThreads(const LockTag &lockTag, LockMask lockMask);

    enum class ReconstructWaitingQueueStrategy {
        ESTIMATE_BASED,
        ENQUEUE_INFO_BASED, /* future optimize? use enqueue info for more accurate estimation */
    };

    struct LockWaitingHashEntry {
        LockTag tag;
        LockMask mask;
        LockWaitingThread waitQueues[DSTORE_LOCK_MODE_MAX];

        void Initialize(const LockTag &lockTag, ReconstructWaitingQueueStrategy strategy =
                        ReconstructWaitingQueueStrategy::ESTIMATE_BASED);
        void AddWaitingThread(LockWaitingThread *thread);
        void CleanupWaitingThread();
        LockWaitingThread *ChooseSoftBlocker(LockMask holdMask);
    };

    class WaitingThreadIterater : public BaseObject {
    public:
        WaitingThreadIterater(const LockTag &lockTag, LockWaitingMap *map);
        ~WaitingThreadIterater() = default;
        DISALLOW_COPY_AND_MOVE(WaitingThreadIterater);

        LockWaitingThread *GetNextThread();
        LockWaitingHashEntry *GetWaitingEntry();
    private:
        LockWaitingHashEntry *m_entry;
        LockWaitingThread *m_currentThread;
        uint32 m_currentQueue;
    };

    class WaitLockIterater : public BaseObject {
    public:
        WaitLockIterater(LockWaitingMap *map);
        ~WaitLockIterater() = default;
        DISALLOW_COPY_AND_MOVE(WaitLockIterater);

        const LockTag *GetNextLockTag();
        LockMask GetWaitMask();
    private:
        LockWaitingMap *m_map;
        HASH_SEQ_STATUS m_status;
        LockWaitingHashEntry *m_entry;
    };

private:
    DstoreMemoryContext m_ctx;
    HTAB *m_lockWaiterMap;
    LWLock m_lockWaiterLock;
    uint32 m_waitingThreadNum;
};

class DeadlockReporter : public BaseObject {
public:
    DeadlockReporter(DstoreMemoryContext ctx);
    ~DeadlockReporter();
    DISALLOW_COPY_AND_MOVE(DeadlockReporter);

    RetStatus Append(const char *fmt, ...);
    RetStatus AppendBytes(const char *bytes, uint32 len);
    char *CopyReport(DstoreMemoryContext ctx);
    void Reset();
    char *GetData();
    uint32 GetDataLen();
    void SetData(char *data, uint32 dataLen);

private:
    DstoreMemoryContext m_ctx;
    StringInfoData m_deadlockReport;
};

class ThreadVertex : public Vertex {
public:
    explicit ThreadVertex(uint32 threadCoreIndex, uint64 waitStartTime, uint64 trxStartTime,
                          bool checkAllEdgeInCycle, int initOutEdgeCnt = 0);
    ~ThreadVertex() = default;
    DISALLOW_COPY_AND_MOVE(ThreadVertex);

    static void BuildVertexTag(VertexTag &tag, ThreadId threadId);

    Vertex *Duplicate(DstoreMemoryContext ctx) override;

    ThreadId GetThreadId();
    uint32 GetThreadCoreIndex();
    uint64 GetWaitStartTime();
    uint64 GetTrxStartTime();

protected:
    uint32 m_threadCoreIndex;
    uint64 m_waitStartTime;
    uint64 m_trxStartTime;
};

class WaitLockEdge : public Edge {
public:
    WaitLockEdge();
    ~WaitLockEdge() = default;
    DISALLOW_COPY_AND_MOVE(WaitLockEdge);

    void SetWaitLockInfo(const LockTag &tag, LockMode waitMode, LockMask blockMask, bool isSoftBlock);

    Edge *Duplicate(DstoreMemoryContext ctx) override;

    RetStatus DumpWaitLock(DeadlockReporter &deadlockReport);
    LockMode GetWaitLockMode();
    RetStatus DumpBlockLockModes(DeadlockReporter &deadlockReport);
    bool IsSoftBlocked();
    LockTag GetWaitLock();
    LockMask GetBlockMask();

protected:
    LockTag m_waitLockTag;
    LockMode m_waitLockMode;
    LockMask m_blockLockMask;
    bool m_isSoftBlock;
};

class DeadlockDetectGlobalTimer : public BaseObject {
public:
    DeadlockDetectGlobalTimer();
    ~DeadlockDetectGlobalTimer() = default;
    DISALLOW_COPY_AND_MOVE(DeadlockDetectGlobalTimer);

    static DeadlockDetectGlobalTimer *GetInstance();
    RetStatus CompeteForNextDetection();
    void SetReadyForNextDetection();

    void UpdateDetectTimestamp();
    uint64 GetDetectTimestamp();

private:
    LWLock m_deadlockDetectLock;
    uint64 m_lastDetectTime;
    static DeadlockDetectGlobalTimer m_self;
};

class DeadlockDetector : public BaseObject {
public:
    DeadlockDetector();
    virtual ~DeadlockDetector() = default;
    DISALLOW_COPY_AND_MOVE(DeadlockDetector);

    RetStatus RunDeadlockDetect();

    static RetStatus GetSQLStatementForThread(ThreadId threadId, char **retString);

protected:
    virtual RetStatus CompeteForDetectionExecutor();
    virtual RetStatus CollectLockWaiters();
    virtual RetStatus CollectLockHoldersAndBuildGraph();
    bool DoesDeadlockExist();
    virtual RetStatus RecheckCycle();
    RetStatus ChooseVictimAndNotify();

    virtual void CleanupData();
    virtual RetStatus AddAllVerticesToGraph();

    virtual LockWaitingThread *CreateWaitingThread();
    RetStatus RecordThreadIfStartWaitingBefore(ThreadCore *core, uint64 startRecordTime);
    virtual RetStatus AddWaitForEdge(LockWaitingThread *waiter, LockWaitingThread *blocker,
        const LockTag &lockTag, LockMask blockMask, bool isSoftEdge);
    RetStatus RecordThreadIfHoldingConflictLocks(ThreadCore *core, uint64 startRecordTime);
    RetStatus AddSoftEdgeIfNotExist(LockWaitingMap::LockWaitingHashEntry *waitEntry, LockWaitingThread *waiter,
        const LockTag &lockTag, LockMask holdMask);
    bool IsThreadStillWaitingBefore(ThreadId threadId, uint32 threadCoreIndex, uint64 startRecordTime);
    Vertex *ChooseOneVictimToBreakCurrentCycle();
    virtual RetStatus NotifyVictim(Vertex *thread, DeadlockReporter &deadlockReport);
    virtual RetStatus GenerateDeadlockReport(DeadlockReporter &deadlockReport);
    virtual RetStatus GetSqlStatementForWaitingThread(ThreadId threadId, uint32 threadCoreIndex,
        DeadlockReporter &deadlockReport);
    RetStatus GetSqlStatementForAnyThread(ThreadId threadId, DeadlockReporter &deadlockReport);
    bool ThreadHasBeenWaitingBefore(ThreadCore *core, uint64 startRecordTime);
    void LogLockWaiters();
    virtual void LogWaitForGraph();

    WaitForGraph m_waitForGraph;
    LockWaitingMap m_lockWaiterMap;
    uint64 m_lastCompeteTime;
};

class SameThreadDeadlockDetector : public BaseObject {
public:
    void PrepareToCheck(const LockTag &tag, LockMode checkMode);
    bool CanLockModeConflict(LockMode mode) const;

private:
    bool HasMoreLocksInTransactionList(LockMode mode) const;

    uint32 m_grantedCnt[DSTORE_LOCK_MODE_MAX];
    LockMode m_checkMode;
};
}

namespace LockInterface {

extern char *GetThreadSqlStatementCallback(pthread_t tid, DSTORE::AllocMemFunc allocFunc);

}
#endif
