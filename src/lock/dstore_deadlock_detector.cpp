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

#include "lock/dstore_deadlock_detector.h"
#include "lock/dstore_lock_entry.h"
#include "lock/dstore_lock_thrd_local.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_transaction.h"

namespace DSTORE {

void DeadlockThrdState::Initialize()
{
    m_startWaitTime = UINT64_MAX;
    m_waitingTransStartTime = 0;
    m_deadlockReport = nullptr;
    m_isDeadlock = false;
    LWLockInitialize(&m_statLock, LWLOCK_GROUP_DEADLOCK_THRD_STAT);
}

void DeadlockThrdState::Destroy()
{
    DstoreLWLockAcquire(&m_statLock, LW_EXCLUSIVE);
    DstorePfreeExt(m_deadlockReport);
    m_deadlockGraphVertexNum = 0;
    LWLockRelease(&m_statLock);
}

void DeadlockThrdState::StartWaiting()
{
    DstoreLWLockAcquire(&m_statLock, LW_EXCLUSIVE);
    /*
     * Here we record the start time of waiting, for deadlock detect filtering.
     * We also need to record transaction start time for breaking deadlock.
     */
    m_startWaitTime = GetSystemTimeInMicrosecond();
    const Transaction *activeTrans = thrd->GetActiveTransaction();
    if (activeTrans != nullptr && activeTrans->IsTransactionRunning()) {
        m_waitingTransStartTime = activeTrans->GetTransStartTime();
    } else {
        m_waitingTransStartTime = m_startWaitTime;
    }

    m_isDeadlock = false;
    DstorePfreeExt(m_deadlockReport);
    LWLockRelease(&m_statLock);
}

void DeadlockThrdState::EndWaiting()
{
    DstoreLWLockAcquire(&m_statLock, LW_EXCLUSIVE);
    m_startWaitTime = UINT64_MAX;
    m_waitingTransStartTime = 0;
    m_isDeadlock = false;
    LWLockRelease(&m_statLock);
}

uint64 DeadlockThrdState::GetWaitingStartTime(uint64 *transStartTime)
{
    uint64 startWaitTime;
    DstoreLWLockAcquire(&m_statLock, LW_SHARED);
    if (transStartTime != nullptr) {
        *transStartTime = m_waitingTransStartTime;
    }
    startWaitTime = m_startWaitTime;
    LWLockRelease(&m_statLock);
    return startWaitTime;
}

bool DeadlockThrdState::IsDeadlock()
{
    bool ret;
    DstoreLWLockAcquire(&m_statLock, LW_SHARED);
    ret = m_isDeadlock;
    LWLockRelease(&m_statLock);
    return ret;
}

RetStatus DeadlockThrdState::SetDeadlock(char *deadlockReport, const uint64 startWaitTime, const int32 vertexNum)
{
    DstoreLWLockAcquire(&m_statLock, LW_EXCLUSIVE);
    if (unlikely(m_startWaitTime != startWaitTime)) {
        /*
         * Start wait time has changed, current thread is no longer waiting for previous lock.
         * We will ignore the deadlock notification.
         */
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
               ErrMsg("Deadlock Detection: "
                      "Found inconsistent start waiting time when notifying victim, thread:%lu-input:%lu.",
                      m_startWaitTime, startWaitTime));
        LWLockRelease(&m_statLock);
        storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
        return DSTORE_FAIL;
    }

    DstorePfreeExt(m_deadlockReport);
    m_isDeadlock = true;
    m_deadlockReport = deadlockReport;
    m_deadlockGraphVertexNum = vertexNum;
    LWLockRelease(&m_statLock);
    return DSTORE_SUCC;
}

/*
 * Returns an error message with details about the most recent deadlock. Even if
 * multiple threads detect the deadlock, only one designated thread (the victim)
 * reports a error message i.e., the function returns empty string when called from other threads.
 * victim selection is handled internally by deadlock detector.
 */
char *DeadlockThrdState::CopyDeadlockReport(int *vertexNum)
{
    char *out = nullptr;

    DstoreLWLockAcquire(&m_statLock, LW_SHARED);
    if (m_deadlockReport != nullptr) {
        size_t len = strlen(m_deadlockReport);
        out = static_cast<char *>(DstorePalloc(len + 1));
        if (!STORAGE_VAR_NULL(out)) {
            errno_t rc = strcpy_s(out, len + 1, m_deadlockReport);
            storage_securec_check(rc, "", "");
        }
        if (!STORAGE_VAR_NULL(vertexNum)) {
            *vertexNum = m_deadlockGraphVertexNum;
        }
    }

    LWLockRelease(&m_statLock);
    return out;
}

void DeadlockThrdState::ClearDeadlockReport()
{
    DstoreLWLockAcquire(&m_statLock, LW_EXCLUSIVE);
    DstorePfreeExt(m_deadlockReport);
    LWLockRelease(&m_statLock);
}

LockWaitingThread::LockWaitingThread()
    : m_threadId(0),
      m_threadCoreIndex(0),
      m_waitLockMode(DSTORE_NO_LOCK),
      m_startWaitTime(0),
      m_trxStartTime(0),
      m_waitLockNum(0),
      m_softBlocker(nullptr),
      m_next(nullptr)
{
}

void LockWaitingThread::SetThreadInfo(ThreadId threadId, uint32 threadCoreIndex)
{
    m_threadId = threadId;
    m_threadCoreIndex = threadCoreIndex;
}

void LockWaitingThread::SetWaitLockInfo(LockMode lockMode, uint64 startWaitTime,
                                        uint64 trxStartTime, uint32 waitLockNum)
{
    m_waitLockMode = lockMode;
    m_startWaitTime = startWaitTime;
    m_trxStartTime = trxStartTime;
    m_waitLockNum = waitLockNum;
}

bool LockWaitingThread::IsSameRequester(LockWaitingThread *otherThread)
{
    return (m_threadId == otherThread->m_threadId);
}

RetStatus LockWaitingThread::Dump(StringInfo stringInfo)
{
    if (STORAGE_FUNC_FAIL(stringInfo->append("Thread %lu, wait lock mode %s, start wait time %lu, "
                       "transaction start time %lu, wait lock number %u.",
                       m_threadId, GetLockModeString(m_waitLockMode), m_startWaitTime,
                       m_trxStartTime, m_waitLockNum))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Append failed when LockWaitingThread::Dump."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

LockWaitingMap::LockWaitingMap()
    : m_ctx(nullptr),
      m_lockWaiterMap(nullptr),
      m_waitingThreadNum(0)
{
    LWLockInitialize(&m_lockWaiterLock, LWLOCK_GROUP_DEADLOCK_DETECT);
}

LockWaitingMap::~LockWaitingMap()
{
    m_ctx = nullptr;
    m_lockWaiterMap = nullptr;
}

void LockWaitingMap::LockWaitingHashEntry::Initialize(const LockTag &lockTag, ReconstructWaitingQueueStrategy strategy)
{
    StorageAssert(strategy == ReconstructWaitingQueueStrategy::ESTIMATE_BASED);
    UNUSED_VARIABLE(strategy);
    tag = lockTag;
    mask = 0;
    for (uint8 i = 0; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); ++i) {
        waitQueues[i].SetThreadInfo(0, 0);
        waitQueues[i].SetWaitLockInfo(DSTORE_NO_LOCK, 0, 0, 0);
        waitQueues[i].m_softBlocker = nullptr;
        waitQueues[i].m_next = nullptr;
    }
}

void LockWaitingMap::LockWaitingHashEntry::AddWaitingThread(LockWaitingThread *thread)
{
    mask |= GetLockMask(thread->m_waitLockMode);
    thread->m_next = waitQueues[thread->m_waitLockMode].m_next;
    waitQueues[thread->m_waitLockMode].m_next = thread;
    return;
}

LockWaitingThread *LockWaitingMap::LockWaitingHashEntry::ChooseSoftBlocker(LockMask holdMask)
{
    LockWaitingThread *blocker = nullptr;
    for (uint8 i = static_cast<uint8>(DSTORE_ACCESS_EXCLUSIVE_LOCK); i > 0; --i) {
        if (waitQueues[i].m_next == nullptr) {
            continue;
        }
        if (HasConflictWithMask(static_cast<LockMode>(i), holdMask)) {
            blocker = waitQueues[i].m_next;
            break;
        }
    }
    return blocker;
}

void LockWaitingMap::LockWaitingHashEntry::CleanupWaitingThread()
{
    LockWaitingThread *thread = nullptr;
    LockWaitingThread *next = nullptr;
    for (uint8 i = 0; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); ++i) {
        for (thread = waitQueues[i].m_next; thread != nullptr; thread = next) {
            next = thread->m_next;
            delete thread;
        }
    }
}

RetStatus LockWaitingMap::BuildMap(DstoreMemoryContext ctx)
{
    HASHCTL info;
    info.keysize = sizeof(LockTag);
    info.entrysize = sizeof(LockWaitingHashEntry);
    info.hash = tag_hash;
    info.hcxt = ctx;
    uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION);
    if (ctx->type == MemoryContextType::SHARED_CONTEXT) {
        hashFlags |= HASH_SHRCTX;
    } else {
        hashFlags |= HASH_CONTEXT;
    }

    m_lockWaiterMap = hash_create("deadlock lock-waiter hash", g_storageInstance->GetGuc()->ncores,
                                  &info, static_cast<int>(hashFlags));
    if (STORAGE_VAR_NULL(m_lockWaiterMap)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
               ErrMsg("Deadlock Detection: Failed to allocate memory for creating a hashmap."));
        return DSTORE_FAIL;
    }
    m_ctx = ctx;
    return DSTORE_SUCC;
}

void LockWaitingMap::DestroyMap()
{
    if (m_lockWaiterMap != nullptr) {
        HASH_SEQ_STATUS hashStatus;
        LockWaitingHashEntry *waitingEntry = nullptr;
        hash_seq_init(&hashStatus, m_lockWaiterMap);
        while ((waitingEntry = static_cast<LockWaitingHashEntry *>(hash_seq_search(&hashStatus))) != nullptr) {
            waitingEntry->CleanupWaitingThread();
        }
        hash_destroy(m_lockWaiterMap);
        m_lockWaiterMap = nullptr;
        m_waitingThreadNum = 0;
    }
}

RetStatus LockWaitingMap::AddLockWaitingThread(const LockTag &lockTag, LockWaitingThread *thread)
{
    bool found = false;
    LockWaitingHashEntry *entry = static_cast<LockWaitingHashEntry *>(hash_search(m_lockWaiterMap,
        static_cast<const void *>(&lockTag), HASH_ENTER, &found));
    if (STORAGE_VAR_NULL(entry)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
               ErrMsg("Deadlock Detection: Failed to allocate memory for entering an entry into the hashmap."));
        return DSTORE_FAIL;
    }

    if (!found) {
        entry->Initialize(lockTag);
    }

    entry->AddWaitingThread(thread);
    m_waitingThreadNum++;
    return DSTORE_SUCC;
}

uint32 LockWaitingMap::GetTotalWaitingThreadNum()
{
    return m_waitingThreadNum;
}

uint32 LockWaitingMap::GetTotalWaitingLockNum()
{
    return static_cast<uint32>(hash_get_num_entries(m_lockWaiterMap));
}

bool LockWaitingMap::HasConflictWithWaitingThreads(const LockTag &lockTag, LockMask lockMask)
{
    bool found = false;
    LockWaitingHashEntry *entry = static_cast<LockWaitingHashEntry *>(hash_search(m_lockWaiterMap,
        static_cast<const void *>(&lockTag), HASH_FIND, &found));
    if (entry == nullptr) {
        return false;
    }

    if (!HasConflictWithMask(entry->mask, lockMask)) {
        return false;
    }
    return true;
}

void LockWaitingMap::LockMap()
{
    DstoreLWLockAcquire(&m_lockWaiterLock, LW_EXCLUSIVE);
}

void LockWaitingMap::UnlockMap()
{
    LWLockRelease(&m_lockWaiterLock);
}

LockWaitingMap::WaitingThreadIterater::WaitingThreadIterater(const LockTag &lockTag, LockWaitingMap *map)
    : m_entry(nullptr),
      m_currentThread(nullptr),
      m_currentQueue(0)
{
    bool found = false;
    m_entry = static_cast<LockWaitingHashEntry *>(hash_search(map->m_lockWaiterMap,
        static_cast<const void *>(&lockTag), HASH_FIND, &found));
    if (m_entry != nullptr) {
        while ((m_currentThread == nullptr) && (m_currentQueue < (DSTORE_LOCK_MODE_MAX - 1))) {
            m_currentQueue++;
            m_currentThread = m_entry->waitQueues[m_currentQueue].m_next;
        }
    }
}

LockWaitingThread *LockWaitingMap::WaitingThreadIterater::GetNextThread()
{
    LockWaitingThread *ret = m_currentThread;
    if (m_currentThread != nullptr) {
        m_currentThread = m_currentThread->m_next;
        while ((m_currentThread == nullptr) && (m_currentQueue < (DSTORE_LOCK_MODE_MAX - 1))) {
            m_currentQueue++;
            m_currentThread = m_entry->waitQueues[m_currentQueue].m_next;
        }
    }
    return ret;
}

LockWaitingMap::LockWaitingHashEntry *LockWaitingMap::WaitingThreadIterater::GetWaitingEntry()
{
    return m_entry;
}

LockWaitingMap::WaitLockIterater::WaitLockIterater(LockWaitingMap *map)
    : m_map(map),
      m_entry(nullptr)
{
    hash_seq_init(&m_status, m_map->m_lockWaiterMap);
}

const LockTag *LockWaitingMap::WaitLockIterater::GetNextLockTag()
{
    m_entry = static_cast<LockWaitingHashEntry *>(hash_seq_search(&m_status));
    if (STORAGE_VAR_NULL(m_entry)) {
        return nullptr;
    }
    return &(m_entry->tag);
}

LockMask LockWaitingMap::WaitLockIterater::GetWaitMask()
{
    if (STORAGE_VAR_NULL(m_entry)) {
        return 0;
    }
    return (m_entry->mask);
}

DeadlockDetectGlobalTimer::DeadlockDetectGlobalTimer()
    : m_lastDetectTime(0)
{
    LWLockInitialize(&m_deadlockDetectLock, LWLOCK_GROUP_DEADLOCK_DETECT);
}

DeadlockDetectGlobalTimer DeadlockDetectGlobalTimer::m_self;

DeadlockDetectGlobalTimer *DeadlockDetectGlobalTimer::GetInstance()
{
    return &m_self;
}

RetStatus DeadlockDetectGlobalTimer::CompeteForNextDetection()
{
    uint64 currentTime = GetSystemTimeInMicrosecond();
    bool locked = DstoreLWLockConditionalAcquire(&m_deadlockDetectLock, LW_EXCLUSIVE);
    if (!locked) {
        return DSTORE_FAIL;
    }

    RetStatus ret;
    if ((currentTime - m_lastDetectTime) < DEADLOCK_DETECT_GLOBAL_CHECK_INTERVAL_US) {
        ret = DSTORE_FAIL;
        LWLockRelease(&m_deadlockDetectLock);
    } else {
        m_lastDetectTime = currentTime;
        ret = DSTORE_SUCC;
    }

    return ret;
}

void DeadlockDetectGlobalTimer::SetReadyForNextDetection()
{
    LWLockRelease(&m_deadlockDetectLock);
}

void DeadlockDetectGlobalTimer::UpdateDetectTimestamp()
{
    DstoreLWLockAcquire(&m_deadlockDetectLock, LW_EXCLUSIVE);
    m_lastDetectTime = GetSystemTimeInMicrosecond();
    LWLockRelease(&m_deadlockDetectLock);
}

uint64 DeadlockDetectGlobalTimer::GetDetectTimestamp()
{
    return m_lastDetectTime;
}


ThreadVertex::ThreadVertex(uint32 threadCoreIndex, uint64 waitStartTime, uint64 trxStartTime,
    bool checkAllEdgeInCycle, int initOutEdgeCnt)
    : Vertex(checkAllEdgeInCycle, initOutEdgeCnt),
      m_threadCoreIndex(threadCoreIndex),
      m_waitStartTime(waitStartTime),
      m_trxStartTime(trxStartTime)
{
}

void ThreadVertex::BuildVertexTag(VertexTag &tag, ThreadId threadId)
{
    tag.field1 = threadId;
    tag.field2 = 0;
    tag.field3 = 0;
    tag.field4 = 0;
    tag.field5 = 0;
}

Vertex *ThreadVertex::Duplicate(DstoreMemoryContext ctx)
{
    ThreadVertex *vertex = DstoreNew(ctx) ThreadVertex(m_threadCoreIndex, m_waitStartTime,
        m_trxStartTime, m_checkAllEdgeInCycle, m_initOutEdgeCnt);
    if (STORAGE_VAR_NULL(vertex)) {
        return nullptr;
    }

    StorageAssert(m_inEdgeCnt == 0);
    StorageAssert(m_outEdgeCnt == 0);
    return vertex;
}

ThreadId ThreadVertex::GetThreadId()
{
    return GetVertexTag()->field1;
}

uint32 ThreadVertex::GetThreadCoreIndex()
{
    return m_threadCoreIndex;
}

uint64 ThreadVertex::GetWaitStartTime()
{
    return m_waitStartTime;
}

uint64 ThreadVertex::GetTrxStartTime()
{
    return m_trxStartTime;
}

WaitLockEdge::WaitLockEdge()
    : Edge(),
      m_waitLockTag(),
      m_waitLockMode(DSTORE_NO_LOCK),
      m_blockLockMask(0),
      m_isSoftBlock(false)
{
}

void WaitLockEdge::SetWaitLockInfo(const LockTag &tag, LockMode waitMode, LockMask blockMask, bool isSoftBlock)
{
    m_waitLockTag = tag;
    m_waitLockMode = waitMode;
    m_blockLockMask = blockMask;
    m_isSoftBlock = isSoftBlock;
}

Edge *WaitLockEdge::Duplicate(DstoreMemoryContext ctx)
{
    WaitLockEdge *edge = DstoreNew(ctx) WaitLockEdge();
    if (STORAGE_VAR_NULL(edge)) {
        return nullptr;
    }
    StorageAssert(m_reverseEdge == nullptr);
    edge->SetWaitLockInfo(m_waitLockTag, m_waitLockMode, m_blockLockMask, m_isSoftBlock);
    return edge;
}

RetStatus WaitLockEdge::DumpWaitLock(DeadlockReporter &deadlockReport)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData str;
    if (unlikely(!str.init())) {
        return DSTORE_FAIL;
    }
    m_waitLockTag.DescribeLockTag(&str);

    RetStatus ret = deadlockReport.Append("%s", str.data);
    DstorePfree(str.data);
    return ret;
}

LockMode WaitLockEdge::GetWaitLockMode()
{
    return m_waitLockMode;
}

RetStatus WaitLockEdge::DumpBlockLockModes(DeadlockReporter &deadlockReport)
{
    RetStatus ret = DSTORE_SUCC;
    bool needSeparator = false;
    for (uint8 i = 0; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); ++i) {
        if ((GetLockMask(static_cast<LockMode>(i)) & m_blockLockMask) != 0) {
            if (needSeparator) {
                ret = deadlockReport.Append(" ");
                if (STORAGE_FUNC_FAIL(ret)) {
                    break;
                }
            }
            ret = deadlockReport.Append("%s", GetLockModeString(static_cast<LockMode>(i)));
            if (STORAGE_FUNC_FAIL(ret)) {
                break;
            }
            needSeparator = true;
        }
    }

    return ret;
}

bool WaitLockEdge::IsSoftBlocked()
{
    return m_isSoftBlock;
}

LockTag WaitLockEdge::GetWaitLock()
{
    return m_waitLockTag;
}

LockMask WaitLockEdge::GetBlockMask()
{
    return m_blockLockMask;
}

DeadlockReporter::DeadlockReporter(DstoreMemoryContext ctx)
    : m_ctx(ctx)
{
    m_deadlockReport.data = nullptr;
    m_deadlockReport.max_len = 0;
}

DeadlockReporter::~DeadlockReporter()
{
    DstorePfreeExt(m_deadlockReport.data);
}

RetStatus DeadlockReporter::Append(const char *fmt, ...)
{
    if (STORAGE_VAR_NULL(m_deadlockReport.data)) {
        AutoMemCxtSwitch autoSwitch{m_ctx};
        if (unlikely(!m_deadlockReport.init())) {
            return DSTORE_FAIL;
        }
    }

    for (;;) {
        va_list args;
        bool success = false;

        /* Try to format the data. */
        va_start(args, fmt);
        success = m_deadlockReport.append_VA(fmt, args);
        va_end(args);

        if (success) {
            break;
        }

        /* Double the buffer size and try again. */
        if (STORAGE_FUNC_FAIL(m_deadlockReport.enlarge(m_deadlockReport.max_len))) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus DeadlockReporter::AppendBytes(const char *bytes, uint32 len)
{
    if (len == 0) {
        return DSTORE_SUCC;
    }

    if (STORAGE_VAR_NULL(m_deadlockReport.data)) {
        AutoMemCxtSwitch autoSwitch{m_ctx};
        if (unlikely(!m_deadlockReport.init())) {
            return DSTORE_FAIL;
        }
    }

    if (STORAGE_FUNC_FAIL(m_deadlockReport.enlarge(m_deadlockReport.max_len))) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_deadlockReport.append_binary(bytes, len))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

char *DeadlockReporter::CopyReport(DstoreMemoryContext ctx)
{
    if (STORAGE_VAR_NULL(m_deadlockReport.data)) {
        return nullptr;
    }

    char *copy = static_cast<char *>(DstoreMemoryContextAlloc(ctx, m_deadlockReport.len + 1));
    if (STORAGE_VAR_NULL(copy)) {
        return nullptr;
    }

    errno_t rc = strncpy_s(copy, m_deadlockReport.len + 1, m_deadlockReport.data, m_deadlockReport.len);
    storage_securec_check(rc, "", "");
    return copy;
}

void DeadlockReporter::Reset()
{
    m_deadlockReport.len = 0;
    m_deadlockReport.cursor = 0;
}

char *DeadlockReporter::GetData()
{
    return m_deadlockReport.data;
}

uint32 DeadlockReporter::GetDataLen()
{
    if (STORAGE_VAR_NULL(m_deadlockReport.data)) {
        return 0;
    }
    return m_deadlockReport.len;
}

void DeadlockReporter::SetData(char *data, uint32 dataLen)
{
    m_deadlockReport.data = data;
    m_deadlockReport.len = static_cast<int>(dataLen);
    m_deadlockReport.max_len = static_cast<int>(dataLen);
    m_deadlockReport.cursor = 0;
}

DeadlockDetector::DeadlockDetector()
    : m_waitForGraph(),
      m_lockWaiterMap(),
      m_lastCompeteTime(GetSystemTimeInMicrosecond())
{
}

void DeadlockDetector::LogLockWaiters()
{
    ErrLog(DSTORE_LOG, MODULE_LOCK,
        ErrMsg("Deadlock detector collect lock waiters done, waiter num %u lock num %u.",
            m_lockWaiterMap.GetTotalWaitingThreadNum(), m_lockWaiterMap.GetTotalWaitingLockNum()));
#ifdef UT
    LockWaitingMap::WaitLockIterater lockIter(&m_lockWaiterMap);
    const LockTag *nextLockTag = nullptr;
    while ((nextLockTag = lockIter.GetNextLockTag()) != nullptr) {
        LockWaitingMap::WaitingThreadIterater threadIter(*nextLockTag, &m_lockWaiterMap);
        LockWaitingThread *thread = nullptr;
        while ((thread = threadIter.GetNextThread()) != nullptr) {
            ErrLog(DSTORE_LOG, MODULE_LOCK,
                ErrMsg("Thread %lu with core index %u is waiting for %s in %s mode.",
                    thread->m_threadId, thread->m_threadCoreIndex, nextLockTag->ToString().CString(),
                    GetLockModeString(thread->m_waitLockMode)));
        }
    }
#endif
}

void DeadlockDetector::LogWaitForGraph()
{
    ErrLog(DSTORE_LOG, MODULE_LOCK,
        ErrMsg("Deadlock detector collect lock holders done, wait for graph vertices num %u.",
            m_waitForGraph.GetVertexNumber()));

    if (m_waitForGraph.GetVertexNumber() > DEADLOCK_DETECT_PRINT_WFG_MAX_VERTEX_NUM) {
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Vertex number too large, skip printing."));
        return;
    }

    Edge *edge = nullptr;
    Vertex *vertex = nullptr;
    WaitForGraph::CycleIterator iter(&m_waitForGraph);
    while ((vertex = iter.GetNextVertex()) != nullptr) {
        ThreadVertex *thread = dynamic_cast<ThreadVertex *>(vertex);
        Vertex::EdgeIterator edgeIter(vertex, false);
        while ((edge = edgeIter.GetNextEdge()) != nullptr) {
            WaitLockEdge *waitLock = dynamic_cast<WaitLockEdge *>(edge);
            ThreadVertex *blockThread = dynamic_cast<ThreadVertex *>(waitLock->GetPeerVertex());
            ErrLog(DSTORE_LOG, MODULE_LOCK,
                ErrMsg("Thread %lu %u is waiting on %s in %s mode, %s by thread %lu %u in mask 0x%x.",
                    thread->GetThreadId(), thread->GetThreadCoreIndex(), waitLock->GetWaitLock().ToString().CString(),
                    GetLockModeString(waitLock->GetWaitLockMode()),
                    waitLock->IsSoftBlocked() ? "soft blocked" : "blocked",
                    blockThread->GetThreadId(), blockThread->GetThreadCoreIndex(), waitLock->GetBlockMask()));
        }
    }
}

RetStatus DeadlockDetector::RunDeadlockDetect()
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    RetStatus ret;
    bool exist;
    ret = CompeteForDetectionExecutor();
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    ret = CollectLockWaiters();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto EXIT;
    }

    LogLockWaiters();
    ret = CollectLockHoldersAndBuildGraph();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto EXIT;
    }

    LogWaitForGraph();
    m_waitForGraph.ScanVerticesNotInCycleAndPushIntoDeleteQueue();
    m_waitForGraph.DeleteAllVerticesNotInCycle();
    exist = DoesDeadlockExist();
    if (!exist) {
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Deadlock detector completed, no deadlock found."));
        goto EXIT;
    }

    ret = RecheckCycle();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto EXIT;
    }

    exist = DoesDeadlockExist();
    if (!exist) {
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Deadlock detector completed, no deadlock found."));
        goto EXIT;
    }

    ret = ChooseVictimAndNotify();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto EXIT;
    }

EXIT:
    CleanupData();
    return ret;
}

void DeadlockDetector::CleanupData()
{
    DeadlockDetectGlobalTimer::GetInstance()->SetReadyForNextDetection();
    m_waitForGraph.DestroyWaitForGraph();
    m_lockWaiterMap.DestroyMap();
}

RetStatus DeadlockDetector::CompeteForDetectionExecutor()
{
    /* Step 1: Skip deadlock detect if current thread is checking too often. */
    uint64 currentTime = GetSystemTimeInMicrosecond();
    if ((currentTime - m_lastCompeteTime) < DEADLOCK_DETECT_THREAD_CHECK_INTERVAL_US) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Only %lu microseconds since last deadlock detection contention, skipping this round.",
                GetSystemTimeInMicrosecond() - m_lastCompeteTime));
        return DSTORE_FAIL;
    }
    m_lastCompeteTime = currentTime;

    /* Step 2: If lazy lock is on, make sure deadlock detector knows as well. */
    if (g_storageInstance->GetGuc()->enableLazyLock) {
        thrd->GetLockCtx()->GetLocalLock()->BackupWaitingLock();
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
        thrd->GetLockCtx()->GetLocalLock()->RestoreWaitingLock();
    }

    /* Step 3: See if current thread is allowed to run global deadlock detector. */
    RetStatus ret = DeadlockDetectGlobalTimer::GetInstance()->CompeteForNextDetection();
    if (STORAGE_FUNC_SUCC(ret)) {
        uint64 transStartTime;
        DeadlockThrdState *thrdDeadlockState = thrd->GetLockCtx()->GetDeadlockState();
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("The current thread has waited for %lu microseconds, starting deadlock detection.",
                currentTime - thrdDeadlockState->GetWaitingStartTime(&transStartTime)));
    }
    return ret;
}

RetStatus DeadlockDetector::CollectLockWaiters()
{
    /* Step 1: Build lock waiter map. */
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    RetStatus ret = m_lockWaiterMap.BuildMap(ctx);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    /* Step 2: Record all threads that are currently waiting for at least DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US. */
    uint64 startRecordTime = DeadlockDetectGlobalTimer::GetInstance()->GetDetectTimestamp() -
                             DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US;
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (STORAGE_FUNC_FAIL(RecordThreadIfStartWaitingBefore(core, startRecordTime))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::AddAllVerticesToGraph()
{
    LockWaitingMap::WaitLockIterater lockIter(&m_lockWaiterMap);
    const LockTag *nextLockTag = nullptr;
    while ((nextLockTag = lockIter.GetNextLockTag()) != nullptr) {
        LockWaitingMap::WaitingThreadIterater threadIter(*nextLockTag, &m_lockWaiterMap);
        LockWaitingThread *thread = nullptr;
        while ((thread = threadIter.GetNextThread()) != nullptr) {
            VertexTag tag;
            ThreadVertex::BuildVertexTag(tag, thread->m_threadId);
            ThreadVertex threadVertex(thread->m_threadCoreIndex, thread->m_startWaitTime, thread->m_trxStartTime,
                                      thread->m_waitLockNum > 1 ? true : false, thread->m_waitLockNum);
            if (STORAGE_FUNC_FAIL(m_waitForGraph.AddVertex(tag, &threadVertex))) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::CollectLockHoldersAndBuildGraph()
{
    /* Step 1: Build empty wait for graph. */
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    RetStatus ret = m_waitForGraph.BuildWaitForGraph(ctx);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    /* Step 2: Add all waiting threads into graph. */
    ret = AddAllVerticesToGraph();
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    /* Step 3: Record all threads that are currently holding locks that conflict with waiting locks. */
    uint64 startRecordTime = DeadlockDetectGlobalTimer::GetInstance()->GetDetectTimestamp() -
                             DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US;
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (STORAGE_FUNC_FAIL(RecordThreadIfHoldingConflictLocks(core, startRecordTime))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

bool DeadlockDetector::DoesDeadlockExist()
{
    return (m_waitForGraph.GetVertexNumber() != 0);
}

bool DeadlockDetector::IsThreadStillWaitingBefore(ThreadId threadId, uint32 threadCoreIndex, uint64 startRecordTime)
{
    ThreadCore *core = g_storageInstance->GetThreadCoreMgr()->GetSpecifiedCore(threadCoreIndex);
    if (STORAGE_VAR_NULL(core) || unlikely(core->pid != threadId)) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Thread %lu with core index %u doesn't exist when checking lock wait time.",
                threadId, threadCoreIndex));
        return false;
    }

    if (ThreadHasBeenWaitingBefore(core, startRecordTime)) {
        return true;
    }

    return false;
}

/*
 * Because we built wait for graph using offline information, it might not match with the real lock waiting situation,
 * so we try recheck if the threads in cycle are still waiting for the same lock to reduce the chance of false alarm.
 */
RetStatus DeadlockDetector::RecheckCycle()
{
    /* Step 1: Get start record time. */
    uint64 startRecordTime = DeadlockDetectGlobalTimer::GetInstance()->GetDetectTimestamp() -
                             DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US;

    /* Step 2: Check whether the start waiting time of the thread in cycle changes. */
    Vertex *vertex = nullptr;
    WaitForGraph::CycleIterator iter(&m_waitForGraph);
    while ((vertex = iter.GetNextVertex()) != nullptr) {
        ThreadVertex *thread = dynamic_cast<ThreadVertex *>(vertex);
        ThreadId threadId = thread->GetThreadId();
        uint32 threadCoreIndex = thread->GetThreadCoreIndex();
        if (!IsThreadStillWaitingBefore(threadId, threadCoreIndex, startRecordTime) && !thread->IsToBeRemoved()) {
            /* The wait time of the thread changes, it is definitely not in the loop. */
            m_waitForGraph.PushIntoDeleteQueue(thread);
        }
    }

    /* Step 3: Delete these vertices and build cycle again. */
    m_waitForGraph.DeleteAllVerticesNotInCycle();
    return DSTORE_SUCC;
}

bool DeadlockDetector::ThreadHasBeenWaitingBefore(ThreadCore *core, uint64 startRecordTime)
{
    DeadlockThrdState *thrdDeadlockState = core->regularLockCtx->GetDeadlockState();
    ThreadLocalLock *localLock = core->regularLockCtx->GetLocalLock();
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    LockMode waitMode = DSTORE_NO_LOCK;
    uint32 waitLocksLen = 0;
    uint64 waitStartTime = 0;
    uint64 txnStartTime = 0;

    if ((waitStartTime = thrdDeadlockState->GetWaitingStartTime(&txnStartTime)) >= startRecordTime ||
        !localLock->GetWaitingLocks(waitTags, &waitMode, ThreadLocalLock::m_waitLockMaxCount, &waitLocksLen)) {
        return false;
    }
    return true;
}

static void *DeadlockMemAlloc(uint32 size)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    return DstorePalloc(size);
}

/* RetStatus is used to record whether the thread is still waiting for a lock.
 * DSTORE_FAIL for not waiting, DSTORE_SUCC for waiting.
 * If the thread is not waiting for a lock, it will be removed from the graph.
 */
RetStatus DeadlockDetector::GetSqlStatementForWaitingThread(ThreadId threadId, uint32 threadCoreIndex,
    DeadlockReporter &deadlockReport)
{
    ThreadCore *core = g_storageInstance->GetThreadCoreMgr()->GetSpecifiedCore(threadCoreIndex);
    if (STORAGE_VAR_NULL(core) || unlikely(core->pid != threadId)) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Deadlock Detection: Thread %lu with core index %u doesn't exist when getting sql statement.",
                threadId, threadCoreIndex));
        storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    char *sqlStatement = LockInterface::GetThreadSqlStatementCallback(threadId, DeadlockMemAlloc);
    if (sqlStatement == nullptr) {
        if (STORAGE_FUNC_FAIL(deadlockReport.Append("DeadLock thread %lu role: %s.\n", threadId, core->threadName))) {
            ErrLog(DSTORE_WARNING, MODULE_LOCK,
                ErrMsg("Out of memory when report deadLock thread %lu role: %s.\n", threadId, core->threadName));
        }
    } else {
        uint64 startRecordTime = DeadlockDetectGlobalTimer::GetInstance()->GetDetectTimestamp() -
                                 DEADLOCK_DETECT_COLLECT_WAIT_MIN_TIME_US;
        if (ThreadHasBeenWaitingBefore(core, startRecordTime)) {
            if (STORAGE_FUNC_FAIL(deadlockReport.Append(
                "DeadLock backend thread %lu (Commands are in reverse order):\n%s", threadId, sqlStatement))) {
                ErrLog(DSTORE_WARNING, MODULE_LOCK,
                    ErrMsg("Out of memory when report deadLock thread %lu role: %s. "
                    "(Commands are in reverse order):\n%s\n",
                    threadId, core->threadName, sqlStatement));
            }
        } else {
            storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Deadlock Detection: While getting SQL statement for thread %lu, "
                          "but found it was not waiting when timestamp=%lu",
                          threadId, startRecordTime));
            ret = DSTORE_FAIL;
        }
        DstorePfreeExt(sqlStatement);
    }
    return ret;
}

RetStatus DeadlockDetector::GetSqlStatementForAnyThread(ThreadId threadId, DeadlockReporter &deadlockReport)
{
    RetStatus ret = DSTORE_SUCC;
    char *sqlStatement = LockInterface::GetThreadSqlStatementCallback(threadId, DeadlockMemAlloc);
    if (sqlStatement == nullptr) {
        ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
        ThreadCore *core = nullptr;
        while ((core = iter.GetNextThreadCore()) != nullptr) {
            if (core->pid == threadId) {
                break;
            }
        }
        if (core != nullptr) {
            if (STORAGE_FUNC_FAIL(deadlockReport.Append("Thread %lu role: %s.\n", threadId, core->threadName))) {
                ErrLog(DSTORE_WARNING, MODULE_LOCK,
                    ErrMsg("Out of memory when get sql statement for thread %lu role: %s.\n",
                        threadId, core->threadName));
            }
        } else {
            storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
               ErrMsg("Deadlock Detection: Failed to find thread %lu", threadId));
            ret = DSTORE_FAIL;
        }
    } else {
        if (STORAGE_FUNC_FAIL(deadlockReport.Append("Backend thread %lu (Commands are in reverse order):\n%s",
            threadId, sqlStatement))) {
            ErrLog(DSTORE_WARNING, MODULE_LOCK,
                ErrMsg("Out of memory when GetSqlStatement for thread %lu (Commands are in reverse order):\n%s",
                    threadId, sqlStatement));
        }
        DstorePfreeExt(sqlStatement);
    }
    return ret;
}

RetStatus DeadlockDetector::GetSQLStatementForThread(ThreadId threadId, char **retString)
{
    DeadlockReporter sqlReport(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    DeadlockDetector tmpDetector;
    RetStatus ret = tmpDetector.GetSqlStatementForAnyThread(threadId, sqlReport);
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    if (sqlReport.GetDataLen() == 0) {
        *retString = nullptr;
        return DSTORE_SUCC;
    }

    *retString = sqlReport.GetData();
    sqlReport.SetData(nullptr, 0);
    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::GenerateDeadlockReport(DeadlockReporter &deadlockReport)
{
    /* Step 1: Traverse all threads in cycle to get all SQL statemant. */
    Vertex *vertex = nullptr;
    DeadlockReporter sqlReport(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    WaitForGraph::CycleIterator statementIter(&m_waitForGraph);
    while ((vertex = statementIter.GetNextVertex()) != nullptr) {
        ThreadVertex *thread = dynamic_cast<ThreadVertex *>(vertex);
        if (STORAGE_FUNC_FAIL(GetSqlStatementForWaitingThread(thread->GetThreadId(),
            thread->GetThreadCoreIndex(), sqlReport))) {
            if (!thread->IsToBeRemoved()) {
                m_waitForGraph.PushIntoDeleteQueue(thread);
            }
        }
    }
    m_waitForGraph.DeleteAllVerticesNotInCycle();

    /* Step 2: Traverse all threads in cycle to report wait for graph. */
    Edge *edge = nullptr;
    WaitForGraph::CycleIterator iter(&m_waitForGraph);
    while ((vertex = iter.GetNextVertex()) != nullptr) {
        ThreadVertex *thread = dynamic_cast<ThreadVertex *>(vertex);
        Vertex::EdgeIterator edgeIter(vertex, false);
        while ((edge = edgeIter.GetNextEdge()) != nullptr) {
            WaitLockEdge *waitLock = dynamic_cast<WaitLockEdge *>(edge);
            ThreadVertex *blockThread = dynamic_cast<ThreadVertex *>(waitLock->GetPeerVertex());
            if (STORAGE_FUNC_FAIL(deadlockReport.Append("Thread %lu is waiting on lock[", thread->GetThreadId())) ||
                STORAGE_FUNC_FAIL(waitLock->DumpWaitLock(deadlockReport)) ||
                STORAGE_FUNC_FAIL(deadlockReport.Append("] in %s mode, %s by thread %lu in ",
                    GetLockModeString(waitLock->GetWaitLockMode()),
                    waitLock->IsSoftBlocked() ? "soft blocked" : "blocked",
                    blockThread->GetThreadId())) ||
                STORAGE_FUNC_FAIL(waitLock->DumpBlockLockModes(deadlockReport)) ||
                STORAGE_FUNC_FAIL(deadlockReport.Append(" mode.\n"))) {
                storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
                return DSTORE_FAIL;
            }
        }
    }

    /* Step 3: Attach SQL statement to deadlock report. */
    if (STORAGE_FUNC_FAIL(deadlockReport.AppendBytes(sqlReport.GetData(), sqlReport.GetDataLen()))) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Out of memory when append deadlock report, wait for graph is %s, all SQL statement is %s",
                (deadlockReport.GetData() ? deadlockReport.GetData() : "nullptr"),
                (sqlReport.GetData() ? sqlReport.GetData() : "nullptr")));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::NotifyVictim(Vertex *thread, DeadlockReporter &deadlockReport)
{
    /* Step 1: Make a copy of deadlock report. */
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    char *victimReport = deadlockReport.CopyReport(ctx);
    if (STORAGE_VAR_NULL(victimReport)) {
        ThreadVertex *threadVertex = dynamic_cast<ThreadVertex *>(thread);
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Out of memory for deadlock report when notifying victim thread %lu.",
                threadVertex->GetThreadId()));
    }

    /* Step 2: Notify the victim and set deadlock report. */
    RetStatus ret = DSTORE_FAIL;
    ThreadVertex *threadVertex = dynamic_cast<ThreadVertex *>(thread);
    ThreadCore *core = g_storageInstance->GetThreadCoreMgr()->GetSpecifiedCore(threadVertex->GetThreadCoreIndex());
    if (likely(core != nullptr) && (core->pid == threadVertex->GetThreadId())) {
        ret = core->regularLockCtx->GetDeadlockState()->SetDeadlock(victimReport, threadVertex->GetWaitStartTime(),
            m_waitForGraph.GetVertexNumber());
        core->Wakeup();
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Deadlock detector notify victim thread %lu %s.",
            core->pid, STORAGE_FUNC_SUCC(ret) ? "succeed" : "failed"));
    } else {
        storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
        if (core == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
               ErrMsg("Deadlock Detection: Failed to find victim thread. Details: ThreadCore pointer is null."));
        } else {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Deadlock Detection: Failed to find victim thread. Details: found pid %lu, expected pid %lu.",
                          core->pid, threadVertex->GetThreadId()));
        }
    }
    return ret;
}

Vertex *DeadlockDetector::ChooseOneVictimToBreakCurrentCycle()
{
    /* Choose the thread who has the longest waiting time. */
    Vertex *vertex = nullptr;
    ThreadVertex *victim = nullptr;
    WaitForGraph::CycleIterator iter(&m_waitForGraph);
    while ((vertex = iter.GetNextVertex()) != nullptr) {
        ThreadVertex *thread = dynamic_cast<ThreadVertex *>(vertex);
        if ((victim == nullptr) || (victim->GetTrxStartTime() < thread->GetTrxStartTime())) {
            victim = thread;
        }
    }

    return victim;
}

RetStatus DeadlockDetector::ChooseVictimAndNotify()
{
    /* Step 1: Generate deadlock report. */
    DeadlockReporter deadlockReport(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
    RetStatus ret = GenerateDeadlockReport(deadlockReport);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Error occur when generating deadlock report, error code 0x%llx.",
                static_cast<unsigned long long>(StorageGetErrorCode())));
    } else {
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("Deadlock detector generates deadlock report completed:\n%s",
                deadlockReport.GetData()));
    }

    /* Step 2: Keep choosing victim and notify until there is no more deadlock exist. */
    while (DoesDeadlockExist()) {
        Vertex *thread = ChooseOneVictimToBreakCurrentCycle();
        StorageReleasePanic(STORAGE_VAR_NULL(thread), MODULE_LOCK, ErrMsg("choose victim failed"));
        ret = NotifyVictim(thread, deadlockReport);
        if (STORAGE_FUNC_FAIL(ret)) {
            return ret;
        }
        m_waitForGraph.PushIntoDeleteQueue(thread);
        m_waitForGraph.DeleteAllVerticesNotInCycle();
    }
    return DSTORE_SUCC;
}

LockWaitingThread *DeadlockDetector::CreateWaitingThread()
{
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    LockWaitingThread *newThread = DstoreNew(ctx) LockWaitingThread();
    if (STORAGE_VAR_NULL(newThread)) {
        return nullptr;
    }
    newThread->m_softBlocker = nullptr;
    newThread->m_next = nullptr;
    return newThread;
}

RetStatus DeadlockDetector::RecordThreadIfStartWaitingBefore(ThreadCore *core, uint64 startRecordTime)
{
    DeadlockThrdState *thrdDeadlockState = core->regularLockCtx->GetDeadlockState();
    ThreadLocalLock *localLock = core->regularLockCtx->GetLocalLock();
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    LockMode waitMode = DSTORE_NO_LOCK;
    uint32 waitLocksLen = 0;
    uint64 waitStartTime = 0;
    uint64 trxStartTime = 0;

    /* Step 1: Bypass the thread if it's not waiting for lock or it didn't wait long enough. */
    if ((waitStartTime = thrdDeadlockState->GetWaitingStartTime(&trxStartTime)) >= startRecordTime ||
        !localLock->GetWaitingLocks(waitTags, &waitMode, ThreadLocalLock::m_waitLockMaxCount,
        &waitLocksLen)) {
        return DSTORE_SUCC;
    }

    /* Step 2: Temporarily add the waiting thread to the map. */
    for (uint32 i = 0; i < waitLocksLen; i++) {
        LockWaitingThread *thread = CreateWaitingThread();
        if (STORAGE_VAR_NULL(thread)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Deadlock Detection: "
                          "Failed to allocate memory for waiting thread information."));
            return DSTORE_FAIL;
        }

        thread->SetThreadInfo(core->pid, core->selfIdx);
        thread->SetWaitLockInfo(waitMode, waitStartTime, startRecordTime - trxStartTime, waitLocksLen);

        RetStatus ret = m_lockWaiterMap.AddLockWaitingThread(waitTags[i], thread);
        if (STORAGE_FUNC_FAIL(ret)) {
            delete thread;
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::AddWaitForEdge(LockWaitingThread *waiter, LockWaitingThread *blocker,
    const LockTag &lockTag, LockMask blockMask, bool isSoftEdge)
{
    VertexTag tag;
    ThreadVertex::BuildVertexTag(tag, waiter->m_threadId);
    Vertex *startVertex = m_waitForGraph.FindVertex(tag);
    if (startVertex == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Deadlock Detection: Vertex[%lu] not found in graph.", waiter->m_threadId));
        storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
        return DSTORE_FAIL;
    }

    ThreadVertex::BuildVertexTag(tag, blocker->m_threadId);
    Vertex *endVertex = m_waitForGraph.FindVertex(tag);
    if (endVertex == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Deadlock Detection: Vertex[%lu] not found in graph.", blocker->m_threadId));
        storage_set_error(LOCK_ERROR_THREAD_NOT_FOUND);
        return DSTORE_FAIL;
    }

    WaitLockEdge edge;
    edge.SetWaitLockInfo(lockTag, waiter->m_waitLockMode, blockMask, isSoftEdge);
    RetStatus ret = m_waitForGraph.AddEdge(&edge, startVertex, endVertex);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Add edge fail with error code 0x%llx.",
                static_cast<unsigned long long>(StorageGetErrorCode())));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus DeadlockDetector::AddSoftEdgeIfNotExist(LockWaitingMap::LockWaitingHashEntry *waitEntry,
    LockWaitingThread *waiter, const LockTag &lockTag, LockMask holdMask)
{
    /* Step 1: Skip if we already added a soft edge. */
    if (waiter->m_softBlocker != nullptr) {
        return DSTORE_SUCC;
    }

    /* Step 2: Choose a blocker, the blocker must exist. */
    LockWaitingThread *blocker = waitEntry->ChooseSoftBlocker(holdMask);
    StorageReleasePanic(blocker == nullptr, MODULE_LOCK, ErrMsg("failed to choose a soft blocker."));

    /* Step 3: Add a soft edge to the blocker and mark added. */
    RetStatus ret = AddWaitForEdge(waiter, blocker, lockTag, GetLockMask(blocker->m_waitLockMode), true);
    if (STORAGE_FUNC_SUCC(ret)) {
        waiter->m_softBlocker = blocker;
    }
    return ret;
}

RetStatus DeadlockDetector::RecordThreadIfHoldingConflictLocks(ThreadCore *core, uint64 startRecordTime)
{
    DeadlockThrdState *thrdDeadlockState = core->regularLockCtx->GetDeadlockState();
    ThreadLocalLock *localLock = core->regularLockCtx->GetLocalLock();
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    LockMode waitMode = DSTORE_NO_LOCK;
    uint32 waitLocksLen = 0;
    uint64 waitStartTime = 0;
    uint64 txnStartTime = 0;

    /* Step 1: Bypass the thread if it didn't wait long enough. */
    if ((waitStartTime = thrdDeadlockState->GetWaitingStartTime(&txnStartTime)) >= startRecordTime ||
        !localLock->GetWaitingLocks(waitTags, &waitMode, ThreadLocalLock::m_waitLockMaxCount,
        &waitLocksLen)) {
        return DSTORE_SUCC;
    }

    /* Step 2: Traverse all threads and build wait for graph. */
    LockWaitingThread holder;
    holder.SetThreadInfo(core->pid, core->selfIdx);

    ThreadLocalLock::HoldLockIterator holdLockIter(localLock);
    const ThreadLocalLock::LocalLockEntry *entry = nullptr;
    while ((entry = holdLockIter.GetNextLock()) != nullptr) {
        /*
         * Step 2.1: If I hold locks that doesn't conflict with any waiting thread,
         * then they are all not waiting for me.
         */
        if (!m_lockWaiterMap.HasConflictWithWaitingThreads(entry->tag, entry->GetMask())) {
            continue;
        }

        /*
         * Step 2.2: For every thread that is waiting for my lock, add a wait-for-edge. There could be 2 senarios:
         * (1) If the waiting mode has conflict with my lock, then it's directly blocked by me and we can add a
         *     wait for edge.
         * (2) If not, then the thread still could be in waiting queue and is soft blocked by me, we can check if
         *     the soft edge exist by choosing the waiter that has the strongest lock mode, and add a wait for
         *     edge to the middle waiter.
         * Note that there could be some false edge since we don't have information for the actual waiting queue,
         * but it's better to false alarm than hang forever.
         */
        LockWaitingThread *waiter = nullptr;
        RetStatus ret = DSTORE_SUCC;
        LockWaitingMap::WaitingThreadIterater hardBlockIter(entry->tag, &m_lockWaiterMap);
        while ((waiter = hardBlockIter.GetNextThread()) != nullptr) {
            if (HasConflictWithMask(waiter->m_waitLockMode, entry->GetMask()) &&
                !waiter->IsSameRequester(&holder)) {
                ret = AddWaitForEdge(waiter, &holder, entry->tag, entry->GetMask(), false);
            }
            if (STORAGE_FUNC_FAIL(ret)) {
                return DSTORE_FAIL;
            }
        }

        LockWaitingMap::WaitingThreadIterater softBlockIter(entry->tag, &m_lockWaiterMap);
        while ((waiter = softBlockIter.GetNextThread()) != nullptr) {
            if (!HasConflictWithMask(waiter->m_waitLockMode, entry->GetMask())) {
                ret = AddSoftEdgeIfNotExist(softBlockIter.GetWaitingEntry(), waiter, entry->tag, entry->GetMask());
            }
            if (STORAGE_FUNC_FAIL(ret)) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

uint64 GetDeadLockDetectThreadCheckInterval()
{
    static constexpr uint64 MICROSEC_PER_MILLISEC = 1000;
    return (g_storageInstance->GetGuc()->deadlockTimeInterval * MICROSEC_PER_MILLISEC);
}

void SameThreadDeadlockDetector::PrepareToCheck(const LockTag &tag, LockMode checkMode)
{
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    localLock->GetHoldLockCnt(&tag, m_grantedCnt, static_cast<uint32>(DSTORE_LOCK_MODE_MAX));
    m_checkMode = checkMode;
}

bool SameThreadDeadlockDetector::CanLockModeConflict(LockMode mode) const
{
    if (HasConflictWithMode(mode, m_checkMode) &&
        (m_grantedCnt[mode] != 0 || HasMoreLocksInTransactionList(mode))) {
        return true;
    }
    return false;
}

bool SameThreadDeadlockDetector::HasMoreLocksInTransactionList(LockMode mode) const
{
    return ((g_storageInstance->GetGuc()->enableLazyLock) && (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK));
}

}
