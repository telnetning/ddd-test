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
 * dstore_thread_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_thread_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_THREAD_INTERFACE_H
#define DSTORE_THREAD_INTERFACE_H

#include <functional>
#include "common/dstore_common_utils.h"
#include "transaction/dstore_transaction_struct.h"

namespace DSTORE {
struct TacSnapshotPayload;

enum class InternalThreadType {
    THREAD_TYPE_INVALID,
    THREAD_INDEX_PARALLEL_BUILD,
    THREAD_INDEX_RECYCLE,
    THREAD_ROLLBACK_WORKER,
    THREAD_FAILOVER_ROLLBACK
};

struct SQLThrdInitCtx {
    PdbId pdbId;
    const char* threadName;
    InternalThreadType type;
    void **sqlThrd;
    bool isCommit;
};

using ThreadInitCallback = std::function<bool(SQLThrdInitCtx *context)>;
using ThreadReleaseCallback = std::function<void(SQLThrdInitCtx *context)>;

/* thread type flag values */
enum class DstoreThrdRole : uint8_t {
    INVALID_THREAD_ROLE,
    SQL_THREAD,
    BACKGROUND_THREAD,
};

/**
 * Set different memory levels for background threads and allow background threads to preempt foreground thread memory
 * based on the level to ensure program stability.
 */
enum class ThreadMemoryLevel : uint8_t {
    THREADMEM_WORKER_INVALID = 0,
    THREADMEM_LOW_PRIORITY,    /* After a thread is down, the thread is restarted, and the user is not aware of it. */
    THREADMEM_MEDIUM_PRIORITY, /* After a thread is down, the thread is restarted, and the user is aware. */
    THREADMEM_HIGH_PRIORITY    /* After the thread is down, a fatal problem occurs, and the program crashes. */
};

/* store a thread's info */
struct ThreadStatsInfo {
    unsigned long int pid;
    long lwpid;
    char* threadName;
    int64_t startTime;
};

class StorageSession;
class ThreadContextInterface {
    /* variables used only by the current thread */
public:
    virtual ~ThreadContextInterface() = default;
    virtual RetStatus
        InitTransactionRuntime(PdbId pdbId, TrxCallback callbck, void *arg, bool allocateZoneId = true) = 0;
    virtual void DestroyTransactionRuntime() = 0;

    virtual PdbId GetXactPdbId() = 0;
    virtual uint64_t GetPdbTerm() = 0;

    virtual void ResetErrorMemory() = 0;
    virtual void ResetQueryMemory() = 0;
    virtual void ResetSmgrMemory() = 0;

    virtual void Sleep(const struct timespec *timeout = nullptr) = 0;
    virtual void Wakeup() = 0;

    virtual int GetNumaId() = 0;
    virtual void SetNumaId(int numaId) = 0;
    virtual uint64_t GetTransactionVirtualCounter() = 0;
    virtual void SetInterruptPending() = 0;
    virtual void ClearInterruptPending() = 0;
    virtual ThreadId GetThreadId() const = 0;
    virtual bool IsCallbackInvoke() = 0;
    virtual void SetCallbackInvoke(bool flag) = 0;
    virtual bool IsCallbackErrExist() = 0;
    virtual void SetCallbackErrExist(bool flag) = 0;
    virtual DSTORE_EXPORT void SetUniqueQueryId(uint64_t queryId) = 0;

    DSTORE_EXPORT virtual RetStatus InitializeBasic() = 0;
    DSTORE_EXPORT virtual RetStatus InitStorageContext(PdbId pdbId) = 0;
    DSTORE_EXPORT virtual void Destroy() = 0;
    DSTORE_EXPORT virtual void AttachSessionToThread(StorageSession *session) = 0;
    DSTORE_EXPORT virtual void SetThreadMemLevel(ThreadMemoryLevel level) = 0;
    DSTORE_EXPORT virtual void DetachSessionFromThread() = 0;
    DSTORE_EXPORT virtual void SetXactPdbId(PdbId pdbId) = 0;
    DSTORE_EXPORT virtual CommitSeqNo GetSnapShotCsn() = 0;
    DSTORE_EXPORT virtual CommitSeqNo GetNextCsn() = 0;
    DSTORE_EXPORT virtual void SetSnapshotCsnForFlashback(CommitSeqNo csn) = 0;
    DSTORE_EXPORT virtual void SetFlashbackCsnMin(CommitSeqNo flashbackCsnMin) = 0;

    DSTORE_EXPORT virtual ErrorCode GetErrorCode() = 0;
    DSTORE_EXPORT virtual const char *GetErrorMessage() = 0;
    DSTORE_EXPORT virtual NodeId GetErrorNodeId() = 0;
    DSTORE_EXPORT virtual void RefreshWorkingVersionNum() = 0;

    DSTORE_EXPORT static RetStatus GetAllThreadsInfo(ThreadStatsInfo **threadInfoArr, size_t *length);
    DSTORE_EXPORT static void FreeThreadInfoArr(ThreadStatsInfo **threadInfoArr, size_t length);

    DSTORE_EXPORT static ThreadContextInterface *Create();
    DSTORE_EXPORT static ThreadContextInterface *GetCurrentThreadContext();

    /* To support Transparent Application Continuity (TAC) */
    DSTORE_EXPORT static bool IsTacReplayDisabled();
    DSTORE_EXPORT static void SetTacReplayDisabled(bool tacReplayDisabled);
    DSTORE_EXPORT static TacSnapshotPayload GetTacSnapshotReceivedPayload();
    DSTORE_EXPORT static void SetTacSnapshotReceivedPayload(CommitSeqNo csn);
    DSTORE_EXPORT static TacSnapshotPayload GetTacSnapshotToSendPayload();
    DSTORE_EXPORT static void SetTacSnapshotToSendPayload(CommitSeqNo csn);
    DSTORE_EXPORT static void AddTacOrphanTrx();
    DSTORE_EXPORT static void ApplyTacSnapshot();
    DSTORE_EXPORT static void ClearTacFirstStatementCsnMin();
    DSTORE_EXPORT static void SetTacFirstStatementCsnMin(CommitSeqNo csn);
    DSTORE_EXPORT static void StartTacFirstStatementCsnMinExpiryTimer();

    DSTORE_EXPORT static void DestroyCurrentThreadContext();
};
}  // namespace DSTORE
#endif
