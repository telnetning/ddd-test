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

#ifndef DSTORE_LOCK_THRD_LOCAL_H
#define DSTORE_LOCK_THRD_LOCAL_H

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_dynahash.h"
#include "lock/dstore_lwlock.h"
#include "lock/dstore_lock_datatype.h"
#include "page/dstore_td.h"
#include "lock/dstore_deadlock_detector.h"
#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {

constexpr uint32 FAST_LOCK_ENTRY_MAP_MAX_SLOT = 512;
constexpr uint32 FAST_PATH_STRONG_LOCK_HASH_BITS = 10;
constexpr uint32 FAST_PATH_STRONG_LOCK_HASH_PARTITIONS = (1 << FAST_PATH_STRONG_LOCK_HASH_BITS);

class LocalLock : public BaseObject {
public:
    LocalLock();
    virtual ~LocalLock();
    DISALLOW_COPY_AND_MOVE(LocalLock);

    virtual RetStatus Initialize(DstoreMemoryContext ctx);
    virtual void Destroy() noexcept;

    struct LocalLockEntry {
        LockTag tag;
        LockMgrType type;
        uint8 grantedByFastPath;
        uint32 grantedTotal;
        uint32 granted[DSTORE_LOCK_MODE_MAX];

        void Initialize(const LockTag *lockTag, LockMgrType mgrType);
        LockMask GetMask() const;

        inline bool IsEmpty() const
        {
            return (grantedTotal == 0);
        }

        inline void SetGrantedByFastPath(LockMode mode)
        {
            /* Only weak lock are allowed to be granted by fast-path. */
            StorageAssert((mode != DSTORE_NO_LOCK) && (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK));
            uint8 targetBit = static_cast<uint8>(1) << static_cast<unsigned char>(mode);
            StorageAssert((grantedByFastPath & targetBit) == 0);
            grantedByFastPath |= targetBit;
        }

        inline void ClearGrantedByFastPath(LockMode mode)
        {
            StorageAssert((mode != DSTORE_NO_LOCK) && (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK));
            uint8 targetBit = static_cast<uint8>(1) << static_cast<unsigned char>(mode);
            StorageAssert((grantedByFastPath & targetBit) != 0);
            grantedByFastPath &= ~targetBit;
        }

        inline bool IsGrantedByFastPath(LockMode mode)
        {
            StorageAssert((mode != DSTORE_NO_LOCK) && (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK));
            uint8 targetBit = static_cast<uint8>(1) << static_cast<unsigned char>(mode);
            return ((grantedByFastPath & targetBit) != 0);
        }
    };

    struct WaitingLockInfo {
        WaitingLockInfo()
            : lockEntry(nullptr), lockMode(DSTORE_NO_LOCK), unlockEntry(nullptr), lockScanned(false)
        {}
        /* If we are waiting for a lock or unlock, return a pointer to that LocalLockEntry. Otherwise return nullptr. */
        LocalLockEntry *GetActiveEntry();

        LocalLockEntry *lockEntry;
        LockMode lockMode;
        LocalLockEntry *unlockEntry;
        bool lockScanned;
    };
    /*
     * If this thread is already holding the lock, we simply increase the hold count and return DSTORE_SUCC.
     * Otherwise, we mark this thread as waiting to lock the tag and return DSTORE_FAIL.
     */
    virtual RetStatus GrantIfAlreadyHold(const LockTagCache &tag, LockMode mode, LockMgrType mgrType);
    /* Records the lock as locally obtained if the ret status is DSTORE_SUCC. Otherwise, clear the waiting tag. */
    virtual RetStatus RecordLockResult(const LockTagCache &tag, LockMode mode, RetStatus status);

    /*
     * Decrements this thread's hold count for the lock. If the lock count is still greater than 0, return DSTORE_SUCC.
     * Otherwise, we mark this thread as waiting to unlock the tag and return DSTORE_FAIL. Also fails with DSTORE_FAIL
     * if we are not holding the lock.
     */
    virtual RetStatus UngrantIfGrantedMultipleTimes(const LockTagCache &tag, LockMode mode);
    /* Decreases this thread's hold count by decreaseCnt. Succeedes if the hold count >= decreaseCnt. */
    virtual RetStatus BatchDecreaseGrantedCount(const LockTagCache &tag, LockMode mode, uint32 decreaseCnt,
                                                bool &isCntZero);
    /* Removes the local lock record for the unlocked tag. */
    virtual RetStatus RemoveLockRecord(const LockTagCache &tag, LockMode mode);
    virtual bool IsClearable(const LocalLockEntry *entry);

    /* Returns if we are waiting for a lock and what lock is being waited on. */
    bool IsWaitingLock(LockTag *tag, LockMode *mode);
    bool IsHoldingLock(const LockTagCache &tag);

    /* Function for getting info about which lock entry is being waited on. */
    virtual WaitingLockInfo *GetWaitingEntry()
    {
        StorageAssert(false);
        return nullptr;
    };
    /* Sets lockScanned to true so we know if we can safely modify the local lock entry. */
    void SetLockScannedForRecovery(bool scanned);
    /* Returns true if the entry we were waiting on was scanned during recovery. */
    bool GetLockScannedByRecovery();

    bool IsEmpty() const;
    uint32 GetNumEntries();
    void GetHoldLockCnt(const LockTag *tag, uint32 *granted, uint32 grantedLen);

    class HoldLockIterator {
    public:
        explicit HoldLockIterator(LocalLock *localLock);
        ~HoldLockIterator() noexcept;
        DISALLOW_COPY_AND_MOVE(HoldLockIterator);

        const LocalLockEntry *GetNextLock();

    private:
        LocalLock *m_localLock;
        HASH_SEQ_STATUS m_status;
        uint32 m_fastLockEntryIndex;
    };

protected:
    /*
     * Stores the lock that the current thread is waiting to lock or unlock.
     * The information is stored in the WaitingLockInfo struct returned by GetWaitingEntry.
     */
    void SetWaitingEntryForLock(LocalLockEntry *entry, LockMode mode);
    void GetWaitingEntryForLock(LocalLockEntry **entry, LockMode *mode = nullptr);
    void SetWaitingEntryForUnlock(LocalLockEntry *entry);
    void GetWaitingEntryForUnlock(LocalLockEntry **entry);

    LocalLockEntry *CreateLock(const LockTagCache &tag, LockMgrType mgrType);
    LocalLockEntry *FindLock(const LockTagCache &tag);
    void GrantLock(LocalLockEntry *entry, LockMode mode) const;
    void UnGrantLock(LocalLockEntry *entry, LockMode mode) const;
    void ClearLock(LocalLockEntry *entry, uint32 tagHashCode);

    LWLockPadded m_mutex;
    HTAB *m_holdLocks;
    uint32 m_fastLockEntryMapUseCnt;
    uint32 m_hashMapUseCnt;
    LocalLockEntry m_fastLockEntryMap[FAST_LOCK_ENTRY_MAP_MAX_SLOT];
};

/**
 * Fast path strong lock data is used to indicate how many strong locks are being held or acquired in progress.
 * Strong lock cnts are stored in an array of FAST_PATH_STRONG_LOCK_HASH_PARTITIONS partitions,
 * and will only slow down weak lock acquisition in the specific partition.
 */
struct FastPathStrongLockData {
    FastPathStrongLockData();
    ~FastPathStrongLockData() = default;

    inline uint32 GetStrongLockHashPartition(uint32 hashcode) const
    {
        return (hashcode) % FAST_PATH_STRONG_LOCK_HASH_PARTITIONS;
    }

    RetStatus DescribeStatus(StringInfo str);

    DstoreSpinLock mutex;
    uint32 counts[FAST_PATH_STRONG_LOCK_HASH_PARTITIONS];
};

/**
 * Comparing to Postgres, this structure is a combination of thread-local lock and fast-path array on each thread.
 * This is to avoid locking the backend for multiple times and reduce cache misses.
 * Most importantly, we don't need to worry about running out of fastpath slots.
 */
class ThreadLocalLock : public LocalLock {
public:
    ThreadLocalLock();
    ~ThreadLocalLock() override;
    DISALLOW_COPY_AND_MOVE(ThreadLocalLock);

    RetStatus Initialize(DstoreMemoryContext ctx) override;
    void Destroy() noexcept override;

    /* Function that grants weak lock by fast path if possible. */
    RetStatus TryGrantByFastPath(const LockTagCache &tag, LockMode mode, LockMgrType mgrType, bool &isAlreadyHeld);
    RetStatus TryReleaseByFastPath(const LockTagCache &tag, LockMode mode);
    RetStatus BatchReleaseByFastPath(const LockTagCache &tag, LockMode mode, uint32 decreaseCnt, bool &unlockFinished);
    static void MarkStrongLockInFastPath(uint32 tagHashCode);
    static void UnmarkStrongLockInFastPath(uint32 tagHashCode);
    static void CheckStrongLocksInFastPathLeak();
    static uint32 GetStrongLockCntInPartition(uint32 hashcode);
    LocalLockEntry *LockAndGetLocalLockEntry(const LockTagCache &tag);
    void UnlockLocalLock();

    /* Functions to wait for multiple locks on one thread. */
    inline bool IsWaitingForMultipleLocks()
    {
        return m_waitLocksLen != 0;
    }
    bool GetWaitingLocks(LockTag *waitTags, LockMode *waitMode, uint32 arrayLen, uint32 *waitLocksLen);
    RetStatus RecordWaitingForMultiLocks(const LockTagCache *waitTagCaches, uint32 count, LockMode waitMode,
                                         LockMgrType mgrType);
    void RecordLockResultForMultiLocks(const LockTagCache *waitTagCaches, uint32 count, RetStatus ret);

    /* Backup lock waiting information during locking for lazy lock transfer. */
    void BackupWaitingLock();
    void RestoreWaitingLock();
    bool IsClearable(const LocalLockEntry *entry) override;

    /* Returns the lock entry we are waiting on for lock or unlock. */
    WaitingLockInfo *GetWaitingEntry() override;
    LocalLockEntry *GetActiveEntry();

    /* Dfx function to dump strong locks. */
    static RetStatus DescribeStrongLocksInFastPath(StringInfo str);

    /* Max number of locks that a thread can be waiting for. */
    static const uint32 m_waitLockMaxCount = MAX_TD_COUNT;

private:
    WaitingLockInfo m_waitingInfo;
    uint32 m_waitLocksLen = 0;
    LocalLockEntry *m_waitLocks[m_waitLockMaxCount];
    LocalLockEntry *m_backupWaitLock;
    LockMode m_backupWaitMode;
    uint32 m_backupWaitLocksLen = 0;
    LocalLockEntry *m_backupWaitLocks[m_waitLockMaxCount];

    /* Array containing strong lock counts used for fast path locking. */
    static FastPathStrongLockData m_strongLocks;

    void CheckForLockLeakage() noexcept;
};

constexpr uint32 TABLE_LOCK_CONTEXT_MAX_SIZE = 32;

struct TableLockThrdContext {
    void Initialize()
    {
        isValid = false;
        mHasToRetry = false;
    }
    void Destroy()
    {
        isValid = false;
        mHasToRetry = false;
    }
    void *GetData()
    {
        return static_cast<void *>(data);
    }

    bool isValid;
    bool mHasToRetry;
    char data[TABLE_LOCK_CONTEXT_MAX_SIZE];
};

class LazyLockHint {
public:
    void Initialize(uint32 partId);
    bool IncreaseLazyLockCnt();
    bool DecreaseLazyLockCnt(uint32 decreaseCnt, bool &hasConflict);
    uint64 DisableLazyLock();
    void EnableLazyLock();
    bool IsLazyLockEnabled();
    uint64 GetLazyLockCnt();

    static constexpr uint32 LAZY_LOCK_HINT_PART_CNT = 128;

private:
    uint64 m_localCnt;
    gs_atomic_uint64 m_lazyLockHintBits;
};

class LockThreadContext : public BaseObject {
public:
    LockThreadContext()
        : m_localLock(),
          m_deadlockState(nullptr),
          m_tableLockContext(),
          isWaiting(),
          m_lockErrorInfo()
    {}

    ~LockThreadContext();
    DISALLOW_COPY_AND_MOVE(LockThreadContext);

    void InitializeBasic();
    RetStatus Initialize();
    void InitializeLazyLockHint();
    void Destroy();

    inline ThreadLocalLock *GetLocalLock()
    {
        return &m_localLock;
    }

    inline DeadlockThrdState *GetDeadlockState()
    {
        return m_deadlockState;
    }

    inline TableLockThrdContext *GetTableLockContext()
    {
        return &m_tableLockContext;
    }
    inline LazyLockHint *GetLazyLockHint(uint32 partId)
    {
        StorageAssert(partId < LazyLockHint::LAZY_LOCK_HINT_PART_CNT);
        return &(m_lazyLockHint[partId]);
    }

    inline LockErrorInfo *GetLockErrorInfo()
    {
        return &m_lockErrorInfo;
    }

    inline void SetLockErrorInfo(LockErrorInfo *info)
    {
        if (!STORAGE_VAR_NULL(info)) {
            m_lockErrorInfo.lockHolder = info->lockHolder;
            m_lockErrorInfo.isHolder = info->isHolder;
            m_lockErrorInfo.nodeId = info->nodeId;
            m_lockErrorInfo.processTimelineId = info->processTimelineId;
            m_lockErrorInfo.nodeTimelineId = info->nodeTimelineId;
            m_lockErrorInfo.threadId = info->threadId;
            m_lockErrorInfo.lockMode = info->lockMode;
        }
    }

    ThreadLocalLock m_localLock;
    DeadlockThrdState *m_deadlockState;
    TableLockThrdContext m_tableLockContext;
    LazyLockHint m_lazyLockHint[LazyLockHint::LAZY_LOCK_HINT_PART_CNT];
    bool isWaiting;
    LockErrorInfo m_lockErrorInfo;
};

/**
 * Helper class for performing lock wait.
 * If mustWaitFinish is set to false, then it allows both lock timeout and cancel error during the lock wait.
 */
class LockWaitScheduler : public BaseObject {
public:
    LockWaitScheduler(DeadlockDetector *detector, bool mustWaitFinish);
    virtual ~LockWaitScheduler();
    DISALLOW_COPY_AND_MOVE(LockWaitScheduler);

    enum WakeupReason : uint8 {
        NORMAL_WAKEUP = 0,
        DEADLOCK_DETECTED = 1,
        WAIT_TIMEOUT = 2,
        WAIT_CANCELED = 3
    };

    void StartWaiting();
    void FinishWaiting();

    WakeupReason WaitForNextCycle();

protected:
    uint64 GetNextWaitTime(bool &almostTimeout);
    void ReportLockWaitTimeout();
    void ReportLockWaitCanceled();
    RetStatus GetLockWaitReportString(StringInfo lockTags, LockMode &lockMode);
    virtual void GetLockWaitErrorInfo();

    DeadlockThrdState *m_deadlockState;
    DeadlockDetector *m_deadlockDetector;
    LockTag *m_waitTag;
    LockMode m_waitMode;
    uint64 m_startWaitTime;
    uint64 m_deadlockCheckInterval;
    uint64 m_waitTimeout;
    bool m_mustWaitFinish;
};

}  // namespace DSTORE

#endif
