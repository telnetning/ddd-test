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

#include "framework/dstore_instance.h"
#include "lock/dstore_lock_thrd_local.h"
#include "common/log/dstore_log.h"
#include "lock/dstore_lock_entry.h"

namespace DSTORE {

void LockRequest::WakeUp()
{
    ThreadCore *core = g_storageInstance->GetThreadCoreMgr()->GetSpecifiedCore(threadCoreIdx);
    if (core == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Thread core index %u exceeds bound of thread core array, skip waking up.", threadCoreIdx));
        return;
    }
    core->regularLockCtx->isWaiting = false;
    core->Wakeup();
}

int32 LockRequest::Compare(const LockRequestInterface *requester) const
{
    StorageAssert(dynamic_cast<const LockRequest *>(requester) != nullptr);
    const LockRequest *compare = static_cast<const LockRequest *>(requester);

    if (this->m_lockMode != compare->m_lockMode) {
        return ((this->m_lockMode > compare->m_lockMode) ? 1 : -1);
    }

    if (this->threadCoreIdx != compare->threadCoreIdx) {
        return ((this->threadCoreIdx > compare->threadCoreIdx) ? 1 : -1);
    }

    return 0;
}

RetStatus LockRequest::DumpLockRequest(StringInfo stringInfo) const
{
    return stringInfo->append("Thread %lu [core index %u] requests for %s mode",
                              threadId, threadCoreIdx, GetLockModeString(m_lockMode));
}

/*
 * Get the conflict lock mask of a lock mode.
 */
static LockMask GetConflictMask(const LockMode mode) noexcept
{
    /*
     * Data structures defining the semantics of the standard lock methods.
     * The conflict table defines the semantics of the various lock modes.
     */
    LockMask lockConflictMap[DSTORE_LOCK_MODE_MAX] = {0U,

        /* DSTORE_ACCESS_SHARE_LOCK */
        GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_ROW_SHARE_LOCK */
        GetLockMask(DSTORE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_ROW_EXCLUSIVE_LOCK */
        GetLockMask(DSTORE_SHARE_LOCK) | GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK */
        GetLockMask(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_SHARE_LOCK) |
            GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_SHARE_LOCK */
        GetLockMask(DSTORE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_SHARE_ROW_EXCLUSIVE_LOCK */
        GetLockMask(DSTORE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_SHARE_LOCK) | GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_EXCLUSIVE_LOCK */
        GetLockMask(DSTORE_ROW_SHARE_LOCK) | GetLockMask(DSTORE_ROW_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_SHARE_LOCK) |
            GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK),

        /* DSTORE_ACCESS_EXCLUSIVE_LOCK */
        GetLockMask(DSTORE_ACCESS_SHARE_LOCK) | GetLockMask(DSTORE_ROW_SHARE_LOCK) |
            GetLockMask(DSTORE_ROW_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_SHARE_LOCK) | GetLockMask(DSTORE_SHARE_ROW_EXCLUSIVE_LOCK) |
            GetLockMask(DSTORE_EXCLUSIVE_LOCK) | GetLockMask(DSTORE_ACCESS_EXCLUSIVE_LOCK)};
    return lockConflictMap[mode];
}

bool HasConflictWithMode(const LockMode requestedMode, const LockMode mode)
{
    return (GetConflictMask(requestedMode) & GetLockMask(mode)) != 0;
}

bool HasConflictWithMask(const LockMode requestedMode, const LockMask mask)
{
    return (GetConflictMask(requestedMode) & mask) != 0;
}

bool HasConflictWithMask(const LockMask requestedMask, const LockMask mask)
{
    for (int i = static_cast<int>(DSTORE_ACCESS_SHARE_LOCK); i < static_cast<int>(DSTORE_LOCK_MODE_MAX); ++i) {
        if ((requestedMask & GetLockMask(static_cast<LockMode>(i))) == 0) {
            continue;
        }
        if (HasConflictWithMask(static_cast<LockMode>(i), mask)) {
            return true;
        }
    }
    return false;
}

LockRequestSkipList::LockRequestSkipList() : m_currHighestLevel(0)
{
    for (int32 level = 0; level < MAX_SKIPLIST_LEVEL; level++) {
        DListInit(GetLevelHead(level));
    }
}

void LockRequestSkipList::RecordTargetRequestLevel(int32 *targetRequestLevel, const int32 level) const
{
    if (targetRequestLevel != nullptr) {
        *targetRequestLevel = level;
    }
}

/*
 * Search for the position of a target request, or the position that target lock request needs to be inserted in
 * if it's not found.
 * If found, record the path to it in stack and record the level of target request.
 */
LockRequestInterface *LockRequestSkipList::SearchLockRequestPosition(const LockRequestInterface *targetRequest,
    LockRequestInterface **stack, int32 *targetRequestLevel)
{
    StorageAssert(targetRequest != nullptr);
    LockRequestInterface *currentRequest = nullptr;
    LockRequestInterface *nextRequest = nullptr;

    for (int32 level = m_currHighestLevel; level >= 0; level--) {
        for (;;) {
            /* If the next dlist_node in this level is NULL, go down one level and continue searching. */
            nextRequest = GetNextLockRequestAtLevel(level, currentRequest);
            if (nextRequest == nullptr) {
                break;
            }

            int32 result = targetRequest->Compare(nextRequest);
            if (result == 0) {
                /* Found the lock request, record the level of the request we are trying to find. */
                StorageAssert(nextRequest->IsSameRequester(targetRequest));
                RecordTargetRequestLevel(targetRequestLevel, level);
                return nextRequest;
            } else if (result > 0) {
                /*
                 * Next lock request is "smaller" than the target lock request,
                 * continue to search in the same level.
                 */
                currentRequest = nextRequest;
                continue;
            } else {
                /*
                 * Next lock request is "bigger" than the target lock request,
                 * go down one level and continue searching.
                 */
                break;
            }
        }

        /*
         * Record the path of how we came to the position of target lock request.
         * For searching and deleting, we don't need to collect the path.
         */
        if (stack != nullptr) {
            stack[level] = currentRequest;
        }
    }

    /*
     * We have searched all levels from top level to level 0 but still haven't found the lock request,
     * This means that the lock request does not exist in the skip list.
     *
     * We have found the place to insert target lock request, after insertion:
     *   currentReuqest -> targetRequest -> ...
     */
    if (targetRequestLevel != nullptr) {
        *targetRequestLevel = 0;
    }
    return nullptr;
}

/*
 * Get a random level when we insert a new lock request.
 * new level range: [0, m_currHighestLevel + 1]
 */
int32 LockRequestSkipList::GetInsertLevel(LockRequestInterface *request)
{
    StorageAssert(request != nullptr);
    constexpr uint32 randomFactor = 2;

    int32 level = 0;
    uint32 rand = request->GetSeed();
    while ((level <= m_currHighestLevel) && (level < MAX_SKIPLIST_LEVEL - 1) && (rand % randomFactor) == 1) {
        rand /= randomFactor;
        level++;
    }
    return level;
}

/*
 * Insert a new lock request to skip list.
 * If the lock request already exists, return failure.
 * Record the path of how we can get to the position of new request, link every node on the path to
 * the new request's linker.
 */
RetStatus LockRequestSkipList::InsertLockRequest(LockRequestInterface *request)
{
    StorageAssert(request != nullptr);
    /*
     * Records the path of lock requests of how we went down to the right position of the lock request.
     * After the lock request is inserted, each of these lock request should have a link to the new request
     * at the certain level.
     */
    LockRequestInterface *requestStack[MAX_SKIPLIST_LEVEL];
    LockRequestInterface *foundRequest = nullptr;

    foundRequest = SearchLockRequestPosition(request, requestStack, nullptr);
    /* If lock request already exists, return failure and let caller to handle. */
    if (foundRequest != nullptr) {
        return DSTORE_FAIL;
    }

    /* Get the level of new request, range in [0, m_currHighestLevel + 1]. */
    int32 newLevel = GetInsertLevel(request);
    if (newLevel > m_currHighestLevel) {
        StorageAssert(newLevel < MAX_SKIPLIST_LEVEL);
        StorageAssert(newLevel == m_currHighestLevel + 1);
        m_currHighestLevel = newLevel;
        InsertRequestToLevelHead(newLevel, request);
        newLevel--;
    }

    for (int32 level = newLevel; level >= 0; level--) {
        if (requestStack[level] == nullptr) {
            InsertRequestToLevelHead(level, request);
        } else {
            InsertRequestNextTo(level, requestStack[level], request);
        }
    }
    return DSTORE_SUCC;
}

RetStatus LockRequestSkipList::DumpSkipList(StringInfo str)
{
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("  Skip list total count by level: ");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    int32 currHighestLevel = GetCurrHighestLevel();
    for (int32 level = 0; level <= currHighestLevel; level++) {
        if (level == currHighestLevel) {
            ret = str->append("level %d: %u.", level, GetLevelRequestCnt(level));
        } else {
            ret = str->append("level %d: %u, ", level, GetLevelRequestCnt(level));
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    ret = str->append("\n  Lock requests in list:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    bool requestExist = false;
    const LockRequestInterface *request = nullptr;
    LockRequestSkipList::ListIterator iter(this, 0);
    for (request = iter.GetNextRequest(); request != nullptr; request = iter.GetNextRequest()) {
        if (STORAGE_FUNC_FAIL(str->append("    ")) ||
            STORAGE_FUNC_FAIL(request->DumpLockRequest(str)) ||
            STORAGE_FUNC_FAIL(str->append(".\n"))) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            ret = DSTORE_FAIL;
            break;
        }
        requestExist = true;
    }
    if (STORAGE_FUNC_SUCC(ret) && !requestExist) {
        ret = str->append("    None.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        }
    }
    return ret;
}

/*
 * Remove a lock request from the skip list and return the found request.
 * if lock request doesn't exist, return nullptr.
 */
LockRequestInterface *LockRequestSkipList::RemoveLockRequest(const LockRequestInterface *request)
{
    StorageAssert(request != nullptr);
    LockRequestInterface *foundRequest = nullptr;
    int32 targetRequestLevel = 0;

    foundRequest = SearchLockRequestPosition(request, nullptr, &targetRequestLevel);
    if (foundRequest == nullptr) {
        return nullptr;
    }

    /* Find the level of target request, from this level down to 0, remove the dlist node. */
    for (int32 level = targetRequestLevel; level >= 0; level--) {
        RemoveRequestFromLevel(level, foundRequest);
    }

    if (targetRequestLevel == m_currHighestLevel) {
        int32 newTopLevel = m_currHighestLevel;
        while ((newTopLevel > 0) && IsLevelEmpty(newTopLevel)) {
            newTopLevel--;
        }
        m_currHighestLevel = newTopLevel;
    }
    return foundRequest;
}

/*
 * Search for a lock request in skip list.
 * If the lock request is found, return the lock request.
 * If not found, return nullptr.
 */
LockRequestInterface *LockRequestSkipList::SearchLockRequest(const LockRequestInterface *request)
{
    StorageAssert(request != nullptr);
    return SearchLockRequestPosition(request, nullptr, nullptr);
}

uint32 LockRequestSkipList::GetLevelRequestCnt(int32 level)
{
    uint32 cnt = 0;
    const LockRequestInterface *request = nullptr;
    LockRequestSkipList::ListIterator iter(this, level);

    for (request = iter.GetNextRequest(); request != nullptr; request = iter.GetNextRequest()) {
        cnt++;
    }
    return cnt;
}

LockRequestSkipList::ListIterator::ListIterator(LockRequestSkipList *skipList, int32 level)
    : m_level(level),
      m_skipList(skipList),
      m_nextRequest(nullptr)
{
    StorageAssert(m_skipList != nullptr);
    m_nextRequest = m_skipList->GetNextLockRequestAtLevel(m_level, nullptr);
}

LockRequestSkipList::ListIterator::~ListIterator()
{
    m_skipList = nullptr;
    m_nextRequest = nullptr;
}

LockRequestInterface *LockRequestSkipList::ListIterator::GetNextRequest()
{
    LockRequestInterface *currentRequest = m_nextRequest;
    m_nextRequest = m_skipList->GetNextLockRequestAtLevel(m_level, m_nextRequest);
    return currentRequest;
}

void LockEntry::Initialize(const LockTag *lockTag)
{
    new(&lockEntryCore) LockEntryCore();
    new(&grantedQueue) LockRequestSkipList();
    lockEntryCore.m_lockTag = *lockTag;
    DListInit(&waitingQueue);
}

bool LockEntry::HasConflictWithAnyHolder(const LockRequestInterface *requester,
                                         LockRequestFreeList *freeList,
                                         LockErrorInfo *info)
{
    /*
     * First check for global conflicts:
     * If no locks conflict with my request, then I get the lock.
     */
    if ((GetConflictMask(requester->GetLockMode()) & lockEntryCore.m_grantMask) == 0) {
        return false;
    }

    /*
     * Some lock requests in granted queue conflict with me. But it could still be my own lock.
     * If the conflicting request was my own, continue to check if there are other conflicting requests.
     * If not, find the conflicting node ID and return true.
     * Note that my request mode A may conflict with both B and C in granted queue (B and C are not conflicted
     * for sure), since B is prior to C, we will return conflict found immediately when we see B and only record
     * B's info in LockErrorInfo.
     */
    LockRequestSkipList::ListIterator iter(&grantedQueue, 0);
    for (LockRequestInterface *holder = iter.GetNextRequest(); holder != nullptr; holder = iter.GetNextRequest()) {
        if (!(holder->IsSameRequester(requester)) &&
            HasConflictWithMode(requester->GetLockMode(), holder->GetLockMode())) {
            if (!(holder->IsValid(lockEntryCore.m_lockTag.recoveryMode))) {
                RetStatus ret = RemoveFromHolderQueue(holder, freeList);
                StorageReleasePanic(ret == DSTORE_FAIL, MODULE_LOCK, ErrMsg("Failed to remove invalid lock request."));
                continue;
            }
            if (info != nullptr) {
                holder->RecordErrInfo(info);
                info->isHolder = true;
            }
            return true;
        }
    }

    return false;
}

void LockEntry::AddToHolderQueue(LockRequestInterface *requester, LockRequestFreeList *freeList)
{
    if (requester->IsOnlyWait()) {
        freeList->Push(requester);
        return;
    }

    RetStatus ret = grantedQueue.InsertLockRequest(requester);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Duplicate lock request found when granting lock."));
        freeList->Push(requester);
        return;
    }

    lockEntryCore.m_grantMask |= GetLockMask(requester->GetLockMode());
    lockEntryCore.m_grantedCnt[requester->GetLockMode()]++;
    lockEntryCore.m_grantedTotal++;
    StorageAssert(grantedQueue.GetLevelRequestCnt(0) == lockEntryCore.m_grantedTotal);
}

RetStatus LockEntry::RemoveFromHolderQueue(const LockRequestInterface *requester, LockRequestFreeList *freeList)
{
    LockRequestInterface *found = grantedQueue.RemoveLockRequest(requester);
    if (found == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Lock%s request in mode %s not found when removing from grant queue, it might be in wait queue.",
                GetLockTag()->ToString().CString(), GetLockModeString(requester->GetLockMode())));
        return DSTORE_FAIL;
    }

    lockEntryCore.m_grantedTotal--;
    lockEntryCore.m_grantedCnt[requester->GetLockMode()]--;
    if (lockEntryCore.m_grantedCnt[requester->GetLockMode()] == 0) {
        lockEntryCore.m_grantMask &= ~GetLockMask(requester->GetLockMode());
    }

    StorageAssert(grantedQueue.GetLevelRequestCnt(0) == lockEntryCore.m_grantedTotal);
    freeList->Push(found);
    return DSTORE_SUCC;
}

void LockEntry::RemoveAllRequests(LockRequestFreeList *freeList)
{
    LockRequestSkipList::ListIterator iter(&grantedQueue, 0);
    for (LockRequestInterface *holder = iter.GetNextRequest(); holder != nullptr; holder = iter.GetNextRequest()) {
        RetStatus ret = RemoveFromHolderQueue(holder, freeList);
        StorageReleasePanic(ret == DSTORE_FAIL, MODULE_LOCK, ErrMsg("Failed to cleanup lock request."));
    }
    StorageAssert(lockEntryCore.m_grantedTotal == 0);

    dlist_mutable_iter waiterIter;
    dlist_foreach_modify(waiterIter, &waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(waiterIter.cur);
        DListDelete(waiter->GetDlistNode());
        freeList->Push(waiter);
        lockEntryCore.m_waitingTotal--;
    }
    StorageAssert(lockEntryCore.m_waitingTotal == 0);

    new(&lockEntryCore) LockEntryCore();
}

/*
 * The code below adds deadlock prevention by implementing "cut-in-line" approach.
 * In this approach, we try to insert our lock request in the waiting queue in such
 * a way as to avoid potential deadlocks.
 * Note that we will not rearrange the waiting queue during the deadlock
 * detection which makes this prevention code necessary to avoid potential deadlocks.
 *
 * If thread 1 already holds a lock but the lock mode is different from the new
 * lock application, when joining the waiting queue, if thread 2 has a lock
 * application before the position to be inserted and the lock mode is
 * incompatible with the lock held by thread 1, the new lock application needs
 * to be inserted before thread 2. This is because thread 2 is not inserted,
 * thread 2 is waiting for the blocked lock held by thread 1 and thread 1's new
 * lock application waits after thread 1.
 * Note: in case where the current thread is holding the lock with mode m1,
 * the new request for the same lock with mode m2 will only be passed to
 * the wait function if m2 is different with m1.
 */
class EarlyDeadLockPreventor {
public:
    EarlyDeadLockPreventor(LockTag *lockTag,
                           LockRequestSkipList *grantedQueue,
                           dlist_head *waitingQueue,
                           const LockRequestInterface *requester)
        : m_cutInLinePosition(nullptr),
          m_lockTag(lockTag),
          m_grantedQueue(grantedQueue),
          m_waitingQueue(waitingQueue),
          m_requester(requester),
          m_result(Result::NO_DEADLOCK),
          m_myOwnedLocks(0),
          m_othersOwnedLocks(0)
    {}
    ~EarlyDeadLockPreventor() = default;

    enum class Result {
        NO_DEADLOCK,
        PREVENT_BY_GRANT_LOCK,
        PREVENT_BY_CUT_IN_LINE,
        CANT_PREVENT_DEADLOCK
    };

    void Start();
    Result GetResult() const;
    dlist_node *GetCutInLinePosition();

    dlist_node *m_cutInLinePosition;

private:
    void PrepareOwnedLock();
    void CheckForDeadLock();
    RetStatus ReportEarlyDeadLock(LockRequestInterface *waiter, LockMask waiterOwnedLocks);

    LockTag *m_lockTag;
    LockRequestSkipList *m_grantedQueue;
    dlist_head *m_waitingQueue;
    const LockRequestInterface *m_requester;
    Result m_result;
    LockMask m_myOwnedLocks;
    LockMask m_othersOwnedLocks;
};

void EarlyDeadLockPreventor::Start()
{
    PrepareOwnedLock();
    if (m_myOwnedLocks == 0) {
        return;
    }
    CheckForDeadLock();
}

void EarlyDeadLockPreventor::PrepareOwnedLock()
{
    LockRequestSkipList::ListIterator iter(m_grantedQueue, 0);
    for (LockRequestInterface *holder = iter.GetNextRequest(); holder != nullptr; holder = iter.GetNextRequest()) {
        if (holder->IsSameRequester(m_requester)) {
            m_myOwnedLocks |= GetLockMask(holder->GetLockMode());
        } else {
            m_othersOwnedLocks |= GetLockMask(holder->GetLockMode());
        }
    }
}

RetStatus EarlyDeadLockPreventor::ReportEarlyDeadLock(LockRequestInterface *waiter, LockMask waiterOwnedLocks)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        return DSTORE_FAIL;
    }

    RetStatus ret = m_requester->DumpLockRequest(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return ret;
    }

    ret = string.append(", holding lock in mode:");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return ret;
    }

    for (int i = 0; i < static_cast<int>(DSTORE_LOCK_MODE_MAX); i++) {
        if ((m_myOwnedLocks & GetLockMask(static_cast<LockMode>(i))) != 0) {
            ret = string.append(" %s", GetLockModeString(static_cast<LockMode>(i)));
            if (STORAGE_FUNC_FAIL(ret)) {
                DstorePfreeExt(string.data);
                return ret;
            }
        }
    }

    ret = string.append(".\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return ret;
    }

    ret = waiter->DumpLockRequest(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return ret;
    }

    ret = string.append(", holding lock in mode:");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return ret;
    }

    for (int i = 0; i < static_cast<int>(DSTORE_LOCK_MODE_MAX); i++) {
        if ((waiterOwnedLocks & GetLockMask(static_cast<LockMode>(i))) != 0) {
            ret = string.append(" %s", GetLockModeString(static_cast<LockMode>(i)));
            if (STORAGE_FUNC_FAIL(ret)) {
                DstorePfreeExt(string.data);
                return ret;
            }
        }
    }

    ErrLog(DSTORE_ERROR, MODULE_LOCK,
        ErrMsg("Early deadlock found in %s:\n%s.", m_lockTag->ToString().CString(), string.data));

    DstorePfreeExt(string.data);
    return ret;
}

void EarlyDeadLockPreventor::CheckForDeadLock()
{
    dlist_iter iter;
    LockMask aheadWaitingLocks = 0;
    LockRequestInterface *firstBlockingWaiter = nullptr;

    /*
     * We iterate queue of LockRequestInterfaces waiting for this lock.
     * And try to find the first LockRequestInterface in the waiting queue that is not
     * sent by the current thread and has conflicting mode with the LockRequestInterface
     * that the current thread owns.
     */
    dlist_foreach(iter, m_waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(iter.cur);
        if (HasConflictWithMask(waiter->GetLockMode(), m_myOwnedLocks)) {
            firstBlockingWaiter = waiter;
            break;
        }
        aheadWaitingLocks |= GetLockMask(waiter->GetLockMode());
    }

    /*
     * No waiter conflicts with me.
     */
    if (firstBlockingWaiter == nullptr) {
        return;
    }

    /*
     * Special case: if I find I should go in front of some waiter, check to
     * see if I conflict with already-held locks or the requests before that
     * waiter. If not, then just grant myself the requested lock immediately.
     */
    if (!HasConflictWithMask(m_requester->GetLockMode(), m_othersOwnedLocks) &&
        (!HasConflictWithMask(m_requester->GetLockMode(), aheadWaitingLocks))) {
        m_result = Result::PREVENT_BY_GRANT_LOCK;
        return;
    }

    /*
     * Must I wait for the waiter request?
     * Collect all owned lock types already held on this
     * lock object by the same backend of the input requester.
     * Then check do I have conflict with any of the owned lock types.
     */
    LockMask waiterOwnedLocks = 0;
    LockRequestSkipList::ListIterator waiterIter(m_grantedQueue, 0);
    for (LockRequestInterface *holder = waiterIter.GetNextRequest();
         holder != nullptr; holder = waiterIter.GetNextRequest()) {
        if (holder->IsSameRequester(firstBlockingWaiter)) {
            waiterOwnedLocks |= GetLockMask(holder->GetLockMode());
        }
    }

    /*
     * If I must wait for the waiter request, then we detect the early deadlock since
     * the thread owning the waiter request is sleeping and if I (the input request) am
     * inserted into the waiting queue and start sleeping then no one will wake up me or the
     * thread owning the waiter request. Hence we set up the error code to inform the
     * caller lock_acquire().
     */
    if (HasConflictWithMask(m_requester->GetLockMode(), waiterOwnedLocks)) {
        m_result = Result::CANT_PREVENT_DEADLOCK;
        if (STORAGE_FUNC_FAIL(ReportEarlyDeadLock(firstBlockingWaiter, waiterOwnedLocks))) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Out of memory when report early deadlock in %s.", m_lockTag->ToString().CString()));
        }
        return;
    }

    /*
     * If not, then we insert our input request before the request in the waiting queue,
     * so that I don't have to wait for the request, deadlock is prevented.
     */
    m_result = Result::PREVENT_BY_CUT_IN_LINE;
    m_cutInLinePosition = iter.cur;
    return;
}

EarlyDeadLockPreventor::Result EarlyDeadLockPreventor::GetResult() const
{
    return m_result;
}

dlist_node *EarlyDeadLockPreventor::GetCutInLinePosition()
{
    StorageAssert(m_result == Result::PREVENT_BY_CUT_IN_LINE);
    return m_cutInLinePosition;
}

void RecordBlockThread(dlist_node* node, LockErrorInfo *info)
{
    if (node == nullptr || info == nullptr) {
        return;
    }

    LockRequestInterface *preRequest = LockRequestInterface::GetLockRequestFromNode(node);
    if (likely(preRequest != nullptr)) {
        preRequest->RecordErrInfo(info);
        info->isHolder = false;
    }
}

/*
 * We return DSTORE_SUCC if immediate grant occurs and DSTORE_FAIL with
 * LOCK_ERROR_DEADLOCK set if early deadlock is found and DSTORE_FAIL
 * with LOCK_INFO_WAITING set if caller has to keep waiting
 */
RetStatus LockEntry::TryAddToWaiterQueue(LockRequestInterface *requester, LockRequestFreeList *freeList,
                                         LockErrorInfo *info)
{
    StorageAssert(requester != nullptr);
    EarlyDeadLockPreventor deadlockPreventor(&lockEntryCore.m_lockTag, &grantedQueue, &waitingQueue, requester);

    /* If a lock request is already in waiting queue, don't add it. */
    dlist_iter iter;
    dlist_foreach(iter, &waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(iter.cur);
        if (waiter->IsSameRequester(requester) && (waiter->GetLockMode() == requester->GetLockMode())) {
            freeList->Push(requester);
            goto EXIT;
        }
    }

    deadlockPreventor.Start();
    switch (deadlockPreventor.GetResult()) {
        case EarlyDeadLockPreventor::Result::CANT_PREVENT_DEADLOCK: {
            /*
             * If early deadlock is detected here, we simply return the failure and
             * LOCK_ERROR_DEADLOCK error code to the caller of Lock(). Caller will
             * decide if they want to retry or rollback.
             */
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Lock failed due to unpreventable deadlock."));
            freeList->Push(requester);
            storage_set_error(LOCK_ERROR_DEADLOCK);
            return DSTORE_FAIL;
        }
        case EarlyDeadLockPreventor::Result::PREVENT_BY_GRANT_LOCK: {
            AddToHolderQueue(requester, freeList);
            return DSTORE_SUCC;
        }
        case EarlyDeadLockPreventor::Result::PREVENT_BY_CUT_IN_LINE: {
            dlist_node *node = deadlockPreventor.GetCutInLinePosition();
            RecordBlockThread(DListHasPrev(&waitingQueue, node) ? DListPrevNode(&waitingQueue, node) : nullptr, info);
            DListInsertBefore(deadlockPreventor.GetCutInLinePosition(), requester->GetDlistNode());
            break;
        }
        case EarlyDeadLockPreventor::Result::NO_DEADLOCK: {
            RecordBlockThread(DListIsEmpty(&waitingQueue) ? nullptr : DListTailNode(&waitingQueue), info);
            DListPushTail(&waitingQueue, requester->GetDlistNode());
            break;
        }
        default: {
            StorageAssert(0);
            break;
        }
    }

    StorageAssert(static_cast<int>(requester->GetLockMode()) < static_cast<int>(DSTORE_LOCK_MODE_MAX));
    lockEntryCore.m_waitMask |= GetLockMask(requester->GetLockMode());
    lockEntryCore.m_waitingCnt[requester->GetLockMode()]++;
    lockEntryCore.m_waitingTotal++;
EXIT:
    storage_set_error(LOCK_INFO_WAITING);
    return DSTORE_FAIL;
}

void LockEntry::RemoveFromWaiterQueue(LockRequestInterface *requester)
{
    DListDelete(requester->GetDlistNode());
    lockEntryCore.m_waitingTotal--;
    lockEntryCore.m_waitingCnt[requester->GetLockMode()]--;
    if (lockEntryCore.m_waitingCnt[requester->GetLockMode()] == 0) {
        lockEntryCore.m_waitMask &= ~GetLockMask(requester->GetLockMode());
    }
}

void LockEntry::AdvanceWaitingQueue(LockRequestFreeList *freeList)
{
    dlist_mutable_iter iter;
    LockErrorInfo info;

    /* poll lock request from waiting queue, and try to grant lock to it */
    dlist_foreach_modify(iter, &waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(iter.cur);
        if (!(waiter->IsValid(lockEntryCore.m_lockTag.recoveryMode))) {
            RemoveFromWaiterQueue(waiter);
            freeList->Push(waiter);
            continue;
        }
        if (HasConflictWithAnyHolder(waiter, freeList, &info)) {
            break;
        }

        RemoveFromWaiterQueue(waiter);
        waiter->WakeUp();
        AddToHolderQueue(waiter, freeList);
    }
}

RetStatus LockEntry::EnqueueLockRequest(const LockRequestInterface *request, LockRequestFreeList *freeList,
    LockErrorInfo *info)
{
    LockRequestInterface *requester = freeList->PopACopyOf(request);
    if (unlikely(requester == nullptr)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    /*
     * If it doesn't conflict with held or previously requested locks, requester gets the lock.
     * Conflicting locks can be enqueued for recovery (FORCE_HOLD).
     */
    if ((!HasConflictWithAnyHolder(requester, freeList, info) &&
        !HasConflictWithMask(requester->GetLockMode(), lockEntryCore.m_waitMask)) ||
        requester->IsForceHold()) {
        AddToHolderQueue(requester, freeList);
        return DSTORE_SUCC;
    }

    if (requester->IsDontWait()) {
        freeList->Push(requester);
        storage_set_error(LOCK_INFO_NOT_AVAIL);
        return DSTORE_FAIL;
    }

    /* If waiting is needed, error code will be set inside TryAddToWaiterQueue(). */
    RetStatus ret = TryAddToWaiterQueue(requester, freeList, info);
    return ret;
}

bool LockEntry::DequeueLockRequest(const LockRequestInterface *requester, LockRequestFreeList *freeList)
{
    /* Remove the corresponding lock request from granted queue */
    RetStatus ret = RemoveFromHolderQueue(requester, freeList);
    if (STORAGE_FUNC_SUCC(ret)) {
        AdvanceWaitingQueue(freeList);
        return true;
    }

    /* If it isn't in granted queue, it might be in waiting queue, remove it */
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(iter.cur);
        if (waiter->IsSameRequester(requester) && (waiter->GetLockMode() == requester->GetLockMode())) {
            RemoveFromWaiterQueue(waiter);
            freeList->Push(waiter);
            AdvanceWaitingQueue(freeList);
            return true;
        }
    }

    ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Lock request not found in queue, dequeue fail."));
    return false;
}

/*
 * Determine whether lock entry is already held by the same requester in the same mode.
 */
bool LockEntry::IsHeldByRequester(const LockRequestInterface *requester)
{
    StorageAssert(requester != nullptr);
    const LockRequestInterface *holder = grantedQueue.SearchLockRequest(requester);
    return (holder != nullptr);
}

RetStatus LockEntry::DumpLockEntry(StringInfo str)
{
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("Lock description: \n  Lock tag: ");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    ret = lockEntryCore.m_lockTag.DescribeLockTag(str);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    ret = str->append(
        ".\n  Grant mask: 0x%x, wait mask: 0x%x, total granted: %u, total waiting: %u.\n  Granted count by mode: ",
        lockEntryCore.m_grantMask, lockEntryCore.m_waitMask, lockEntryCore.m_grantedTotal,
        lockEntryCore.m_waitingTotal);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    for (uint8 i = 1; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); ++i) {
        ret = str->append("%u", lockEntryCore.m_grantedCnt[i]);
        if (STORAGE_FUNC_SUCC(ret) && i + 1 < static_cast<uint8>(DSTORE_LOCK_MODE_MAX)) {
            ret = str->append_char('-');
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    ret = str->append("\n  Waiting count by mode: ");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    for (uint8 i = 1; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); ++i) {
        ret = str->append("%u", lockEntryCore.m_waitingCnt[i]);
        if (STORAGE_FUNC_SUCC(ret) && i + 1 < static_cast<uint8>(DSTORE_LOCK_MODE_MAX)) {
            ret = str->append_char('-');
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    ret = str->append("\nGranted queue:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    ret = grantedQueue.DumpSkipList(str);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = str->append("Waiting queue:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    bool found = false;
    dlist_iter iter;
    dlist_foreach(iter, &waitingQueue) {
        LockRequestInterface *waiter = LockRequestInterface::GetLockRequestFromNode(iter.cur);
        ret = str->append("    ");
        if (STORAGE_FUNC_FAIL(str->append("    ")) ||
            STORAGE_FUNC_FAIL(waiter->DumpLockRequest(str)) ||
            STORAGE_FUNC_FAIL(str->append(".\n"))) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
        found = true;
    }
    if (!found) {
        ret = str->append("    None.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

LockRequestFreeList::LockRequestFreeList(uint32 maxLength, DstoreMemoryContext ctx)
    : m_freeListLength(0),
      m_maxLength(maxLength),
      m_ctx(ctx)
{
    DListInit(&m_freeList);
}

void LockRequestFreeList::FreeAllMemory()
{
    while (!DListIsEmpty(&m_freeList)) {
        LockRequestInterface *mem = LockRequestInterface::GetLockRequestFromNode(DListPopHeadNode(&m_freeList));
        delete mem;
    }
}

LockRequestInterface *LockRequestFreeList::PopACopyOf(const LockRequestInterface *request)
{
    LockRequestInterface *copy = nullptr;
    if (unlikely(DListIsEmpty(&m_freeList))) {
        copy = request->Duplicate(m_ctx);
    } else {
        copy = LockRequestInterface::GetLockRequestFromNode(DListPopHeadNode(&m_freeList));
        copy = request->Duplicate(copy);
        m_freeListLength--;
    }
    return copy;
}

void LockRequestFreeList::Push(LockRequestInterface *request)
{
    if (unlikely(m_freeListLength >= m_maxLength)) {
        delete request;
    } else {
        DListPushTail(&m_freeList, request->GetDlistNode());
        m_freeListLength++;
    }
}

uint32 LockRequestFreeList::GetCurLength()
{
    return m_freeListLength;
}

uint32 LockRequestFreeList::GetMaxLength()
{
    return m_maxLength;
}

LockRequestFreeLists::LockRequestFreeLists()
    : m_partitionNum(0),
      m_partitionFreeLists(nullptr),
      m_maxLength(0),
      m_ctx(nullptr)
{}

LockRequestFreeLists::~LockRequestFreeLists()
{
    m_partitionFreeLists = nullptr;
    m_ctx = nullptr;
}

RetStatus LockRequestFreeLists::BuildFreeLists(uint32 partitionNum, uint32 maxLength, DstoreMemoryContext ctx)
{
    StorageAssert(ctx->type == MemoryContextType::SHARED_CONTEXT);
    m_ctx = DstoreAllocSetContextCreate(ctx,
                                        "Lock request mem context",
                                        ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE,
                                        ALLOCSET_DEFAULT_MAXSIZE,
                                        MemoryContextType::SHARED_CONTEXT);
    if (STORAGE_VAR_NULL(m_ctx)) {
        return DSTORE_FAIL;
    }

    m_partitionNum = partitionNum;
    Size memLen = sizeof(LockRequestFreeList *) * m_partitionNum;
    m_partitionFreeLists = static_cast<LockRequestFreeList **>(DstoreMemoryContextAllocZero(m_ctx, memLen));
    if (STORAGE_VAR_NULL(m_partitionFreeLists)) {
        DestroyFreeLists();
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    m_maxLength = maxLength;
    for (uint32 i = 0; i < m_partitionNum; i++) {
        m_partitionFreeLists[i] = DstoreNew(m_ctx) LockRequestFreeList(m_maxLength, m_ctx);
        if (STORAGE_VAR_NULL(m_partitionFreeLists[i])) {
            DestroyFreeLists();
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void LockRequestFreeLists::DestroyFreeLists()
{
    if (m_partitionFreeLists != nullptr) {
        for (uint32 i = 0; i < m_partitionNum; i++) {
            if (m_partitionFreeLists[i] == nullptr) {
                continue;
            }
            m_partitionFreeLists[i]->FreeAllMemory();
            delete m_partitionFreeLists[i];
        }
        DstorePfree(m_partitionFreeLists);
        m_partitionFreeLists = nullptr;
    }

    if (m_ctx != nullptr) {
        DstoreMemoryContextDelete(m_ctx);
        m_ctx = nullptr;
    }

    m_partitionNum = 0;
    m_maxLength = 0;
}

} /* namespace DSTORE */
