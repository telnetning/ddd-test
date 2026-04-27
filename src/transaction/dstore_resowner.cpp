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
 * dstore_resowner.cpp
 *
 *
 * IDENTIFICATION
 *        storage/src/common/dstore_resowner.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "transaction/dstore_resowner.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "port/dstore_port.h"

namespace DSTORE {

void LockResource::LockResourceRecord::InitRecord(const LockTag& lockTag, LockMode mode, LockMgrType mgrType)
{
    m_tag.lockTag = lockTag;
    m_tag.lockMode = mode;
    m_tag.lockMgrType = mgrType;
    m_tag.pad = 0;
    m_totalLockCnt = 0;
    m_subResCnt = 1;
    DListInit(&m_extendSubLockResList);
    m_currentSubRes = &m_defaultSubLockRes;
    m_currentSubResIndex = -1;
}

RetStatus LockResource::LockResourceRecord::StartNewSubResourceIfNeeded(DstoreMemoryContext ctx, uint32 subResId)
{
    /* If the current sub resource ID is equal to new sub resource ID, then current sub resource is usable. */
    if (!IsCurrentEmpty() && (m_currentSubRes->subResId[m_currentSubResIndex] >= subResId)) {
        return DSTORE_SUCC;
    }

    /* If there is not enough space, we need to expand the space to store it. */
    if (IsCurrentFull()) {
        SubLockResourceBlock *newRes = static_cast<SubLockResourceBlock *>
            (DstoreMemoryContextAlloc(ctx, sizeof(SubLockResourceBlock)));
        if (unlikely(newRes == nullptr)) {
            storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
        DListNodeInit(&(newRes->nodeInList));
        DListPushHead(&m_extendSubLockResList, &(newRes->nodeInList));
        m_currentSubRes = newRes;
        m_currentSubResIndex = -1;
        m_subResCnt++;
    }

    m_currentSubResIndex++;
    m_currentSubRes->subResId[m_currentSubResIndex] = subResId;
    m_currentSubRes->cnt[m_currentSubResIndex] = 0;
    return DSTORE_SUCC;
}

RetStatus LockResource::LockResourceRecord::AddLock(DstoreMemoryContext ctx, SubLockResourceID subResId)
{
    if (STORAGE_FUNC_FAIL(StartNewSubResourceIfNeeded(ctx, subResId))) {
        return DSTORE_FAIL;
    }

    m_currentSubRes->cnt[m_currentSubResIndex]++;
    m_totalLockCnt++;
    return DSTORE_SUCC;
}

LockResource::LockResourceRecord::SubLockResourceBlock *LockResource::LockResourceRecord::GetSubLockResBlockFromNode(
    dlist_node *node)
{
    return static_cast<SubLockResourceBlock *>(dlist_container(SubLockResourceBlock, nodeInList, node));
}

void LockResource::LockResourceRecord::RemoveCurrentSubResource()
{
    StorageAssert(m_subResCnt > 1);
    StorageAssert(m_currentSubRes != &m_defaultSubLockRes);
    SubLockResourceBlock *nextCurrent;
    if (DListHasNext(&m_extendSubLockResList, &m_currentSubRes->nodeInList)) {
        dlist_node *node = DListNextNode(&m_extendSubLockResList, &m_currentSubRes->nodeInList);
        nextCurrent = GetSubLockResBlockFromNode(node);
    } else {
        nextCurrent = &m_defaultSubLockRes;
    }

    DListDelete(&m_currentSubRes->nodeInList);
    DstorePfree(m_currentSubRes);
    m_currentSubRes = nextCurrent;
    SetCurrentFull();
    m_subResCnt--;
}

void LockResource::LockResourceRecord::RemoveLatestLock()
{
    StorageAssert(m_currentSubRes->cnt[m_currentSubResIndex] > 0);
    m_currentSubRes->cnt[m_currentSubResIndex]--;
    m_totalLockCnt--;
    if (m_currentSubRes->cnt[m_currentSubResIndex] != 0) {
        return;
    }

    m_currentSubResIndex--;
    if (IsCurrentEmpty() && (m_subResCnt > 1)) {
        RemoveCurrentSubResource();
    }
}

static void BatchReleaseLocks(const LockTag *lockTag, LockMode mode, LockMgrType mgrType, uint32 releaseCnt)
{
    switch (mgrType) {
        case LOCK_MGR: {
            for (uint32 i = 0; i < releaseCnt; i++) {
                g_storageInstance->GetLockMgr()->Unlock(lockTag, mode);
            }
            break;
        }
        case TABLE_LOCK_MGR: {
            g_storageInstance->GetTableLockMgr()->BatchUnlock(lockTag, mode, releaseCnt);
            break;
        }
        case XACT_LOCK_MGR:
        case LOCK_MGR_TYPE_MAX:
        default: {
            StorageAssert(0);
            break;
        }
    }
}

uint32 LockResource::LockResourceRecord::RemoveLocksAfter(SubLockResourceID subResId)
{
    bool foundAll = false;
    uint32 removeCnt = 0;
    while (!IsCurrentEmpty() && !foundAll) {
        for (int32 i = m_currentSubResIndex; i >= 0; i--) {
            if (m_currentSubRes->subResId[i] >= subResId) {
                removeCnt += m_currentSubRes->cnt[i];
                m_currentSubResIndex--;
            } else {
                foundAll = true;
                break;
            }
        }

        if ((m_subResCnt > 1) && !foundAll) {
            RemoveCurrentSubResource();
        }
    }

    StorageAssert(m_totalLockCnt >= removeCnt);
    m_totalLockCnt -= removeCnt;
    return removeCnt;
}

uint32 LockResource::LockResourceRecord::RemoveAll()
{
    uint32 removeCnt = 0;
    while (!IsCurrentEmpty()) {
        for (int32 i = m_currentSubResIndex; i >= 0; i--) {
            removeCnt += m_currentSubRes->cnt[i];
            m_currentSubResIndex--;
        }

        if (m_subResCnt > 1) {
            RemoveCurrentSubResource();
        }
    }

    StorageAssert(m_totalLockCnt == removeCnt);
    m_totalLockCnt = 0;
    return removeCnt;
}

void LockResource::LockResourceRecord::CleanUp()
{
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_extendSubLockResList) {
        SubLockResourceBlock *block = GetSubLockResBlockFromNode(iter.cur);
        DListDelete(iter.cur);
        DstorePfree(block);
    }
    m_totalLockCnt = 0;
    m_subResCnt = 1;
    m_currentSubRes = &m_defaultSubLockRes;
    m_currentSubResIndex = -1;
}

struct TempRecordItem {
    LockResource::SubLockResourceID resId;
    uint32 cnt;
};

void MergeToTail(TempRecordItem *items, uint32 &itemsCnt, UNUSE_PARAM uint32 maxItemsCnt,
                 LockResource::SubLockResourceID resId, uint32 cnt)
{
    if (itemsCnt == 0 || items[itemsCnt - 1].resId != resId) {
        itemsCnt++;
        StorageAssert(maxItemsCnt >= itemsCnt);
        items[itemsCnt - 1].resId = resId;
        items[itemsCnt - 1].cnt = 0;
    }
    items[itemsCnt - 1].cnt += cnt;
}

RetStatus LockResource::LockResourceRecord::MergeRecord(DstoreMemoryContext ctx,
                                                        LockResource::LockResourceRecord *record)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION)};

    uint32 itemsCnt = 0;
    uint32 maxItemsCnt = (m_subResCnt + record->m_subResCnt) * SubLockResourceBlock::defaultSubResNum;
    TempRecordItem *items = static_cast<TempRecordItem *>(DstorePalloc0(sizeof(TempRecordItem) * maxItemsCnt));
    if (STORAGE_VAR_NULL(items)) {
        return DSTORE_FAIL;
    }

    /* Merge the two sub resource list. */
    SubLockResourceIter iter1(this);
    SubLockResourceIter iter2(record);
    while (!iter1.IsEnd() && !iter2.IsEnd()) {
        if (iter1.GetSubResourceID() < iter2.GetSubResourceID()) {
            MergeToTail(items, itemsCnt, maxItemsCnt, iter1.GetSubResourceID(), iter1.GetSubResourceCnt());
            iter1.Next();
        } else {
            MergeToTail(items, itemsCnt, maxItemsCnt, iter2.GetSubResourceID(), iter2.GetSubResourceCnt());
            iter2.Next();
        }
    }

    while (!iter1.IsEnd()) {
        MergeToTail(items, itemsCnt, maxItemsCnt, iter1.GetSubResourceID(), iter1.GetSubResourceCnt());
        iter1.Next();
    }

    while (!iter2.IsEnd()) {
        MergeToTail(items, itemsCnt, maxItemsCnt, iter2.GetSubResourceID(), iter2.GetSubResourceCnt());
        iter2.Next();
    }

    /* Write merged sub resource list to a temporary record. */
    LockResourceRecord tempRecord;
    tempRecord.InitRecord(m_tag.lockTag, m_tag.lockMode, m_tag.lockMgrType);
    for (uint32 i = 0; i < itemsCnt; i++) {
        for (uint32 cnt = 0; cnt < items[i].cnt; cnt++) {
            if (STORAGE_FUNC_FAIL(tempRecord.AddLock(ctx, items[i].resId))) {
                tempRecord.CleanUp();
                DstorePfree(items);
                return DSTORE_FAIL;
            }
        }
    }

    /* Copy the temporary record into this record. */
    CleanUp();
    *this = tempRecord;
    DListInit(&m_extendSubLockResList);
    if (m_currentSubRes == &tempRecord.m_defaultSubLockRes) {
        m_currentSubRes = &m_defaultSubLockRes;
    }

    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &tempRecord.m_extendSubLockResList) {
        DListDelete(iter.cur);
        DListPushTail(&m_extendSubLockResList, iter.cur);
    }

    DstorePfree(items);
    return DSTORE_SUCC;
}

LockResource::LockResourceRecord::SubLockResourceIter::SubLockResourceIter(LockResourceRecord *resRecord)
    : m_record(resRecord),
      m_currentSubRes(&resRecord->m_defaultSubLockRes),
      m_resIndex(0)
{}

void LockResource::LockResourceRecord::SubLockResourceIter::Next()
{
    StorageAssert(!IsEnd());
    if (m_resIndex != (SubLockResourceBlock::defaultSubResNum - 1) || m_currentSubRes == m_record->m_currentSubRes) {
        m_resIndex++;
        return;
    }

    dlist_node *node = nullptr;
    if (m_currentSubRes == &m_record->m_defaultSubLockRes) {
        StorageAssert(!DListIsEmpty(&m_record->m_extendSubLockResList));
        node = DListTailNode(&m_record->m_extendSubLockResList);
    } else {
        StorageAssert(DListHasPrev(&m_record->m_extendSubLockResList, &m_currentSubRes->nodeInList));
        node = DListPrevNode(&m_record->m_extendSubLockResList, &m_currentSubRes->nodeInList);
    }

    m_currentSubRes = LockResourceRecord::GetSubLockResBlockFromNode(node);
    m_resIndex = 0;
}

bool LockResource::LockResourceRecord::SubLockResourceIter::IsEnd() const
{
    if (m_currentSubRes == m_record->m_currentSubRes && m_resIndex > m_record->m_currentSubResIndex) {
        return true;
    }
    return false;
}

LockResource::SubLockResourceID LockResource::LockResourceRecord::SubLockResourceIter::GetSubResourceID() const
{
    return m_currentSubRes->subResId[m_resIndex];
}

uint32 LockResource::LockResourceRecord::SubLockResourceIter::GetSubResourceCnt() const
{
    return m_currentSubRes->cnt[m_resIndex];
}

LockResource::LockResource() : m_ctx(nullptr),
                               m_resTable(nullptr),
                               m_subLockResId(FIRST_SUB_LOCK_RES_ID),
                               m_lazyLockCnt(0)
{
    for (uint32 i = 0; i < LAZY_LOCK_SLOT_MAX; i++) {
        m_lazyLockSlotLinePos[i] = 0;
        m_lazyLockSlotActualPos[i] = 0;
    }
}

LockResource::~LockResource()
{
    m_resTable = nullptr;
    m_ctx = nullptr;
}

RetStatus LockResource::Initialize(DstoreMemoryContext ctx)
{
    static constexpr int tableSize = 256;
    HASHCTL info;
    info.keysize = sizeof(LockResourceTag);
    info.entrysize = sizeof(LockResourceRecord);
    info.hash = tag_hash;
    info.hcxt = ctx;
    m_ctx = ctx;

    m_resTable = hash_create("lock resource hash", tableSize, &info, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    if (m_resTable == nullptr) {
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    LockTag dummyTag;
    for (uint32 i = 0; i < LAZY_LOCK_SLOT_MAX; i++) {
        m_lazyLockSlots[i].InitRecord(dummyTag, DSTORE_NO_LOCK, TABLE_LOCK_MGR);
    }
    m_lazyLockCnt = 0;

    return DSTORE_SUCC;
}

void LockResource::Destroy()
{
    if (m_resTable != nullptr) {
        hash_destroy(m_resTable);
        m_resTable = nullptr;
    }
}

LockResource::LockResourceRecord *LockResource::FindOrCreateResource(const LockTag& tag,
                                                                     LockMode mode, LockMgrType mgrType)
{
    LockResourceTag resTag{tag, mode, mgrType, 0};
    uint32 hashCode = get_hash_value(m_resTable, static_cast<const void *>(&resTag));
    bool found = false;
    LockResourceRecord *lock = static_cast<LockResourceRecord *>(hash_search_with_hash_value(m_resTable,
        static_cast<const void *>(&resTag), hashCode, HASH_ENTER, &found));
    if (unlikely(lock == nullptr)) {
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        return lock;
    }

    if (!found) {
        lock->InitRecord(tag, mode, mgrType);
    }
    return lock;
}

LockResource::LockResourceRecord *LockResource::FindResource(const LockTag& tag, LockMode mode, LockMgrType mgrType)
{
    LockResourceTag resTag{tag, mode, mgrType, 0};
    uint32 hashCode = get_hash_value(m_resTable, static_cast<const void *>(&resTag));

    bool found = false;
    return static_cast<LockResourceRecord *>(hash_search_with_hash_value(m_resTable,
                                                                         static_cast<const void *>(&resTag),
                                                                         hashCode, HASH_FIND, &found));
}

void LockResource::DeleteResource(LockResource::LockResourceRecord *resEntry)
{
    const void *resTag = resEntry->GetHashTag();
    uint32 hashCode = get_hash_value(m_resTable, resTag);

    bool found = false;
    (void)hash_search_with_hash_value(m_resTable, resTag, hashCode, HASH_REMOVE, &found);
}

RetStatus LockResource::RememberLock(const LockTag& tag, LockMode mode, LockMgrType mgrType)
{
    LockResourceRecord *lock = FindOrCreateResource(tag, mode, mgrType);
    if (unlikely(lock == nullptr)) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(lock->AddLock(m_ctx, m_subLockResId))) {
        if (lock->IsEmpty()) {
            DeleteResource(lock);
        }
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

bool LockResource::IsLockExist(const LockTag& tag, LockMode mode, LockMgrType mgrType)
{
    if (mode <= DSTORE_ROW_EXCLUSIVE_LOCK) {
        LockTagCache tagCache(&tag);
        uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
        uint32 hashCode = GetLazyLockSlotHash(partId, mode);
        if (m_lazyLockSlots[hashCode].IsMatch(tag, mode)) {
            return true;
        }
    }

    LockResourceRecord *lock = FindResource(tag, mode, mgrType);
    if (unlikely(lock == nullptr)) {
        return false;
    }
    return true;
}

/*
 * Normally this function is not supposed to be called,
 * the transaction will release all locks when commit/abort.
 */
void LockResource::ForgetLock(const LockTag& tag, LockMode mode, LockMgrType mgrType)
{
    LockResourceRecord *lock = FindResource(tag, mode, mgrType);
    StorageAssert(lock != nullptr);

    lock->RemoveLatestLock();
    if (lock->IsEmpty()) {
        DeleteResource(lock);
    }
}

uint32 LockResource::GetLockCnt(const LockTag &tag, LockMode mode, LockMgrType mgrType)
{
    uint32 cnt = 0;
    LockResourceRecord *lock = FindResource(tag, mode, mgrType);
    if (lock != nullptr) {
        cnt += lock->GetTotalCnt();
    }

    if ((!g_storageInstance->GetGuc()->enableLazyLock) || (mode > DSTORE_ROW_EXCLUSIVE_LOCK)) {
        return cnt;
    }

    LockTagCache tagCache(&tag);
    uint32 hashCode = GetLazyLockSlotHash(GetLazyLockPartIdFromTagHash(tagCache.hashCode), mode);
    if (!m_lazyLockSlots[hashCode].IsMatch(tag, mode)) {
        return cnt;
    }

    return (cnt + m_lazyLockSlots[hashCode].GetTotalCnt());
}

bool LockResource::HasRememberedLock()
{
    return (hash_get_num_entries(m_resTable) > 0);
}

LockResource::SubLockResourceID LockResource::GenerateSubLockResourceID()
{
    m_subLockResId++;
    return m_subLockResId;
}

void LockResource::ReleaseLocksAcquiredAfter(SubLockResourceID resId)
{
    HASH_SEQ_STATUS status;
    LockResourceRecord *lock = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lock = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        uint32 releaseCnt = lock->RemoveLocksAfter(resId);
        if (releaseCnt > 0) {
            BatchReleaseLocks(lock->GetLockTag(), lock->GetLockMode(), lock->GetLockMgrType(), releaseCnt);
        }
        if (lock->IsEmpty()) {
            (void)hash_search(m_resTable, lock->GetHashTag(), HASH_REMOVE, nullptr);
        }
    }

    if (g_storageInstance->GetGuc()->enableLazyLock) {
        ReleaseLazyLocksAcquiredAfter(resId);
    }
}

bool LockResource::CheckForLockLeaks(const char *action)
{
    if (likely(!HasRememberedLock())) {
        return false;
    }
    StringInfo leakWarning = StringInfoData::make();
    if (STORAGE_VAR_NULL(leakWarning)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Lock memory allcote fail, leakWarning is nullptr."));
        return true;
    }

    HASH_SEQ_STATUS status;
    LockResourceRecord *lockRecord = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lockRecord = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        if (STORAGE_FUNC_FAIL(leakWarning->append("Leak lock tag: ")) ||
            STORAGE_FUNC_FAIL(lockRecord->GetLockTag()->DescribeLockTag(leakWarning)) ||
            STORAGE_FUNC_FAIL(leakWarning->append(", mode: %d", static_cast<int>(lockRecord->GetLockMode()))) ||
            STORAGE_FUNC_FAIL(leakWarning->append(", mgrType: %d", static_cast<int>(lockRecord->GetLockMgrType()))) ||
            STORAGE_FUNC_FAIL(leakWarning->append(", count: %u.", lockRecord->GetTotalCnt()))) {
            ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory for CheckForLockLeaks."));
            break;
        }
    }

    ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("lock leak in %s: %s", action, leakWarning->data));

    DstorePfreeExt(leakWarning->data);
    DstorePfreeExt(leakWarning);
    return true;
}

bool LockResource::HasLockInMode(LockMode mode)
{
    HASH_SEQ_STATUS status;
    LockResourceRecord *lock = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lock = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        if (lock->GetLockMode() == mode) {
            return true;
        }
    }
    /* We don't need to check lazy lock in this interface yet, because it's only for lockmode 8. */
    return false;
}

void LockResource::ReleaseAllLocks()
{
    HASH_SEQ_STATUS status;
    LockResourceRecord *lock = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lock = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        uint32 releaseCnt = lock->RemoveAll();
        BatchReleaseLocks(lock->GetLockTag(), lock->GetLockMode(), lock->GetLockMgrType(), releaseCnt);
        StorageAssert(lock->IsEmpty());
        (void)hash_search(m_resTable, lock->GetHashTag(), HASH_REMOVE, nullptr);
    }

    m_subLockResId = FIRST_SUB_LOCK_RES_ID;
    if (g_storageInstance->GetGuc()->enableLazyLock) {
        ReleaseAllLazyLocks();
    }
}

void LockResource::ReleaseAllLocksByType(LockTagType tagType)
{
    HASH_SEQ_STATUS status;
    LockResourceRecord *lock = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lock = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        if (lock->GetLockTag()->lockTagType != tagType) {
            continue;
        }
        uint32 releaseCnt = lock->RemoveAll();
        BatchReleaseLocks(lock->GetLockTag(), lock->GetLockMode(), lock->GetLockMgrType(), releaseCnt);
        StorageAssert(lock->IsEmpty());
        (void)hash_search(m_resTable, lock->GetHashTag(), HASH_REMOVE, nullptr);
    }
    /* We don't need to support lazy lock for this interface yet. */
}

void LockResource::ReleaseAllLocksExceptTypes(const LockTagType *tagTypes, size_t typeNum)
{
    HASH_SEQ_STATUS status;
    LockResourceRecord *lock = nullptr;

    hash_seq_init(&status, m_resTable);
    while ((lock = static_cast<LockResourceRecord *>(hash_seq_search(&status))) != nullptr) {
        bool ignore = false;
        for (size_t i = 0; i < typeNum; i++) {
            if (lock->GetLockTag()->lockTagType == tagTypes[i]) {
                ignore = true;
                break;
            }
        }
        if (ignore) {
            continue;
        }
        uint32 releaseCnt = lock->RemoveAll();
        BatchReleaseLocks(lock->GetLockTag(), lock->GetLockMode(), lock->GetLockMgrType(), releaseCnt);
        StorageAssert(lock->IsEmpty());
        (void)hash_search(m_resTable, lock->GetHashTag(), HASH_REMOVE, nullptr);
    }
    /* We don't need to support lazy lock for this interface yet. */
}

static constexpr uint64 LAZY_LOCK_POTENTIAL_CONFLICT = 1UL << 63;
class LazyLockConflictHint {
public:
    LazyLockConflictHint() noexcept;

    class AutoLock {
    public:
        explicit AutoLock(LazyLockConflictHint *conflictHint) : m_conflictHint(conflictHint)
        {
            DstoreLWLockAcquire(&conflictHint->m_conflictHintlock, LW_EXCLUSIVE);
        }
        ~AutoLock()
        {
            LWLockRelease(&m_conflictHint->m_conflictHintlock);
            m_conflictHint = nullptr;
        }
    private:
        LazyLockConflictHint *m_conflictHint;
    };

    struct LazyLockConflictRequest {
        dlist_node listNode;
        LockTag lockTag;
        LockRequestInterface *lockRequest;

        static LazyLockConflictRequest *GetRequestFromNode(dlist_node *node);
    };

    RetStatus AddRequest(uint32 partId, const LockTag &lockTag, const LockRequestInterface *lockRequest);
    LazyLockConflictRequest *FindRequest(uint32 partId, const LockTag &lockTag,
                                         const LockRequestInterface *lockRequest);
    void DeleteRequest(LazyLockConflictRequest *request) const;
    void WakeupAllRequests(uint32 partId);
    bool IsNoRequest(uint32 partId);
    void RemoveInvalidRequests();
    bool IsWaiting(uint32 partId) const;
    void SetWaiting(uint32 partId, bool isWaiting);
    RetStatus DumpByLockTag(uint32 partId, const LockTag &lockTag, StringInfo str);

private:
    LWLock     m_conflictHintlock;
    dlist_head m_lockRequestList[LazyLockHint::LAZY_LOCK_HINT_PART_CNT];
    bool       m_isWaiting[LazyLockHint::LAZY_LOCK_HINT_PART_CNT];
};
static LazyLockConflictHint g_lazyLockConflictHint;

void LazyLockHint::Initialize(uint32 partId)
{
    m_localCnt = 0;
    uint64 hint = g_lazyLockConflictHint.IsNoRequest(partId) ? 0 : LAZY_LOCK_POTENTIAL_CONFLICT;
    GsAtomicInitU64(&m_lazyLockHintBits, hint);
}

bool LazyLockHint::IncreaseLazyLockCnt()
{
    uint64 expected = m_localCnt;
    bool succ = GsAtomicCompareExchangeU64(&m_lazyLockHintBits, &expected, m_localCnt + 1);
    if (unlikely(!succ)) {
        StorageAssert(expected & LAZY_LOCK_POTENTIAL_CONFLICT);
        return false;
    }

    m_localCnt++;
    return true;
}

bool LazyLockHint::DecreaseLazyLockCnt(uint32 decreaseCnt, bool &hasConflict)
{
    if (m_localCnt == 0) {
        return false;
    }

    StorageAssert(m_localCnt >= decreaseCnt);
    m_localCnt -= decreaseCnt;
    uint64 hint = GsAtomicSubFetchU64(&m_lazyLockHintBits, decreaseCnt);
    if (unlikely(hint & LAZY_LOCK_POTENTIAL_CONFLICT)) {
        hasConflict = true;
    }

    StorageAssert((hint & ~LAZY_LOCK_POTENTIAL_CONFLICT) == m_localCnt);
    return true;
}

uint64 LazyLockHint::DisableLazyLock()
{
    uint64 ret = GsAtomicFetchOrU64(&m_lazyLockHintBits, LAZY_LOCK_POTENTIAL_CONFLICT);
    return (ret & ~LAZY_LOCK_POTENTIAL_CONFLICT);
}

void LazyLockHint::EnableLazyLock()
{
    (void)GsAtomicFetchSubU64(&m_lazyLockHintBits, LAZY_LOCK_POTENTIAL_CONFLICT);
}

bool LazyLockHint::IsLazyLockEnabled()
{
    return ((GsAtomicReadU64(&m_lazyLockHintBits) & LAZY_LOCK_POTENTIAL_CONFLICT) == 0UL);
}

uint64 LazyLockHint::GetLazyLockCnt()
{
    return (GsAtomicReadU64(&m_lazyLockHintBits) & ~LAZY_LOCK_POTENTIAL_CONFLICT);
}

LazyLockConflictHint::LazyLockConflictHint() noexcept
{
    LWLockInitialize(&m_conflictHintlock, LWLOCK_GROUP_CONFLICT_HINT);
    for (uint32 partId = 0; partId < LazyLockHint::LAZY_LOCK_HINT_PART_CNT; partId++) {
        DListInit(&m_lockRequestList[partId]);
        m_isWaiting[partId] = false;
    }
}

bool LazyLockConflictHint::IsWaiting(uint32 partId) const
{
    return m_isWaiting[partId];
}

void LazyLockConflictHint::SetWaiting(uint32 partId, bool isWaiting)
{
    m_isWaiting[partId] = isWaiting;
}

LazyLockConflictHint::LazyLockConflictRequest *LazyLockConflictHint::LazyLockConflictRequest::GetRequestFromNode(
    dlist_node *node)
{
    return static_cast<LazyLockConflictRequest *>(dlist_container(LazyLockConflictRequest, listNode, node));
}

LazyLockConflictHint::LazyLockConflictRequest *LazyLockConflictHint::FindRequest(uint32 partId,
    const LockTag &lockTag, const LockRequestInterface *lockRequest)
{
    dlist_iter iter;
    dlist_foreach(iter, &m_lockRequestList[partId]) {
        LazyLockConflictRequest *request = LazyLockConflictRequest::GetRequestFromNode(iter.cur);
        if ((request->lockTag == lockTag) && (request->lockRequest->Compare(lockRequest) == 0)) {
            return request;
        }
    }
    return nullptr;
}

RetStatus LazyLockConflictHint::AddRequest(uint32 partId, const LockTag &lockTag,
    const LockRequestInterface *lockRequest)
{
    /* Alloc new node. */
    DstoreMemoryContext memCtx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    LazyLockConflictRequest *conflictNode = static_cast<LazyLockConflictRequest *>(
        DstoreMemoryContextAlloc(memCtx, sizeof(LazyLockConflictRequest)));
    if (unlikely(conflictNode == nullptr)) {
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    /* Copy request. */
    LockRequestInterface *newRequest = lockRequest->Duplicate(memCtx);
    if (unlikely(newRequest == nullptr)) {
        DstorePfree(conflictNode);
        storage_set_error(TRANSACTION_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    conflictNode->lockTag = lockTag;
    conflictNode->lockRequest = newRequest;
    DListPushHead(&m_lockRequestList[partId], &conflictNode->listNode);
    return DSTORE_SUCC;
}

void LazyLockConflictHint::DeleteRequest(LazyLockConflictRequest *request) const
{
    DListDelete(&request->listNode);
    delete request->lockRequest;
    DstorePfree(request);
}

void LazyLockConflictHint::WakeupAllRequests(uint32 partId)
{
    dlist_iter iter;
    dlist_foreach(iter, &m_lockRequestList[partId]) {
        LazyLockConflictRequest *request = LazyLockConflictRequest::GetRequestFromNode(iter.cur);
        StorageAssert(request != nullptr);
        request->lockRequest->WakeUp();
    }
    SetWaiting(partId, false);
}

bool LazyLockConflictHint::IsNoRequest(uint32 partId)
{
    return DListIsEmpty(&m_lockRequestList[partId]);
}

void LazyLockConflictHint::RemoveInvalidRequests()
{
    ThreadCoreMgr::ThreadIterator thrdIter(g_storageInstance->GetThreadCoreMgr());
    LazyLockConflictHint::AutoLock autoLock(this);
    for (uint32 partId = 0; partId < LazyLockHint::LAZY_LOCK_HINT_PART_CNT; partId++) {
        if (IsNoRequest(partId)) {
            continue;
        }

        dlist_mutable_iter iter;
        dlist_foreach_modify(iter, &m_lockRequestList[partId]) {
            LazyLockConflictRequest *request = LazyLockConflictRequest::GetRequestFromNode(iter.cur);
            if (!request->lockRequest->IsValid(request->lockTag.recoveryMode)) {
                DeleteRequest(request);
            }
        }

        if (!IsNoRequest(partId)) {
            continue;
        }

        ThreadCore *core = nullptr;
        while ((core = thrdIter.GetNextThreadCore()) != nullptr) {
            core->regularLockCtx->GetLazyLockHint(partId)->EnableLazyLock();
        }
    }
}

RetStatus LazyLockConflictHint::DumpByLockTag(uint32 partId, const LockTag &lockTag, StringInfo str)
{
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("  DDL status: %s. DDL lock requests:\n", IsWaiting(partId) ? "waiting" : "pass through");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    bool found = false;
    dlist_iter iter;
    dlist_foreach(iter, &m_lockRequestList[partId]) {
        LazyLockConflictRequest *request = LazyLockConflictRequest::GetRequestFromNode(iter.cur);
        if (request->lockTag == lockTag) {
            if (STORAGE_FUNC_FAIL(str->append("    ")) ||
                STORAGE_FUNC_FAIL(request->lockRequest->DumpLockRequest(str)) ||
                STORAGE_FUNC_FAIL(str->append(".\n"))) {
                storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
                return DSTORE_FAIL;
            }
            found = true;
        }
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

RetStatus LockResource::AsyncDisableLazyLockOnAllThreads(const LockTagCache &tagCache,
    const LockRequestInterface *lockRequest)
{
    /* Step 1: If the lock request already exist, skip process. */
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    LazyLockConflictHint::AutoLock autoLock(&g_lazyLockConflictHint);
    uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
    if (g_lazyLockConflictHint.FindRequest(partId, *tagCache.lockTag, lockRequest) != nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Same lock request found when adding conflicting lazy lock."));
        if (g_lazyLockConflictHint.IsWaiting(partId)) {
            storage_set_error(LOCK_INFO_WAITING);
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    /* Step 2: Add request into request list. */
    bool needSetWaitFlag = g_lazyLockConflictHint.IsNoRequest(partId);
    RetStatus ret = g_lazyLockConflictHint.AddRequest(partId, *tagCache.lockTag, lockRequest);
    if (STORAGE_FUNC_FAIL(ret)) {
        return ret;
    }

    if (needSetWaitFlag) {
        /* Step 3: Disable lazy lock on all threads. */
        bool needWait = false;
        ThreadCore *core = nullptr;
        while ((core = iter.GetNextThreadCore()) != nullptr) {
            uint64 lazyLockCnt = core->regularLockCtx->GetLazyLockHint(partId)->DisableLazyLock();
            if (lazyLockCnt > 0) {
                needWait = true;
            }
        }
        g_lazyLockConflictHint.SetWaiting(partId, needWait);
    }

    /* Step 4: No lazy lock exist on all threads, return success. */
    if (!g_lazyLockConflictHint.IsWaiting(partId)) {
        return DSTORE_SUCC;
    }

    /*
     * Step 5: Lazy lock exist on some threads, return fail because we have to wait
     * until they don't hold any lazy lock.
     */
    storage_set_error(LOCK_INFO_WAITING);
    return DSTORE_FAIL;
}

void LockResource::EnableLazyLockOnAllThreads(const LockTagCache &tagCache,
    const LockRequestInterface *lockRequest)
{
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    LazyLockConflictHint::AutoLock autoLock(&g_lazyLockConflictHint);
    uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
    LazyLockConflictHint::LazyLockConflictRequest *request;

    /* Step 1: If the lock request doesn't exist, return fail. */
    request = g_lazyLockConflictHint.FindRequest(partId, *tagCache.lockTag, lockRequest);
    if (request == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Lock request not found when release conflicting lazy lock."));
        return;
    }

    /* Step 2: Delete the request. */
    g_lazyLockConflictHint.DeleteRequest(request);
    if (!g_lazyLockConflictHint.IsNoRequest(partId)) {
        return;
    }

    /* Step 3: If there is no strong lock then enable lazy lock for the partition. */
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        core->regularLockCtx->GetLazyLockHint(partId)->EnableLazyLock();
    }
}

RetStatus LockResource::DumpLazyLockCntsByLockTag(const LockTagCache &tagCache, StringInfo str)
{
    RetStatus ret = DSTORE_SUCC;
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    LazyLockConflictHint::AutoLock autoLock(&g_lazyLockConflictHint);
    uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
    ret = str->append("Lazy lock summary for partition id %u:\n", partId);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    ret = g_lazyLockConflictHint.DumpByLockTag(partId, *tagCache.lockTag, str);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    ret = str->append("  DML lazy lock counts:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    bool found = false;
    ThreadCore *core = nullptr;
    while ((core = iter.GetNextThreadCore()) != nullptr) {
        uint64 cnt = core->regularLockCtx->GetLazyLockHint(partId)->GetLazyLockCnt();
        bool isEnabled = core->regularLockCtx->GetLazyLockHint(partId)->IsLazyLockEnabled();
        if ((cnt > 0) || (!isEnabled)) {
            ret = str->append("    Thread %lu [core index %u] %s lazy lock, has %lu lock(s).\n", core->pid,
                              core->selfIdx, isEnabled ? "enables" : "disables", cnt);
            if (STORAGE_FUNC_FAIL(ret)) {
                storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
                return DSTORE_FAIL;
            }
            found = true;
        }
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

void LockResource::TryActuallyAcquireLazyLocksOnCurrentThread()
{
    /* Step 1: Try push conflicting lazy locks on thread into lock manager. */
    RetStatus ret = thrd->ActuallyAcquireLazyLocksOnCurrentThread();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("Actually acquiring lazy lock on thread failed, %s.", StorageGetMessage()));
        return;
    }

    /* Step 2: Try wakeup strong requester if there is no lazy lock conflicts. */
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    LazyLockConflictHint::AutoLock autoLock(&g_lazyLockConflictHint);
    for (uint32 partId = 0; partId < LazyLockHint::LAZY_LOCK_HINT_PART_CNT; partId++) {
        if (g_lazyLockConflictHint.IsNoRequest(partId) || !g_lazyLockConflictHint.IsWaiting(partId)) {
            continue;
        }

        bool needWait = false;
        ThreadCore *core = nullptr;
        while ((core = iter.GetNextThreadCore()) != nullptr) {
            uint64 lazyLockCnt = core->regularLockCtx->GetLazyLockHint(partId)->GetLazyLockCnt();
            if (lazyLockCnt > 0) {
                needWait = true;
                break;
            }
        }

        if (!needWait) {
            g_lazyLockConflictHint.WakeupAllRequests(partId);
        }
    }
}

RetStatus LockResource::ActuallyAcquireLocksFromLazyLockSlot(LockResourceRecord *lazyLockSlot)
{
    const LockTag *lockTag = lazyLockSlot->GetLockTag();
    LockMgrType mgrType = lazyLockSlot->GetLockMgrType();
    uint32 totalCnt = lazyLockSlot->GetTotalCnt();
    LockMode mode = lazyLockSlot->GetLockMode();
    RetStatus ret = DSTORE_SUCC;
    LockErrorInfo info = {};

    for (uint32 acquired = 0; acquired < totalCnt; acquired++) {
        switch (mgrType) {
            case LOCK_MGR: {
                ret = g_storageInstance->GetLockMgr()->Lock(lockTag, mode, true, &info);
                break;
            }
            case TABLE_LOCK_MGR: {
                ret = g_storageInstance->GetTableLockMgr()->Lock(lockTag, mode, true, &info);
                break;
            }
            case XACT_LOCK_MGR:
            case LOCK_MGR_TYPE_MAX:
            default: {
                StorageAssert(0);
                break;
            }
        }

        if (STORAGE_FUNC_FAIL(ret)) {
            if (StorageGetErrorCode() != LOCK_INFO_NOT_AVAIL) {
                ErrLog(DSTORE_WARNING, MODULE_LOCK,
                    ErrMsg("Acquiring lazy lock %s, mode %s failed, error code=0x%llx, error message: %s.",
                        lazyLockSlot->GetLockTag()->ToString().CString(),
                        GetLockModeString(lazyLockSlot->GetLockMode()),
                        static_cast<unsigned long long>(StorageGetErrorCode()), StorageGetMessage()));
            }
            if (acquired > 0) {
                BatchReleaseLocks(lockTag, mode, mgrType, acquired);
            }
            return ret;
        }
    }

    LockResourceRecord *lockRecord = FindOrCreateResource(*lockTag, mode, mgrType);
    if (unlikely(lockRecord == nullptr)) {
        BatchReleaseLocks(lockTag, mode, mgrType, totalCnt);
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(lockRecord->MergeRecord(m_ctx, lazyLockSlot))) {
        BatchReleaseLocks(lockTag, mode, mgrType, totalCnt);
        if (lockRecord->IsEmpty()) {
            DeleteResource(lockRecord);
        }
        return DSTORE_FAIL;
    }

    lazyLockSlot->CleanUp();
    StorageAssert(lazyLockSlot->IsEmpty());
    StorageAssert(lazyLockSlot->GetTotalCnt() == 0);
    lazyLockSlot->SetInvalid();
    return ret;
}

void LockResource::AddSlotToLine(uint32 actualPos)
{
    m_lazyLockSlotLinePos[actualPos] = m_lazyLockCnt;
    m_lazyLockSlotActualPos[m_lazyLockCnt] = actualPos;
    m_lazyLockCnt++;
}

void LockResource::RemoveSlotFromLine(uint32 actualPos)
{
    m_lazyLockCnt--;
    if (m_lazyLockCnt > 0) {
        StorageAssert(m_lazyLockCnt < LAZY_LOCK_SLOT_MAX);
        uint32 linePos = m_lazyLockSlotLinePos[actualPos];
        uint32 lastActualPos = m_lazyLockSlotActualPos[m_lazyLockCnt];
        m_lazyLockSlotActualPos[linePos] = lastActualPos;
        m_lazyLockSlotLinePos[lastActualPos] = linePos;
    }
}

RetStatus LockResource::ActuallyAcquireLazyLocks()
{
    bool hasConflict = false;
    RetStatus ret = DSTORE_SUCC;
    LazyLockHint *lazyLockHint = nullptr;

    for (uint32 i = 0; i < m_lazyLockCnt; i++) {
        uint32 hash = m_lazyLockSlotActualPos[i];
        lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(hash / LAZY_LOCK_SLOT_MODES);

        /* Try to acquire the lock, and remove lazy lock record. */
        StorageAssert(!m_lazyLockSlots[hash].IsInvalid());
        uint32 totalCnt = m_lazyLockSlots[hash].GetTotalCnt();
        ret = ActuallyAcquireLocksFromLazyLockSlot(&m_lazyLockSlots[hash]);
        if (STORAGE_FUNC_FAIL(ret)) {
            break;
        }

        /* Decrease lazy lock count as well. */
        bool decreaseSucc = lazyLockHint->DecreaseLazyLockCnt(totalCnt, hasConflict);
        UNUSED_VARIABLE(decreaseSucc);
        StorageAssert(decreaseSucc);
    }

    /* Rearrange slot lines for next traversal. */
    m_lazyLockCnt = 0;
    if (STORAGE_FUNC_FAIL(ret)) {
        for (uint32 i = 0; i < LAZY_LOCK_SLOT_MAX; i++) {
            if (!m_lazyLockSlots[i].IsInvalid()) {
                AddSlotToLine(i);
            }
        }
    }
    return ret;
}

/**
 * Lazy lock acquisition should follow:
 * (1) If a strong lock exists, no thread should acquire a weak lock through lazy lock.
 * (2) All lazy locks are actually acquired before a strong lock starts to calculate lock conflicts.
 *
 * Side affect: In some cases the lazy lock does not set isAlreadyHeld = true, but
 * since this is to invalidate relcache, which is impossible to change when we already
 * hold the same lock, it should not affect the correctness of the SQL engine.
 */
RetStatus LockResource::LazyLock(const LockTag& tag, LockMode mode, LockMgrType mgrType, bool &isAlreadyHeld)
{
    StorageAssert(!isAlreadyHeld);

    /* Step 0: Lazy lock is only available for weak lock mode. */
    if (unlikely(mode > DSTORE_ROW_EXCLUSIVE_LOCK)) {
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
        return DSTORE_FAIL;
    }

    /* Step 1: Increase partitioned lazy lock count. If lock conflict exist then we give up the lazy lock. */
    LockTagCache tagCache(&tag);
    uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(partId);
    bool increaseSucc = lazyLockHint->IncreaseLazyLockCnt();
    if (unlikely(!increaseSucc)) {
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
        return DSTORE_FAIL;
    }

    /* Step 2: Record the lock in lazy lock slot. */
    RetStatus ret = DSTORE_FAIL;
    uint32 hashCode = GetLazyLockSlotHash(partId, mode);
    if (m_lazyLockSlots[hashCode].IsInvalid()) {
        m_lazyLockSlots[hashCode].InitRecord(tag, mode, mgrType);
        ret = m_lazyLockSlots[hashCode].AddLock(m_ctx, m_subLockResId);
        if (STORAGE_FUNC_SUCC(ret)) {
            AddSlotToLine(hashCode);
            return DSTORE_SUCC;
        }
        m_lazyLockSlots[hashCode].SetInvalid();
    } else if (m_lazyLockSlots[hashCode].IsMatch(tag, mode)) {
        ret = m_lazyLockSlots[hashCode].AddLock(m_ctx, m_subLockResId);
        if (STORAGE_FUNC_SUCC(ret)) {
            isAlreadyHeld = true;
            return DSTORE_SUCC;
        }
    }

    /* Step 3: If record failed, then rollback the lazy lock count. */
    bool hasConflict = false;
    bool decreaseSucc = lazyLockHint->DecreaseLazyLockCnt(1, hasConflict);
    StorageAssert(decreaseSucc);
    UNUSED_VARIABLE(decreaseSucc);
    return DSTORE_FAIL;
}

RetStatus LockResource::ReleaseLazyLock(const LockTag& tag, LockMode mode, UNUSE_PARAM LockMgrType mgrType)
{
    /* Step 0: Lazy lock is only available for weak lock mode. */
    if (unlikely(mode > DSTORE_ROW_EXCLUSIVE_LOCK)) {
        return DSTORE_FAIL;
    }

    /* Step 1: Check for lazy lock slot by hashcode, if we do have the lock record then continue releasing. */
    LockTagCache tagCache(&tag);
    uint32 partId = GetLazyLockPartIdFromTagHash(tagCache.hashCode);
    uint32 hashCode = GetLazyLockSlotHash(partId, mode);
    if (!m_lazyLockSlots[hashCode].IsMatch(tag, mode)) {
        return DSTORE_FAIL;
    }

    /* Step 2: Decrease partitioned lazy lock count, and check for potential lock conflict. */
    bool hasConflict = false;
    bool decreaseSucc = thrd->GetLockCtx()->GetLazyLockHint(partId)->DecreaseLazyLockCnt(1, hasConflict);
    if (unlikely(!decreaseSucc)) {
        return DSTORE_FAIL;
    }

    /* Step 3: Remove the lock from lock record. */
    m_lazyLockSlots[hashCode].RemoveLatestLock();
    if (m_lazyLockSlots[hashCode].IsEmpty()) {
        m_lazyLockSlots[hashCode].SetInvalid();
        RemoveSlotFromLine(hashCode);
    }

    /* Step 4: If there is a potential lock conflict, try push conflicting lazy locks into lock manager. */
    if (unlikely(hasConflict)) {
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
    }
    return DSTORE_SUCC;
}

void LockResource::ReleaseAllLazyLocks()
{
    bool hasConflict = false;

    /* Step 1: Traverse through all slots and remove lazy lock records. */
    for (uint32 i = 0; i < m_lazyLockCnt; i++) {
        uint32 hash = m_lazyLockSlotActualPos[i];
        StorageAssert(!m_lazyLockSlots[hash].IsInvalid());
        uint32 releaseCnt = m_lazyLockSlots[hash].RemoveAll();
        uint32 partId = GetLazyLockPartIdFromSlotHash(hash);
        bool conflict = false;
        bool decreaseSucc = thrd->GetLockCtx()->GetLazyLockHint(partId)->DecreaseLazyLockCnt(releaseCnt, conflict);
        hasConflict = conflict ? true : hasConflict;
        UNUSED_VARIABLE(decreaseSucc);
        StorageAssert(decreaseSucc);
        StorageAssert(m_lazyLockSlots[hash].IsEmpty());
        m_lazyLockSlots[hash].SetInvalid();
    }

    /* Step 2: If there is some potential lock conflict, try push conflicting lazy locks into lock manager. */
    m_lazyLockCnt = 0;
    if (unlikely(hasConflict)) {
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
    }
}

void LockResource::ReleaseLazyLocksAcquiredAfter(SubLockResourceID resId)
{
    bool hasConflict = false;

    /* Step 1: Traverse through all slots and remove lazy lock records larger than sub resource id. */
    for (uint32 i = 0; i < m_lazyLockCnt; i++) {
        uint32 hash = m_lazyLockSlotActualPos[i];
        StorageAssert(!m_lazyLockSlots[hash].IsInvalid());
        uint32 releaseCnt = m_lazyLockSlots[hash].RemoveLocksAfter(resId);
        uint32 partId = GetLazyLockPartIdFromSlotHash(hash);
        bool conflict = false;
        bool decreaseSucc = thrd->GetLockCtx()->GetLazyLockHint(partId)->DecreaseLazyLockCnt(releaseCnt, conflict);
        hasConflict = conflict ? true : hasConflict;
        UNUSED_VARIABLE(decreaseSucc);
        StorageAssert(decreaseSucc);
        if (m_lazyLockSlots[hash].IsEmpty()) {
            m_lazyLockSlots[hash].SetInvalid();
        }
    }

    /* Step 2: Rearrange slot lines for next traversal. */
    m_lazyLockCnt = 0;
    for (uint32 i = 0; i < LAZY_LOCK_SLOT_MAX; i++) {
        if (!m_lazyLockSlots[i].IsInvalid()) {
            AddSlotToLine(i);
        }
    }

    /* Step 3: If there is some potential lock conflict, try push conflicting lazy locks into lock manager. */
    if (unlikely(hasConflict)) {
        LockResource::TryActuallyAcquireLazyLocksOnCurrentThread();
    }
}

}  // namespace DSTORE
