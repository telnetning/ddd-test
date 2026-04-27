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
 * dstore_resowner.h
 *
 *
 * IDENTIFICATION
 *        storage/include/common/dstore_resowner.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_RESOWNER_H
#define DSTORE_DSTORE_RESOWNER_H

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_hsearch.h"
#include "transaction/dstore_transaction_types.h"
#include "lock/dstore_lock_thrd_local.h"

namespace DSTORE {

/*
 * ResourceOwner objects are an opaque data structure known only within
 * resowner.c.
 */
using ResourceOwner = struct ResourceOwnerData *;

class LockResource : public BaseObject {
public:
    LockResource();

    ~LockResource();
    DISALLOW_COPY_AND_MOVE(LockResource);

    RetStatus Initialize(DstoreMemoryContext ctx);
    void Destroy();

    /**
     * Two phase locking support:
     * After acquiring a lock in lock manager, remember the lock in res owner,
     * so that when the transaction commit/abort, the locks will be released all at once.
     */
    RetStatus RememberLock(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    void ForgetLock(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    bool IsLockExist(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    bool HasLockInMode(LockMode mode);
    uint32 GetLockCnt(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    void ReleaseAllLocks();
    void ReleaseAllLocksByType(LockTagType tagType);
    void ReleaseAllLocksExceptTypes(const LockTagType *tagTypes, size_t typeNum);
    bool HasRememberedLock();

    /**
     * Lazy lock feature:
     * The resource owner states that it owns the lock, but in reality it will wait until someone
     * needs the potentially conflicting lock before going to the lock manager to acquire the lock.
     * Therefore, when the lazy lock is open, strong lock acquisition will require additional waiting.
     */
    RetStatus LazyLock(const LockTag& tag, LockMode mode, LockMgrType mgrType, bool &isAlreadyHeld);
    RetStatus ReleaseLazyLock(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    RetStatus ActuallyAcquireLazyLocks();
    static RetStatus AsyncDisableLazyLockOnAllThreads(const LockTagCache &tagCache,
                                                      const class LockRequestInterface *lockRequest);
    static void EnableLazyLockOnAllThreads(const LockTagCache &tagCache, const LockRequestInterface *lockRequest);
    static void TryActuallyAcquireLazyLocksOnCurrentThread();
    static RetStatus DumpLazyLockCntsByLockTag(const LockTagCache &tagCache, StringInfo str);

    /**
     * Sub lock resource ID:
     * Conceptually, a lock resource is divided into several sub-lock resources with increasing IDs,
     * and every lock will be remembered along with the ID.
     * Each savepoint will generate a new sub lock res id, and the transaction will be able to rollback
     * to the specific savepoint by releasing all locks acquired after that res id.
     */
    using SubLockResourceID = uint32;
    SubLockResourceID GenerateSubLockResourceID();

    void ReleaseLocksAcquiredAfter(SubLockResourceID resId);
    bool CheckForLockLeaks(const char *action);

#ifndef UT
private:
#endif
    struct LockResourceTag {
        LockTag     lockTag;
        LockMode    lockMode;
        LockMgrType lockMgrType;
        uint16      pad;

        bool operator==(const LockResourceTag &tag) const
        {
            return ((LockTag::HashCompareFunc(&lockTag, &tag.lockTag, sizeof(LockTag)) == 0) &&
                    (lockMode == tag.lockMode));
        }
    };
    static_assert(sizeof(LockResourceTag) == 28, "LockResourceTag is possibly not a tight structure.");

    /**
     * In each LockResourceRecord, current sub resource always point to refcount with
     * the latest sub IDs, while actual refcounts are stored in the default block or extend list.
     *  extend sub res list: {sub res 25-32}->{sub res 17-24}->{sub res 9-16}
     *  default sub res: {sub res 1-8}
     *  current ptr->{sub res 25-32}
     */
    class LockResourceRecord {
    public:
        void InitRecord(const LockTag& lockTag, LockMode mode, LockMgrType mgrType);

        RetStatus AddLock(DstoreMemoryContext ctx, SubLockResourceID subResId);
        void RemoveLatestLock();

        uint32 RemoveLocksAfter(SubLockResourceID subResId);
        uint32 RemoveAll();

        RetStatus MergeRecord(DstoreMemoryContext ctx, LockResourceRecord *record);
        void CleanUp();

        inline bool IsEmpty() const
        {
            return ((m_subResCnt == 1) && (m_currentSubResIndex == -1));
        }

        inline const void *GetHashTag() const
        {
            return static_cast<const void *>(&m_tag);
        }

        inline uint32 GetTotalCnt() const
        {
            return m_totalLockCnt;
        }

        inline void SetInvalid()
        {
            m_tag.lockTag.SetInvalid();
        }

        inline bool IsInvalid() const
        {
            return m_tag.lockTag.IsInvalid();
        }

        inline bool IsMatch(const LockTag& lockTag, LockMode mode) const
        {
            return ((LockTag::HashCompareFunc(&m_tag.lockTag, &lockTag, sizeof(LockTag)) == 0) &&
                    (m_tag.lockMode == mode));
        }

        inline const LockTag *GetLockTag() const
        {
            return &m_tag.lockTag;
        }

        inline LockMode GetLockMode() const
        {
            return m_tag.lockMode;
        }

        inline LockMgrType GetLockMgrType() const
        {
            return m_tag.lockMgrType;
        }

        struct SubLockResourceBlock {
            dlist_node              nodeInList;
            static constexpr int32  defaultSubResNum = 8;
            SubLockResourceID       subResId[defaultSubResNum];
            uint32                  cnt[defaultSubResNum];
        };

        class SubLockResourceIter {
        public:
            explicit SubLockResourceIter(LockResourceRecord *resRecord);
            void Next();
            bool IsEnd() const;
            SubLockResourceID GetSubResourceID() const;
            uint32 GetSubResourceCnt() const;
        private:
            LockResourceRecord   *m_record;
            SubLockResourceBlock *m_currentSubRes;
            int32 m_resIndex;
        };

    private:
        inline bool IsCurrentFull() const
        {
            return (m_currentSubResIndex == (SubLockResourceBlock::defaultSubResNum - 1));
        }

        inline bool IsCurrentEmpty() const
        {
            return (m_currentSubResIndex < 0);
        }

        inline void SetCurrentFull()
        {
            m_currentSubResIndex = (SubLockResourceBlock::defaultSubResNum - 1);
        }

        RetStatus StartNewSubResourceIfNeeded(DstoreMemoryContext ctx, uint32 subResId);
        void RemoveCurrentSubResource();

        static SubLockResourceBlock *GetSubLockResBlockFromNode(dlist_node *node);

        LockResourceTag            m_tag;
        uint32                     m_totalLockCnt;
        uint32                     m_subResCnt;
        dlist_head                 m_extendSubLockResList;
        SubLockResourceBlock      *m_currentSubRes;
        int32                      m_currentSubResIndex;
        SubLockResourceBlock       m_defaultSubLockRes;
    };

private:
    LockResourceRecord *FindOrCreateResource(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    LockResourceRecord *FindResource(const LockTag& tag, LockMode mode, LockMgrType mgrType);
    void DeleteResource(LockResourceRecord *resEntry);

    void ReleaseAllLazyLocks();
    void ReleaseLazyLocksAcquiredAfter(SubLockResourceID resId);
    RetStatus ActuallyAcquireLocksFromLazyLockSlot(LockResourceRecord *lazyLockSlot);
    void AddSlotToLine(uint32 actualPos);
    void RemoveSlotFromLine(uint32 actualPos);

    static constexpr uint32 LAZY_LOCK_SLOT_MODES = 3;
    static constexpr uint32 LAZY_LOCK_SLOT_MAX = LazyLockHint::LAZY_LOCK_HINT_PART_CNT * LAZY_LOCK_SLOT_MODES;

    static inline uint32 GetLazyLockPartIdFromTagHash(uint32 tagHash)
    {
        return (tagHash % LazyLockHint::LAZY_LOCK_HINT_PART_CNT);
    }

    static inline uint32 GetLazyLockSlotHash(uint32 partId, LockMode mode)
    {
        return (partId * LAZY_LOCK_SLOT_MODES + (static_cast<uint32>(mode) - 1));
    }

    static inline uint32 GetLazyLockPartIdFromSlotHash(uint32 slotHash)
    {
        return (slotHash / LAZY_LOCK_SLOT_MODES);
    }

    DstoreMemoryContext m_ctx;
    HTAB               *m_resTable;
    uint32              m_subLockResId;

    LockResourceRecord m_lazyLockSlots[LAZY_LOCK_SLOT_MAX];
    uint32 m_lazyLockSlotLinePos[LAZY_LOCK_SLOT_MAX];
    uint32 m_lazyLockSlotActualPos[LAZY_LOCK_SLOT_MAX];
    uint32 m_lazyLockCnt;
};

constexpr LockResource::SubLockResourceID INVALID_SUB_LOCK_RES_ID = 0;
constexpr LockResource::SubLockResourceID FIRST_SUB_LOCK_RES_ID = 1;

}  // namespace DSTORE

#endif
