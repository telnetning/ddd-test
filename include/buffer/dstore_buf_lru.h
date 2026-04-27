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

#ifndef DSTORE_BUF_LRU_H
#define DSTORE_BUF_LRU_H
#include <atomic>

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_hsearch.h"
#include "common/algorithm/dstore_ilist.h"
#include "lock/dstore_lwlock.h"
#include "buffer/dstore_buf.h"
#include "buffer/dstore_buf_memchunk.h"
#include "buffer/dstore_buf_table.h"
#include "common/algorithm/dstore_string_info.h"
#include "common/dstore_datatype.h"

namespace DSTORE {
/*
 * Default values for buffer LRU.
 */
const double BUFLRU_DEFAULT_HOT_RATIO = 0.5;

/*
 * Counters for LRU cache hit ratio
 */
struct LruCounters {
    uint64 addIntoList;
    uint64 removeFromList;
    uint64 moveWithinList;
    uint64 missInList;

    LruCounters() : addIntoList{0}, removeFromList{0}, moveWithinList{0}, missInList{0}
    {
    }
};

class ConcurrentDList {
public:
    ConcurrentDList() : m_lwlock{}, m_list{}
    {
        DListInit(&m_list);
    }

    void Initialize()
    {
        LWLockInitialize(&m_lwlock, LWLOCK_GROUP_BUF_LRU);
    }

    ~ConcurrentDList() = default;

    template<bool isForward>
    class Iterator {
    public:
        explicit Iterator(ConcurrentDList *list, dlist_node *curNode = nullptr)
            : m_list(list),
              m_cur(curNode),
              m_next(nullptr)
        {
            dlist_node *end = &m_list->m_list.head;
            if (m_cur == nullptr) {
                if (isForward) {
                    m_cur = end->next ? end->next : end;
                } else {
                    m_cur = end->prev ? end->prev : end;
                }
            }
            m_next = isForward ? m_cur->next : m_cur->prev;
        }

        Iterator(const Iterator &) = default;
        Iterator &operator=(const Iterator &) = default;
        Iterator(Iterator &&) = default;
        Iterator &operator=(Iterator &&) = default;
        ~Iterator() = default;

        bool operator==(const Iterator &node) const
        {
            return m_cur == node.m_cur;
        }

        bool operator!=(const Iterator &node) const
        {
            return m_cur != node.m_cur;
        }

        dlist_node &operator*()
        {
            return *m_cur;
        }

        Iterator &operator++()
        {
            m_cur = m_next;
            m_next = isForward ? m_cur->next : m_cur->prev;
            return *this;
        }

    protected:
        ConcurrentDList *m_list;
        dlist_node *m_cur;  /* current element */
        dlist_node *m_next; /* next element */
    };

    using ForwardIterator = Iterator<true>;
    using ReverseIterator = Iterator<false>;

    void LockList(LWLockMode mode)
    {
        DstoreLWLockAcquireByMode(&m_lwlock, mode)
    }

    void UnlockList()
    {
        LWLockRelease(&m_lwlock);
    }

    void PushToHead(dlist_node *node)
    {
        DListPushHead(&m_list, node);
    }

    void PushToTail(dlist_node *node)
    {
        DListPushTail(&m_list, node);
    }

    void RemoveNode(dlist_node *node) const
    {
        DListDelete(node);
    }

    void RemoveNode(ReverseIterator &iterator)
    {
        DListDelete(&(*iterator));
    }

    dlist_node *PopFromHead()
    {
        if (DListIsEmpty(&m_list)) {
            return nullptr;
        }
        return DListPopHeadNode(&m_list);
    }

    dlist_node *PopFromTail()
    {
        if (DListIsEmpty(&m_list)) {
            return nullptr;
        }
        return DListPopTailNode(&m_list);
    }

    void MoveHead(dlist_node *node)
    {
        DListMoveHead(&m_list, node);
    }

    bool IsListEmpty()
    {
        return DListIsEmpty(&m_list);
    }

    ReverseIterator RBegin()
    {
        return ReverseIterator{this};
    }

    ReverseIterator REnd()
    {
        return ReverseIterator{this, &m_list.head};
    }

    ForwardIterator begin()
    {
        return ForwardIterator{this};
    }

    ForwardIterator end()
    {
        return ForwardIterator{this, &m_list.head};
    }

private:
    LWLock m_lwlock;
    dlist_head m_list;
};

/*
 * LruGenericList
 *
 * This is a generic LRU list for candidate list and invalidation list (for elastic bufferpool).
 * It serves as a queue which only allows PushTail() and PopHead().
 * The methods from ConcurrentDList are by default hidden by using private inheritance.
 */
class LruGenericList : private ConcurrentDList {
public:
    /* The following methods are made public. */
    using ConcurrentDList::Initialize;
    using ConcurrentDList::LockList;
    using ConcurrentDList::UnlockList;

    explicit LruGenericList(LruNodeType lruNodeType)
        : ConcurrentDList(), m_length{0}, m_lruNodeType{lruNodeType}, m_lruCounters()
    {
    }

    virtual ~LruGenericList() = default;

    void PushTail(LruNode *node)
    {
        LockList(LW_EXCLUSIVE);

        /* Only the node that is not in any list can be pushed. */
        StorageAssert(node->IsInPendingState());

        ConcurrentDList::PushToTail(&node->m_list_node);
        node->m_type = m_lruNodeType;
        m_length++;
        m_lruCounters.addIntoList++;

        UnlockList();
    }

    LruNode *PopHead()
    {
        LockList(LW_EXCLUSIVE);

        dlist_node *node = ConcurrentDList::PopFromHead();
        LruNode *lruNode = nullptr;
        if (node != nullptr) {
            lruNode = dlist_container(LruNode, m_list_node, node);
            StorageAssert(lruNode->m_type == m_lruNodeType);
            lruNode->m_type = LN_PENDING;
            StorageAssert(m_length > 0);
            m_length--;
            m_lruCounters.removeFromList++;
        } else {
            StorageAssert(m_length == 0);
            StorageAssert(lruNode == nullptr);
            m_lruCounters.missInList++;
        }

        UnlockList();
        return lruNode;
    }

    bool Remove(LruNode *node)
    {
        if (STORAGE_VAR_NULL(node)) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Node is nullptr."));
        }
        LockList(LW_EXCLUSIVE);

        if (node->m_type != m_lruNodeType) {
            UnlockList();
            return false;
        }

        ConcurrentDList::RemoveNode(&node->m_list_node);
        node->m_type = LN_PENDING;
        m_length--;
        m_lruCounters.removeFromList++;

        UnlockList();
        return true;
    }

    Size Length()
    {
        LockList(LW_SHARED);
        Size length = m_length;
        UnlockList();
        return length;
    }

    LruNodeType GetLruNodeType() const
    {
        return m_lruNodeType;
    }

    /*
     * Get the LruCounters of the list.
     */
    LruCounters GetLruCounters()
    {
        LockList(LW_SHARED);
        LruCounters counters = m_lruCounters;
        UnlockList();
        return counters;
    }

    bool IsEmpty()
    {
        LockList(LW_SHARED);
        bool empty = ConcurrentDList::IsListEmpty();
        UnlockList();
        return empty;
    }

private:
    Size m_length;
    LruNodeType m_lruNodeType; /* All nodes being pushed to this list will be set to this node type. */
    LruCounters m_lruCounters; /* Counters for LRU cache hit ratio. */
};

class LruCandidateList : public LruGenericList {
public:
    LruCandidateList() : LruGenericList(LN_CANDIDATE) {}

    void Push(LruNode *node)
    {
        PushTail(node);
    }

    LruNode *Pop()
    {
        return PopHead();
    }
};

class LruHotList {
public:
    explicit LruHotList(Size size = 16)
        : mHotList{}, mMaxSize{size}, mCurSize{0}, mLruCounters()
    {
    }

    ~LruHotList() = default;

    void Initialize()
    {
        mHotList.Initialize();
    }

    bool Push(LruNode* node)
    {
        mHotList.LockList(LW_EXCLUSIVE);

        /* only node in pending state will be pushed to the hot list */
        StorageAssert(node->IsInPendingState());

        if (mCurSize == mMaxSize) {
            mHotList.UnlockList();
            return false;
        }

        mHotList.PushToTail(&node->m_list_node);
        node->m_type = LN_HOT;
        mCurSize++;
        mLruCounters.addIntoList++;

        mHotList.UnlockList();

        return true;
    }

    LruNode* Pop()
    {
        mHotList.LockList(LW_EXCLUSIVE);

        dlist_node* node = mHotList.PopFromHead();
        LruNode* lruNode = nullptr;
        if (node != nullptr) {
            lruNode = dlist_container(LruNode, m_list_node, node);
            StorageAssert(lruNode->m_type == LN_HOT);
            lruNode->m_type = LN_PENDING;
            mCurSize--;
            mLruCounters.removeFromList++;
        }

        mHotList.UnlockList();

        return lruNode;
    }

    bool Remove(LruNode* node)
    {
        mHotList.LockList(LW_EXCLUSIVE);

        if (STORAGE_VAR_NULL(node)) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Node is nullptr."));
        }
        if (!node->IsInHotList()) {
            mHotList.UnlockList();
            return false;
        }

        mHotList.RemoveNode(&node->m_list_node);
        node->m_type = LN_PENDING;

        mCurSize--;
        mLruCounters.removeFromList++;

        mHotList.UnlockList();
        return true;
    }

    /*
     * NOTE: this method should be protected by list lock.
     */
    void RemoveUnderListLock(LruNode* node)
    {
        StorageAssert(node->m_type == LN_HOT);
        mHotList.RemoveNode(&node->m_list_node);
        node->m_type = LN_PENDING;
        mCurSize--;
        mLruCounters.removeFromList++;
    }

    /*
     * the length() can not return the accurate size of the queue
     */
    Size Length()
    {
        mHotList.LockList(LW_SHARED);
        Size length = mCurSize;
        mHotList.UnlockList();

        return length;
    }

    /*
     * Get the LruCounters of the list.
     */
    const LruCounters* GetLruCounters() const
    {
        return &mLruCounters;
    }

    Size GetMaxSize() const
    {
        return mMaxSize;
    }

    void SetMaxSize(Size newSize)
    {
        this->mMaxSize = newSize;
    }

    void LockList()
    {
        mHotList.LockList(LW_EXCLUSIVE);
    }

    void UnlockList()
    {
        mHotList.UnlockList();
    }

    ConcurrentDList::ForwardIterator begin()
    {
        return  mHotList.begin();
    }

    ConcurrentDList::ForwardIterator end()
    {
        return mHotList.end();
    }

private:
    ConcurrentDList mHotList;
    Size mMaxSize;
    Size mCurSize;
    /*
     * Counters for LRU cache hit ratio
     */
    LruCounters mLruCounters;
};

class LruList {
public:

    LruList() : m_lru_list{}, m_length{0}, mLruCounters()
    {
    }

    void Initialize()
    {
        m_lru_list.Initialize();
    }

    ~LruList() = default;

    inline void LockList()
    {
        m_lru_list.LockList(LW_EXCLUSIVE);
    }

    inline void SharedLockList()
    {
        m_lru_list.LockList(LW_SHARED);
    }

    inline void UnlockList()
    {
        m_lru_list.UnlockList();
    }

    /*
     * Add the node to the tail of the list.
     *
     * NOTE: this method should be protected by list lock.
     */
    void AddTail(LruNode* node)
    {
        StorageAssert(node->IsInPendingState());
        m_lru_list.PushToTail(&node->m_list_node);
        node->m_type = LN_LRU;
        m_length++;
        mLruCounters.addIntoList++;
    }

    /*
     * Add the node to the head of the list
     *
     * NOTE: this method should be protected by list lock.
     */
    void AddHead(LruNode* node)
    {
        StorageAssert(node->IsInPendingState());
        m_lru_list.PushToHead(&node->m_list_node);
        node->m_type = LN_LRU;
        m_length++;
        mLruCounters.addIntoList++;
    }

    /*
     * move the head to the head of the list.
     *
     * NOTE: this method should be protected by list lock.
     */
    void MoveHead(LruNode* node)
    {
        StorageAssert(node->m_type == LN_LRU);
        m_lru_list.MoveHead(&node->m_list_node);
        mLruCounters.moveWithinList++;
    }

    /*
     * remove the node from the list.
     *
     * NOTE: this method should be protected by list lock.
     */
    void Remove(LruNode* node)
    {
        StorageAssert(node->m_type == LN_LRU);
        m_lru_list.RemoveNode(&node->m_list_node);
        node->ResetUsage();
        node->m_type = LN_PENDING;
        m_length--;
        mLruCounters.removeFromList++;
    }

    /*
     * pop the last node in the list. If the list is empty, return nullptr.
     *
     * NOTE: this method should be protected by list lock.
     */
    LruNode* PopTail()
    {
        dlist_node* node = m_lru_list.PopFromTail();
        if (node == nullptr) {
            return nullptr;
        }
        LruNode* lruNode = dlist_container(LruNode, m_list_node, node);
        lruNode->m_type = LN_PENDING;
        m_length--;
        mLruCounters.removeFromList++;
        return lruNode;
    }

    /*
     * Get the length of the list.
     *
     * NOTE: this method should be protected by list lock.
     */
    Size Length()
    {
        Size len;
        LockList();
        len = m_length;
        UnlockList();
        return len;
    }

    /*
     * Get the LruCounters of the list.
     */
    const LruCounters* GetLruCounters() const
    {
        return &mLruCounters;
    }

    /*
     * Get a reverse iterator and it will iterate from the tail of the list to the list head.
     *
     * NOTE: this method should be protected by list lock.
     */
    ConcurrentDList::ReverseIterator RBegin()
    {
        return m_lru_list.RBegin();
    }

    /*
     * Get a reverse iterator which indicate the end of iteration.
     *
     * NOTE: this method should be protected by list lock.
     */
    ConcurrentDList::ReverseIterator REnd()
    {
        return m_lru_list.REnd();
    }

    ConcurrentDList::ForwardIterator begin()
    {
        return m_lru_list.begin();
    }

    ConcurrentDList::ForwardIterator end()
    {
        return m_lru_list.end();
    }
private:
    ConcurrentDList m_lru_list;
    Size m_length;
    /*
     * Counters for LRU cache hit ratio
     */
    LruCounters mLruCounters;
};

class BufLruList : public BaseObject {
public:
    explicit BufLruList(uint32 idx, Size hotListSize);
    ~BufLruList() = default;

    void Initialize();
    /*
     * get candidate buffer that the PageId is not equal to {page_id} from candidate list.
     */
    BufferDesc *GetCandidateBuffer(const BufferTag &bufTag, bool ignoreDirtyPage);

    /*
     * Push the BufferDesc back to the lru list.
     * If the buffer is reused success, put it to the head of lru list,
     * else it will be pushed to the tail of the lru list.
     */
    void PushBackToLru(BufferDesc* bufferDesc,  bool reuseSuccess);

    /*
     * Push the BufferDesc back to the candidate list.
     */
    void PushBackToCandidate(BufferDesc *bufferDesc);

    /*
     *  Maintain the access stat of the buffer.
     *  If
     *  (1) Buffer in hot chain: don't move buffer;
     *  (2) Buffer in lru chain: move the buffer to the head of lru list, and increase the usage count.
     *                           if the count reach the LRU_MAX_USAGE, move the buffer to hot chain.
     *  (3) Buffer in free chain: move the buffer to the head of lru list;
     */
    void BufferAccessStat(BufferDesc* bufferDesc);

    /*
     * Move the buffer to the candidate list.
     */
    void MoveToCandidateList(BufferDesc* bufferDesc);

    /*
     * Remove a buffer from the hot list and move it to the lru list.
     */
    void TryMoveOneNodeFromHotToLruList();

    /*
     * Try to remove a buffer from the lru list.
     */
    bool TryRemoveFromLruList(BufferDesc *bufferDesc);

    /*
     * Remove a buffer from candidate/LRU/hot lists and return the buffer's previous LruNodeType.
     */
    LruNodeType Remove(BufferDesc *bufferDesc);

    /*
     * add buffer into the candidate list, after that the buffer can be used by buffer pool.
     */
    void AddNewBuffer(BufferDesc* bufferDesc, uint32 numBufDesc = 1, bool isInit = false);

    LruCounters GetHotCounters()
    {
        LruCounters counters;
        mHotList.LockList();
        counters = *mHotList.GetLruCounters();
        mHotList.UnlockList();
        return counters;
    }

    LruCounters GetLruCounters()
    {
        LruCounters counters;
        mLruList.LockList();
        counters = *mLruList.GetLruCounters();
        mLruList.UnlockList();
        return counters;
    }

    LruCounters GetCandidateCounters()
    {
        return mCandidateList.GetLruCounters();
    }

    LruHotList *GetHotList()
    {
        return &mHotList;
    }

    LruList *GetLruList()
    {
        return &mLruList;
    }

    LruCandidateList *GetCandidateList()
    {
        return &mCandidateList;
    }

    LruGenericList *GetInvalidationList()
    {
        return &mInvalidationList;
    }

    static LruNode* GetNode(dlist_node &iter)
    {
        return dlist_container(LruNode, m_list_node, &iter);
    }

    char* DumpSummaryInfo();

    LruHotList mHotList;
    LruList mLruList;
    LruCandidateList mCandidateList;
    LruGenericList mInvalidationList; /* The list to temporarily store buffers to be invalidated. */

private:
    LWLock mExpandLwlock;
    const uint32 mIndex;

    BufferDesc *ScanLruListToFindCandidateBuffer(const BufferTag &bufTag, Size expectScanSize,
        bool ignoreDirtyPage);
};

class BufLruListArray : public BaseObject {
public:
    BufLruListArray(Size lruPartition);
    ~BufLruListArray() = default;

    void InitBufLruListArray(Size bufferPoolSize)
    {
        Size lruIndex = 0;
        Size hotListSize = static_cast<Size>(
            (static_cast<float>(bufferPoolSize) / static_cast<float>(m_lru_partitions)) * BUFLRU_DEFAULT_HOT_RATIO);
        /* Initialize buffer pool HOT/LRU/CANDIDATE lists. */
        m_buf_lru_list = (BufLruList **)
            DstoreMemoryContextAlloc(g_dstoreCurrentMemoryContext, m_lru_partitions * sizeof(BufLruList *));
        StorageReleasePanic(m_buf_lru_list == nullptr, MODULE_BUFMGR, ErrMsg("alloc memory for bufLruList fail!"));
        for (lruIndex = 0; lruIndex < m_lru_partitions; lruIndex++) {
            m_buf_lru_list[lruIndex] = DstoreNew(g_dstoreCurrentMemoryContext) BufLruList(lruIndex, hotListSize);
            StorageReleasePanic(m_buf_lru_list[lruIndex] == nullptr, MODULE_BUFMGR,
                ErrMsg("alloc memory for m_buf_lru_list[%zu] fail!", lruIndex));
            m_buf_lru_list[lruIndex]->Initialize();
        }
    }

    void ResetLruIndex()
    {
        m_lruIndex = 0;
    }

    void AddNewBufferDesc(BufferDesc *bufferDesc, bool isBootstrap)
    {
        m_buf_lru_list[m_lruIndex]->AddNewBuffer(bufferDesc, 1, isBootstrap);
        m_lruIndex = ((m_lruIndex + 1) % m_lru_partitions);
    }

    void Destroy();

    /*
     * Update the size of hot list for each LRU partition to be proportional to the bufferpool size.
     */
    void ResizeHotList(Size bufferPoolSize);

    /*
     * Get candidate buffer from one of LRU list partition.
     * If success, return candidate buffer which has been pinned and buffer state;
     * else return INVALID_BUFFER_DESC.
     */
    BufferDesc *GetCandidateBuffer(const BufferTag *bufTag, uint64 *bufState);

    /*
     * Put the buffer back to the LRU list.
     */
    void PushBufferBackToLru(BufferDesc *bufferDesc, bool reuseSuccess);

    /*
     * Put the free buffer back to Candidate list.
     */
    void PushBufferBackToCandidate(BufferDesc *bufferDesc);

    /*
     * Maintain the access stat of the buffer.
     */
    void BufferAccessStat(BufferDesc *bufferDesc);

    /*
     * When invalidate buffer, move the buffer back to the LRU list.
     */
    void MoveToCandidateList(BufferDesc *bufferDesc);

    /*
     * Remove buffer from BufLruList
     */
    void Remove(BufferDesc *bufferDesc);

    /*
     * Move all buffers in this memchunk from candidate/LRU/hot lists to invalidation list,
     * or the other way round.
     */
    void RemoveMemChunkFromLru(BufferMemChunk *memChunk);
    void RestoreMemChunkToLru(BufferMemChunk *memChunk);

    BufLruList *GetLruListAt(Size i);

    StringInfo DumpLruSummary();

    Size GetLruPartition()
    {
        return m_lru_partitions;
    }

private:
    BufLruList **m_buf_lru_list;
    Size m_lru_partitions;
    Size m_lruIndex;

    uint32 FindBufLruList(const BufferTag *bufTag) const;
};

class LruPageClean {
public:
    LruPageClean(BufLruListArray *lruListArray, uint64 initCandidateListLength, BufTable *bufTable);

    void Init();

    void StartWorkThreads();

    void Run();

    void TryCleanLruListToCandidate(bool ignoreDirtyPage, int32 &needFlushPageNum, BufLruList *bufLru, bool &pageMoved);

    void StopWorkThreads();

    void setIsStop(bool isStop)
    {
        m_isStop.store(isStop, std::memory_order_release);
    }

    ~LruPageClean();

private:
    double m_candidateSafePercent;
    uint64 m_initCandidateListLength;
    uint32 m_lruScanDepth;
    BufLruListArray *m_lruListArray;
    BufTable *m_bufTable;
    std::atomic<bool> m_isStop;
    std::thread *m_workThread;
};

extern void AppendCacheSummary(StringInfo cacheSummary, const char *message, uint64 count, uint64 total = 0);
}

#endif
