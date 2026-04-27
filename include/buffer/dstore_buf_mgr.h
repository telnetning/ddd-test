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

#ifndef DSTORE_BUF_MGR_H
#define DSTORE_BUF_MGR_H
#include "buffer/dstore_buf.h"
#include "buffer/dstore_buf_table.h"
#include "buffer/dstore_buf_lru.h"
#include "transaction/dstore_transaction_types.h"
#include "buffer/dstore_buf_refcount.h"
#include "page/dstore_data_page.h"
#include "framework/dstore_vfs_adapter.h"

namespace DSTORE {

union BufferPoolReadFlag {
    struct {
        uint32 mIsNewPage : 1;
        uint32 mIsRecoveryRead : 1;
        uint32 mUnused : 30;
    };
    uint32 mFlag;

    BufferPoolReadFlag() : mFlag(0) {}

    static const BufferPoolReadFlag CreateNewPage()
    {
        BufferPoolReadFlag flag;
        flag.SetIsNewPage(true);
        return flag;
    }

    static const BufferPoolReadFlag CreateFlagWithRecoveryRead()
    {
        BufferPoolReadFlag flag;
        flag.SetIsRecoveryRead(true);
        return flag;
    }

    bool IsNewPage() const
    {
        return (mIsNewPage == 1);
    }

    bool IsRecoveryRead() const
    {
        return (mIsRecoveryRead == 1);
    }

    void SetIsNewPage(const bool isNewPage)
    {
        mIsNewPage = (isNewPage ? 1 : 0);
    }

    void SetIsRecoveryRead(const bool isRecoveryRead)
    {
        mIsRecoveryRead = (isRecoveryRead ? 1 : 0);
    }
};

union BufferPoolUnlockContentFlag {
    struct {
        uint32 mIsCheckCrc : 1;
        uint32 mUnused : 31;
    };
    uint32 mFlag;

    BufferPoolUnlockContentFlag() : mFlag(1) {}

    static const BufferPoolUnlockContentFlag DontCheckCrc()
    {
        BufferPoolUnlockContentFlag flag;
        flag.SetCheckCrc(false);
        return flag;
    }

    bool IsCheckCrc() const
    {
        return (mIsCheckCrc == 1);
    }

    void SetCheckCrc(bool isCheckCrc)
    {
        mIsCheckCrc = (isCheckCrc ? 1 : 0);
    }
};

static_assert(sizeof(BufferPoolReadFlag) == sizeof(uint32),
              "BufferPoolReadFlag size must be 4 bytes.");

static_assert(sizeof(BufferPoolUnlockContentFlag) == sizeof(uint32),
              "BufferPoolUnlockContentFlag size must be 4 bytes.");

struct PageDirectoryInfo;
struct ConsistentReadContext {
    /* Input */
    PdbId pdbId;
    PageId pageId;
    Xid currentXid;
    Snapshot snapshot;
    void *dataPageExtraInfo;

    /* Output */
    DataPage *destPage;
    BufferDesc *crBufDesc;
};

struct FlushThreadContext {
    bool isBootstrap;
    bool onlyOwnedByMe;
    gs_atomic_uint64 actualFlushNum;
    PdbId pdbId;
    DstoreSpinLock memChunkQueueLock;
    BufferMemChunkWrapper <BufferMemChunk> *nextMemChunk;
    RetStatus rc;
};

struct InvalidateThreadContext {
    PdbId pdbId;
    FileId fileId;
    DstoreSpinLock memChunkQueueLock;
    BufferMemChunkWrapper <BufferMemChunk> *nextMemChunk;
    RetStatus rc;
};

struct IoInfo {
    uint64 readCount;
    uint64 writeCount;
    uint64 avgReadLatency;
    uint64 avgWriteLatency;
    uint64 maxReadLatency;
    uint64 maxWriteLatency;
    uint64 minReadLatency;
    uint64 minWriteLatency;
};

struct LruInfo {
    uint64 totalAddIntoHot = 0;
    uint64 totalRemoveFromHot = 0;
    uint64 totalAddIntoLru = 0;
    uint64 totalMoveWithinLru = 0;
    uint64 totalRemoveFromLru = 0;
    uint64 totalAddIntoCandidate = 0;
    uint64 totalRemoveFromCandidate = 0;
    uint64 totalMissInCandidate = 0;
};

class BufMgrInterface : public BaseObject {
public:
    virtual ~BufMgrInterface() = default;
    /* Initialize buffer pool memory. */
    virtual RetStatus Init() = 0;
    /* Release buffer pool resource. */
    virtual void Destroy() = 0;

    /* Read page from buffer pool AND lock the buffer. */
    virtual BufferDesc *Read(const PdbId &pdbId, const PageId &pageId, LWLockMode mode,
        BufferPoolReadFlag flag = BufferPoolReadFlag(), BufferRing bufRing = nullptr) = 0;

    virtual BufferDesc *ReadCr(BufferDesc *baseBufferDesc, Snapshot snapshot) = 0;
    virtual BufferDesc *ReadOrAllocCr(BufferDesc *baseBufDesc, uint64 lastPageModifyTime,
                                      BufferRing bufRing = nullptr) = 0;
    /* Read a data page that match with given snapshot. */
    virtual RetStatus ConsistentRead(ConsistentReadContext &crContext, BufferRing bufRing = nullptr) = 0;

    virtual RetStatus BatchCreateNewPage(const PdbId &pdbId, PageId *pageId, uint64 pageCount, BufferDesc **newBuffer,
                                         BufferRing bufRing = nullptr) = 0;
    /* Release the pin on a buffer. */
    virtual void Release(BufferDesc *bufferDesc) = 0;
    /* Mark buffer contents as dirty. */
    virtual RetStatus MarkDirty(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn = true) = 0;
    /* Write out a shared buffer content. */
    virtual RetStatus Flush(BufferTag &bufTag, void* aioCtx = nullptr) = 0;
    virtual RetStatus FlushAll(bool isBootstrap, bool onlyOwnedByMe = false, PdbId pdbId = g_defaultPdbId) = 0;

    /* Try flush the dirty page. It does two things:
     * (1).It can avoid lock conflicts. Because if the service thread keeps holding the LW_EXCLUSIVE lock,
     *      when BgPageWriter flush the dirty page, it will request the LW_SHARED lock. A lock conflict will occur.
     *
     * (2).It's to avoid the BgPageWriter advance the dirty queue failed because the service thread keeps holding the
     *      LW_EXCLUSIVE lock, so the BgPageWriter keeps waiting for LW_SHARED lock.
     *      As s result, the service thread will be blocked when the dirty page queue is full.
     */
    virtual RetStatus TryFlush(BufferDesc *bufferDesc) = 0;
    /* Mark a shared buffer invalid, and return it to lru freelist. */
    virtual RetStatus Invalidate(BufferDesc *bufferDesc) = 0;
    virtual RetStatus InvalidateByBufTag(BufferTag bufTag, bool needFlush) = 0;
    virtual RetStatus InvalidateBaseBuffer(BufferDesc *bufferDesc, BufferTag bufTag, bool needFlush) = 0;
    virtual char *GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId) = 0;
    /* Acquire the contentLwLock for the buffer. */
    virtual RetStatus LockContent(BufferDesc *bufferDesc, LWLockMode mode) = 0;
    /* Try acquire the contentLwLock for the buffer. */
    virtual bool TryLockContent(BufferDesc *bufferDesc, LWLockMode mode) = 0;
    /* Pin buffer and acquire the contentLwLock for the buffer. */
    virtual void PinAndLock(BufferDesc *bufferDesc, LWLockMode mode) = 0;

    virtual RetStatus InvalidateUsingGivenPdbId(PdbId pdbId) = 0;

    virtual RetStatus InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId) = 0;

    /* Release the contentLwLock.
     *
     * NOTE: checkCrc indicate if it need to check page crc value when unlock buffer
     *       If the checkCrc is true, then it will trigger assert when unlock the buffer in exclusive mode,
     *       the buffer is not set BUF_CONTENT_DIRTY flag but the page crc value is different from that of its lock
     *       time. This is used to avoid changing pages by mistake.
     */
    virtual void UnlockContent(BufferDesc *bufferDesc,
                               BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) = 0;
    /* Unlock and release the buffer. This is just a shorthand for a common combination.
     *
     * NOTE: checkCrc indicate if it need to check page crc value when unlock buffer
     *       If the checkCrc is true, then it will trigger assert when unlock the buffer in exclusive mode,
     *       the buffer is not set BUF_CONTENT_DIRTY flag but the page crc value is different from that of its lock
     *       time. This is used to avoid changing pages by mistake.
     */
    virtual void UnlockAndRelease(BufferDesc *bufferDesc,
                                  BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) = 0;
    virtual void FinishCrBuild(BufferDesc *crBufferDesc, CommitSeqNo pageMaxCsn) = 0;

    virtual BufferDesc *RecoveryRead(const PdbId &pdbId, const PageId &pageId) = 0;

    virtual RetStatus GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr) = 0;

    virtual RetStatus GetPDBucketInfo(Size *length, char ***chashBucketInfo, uint32 startBucket, uint32 endBucket) = 0;

    virtual RetStatus GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr) = 0;

    virtual uint8 GetBufDescResponseType(BufferDesc *bufferDesc) = 0;

    virtual RetStatus DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage) = 0;

    virtual char *GetPdBucketLockInfo() = 0;

#ifdef DSTORE_USE_ASSERT_CHECKING
    virtual void AssertHasHoldBufLock(const PdbId pdbId, const PageId pageId, LWLockMode lockMode) = 0;
#endif

    virtual char *PrintPdRecoveryInfo() { return nullptr; }
    virtual char *PrintBufferpoolStatistic() { return nullptr; }
    virtual char *PrintAntiCacheInfo(UNUSE_PARAM bool allPartition,
        UNUSE_PARAM uint32 partitionId)
    {
        StringInfoData str;
        if (unlikely(!str.init())) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("cannot allocate memory for AntiCacheInfo dump info."));
            return nullptr;
        }
        str.append("AntiCache is not support.\n");
        return str.data;
    }
    virtual char *ResetBufferpoolStatistic() { return nullptr; }
};

class BufMgr : public BufMgrInterface {
public:
    BufMgr(Size size, Size lruPartitions, Size maxSize = 0);
    ~BufMgr() override;
    RetStatus Init() override;
    void Destroy() override;

    BufferDesc *Read(const PdbId &pdbId, const PageId &pageId, LWLockMode mode,
                     BufferPoolReadFlag flag = BufferPoolReadFlag(), BufferRing bufRing = nullptr) override;
    BufferDesc *ReadCr(BufferDesc *baseBufferDesc, Snapshot snapshot) override;
    BufferDesc *ReadOrAllocCr(BufferDesc *baseBufDesc, uint64 lastPageModifyTime, BufferRing bufRing = nullptr) final;
    RetStatus ConsistentRead(ConsistentReadContext &crContext, BufferRing bufRing = nullptr) override;
    RetStatus ConsistentReadInternal(ConsistentReadContext &crContext, bool needCheckPoFlag,
                                     BufferRing bufRing = nullptr);
    RetStatus BatchCreateNewPage(const PdbId &pdbId, PageId *pageId, uint64 pageCount, BufferDesc **newBuffer,
                                 BufferRing bufRing = nullptr) override;

    void Release(BufferDesc *bufferDesc) final;
    /*
     * Mark page dirty to flush
     *
     * @param bufferDesc the buffer will be pushed dirty page list
     * @param needUpdateRecoveryPlsn means just pushed to dirty page list not update recoveryPlsn
     */
    RetStatus MarkDirty(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn = true) override;
    RetStatus Flush(BufferTag &bufTag, void* aioCtx = nullptr) override;

    RetStatus FlushIfDirty(BufferDesc *bufferDesc);
    RetStatus FlushAll(bool isBootstrap, bool onlyOwnedByMe = false, PdbId pdbId = g_defaultPdbId) override;
    /* TryFlush
     *
     * Try to flush dirty buffer, it will use TryLockContent to acquire
     * shared-content lock to avoid deadlock. And if acquire content lock
     * or flush page failed, this method will return false, otherwise return true.
     */
    RetStatus TryFlush(BufferDesc *bufferDesc) override;
    RetStatus Invalidate(BufferDesc *bufferDesc) final;
    RetStatus InvalidateByBufTag(BufferTag bufTag, bool needFlush) override;
    RetStatus InvalidateBaseBuffer(BufferDesc *bufferDesc, BufferTag bufTag, bool needFlush) override;

    char *GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId) override;

    RetStatus InvalidateCrBuffer(BufferDesc *crBufferDesc);
    RetStatus InvalidateCrBufferOfBaseBuffer(BufferDesc *baseBufferDesc);

    void MoveBuffersToInvalidationList(PdbId pdbId);
    void InvalidateCrBufferUsingGivenPdbIdOrFileId(BufferDesc *crBufferDesc, PdbId pdbId,
        FileId fileId = INVALID_VFS_FILE_ID);
    void TryFlushBeforeInvalidateBuffer(BufferDesc *baseBufferDesc, PdbId pdbId, FileId fileId);
    void InvalidateBaseBufferUsingGivenPdbIdOrFileId(BufferDesc *baseBufferDesc, PdbId pdbId, FileId fileId);
    void InvalidateOneBufferUsingGivenPdbIdOrFileId(BufferDesc *bufferDesc, PdbId pdbId, FileId fileId);
    RetStatus InvalidateUsingGivenPdbIdOrFileId(PdbId pdbId, FileId fileId = INVALID_VFS_FILE_ID);
    RetStatus InvalidateUsingGivenPdbId(PdbId pdbId) override;
    RetStatus InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId) override;

    void RemoveBufferFromLruAndHashTable(BufferDesc *bufferDesc);

    RetStatus LockContent(BufferDesc *bufferDesc, LWLockMode mode) override;
    bool TryLockContent(BufferDesc *bufferDesc, LWLockMode mode) final;
    void PinAndLock(BufferDesc *bufferDesc, LWLockMode mode) final;

    /* Release the contentLwLock.
     *
     * NOTE: checkCrc indicate if it need to check page crc value when unlock buffer
     *       If the checkCrc is true, then it will trigger assert when unlock the buffer in exclusive mode,
     *       the buffer is not set BUF_CONTENT_DIRTY flag but the page crc value is different from that of its lock
     *       time. This is used to avoid changing pages by mistake.
     */
    void UnlockContent(BufferDesc *bufferDesc,
                       BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;
    /* Unlock and release the buffer. This is just a shorthand for a common combination.
     *
     * NOTE: checkCrc indicate if it need to check page crc value when unlock buffer
     *       If the checkCrc is true, then it will trigger assert when unlock the buffer in exclusive mode,
     *       the buffer is not set BUF_CONTENT_DIRTY flag but the page crc value is different from that of its lock
     *       time. This is used to avoid changing pages by mistake.
     */
    void UnlockAndRelease(BufferDesc *bufferDesc,
                          BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) final;

    void FinishCrBuild(BufferDesc *crBufferDesc, CommitSeqNo pageMaxCsn) final;

    RetStatus DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage) override;
    /*
     * This is the helper function for DoWhenBufferpoolResize. It calls template function
     * to allocate new memchunks.
     */
    virtual RetStatus DoWhenBufferpoolResizeHelper(Size bufferPoolNewSize, Size numMemChunksToChange, bool isUpSize,
                                                   StringInfoData &outputMessage);
    template <class BufferMemChunk_T>
    RetStatus DoWhenBufferpoolResizeTemplate(Size bufferPoolNewSize, Size numMemChunksToChange, bool isUpSize,
                                             StringInfoData &outputMessage);
    RetStatus DoWhenBufferpoolDownSize(const Size numMemChunks);
    RetStatus DoWhenBufferpoolDownSizeHelper(BufferMemChunk *memChunk);
    void InvalidateMemChunkWorker(const Size seed, RetStatus *rc);
    RetStatus InvalidateMemChunkWorkerHelper(BufferDesc *bufferDesc);
    RetStatus BBoxBlackListGetBuffer(char*** chunk_addr, uint64** chunk_size, uint64* numOfChunk);

    BufferDesc *RecoveryRead(const PdbId &pdbId, const PageId &pageId) override
    {
        return Read(pdbId, pageId, LW_EXCLUSIVE);
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    void AssertHasHoldBufLock(const PdbId pdbId, const PageId pageId, LWLockMode lockMode) final;
#endif
    Size GetBufMgrSize() const;
    Size GetLruPartition() const;
    double GetHotListRatio() const;
    /*
     * GetBufferHitRatio
     * Returns a char * which contains the dump of the cache
     * hit ratio statistics recorded from functions in BufMgr and BufLruListArray.
     */
    char *GetBufferHitRatio();
    char *PrintBufferDescByTag(PdbId pdbId, FileId fileId, BlockNumber blockId);
    char *PrintLruInfo(BufferDesc *bufferDesc);
    char *PrintBufferpoolStatistic() override;
    char *ResetBufferpoolStatistic() override;
    void PrintMemChunkStatistics(StringInfoData &str);

    BufLruList *GetBufLruList(Size queueIdx);

    RetStatus GetLruSummary(LruInfo *lruInfo);

    RetStatus GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr) override;

    RetStatus GetPDBucketInfo(Size *length, char ***chashBucketInfo, uint32 startBucket, uint32 endBucket) override;

    RetStatus GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr) final;

    uint8 GetBufDescResponseType(BufferDesc *bufferDesc) override;

    char *GetPdBucketLockInfo();

    uint64 GetBufferAllocCnt() const;

    RetStatus InvalidateMemChunkForDebug(Size memChunkId);

    void ReportBgwriter(PdbId pdbId, const int64 slotId);

    std::mutex m_mutex;
    /* The key of latency count */
    std::atomic<bool> m_isCount{false};
    /* The count of read */
    std::atomic<uint64> m_readCount;
    /* The count of write */
    std::atomic<uint64> m_writeCount;

    union {
        uint8 antiCacheFlag;
        struct {
            uint8 isAntiCacheOn : 1;
            uint8 isAntiCacheOnInMemory : 1;
            uint8 isEnablePageMissDirtyCheck : 1;
            uint8 reserved : 5;
        } flag;
    } m_antiCacheStat;

    bool IsAntiCacheOn() const
    {
        return m_antiCacheStat.flag.isAntiCacheOn;
    }
    bool IsAntiCacheOnInMemory() const
    {
        return m_antiCacheStat.flag.isAntiCacheOnInMemory;
    }
    bool IsEnablePageMissingDirtyCheck() const
    {
        return (m_antiCacheStat.flag.isAntiCacheOn && m_antiCacheStat.flag.isEnablePageMissDirtyCheck);
    }

    virtual RetStatus InsertAntiCache(UNUSE_PARAM const BufferTag &bufferTag, UNUSE_PARAM const PageVersion &pageVer,
                                      UNUSE_PARAM uint32 bucketId, UNUSE_PARAM bool needBucket)
    {
        return DSTORE_SUCC;
    }

    virtual void RemoveFromAntiCache(UNUSE_PARAM const BufferTag &bufferTag, UNUSE_PARAM uint32 bucketId,
        UNUSE_PARAM PageVersion pageVersion = INVALID_PAGE_LSN)
    {
        return;
    }

    virtual RetStatus AntiCacheMissingDirtyCheck(UNUSE_PARAM BufferDesc *bufferDesc)
    {
        return DSTORE_SUCC;
    }

    virtual void RemoveAntiEntryByPdbIdOrFileId(UNUSE_PARAM PdbId pdbId, UNUSE_PARAM FileId fileId)
    {
        return;
    }

    virtual RetStatus AntiCacheHandleBufferEvicted(UNUSE_PARAM BufferDesc *bufferDesc)
    {
        return DSTORE_SUCC;
    }

    virtual RetStatus AntiCacheHandleReadBlock(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM const uint64 &glsn,
        UNUSE_PARAM const uint64 &plsn)
    {
        return DSTORE_SUCC;
    }
protected:
    /* Counters for Buffer Pool events. */
    struct BufferPoolCounters {
        std::atomic<uint64> allocCnt;
        std::atomic<uint64> loadFromDiskCnt;
        std::atomic<uint64> flushCnt;
        std::atomic<uint64> mismatchCnt;
        std::atomic<uint64> matchCnt;

        /* Counters for CR Buffer events. */
        std::atomic<uint64> crAllocCnt;    /* Counter of allocating a CR slot. crAllocSlotCnt + crReuseSlotCnt =
                                                  crBuildCnt. */
        std::atomic<uint64> crTryReuseSlotCnt; /* Counter of trying to resue a CR slot. */
        std::atomic<uint64> crReuseSlotCnt;    /* Counter of reusing a CR slot. */
        std::atomic<uint64> crBuildCnt;        /* Counters of building CR buffers. */

        /* crMismatchCnt + crMatchCnt is equal the number of accesses to the cr buffers. */
        std::atomic<uint64> crMismatchCnt; /* Counter of reading the CR buffer of a specified snapshot is fails.
                                              crMismatchCnt > crBuildCnt. */
        std::atomic<uint64> crMatchCnt;    /* Counter of reading the CR buffer of a specified snapshot is hits. */

        void Initialize()
        {
            allocCnt = 0;
            loadFromDiskCnt = 0;
            flushCnt = 0;
            mismatchCnt = 0;
            matchCnt = 0;

            crAllocCnt = 0;
            crTryReuseSlotCnt = 0;
            crReuseSlotCnt = 0;
            crBuildCnt = 0;
            crMismatchCnt = 0;
            crMatchCnt = 0;
        }
    } m_bufPoolCnts;
    /* Number of buffer pool blocks. It defines the length for both m_buffer_desc and m_blocks. */
    Size m_size;
    Size m_maxSize;
    /* The lock to prevent multiple threads from doing bufferpool resize at the same time. */
    LWLock m_sizeLock;
    /* Buffer pool hash table. It defines a way to access m_buffer_desc by BufferTag. */
    BufTable *m_buftable;
    /* Point to the head of the linked list of BufferMemChunk.
     * The Buffer pool descriptor and Buffer pool block pointer are included inside each BufferMemChunk. */
    BufferMemChunkList *m_bufferMemChunkList;
    /* Buffer pool lru lists. */
    BufLruListArray *m_buffer_lru;
    /* The size of Lru list partition. */
    Size m_lru_partitions;
    LruPageClean *m_lruPageClean;
    virtual void InitMemChunkAndLru();
    virtual void DestroyMemChunkAndLru();
    template <class BufferMemChunk_T>
    void AddBuffersInSpecifiedChunksToLru(BufferMemChunkWrapper<BufferMemChunk_T> *firstMemChunkToAdd);
    template <class BufferMemChunk_T>
    void InitMemChunkAndLruTemplate();
    template <class BufferMemChunk_T>
    void DestroyMemChunkAndLruTemplate();

    /*
     * Function to reset initial page information (glsn, plsn, walId) of the given bufferDesc's controller.
     * This function should only be used to clean up initial page information for page version sanity checks.
     */
    virtual void ResetBufferInitialPageInfo(BufferDesc *bufferDesc);

    /**
     * This function records any base buffers that are evicted from LRU. Usually, it's a placeholder function for
     * single-machine buffer manager.
     *
     * @param bufTag the tag of the evicted buffer
     * @param bufState the state of the evicted buffer
     * @param timestamp the Unix timestamp when the buffer gets evicted. The timestamp should be acquired under the
     * protection of buffer table's partition lock, so that we can differentiate the time series when the buffer gets
     * swapped in and out.
     */
    virtual void EvictBaseBufferCallback(const BufferTag &bufTag, uint64 bufState, uint64 timestamp);

    virtual void EvictBaseBufferInstantly(PdbId pdbId, FileId fileId);

    BufferDesc * Read(const PdbId &pdbId, const PageId &pageId, UNUSE_PARAM BufferPoolReadFlag flag,
                             BufferRing bufRing /* = nullptr */);
    /*
     * Alloc a buffer when the buf not in hashTable. If the buffer has been insert hashTable and load block from
     * storage, then, we will return the buffer that it has been insert into hashTable and the isValid param will be
     * true, otherwise,it will be false.
     */
    BufferDesc *AllocBufferForBaseBuffer(const BufferTag &bufTag, bool& isValid, bool& otherInsertHash,
        BufferRing bufRing = nullptr);

    virtual RetStatus ReadBlock(BufferDesc *bufferDesc);
    RetStatus ReadBlock(BufferTag bufTag, BufBlock block);
    RetStatus WriteBlock(BufferDesc *bufferDesc);
    RetStatus WriteBlockAsync(BufferDesc *bufferDesc, void* aioCtx);
    RetStatus SkipWriteBlock(BufferDesc *bufferDesc);

    /*
     * Loop up the base buffer associated with BufferTag in the hash table.
     *
     * If this method find the buffer in the hash table, pin and return the buffer.
     * If not find the buffer, return INVALID_BUFFER_DESC.
     */
    BufferDesc *LookupBuffer(const BufferTag &bufTag);
    /*
     * Loop up the base buffer associated with BufferTag in the hash table with shared lock.
     *
     * If this method find the buffer in the hash table, pin and return the buffer.
     * If not find the buffer, return INVALID_BUFFER_DESC.
     */
    BufferDesc *LookupBufferWithLock(const BufferTag &bufTag);
    /*
     * Make the buffer with base page free.
     *
     * (1) If the buffer has CR buffer, try to find a free CR buffer from the base buffer. If that successes,
     * release the base buffer and return the free CR buffer we get.
     * (2) If the base buffer has no CR buffer, it will flush the base buffer if it's dirty and return the base buffer.
     *
     * This method will retry several times, if it fails to free the buffer finally, it will release the buffer
     * and return INVALID_BUFFER_DESC.
     */
    BufferDesc *MakeBaseBufferFree(BufferDesc *baseCandidateBuffer, bool *needRetry, BufferRing bufRing = nullptr);
    /*
     * Try to remove the CR buffer from the CR list in the base buffer without any pin on the CR buffer.
     * After the removal from CR list, the CR buffer is completely free.
     *
     * If this method make the CR buffer free success, return true.
     * If fail to make it free, return false.
     *
     * NOTE: this method should be protected under CR assign lock.
     */
    bool MakeCrBufferFree(BufferDesc *crBufferDesc);
    /*
     * Scan the CR list in the base buffer, and try to free one of CR buffer.
     *
     * If this method make one of CR buffer free success, pin and return the free CR buffer;
     * If fail to make any CR buffer free, return INVALID_BUFFER_DESC.
     */
    BufferDesc *FindFreeCrBufferFromBaseBuffer(BufferDesc *baseBuffer, bool *needRetry);
    /*
     * Reuse the CR buffer for a base page
     * Before reuse the CR buffer, we should make sure that the CR buffer is complete free. And then try to insert
     * BufferLookupEnt into hash table.
     *  If the BufferLookupEnt is existing, set the usedBuffer as exist buffer in the hash table and return false.
     *  If not, set usedBuffer as the freeBuffer and return true.
     */
    bool ReuseCrBufferForBasePage(const BufferTag &bufTag, BufferDesc *freeBuffer, BufferDesc **usedBuffer);
    /*
     * Determine if page can be re-used in ReuseBaseBufferForBasePage
     */
    bool IsBaseBufferReusable(const uint64 bufState, BufferDesc *candidateBuffer, const PrivateRefCountEntry *ref,
                              PdbId newPdbId);
    /*
     * This function only can be called when hold hdr lock.
     */
    bool IsBufferReusableByCurrentPdb(BufferDesc *candidateBuffer, PdbId newPdbId);

    /*
     * Push buffer back to LRU or Candidate
     */
    void PushBufferBack(BufferDesc *buffer);

    /*
     * Reuse the base buffer for a base page
     * Before reusing a base buffer, it should be flushed if it's dirty and evict all the CR buffer it own.
     * After that try to insert BufferLookupEnt into hash table.
     *
     * If the BufferLookupEnt is existing, set the usedBuffer as exist buffer in the hash table and return false.
     * If the BufferLookupEnt is not existing, test if the base buffer can reuse now:
     *  -> if it can be reuse right now, set usedBuffer as the candidateBuffer and return true;
     *  -> if it can not be reused, set usedBuffer to INVALID_BUFFER_DESC and return false;
     */
    virtual bool ReuseBaseBufferForBasePage(const BufferTag &bufTag, BufferDesc *candidateBuffer,
                                            BufferDesc **usedBuffer, BufferRing bufRing = nullptr);
    /*
     * Scan candidate list to find a free buffer for base page.
     *
     * If the buffer state has BM_VALID flag, isValid is set as true, otherwise false.
     */
    BufferDesc *TryToReuseBufferForBasePage(const BufferTag &bufTag, bool *isValid, bool *needRetry,
        bool* otherInsertHash, BufferRing bufRing = nullptr);
    /*
     * Try to reuse the buffer cache the CR page for another CR page.
     */
    bool ReuseCrBufferForCrPage(BufferDesc *baseBufferDesc, BufferDesc *freeBufferDesc);
    /*
     * Try to reuse the buffer cache the base page for a CR page.
     * If the buffer reuse success, it will return true and put the candidate buffer in the CR array, otherwise it
     * will return false.
     */
    bool ReuseBaseBufferForCrPage(BufferDesc *baseBufferDesc, BufferDesc *candidateBufferDesc,
                                  BufferRing bufRing = nullptr);
    /*
     * Scan candidate list to find a free buffer for CR page and put the free buffer in the CR array of the base
     * buffer. If reuse buffer success, return the free CR buffer, otherwise return INVALID_BUFFER_DESC.
     */
    BufferDesc *TryToReuseBufferForCrPage(BufferDesc *baseBufferDesc, bool *needRetry, BufferRing bufRing = nullptr);
    /*
     * Scan the CR array in the BufferDesc to find a free slot.
     * If it finds a free slot, it will get a candidate buffer as the cr buffer and return the candidate buffer;
     * If no free slot is found, try to free a CR buffer in the CR array.
     * And set the offset as the free buffer's slot and return the free buffer.
     *
     * NOTE: This method should be protected by cr assign lock,
     * and it will be released in set_cr_range(). And this method will loop
     * until it find or free a slot. So it must make sure that one of CR buffer should be freed.
     */
    BufferDesc *AllocCrEntry(BufferDesc *baseBufferDesc, BufferRing bufRing = nullptr);
    /*
     * Scan the CR array and find the buffer which meet the snapshot.
     * Return the CR buffer if find it in the CR array, otherwise return INVALID_BUFFER_DESC.
     *
     * NOTE: This method should be protected under CR assign lock.
     */
    BufferDesc *FindCrBuffer(BufferDesc *baseBufferDesc, Snapshot snapshot);
    /*
     * Get candidate buffer from BufLruList, and try to make the buffer free.
     * For now, there are two kinds of candidate buffer we can get from BufLruList, the buffer cache the base page
     * and the buffer cache the CR page.
     *
     * For the buffer cache the base page, the free buffer should meet:
     * (1) not dirty;
     * (2) don't have associate CR buffers;
     * Although it's not complete free, because the hash entry still in the hash table, and it still can be accessed
     * by others. But we have finished most of 'dirty job', and it will be easy to reuse.
     *
     * For the buffer cache the CR page, the free buffer should meet:
     * (1) removed from the CR array in the base buffer;
     *
     * After the removal from the CR array, the buffer is complete free because on one can access it anymore.
     *
     * In this method, we get candidate buffer from BufLruList, and if the buffer cache the CR page, we try to make
     * it complete free. If the buffer cache the base page, we first try to find a free buffer in its CR array, and
     * if not find, then try to make the buffer free.
     *
     * If fail to find a free buffer, it will return INVALID_BUFFER_DESC.
     */
    BufferDesc *GetFreeBuffer(const BufferTag *bufTag, bool *needRetry, BufferRing bufRing = nullptr);

    /*
     * Check if it has some buffer is not in lru list when buf mgr destroy.
     */
    Size CheckForBufLruLeak(bool isCleanup = false);

    void StartFlushThread(FlushThreadContext *context);
    /*
     * Just flush PDB buffers without creating checkpoint. If pdbId = INVALID_PDB_ID, then flush all pdb's buffers.
     */
    RetStatus FlushBuffers(bool isBootstrap, bool onlyOwnedByMe, PdbId pdbId = INVALID_PDB_ID);
    virtual RetStatus Flush(BufferDesc *bufferDesc, gs_atomic_uint64 &actualFlushNum);

    void StartInvalidateThread(InvalidateThreadContext *context);
};

struct BufferAioContext {
    BufferDesc *bufferDesc;
    void *batchCtxMgr;
    Page *pageCopy;
    char pageData[BLCKSZ + AIO_MAX_REQNUM];
    bool hasStartAioFlag;
    uint64 submittedTime;
};
constexpr int BATCH_AIO_SIZE = 1000;
class BatchBufferAioContextMgr : public BaseObject {
public:
    BatchBufferAioContextMgr() : m_curFreeMinIndex(0), m_needCountTotal(false),
        m_memoryContext(nullptr), m_bufMgr(nullptr), m_bufferAioCtxArr(nullptr),
        m_size(0) {}

    ~BatchBufferAioContextMgr() {}

    RetStatus InitBatch(bool needCountTotal, BufMgrInterface *bufMgr, uint32 size = BATCH_AIO_SIZE);

    void DestoryBatch()
    {
        DstorePfreeExt(m_bufferAioCtxArr);
        m_bufferAioCtxArr = nullptr;
        if (m_memoryContext != nullptr) {
            DstoreMemoryContextDelete(m_memoryContext);
            m_memoryContext = nullptr;
        }
    }

    void AddOneInProgressPage()
    {
        (void)m_inProgressPages.fetch_add(1, std::memory_order_acq_rel);
    }

    void SubOneInProgressPage()
    {
        (void)m_inProgressPages.fetch_sub(1, std::memory_order_acq_rel);
    }

    void AddOneFlushedPage()
    {
        (void)m_totalFlushedCount.fetch_add(1U, std::memory_order_acq_rel);
    }

    int32 GetInProgressPages()
    {
        return m_inProgressPages.load(std::memory_order_acquire);
    }

    uint32 GetFlushedPagesNum()
    {
        /*
         * just show many local use async Flushed pages,
         * neither include remote flush or rare some local sync flush
         */
        return m_totalFlushedCount.load(std::memory_order_acquire);
    }

    void FsyncBatch();

    inline void ReuseBatch()
    {
        m_curFreeMinIndex = 0;
    }

    inline bool IsCountFlushedPages()
    {
        return m_needCountTotal;
    }

    BufferAioContext* GetBufferAioContext()
    {
        return m_bufferAioCtxArr;
    }

    /* DONOT support concurrent on the same batch for multi-thread write page. one thread own one batch. */
    RetStatus AsyncFlushPage(BufferTag &bufTag);

    static void CallbackForAioBatchFlushBuffers(ErrorCode errorCode, int64_t successSize, void *asyncContext);
private:
    uint32 m_curFreeMinIndex;
    bool m_needCountTotal;
    DstoreMemoryContext m_memoryContext;
    BufMgrInterface *m_bufMgr;
    BufferAioContext *m_bufferAioCtxArr;
    uint32 m_size;

    std::atomic<int32> m_inProgressPages;
    std::atomic<uint32> m_totalFlushedCount;
};

constexpr int MAX_OPEN_TIMES = 2;

extern bool StartIo(BufferDesc *bufferDesc, bool forInput);

extern void TerminateIo(BufferDesc *bufferDesc, bool clearDirty, uint64 setFlagBits);
extern void TerminateAsyncIo(BufferDesc* bufferDesc, bool clearDirty, uint64 setFlagBit);

extern inline void ReleasePageLockForAsyncFlush(RetStatus ret, BufferDesc *bufferDesc, void *aioCtx)
{
    AsyncIoContext *aioContext = static_cast<AsyncIoContext *>(aioCtx);
    BufferAioContext *bufferAioCtx = static_cast<BufferAioContext *>(aioContext->asyncContext);
    bool hasStartAioFlag = bufferAioCtx->hasStartAioFlag;
    if (STORAGE_FUNC_FAIL(ret) && hasStartAioFlag) {
        bufferDesc->UnpinForAio();
    }
}


template <class BufferMemChunk_T>
RetStatus BufMgr::DoWhenBufferpoolResizeTemplate(Size bufferPoolNewSize, Size numMemChunksToChange, bool isUpSize,
    StringInfoData &outputMessage)
{
    StorageAssert(LWLockHeldByMe(&m_sizeLock));
    RetStatus rc = DSTORE_SUCC;

    if (isUpSize) {
        /*
         * Similar to bufferpool initialization, new memchunks will be allocated and appended
         * to the existing list.
         */
        BufferMemChunkWrapper<BufferMemChunk_T> *firstMemChunkToAdd = nullptr;
        rc = m_bufferMemChunkList->AppendBufferMemChunkList<BufferMemChunk_T>(numMemChunksToChange,
            &firstMemChunkToAdd);
        if (STORAGE_FUNC_FAIL(rc)) {
            outputMessage.append("Failed to allocate %lu memchunks.", numMemChunksToChange);
            return rc;
        }

        /* New memchunks are added to LRU candidate list. */
        AddBuffersInSpecifiedChunksToLru(firstMemChunkToAdd);
        m_buffer_lru->ResizeHotList(m_size);
    } else {
        /* For bufferpool shrink, shrink hot lists before invalidating memchunks. */
        m_buffer_lru->ResizeHotList(bufferPoolNewSize);
        rc = DoWhenBufferpoolDownSize(numMemChunksToChange);
        if (STORAGE_FUNC_FAIL(rc)) {
            m_buffer_lru->ResizeHotList(m_size);
            outputMessage.append("Failed to resize the bufferpool. Please try again later. ");
        }
    }

    return rc;
}

template <class BufferMemChunk_T>
void BufMgr::AddBuffersInSpecifiedChunksToLru(BufferMemChunkWrapper<BufferMemChunk_T> *firstMemChunkToAdd)
{
    m_buffer_lru->ResetLruIndex();
    m_bufferMemChunkList->LockBufferMemChunkList(LW_SHARED);
    Size memChunkSize = m_bufferMemChunkList->GetNumOfBufInMemChunk();
    BufferMemChunkWrapper<BufferMemChunk_T> *memChunkWrapper = firstMemChunkToAdd;
    while (memChunkWrapper != nullptr) {
        for (Size i = 0; i < memChunkSize; i++) {
            BufferDesc *bufferDesc = memChunkWrapper->memChunk->GetBufferDesc(i);
            m_buffer_lru->AddNewBufferDesc(bufferDesc, true);
        }
        memChunkWrapper = memChunkWrapper->GetNext();
    }
    m_bufferMemChunkList->UnlockBufferMemChunkList();
}


template <class BufferMemChunk_T>
void BufMgr::InitMemChunkAndLruTemplate()
{
    RetStatus rc = DSTORE_SUCC;
    Size numOfBufInMemChunk = BUF_MEMCHUNK_MAX_NBLOCKS;
    StorageAssert(m_size > 0);
    StorageAssert(m_lru_partitions > 0);
    StorageAssert(numOfBufInMemChunk > 0);
    ErrLog(DSTORE_LOG, MODULE_BUFMGR,
           ErrMsg("Initial value for m_size:%lu, numOfBufInMemChunk:%lu, m_lru_partitions:%lu",
                  m_size, numOfBufInMemChunk, m_lru_partitions));

    /*
     * Step 1.1
     * Adjust down memchunk size and LRU partitions if they are too large relative to buffer pool size.
     *
     * If the configured buffer pool size is less than (memchunk size * BUF_MEMCHUNK_MIN_NCHUNKS),
     * then the memchunk size will be adjusted down.
     * For example, if buffer pool size = 128kB (16 blocks), memchunk size = 10GB (10 * 131072 blocks),
     * then memchunk size will be adjusted to 16kB (2 blocks).
     *
     * If the configured buffer pool size is greater than (memchunk size * BUF_MEMCHUNK_MIN_NCHUNKS),
     * then the buffer pool size will be adjusted down.
     * For example, if buffer pool size = 105GB (105 * 131072 blocks), memchunk size = 10GB (10 * 131072 blocks),
     * then buffer pool size will be adjusted to 100GB (100 * 131072 blocks).
     */
    numOfBufInMemChunk = DstoreMin(numOfBufInMemChunk, m_size / BUF_MEMCHUNK_MIN_NCHUNKS);
    /*
     * If the configured buffer pool size is less than the number of LRU partitions,
     * then the number of LRU partitions will be adjusted down.
     * For example, if buffer pool size = 128kB (16 blocks), LRU partitions = 32,
     * then we can only accommodate a maximum of 16 LRU partitions.
     */
    m_lru_partitions = DstoreMin(m_lru_partitions, m_size);
    /*
     * Step 1.2
     * Align memchunk size and LRU partitions to each other such that one must be divisible by the other.
     */
    if (numOfBufInMemChunk > m_lru_partitions) {
        numOfBufInMemChunk = DstoreRoundDown<Size>(numOfBufInMemChunk, m_lru_partitions);
    } else {
        m_lru_partitions = DstoreRoundDown<Size>(m_lru_partitions, numOfBufInMemChunk);
    }
    StorageAssert(m_lru_partitions > 0);
    StorageAssert(numOfBufInMemChunk > 0);

    /*
     * Step 1.3
     * Align buffer pool size such that it must be divisible by both memchunk size and LRU partitions.
     */
#ifdef UT
    m_size = DstoreRoundUp<Size>(m_size, DstoreMax(numOfBufInMemChunk, m_lru_partitions));
#else
    m_size = DstoreRoundDown<Size>(m_size, DstoreMax(numOfBufInMemChunk, m_lru_partitions));
#endif
    StorageAssert(m_size > 0);
    ErrLog(DSTORE_LOG, MODULE_BUFMGR,
           ErrMsg("Aligned value for m_size:%lu using numOfBufInMemChunk:%lu and m_lru_partitions:%lu",
                  m_size, numOfBufInMemChunk, m_lru_partitions));

    /*
     * Step 2
     * Initialize buffer memchunk doubly linked list and allocate all memchunks.
     */
    Size numMemChunksToAdd = Size(m_size / numOfBufInMemChunk);
    BufferMemChunkWrapper<BufferMemChunk_T> *firstMemChunkToAdd = nullptr;
    m_bufferMemChunkList = DstoreNew(g_dstoreCurrentMemoryContext) BufferMemChunkList(m_size, numOfBufInMemChunk);
    StorageReleasePanic(m_bufferMemChunkList == nullptr, MODULE_BUFMGR,
        ErrMsg("alloc memory for bufferMemChunkList fail!"));
    rc = m_bufferMemChunkList->AppendBufferMemChunkList<BufferMemChunk_T>(numMemChunksToAdd, &firstMemChunkToAdd);
    if (firstMemChunkToAdd != nullptr) {
        ErrLog(DSTORE_LOG, MODULE_BUFMGR,
               ErrMsg("Allocating %lu memchunks for bufferpool size of %lu (blocks) and, "
                      "memchunk size of %lu (blocks). MemChunk total size:%lu, sizeof MemChunk:%lu, "
                      "bufferDesc size:%lu, blockSize:%lu, controllerSize:%lu, sizeof lru list partition:%lu, "
                      "BUF_MEMCHUNK_MIN_NCHUNKS:%lu, BUF_MEMCHUNK_MAX_NBLOCKS:%lu",
                      numMemChunksToAdd, m_size, numOfBufInMemChunk,
                      firstMemChunkToAdd->memChunk->GetTotalSize(), sizeof(BufferMemChunk_T),
                      firstMemChunkToAdd->memChunk->GetBufferDescSize(),
                      firstMemChunkToAdd->memChunk->GetBufferBlockSize(),
                      firstMemChunkToAdd->memChunk->GetBufferControllerSize(),
                      m_lru_partitions, BUF_MEMCHUNK_MIN_NCHUNKS, BUF_MEMCHUNK_MAX_NBLOCKS));
    }
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR,
               ErrMsg("Failed to allocate %lu memchunks for bufferpool size of %lu (blocks) and"
                      "memchunk size of %lu (blocks).",
                      numMemChunksToAdd, m_size, numOfBufInMemChunk));
    } else {
        ErrLog(DSTORE_LOG, MODULE_BUFMGR,
               ErrMsg("Allocate %lu memchunks for bufferpool size of %lu (blocks) and"
                      "memchunk size of %lu (blocks) success.",
                      numMemChunksToAdd, m_size, numOfBufInMemChunk));
    }
    StorageAssert(m_bufferMemChunkList->GetSize() == numMemChunksToAdd);

    /*
     * Step 3
     * Initialize buffer pool HOT/LRU/CANDIDATE lists and add the new buffers to CANDIDATE list initially.
     */
    m_buffer_lru = DstoreNew(g_dstoreCurrentMemoryContext) BufLruListArray(m_lru_partitions);
    StorageReleasePanic(m_buffer_lru == nullptr, MODULE_BUFMGR, ErrMsg("alloc memory for m_buffer_lru fail!"));
    m_buffer_lru->InitBufLruListArray(m_size);
    AddBuffersInSpecifiedChunksToLru(firstMemChunkToAdd);

    m_lruPageClean = new LruPageClean(
        m_buffer_lru, static_cast<uint64>(static_cast<float>(m_size) / static_cast<float>(m_lru_partitions)),
        m_buftable);
    m_lruPageClean->Init();
#ifndef UT
    m_lruPageClean->StartWorkThreads();
#endif
}

template <class BufferMemChunk_T>
void BufMgr::DestroyMemChunkAndLruTemplate()
{
    m_lruPageClean->StopWorkThreads();
    delete m_lruPageClean;
    m_buffer_lru->Destroy();
    delete m_buffer_lru;

    m_bufferMemChunkList->DestroyBufferMemChunkList<BufferMemChunk_T>();
    delete m_bufferMemChunkList;
}

} /* namespace DSTORE */

#endif
