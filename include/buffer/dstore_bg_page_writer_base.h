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
 * dstore_bg_page_writer_base.h
 *
 * IDENTIFICATION
 *        include/buffer/dstore_bg_page_writer_base.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_BG_PAGE_WRITER_BASE_H
#define DSTORE_BG_PAGE_WRITER_BASE_H

#include <condition_variable>
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_thread.h"
#include "buffer/dstore_buf.h"

namespace DSTORE {
/*
 * BgPageWriter handle dirty pages flush in a background thread, and it's associated with a WalStream.
 * In the BgPageWriter, it will initialize multi dirty page queue to store dirty pages. And pages are
 * partitioned by PageId.
 */
class BgPageWriterBase : public BaseObject {
public:
    BgPageWriterBase();
    virtual ~BgPageWriterBase() = default;
    DISALLOW_COPY_AND_MOVE(BgPageWriterBase);

    virtual RetStatus Init()
    {
        m_memContext =
            DstoreAllocSetContextCreate(thrd->m_memoryMgr->GetRoot(), "BgPageMasterWriter", ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        if (STORAGE_VAR_NULL(m_memContext)) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("filed to create m_memContext"));
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    /* Stop BgPageWriter background thread. */
    virtual void Stop();
    bool IsStop() const;

    /* Check if the BgPageWriter thread is ready */
    bool IsReady() const;
    void SetReady();

    /* Release some resource before BgPageWriter exit */
    virtual void BgPageWriterExit();

    /* Find valid DirtyPageQueueSlot in the queue, and skip the invalid one. */
    BufferDesc *FindValidSlotFromDirtyPageQueue(BufferDesc *start, const int64 slotId);

    DstoreMemoryContext m_memContext;
    std::atomic_bool m_shutdownRequest;
    std::atomic_bool m_isReady;
};

union UnionU64 {
    uint64 u64;
    uint32 u32[2];
};
constexpr uint64 FLUSH_CXT_MAGIC_NUMBER = 0xbeef5a5a;
struct CandidateFlushCxt {
    std::atomic<uint64> info;
    uint32 capacity;
    uint64 *magicNumberHead;
    BufferDesc **candidateFlushArray;
    uint64 *magicNumberTail;

    uint32 GetValidSize()
    {
        UnionU64 tmp;
        tmp.u64 = info.load();
        return tmp.u32[0];
    }

    uint32 GetStartFlushLoc()
    {
        UnionU64 tmp;
        tmp.u64 = info.load();
        return tmp.u32[1];
    }

    uint32 ScrambleLoc(uint32 batch)
    {
        UnionU64 newUnion;
        uint64 originValue = info.load();
        newUnion.u64 = originValue;
        newUnion.u32[1] = newUnion.u32[1] + batch;
        while (!info.compare_exchange_strong(originValue, newUnion.u64)) {
            newUnion.u64 = originValue;
            newUnion.u32[1] = newUnion.u32[1] + batch;
        }
        return newUnion.u32[1] - batch;
    }

    void SetValidSize(uint32 loc, uint32 orignLoc)
    {
        UnionU64 originUnion;
        UnionU64 newUnion;
        uint64 originValue = info.load();
        originUnion.u64 = originValue;
        newUnion.u32[0] = loc;
        if (originUnion.u32[1] < orignLoc) {
            newUnion.u32[1] = originUnion.u32[1];
        } else {
            newUnion.u32[1] = orignLoc;
        }

        while (!info.compare_exchange_strong(originValue, newUnion.u64)) {
            originUnion.u64 = originValue;
            if (originUnion.u32[1] < orignLoc) {
                newUnion.u32[1] = originUnion.u32[1];
            } else {
                newUnion.u32[1] = orignLoc;
            }
        }
        CheckMagicNumber();
    }

    void Set(uint64 num)
    {
        info.store(num);
    }

    RetStatus Init(uint32 bufferSize)
    {
        capacity = bufferSize;
        info = 0;
        void *array = DstorePalloc(sizeof(BufferDesc *) * capacity + sizeof(uint64) * 2);
        if (STORAGE_VAR_NULL(array)) {
            ErrLog(DSTORE_ERROR, MODULE_BGPAGEWRITER, ErrMsg("Failed to malloc memory for candidateFlushArray."));
            return DSTORE_FAIL;
        }
        magicNumberHead = static_cast<uint64 *>(array);
        candidateFlushArray = static_cast<BufferDesc **>(static_cast<void *>(
            static_cast<char *>(array) + sizeof(uint64)));
        magicNumberTail = static_cast<uint64 *>(static_cast<void *>(
            static_cast<char *>(array) + sizeof(uint64) + sizeof(BufferDesc *) * capacity));
        
        *magicNumberHead = FLUSH_CXT_MAGIC_NUMBER;
        *magicNumberTail = FLUSH_CXT_MAGIC_NUMBER;
        return DSTORE_SUCC;
    }

    void Destroy()
    {
        capacity = 0;
        info = 0;
        DstorePfreeExt(magicNumberHead);
        magicNumberHead = nullptr;
        candidateFlushArray = nullptr;
        magicNumberTail = nullptr;
    }

    inline void CheckMagicNumber()
    {
        StorageReleasePanic((*magicNumberHead != FLUSH_CXT_MAGIC_NUMBER), MODULE_BUFFER,
            ErrMsg("candidate queue is corrupt"));
        StorageReleasePanic((*magicNumberTail != FLUSH_CXT_MAGIC_NUMBER), MODULE_BUFFER,
            ErrMsg("candidate queue is corrupt"));
    }
};

class BgPageSlaveWriter : public BgPageWriterBase {
public:
    explicit BgPageSlaveWriter(CandidateFlushCxt *flushCxt);
    ~BgPageSlaveWriter() override = default;
    DISALLOW_COPY_AND_MOVE(BgPageSlaveWriter);

    bool IsFlushing();

    static void StartUp(PdbId pdbId);

    virtual void SeizeDirtyPageListForFlush();

    void BgPageWriterExit() final
    {
        m_isFlushing.store(false, std::memory_order_release);
        BgPageWriterBase::BgPageWriterExit();
    }

    void WakeupIfStopping();

    uint32 m_startFlushLoc;
    uint32 m_needFlushCnt;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::atomic_bool m_isFlushing;
    CandidateFlushCxt *m_flushCxt;
};

}

#endif