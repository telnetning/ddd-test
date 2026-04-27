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
 * dstore_heap_handler.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/heap/dstore_heap_handler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_HANDLER_H
#define DSTORE_HEAP_HANDLER_H

#include "common/log/dstore_log.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "wal/dstore_wal_write_context.h"
#include "framework/dstore_instance.h"

namespace DSTORE {
class TableStorageMgr : public BaseObject {
public:
    explicit TableStorageMgr(PdbId pdbId, int fillFactor, TupleDesc tupDesc, RelationPersistence persistenceMethod);
    ~TableStorageMgr();
    DISALLOW_COPY_AND_MOVE(TableStorageMgr);
    PageId GetPageFromFSM(uint32 size, uint16 retryTime, uint32 *spaceInFsm);
    PageId GetNewPage();
    RetStatus UpdateFSM(const PageId &pageId, uint32 size);
    RetStatus UpdateFSM(const FsmIndex &fsmIndex, uint32 size);
    RetStatus UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded,
        uint16 retryTimes, PageId *pageId, uint32* spaceInFsm);
    uint32 GetSaveFreeSpace() const;
    RetStatus Init(const PageId &segmentId, TablespaceId tablespaceId, DstoreMemoryContext context);
    void SetLastPageIdForInsert(const PageId &pageId);
    PageId GetLastPageIdForInsert();
    PageId GetSegMetaPageId();
    uint64 GetTableBlockCount();
    HeapSegment *GetSegment();
    TupleDesc GetTupleDesc();
    bool NeedUpdateFsm(uint32 prevSize, uint32 curSize) const;
    RelationPersistence GetPersistenceMethod() const;
    bool IsGlobalTempTable() const;
    void SetTupleDesc(TupleDesc tupDesc);
    Oid GetTableOid() const;
    void SetTableOid(Oid tableOid);
    HeapSegment *m_segment;
    TupleDesc m_tupDesc;
    PdbId m_pdbId;
private:
    PageId m_lastPageIdForInsert;
    PageId m_segMetaPageId;
    RelationPersistence m_persistenceMethod;

#ifdef DSTORE_USE_ASSERT_CHECKING
    ThreadId m_threadPid;
#endif
    int m_fillFactor;

    DstoreMemoryContext m_ctx;
    Oid m_tableOid;
};

class HeapHandler : virtual public BaseObject {
public:
    HeapHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                bool isLobOperation = false, bool isUseRingBuf = false);

    virtual ~HeapHandler() = default;

    TableStorageMgr *GetTableSmgr() const;

    StorageRelation m_heapRel;

    void ReportHeapBufferReadStat(StorageRelation relation)
    {
        Oid relOid = relation->relOid;
        if (likely(!g_storageInstance->GetGuc()->enableTrackActivities)) {
            return;
        }
        if (likely(
            relOid != DSTORE_INVALID_OID && !IsTemplate(m_heapRel->m_pdbId) && g_storageInstance->IsInit() &&
            !g_storageInstance->IsBootstrapping() && !thrd->m_bufferReadStat.isReporting)) {
            thrd->m_bufferReadStat.isReporting = true;
            g_storageInstance->GetStat()->ReportCountBuffer(relation, thrd->m_bufferReadStat.bufferReadCount,
                                                            thrd->m_bufferReadStat.bufferReadHit);
            thrd->m_bufferReadStat.resetBufferReadStat();
        }
        return;
    }

    inline Oid GetTableOid() const
    {
        return m_isLob ? m_heapRel->lobTableSmgr->GetTableOid() : m_heapRel->tableSmgr->GetTableOid();
    }

protected:
    HeapTuple *AssembleTuples(HeapTuple **tupChunks, uint32 numTupChunks);
    void TryToUnlockReleaseBufferDesc();
    void GenerateAllocTdWal(BufferDesc *bufferDesc);

    /*
     * Error handling when get buffer. If can't get buffer for undo page after insert heap page done, We must
     * rollback the change of heap page.
     */
    void HandleErrorWhenGetUndoBuffer(BufferDesc *bufDesc, UndoRecord *undoRec, void *undoDataRec = nullptr);
    UndoRecPtr InsertUndoAndCheck(UndoRecord* undoRecord);
    HeapTuple* FormIdentityTuple(HeapTuple *tuple, Bitmapset *replicaKeyAttrs);

    /* Check the page read from buffer by pageid and pagetype */
    inline void CheckBufferedPage(Page *page, const PageId &pageIdIn) const
    {
        if (unlikely(page->PageNoInit())) {
            return;
        }
        PageId selfPageId = page->GetSelfPageId();
        if (unlikely(selfPageId != pageIdIn)) {
            char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(m_heapRel->m_pdbId, selfPageId.m_fileId,
                selfPageId.m_blockId);
            if (likely(clusterBufferInfo != nullptr)) {
                ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("%s", clusterBufferInfo));
                DstorePfreeExt(clusterBufferInfo);
            }
            PrintBackTrace();
            ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("Page Confused! "
                "bufTag:(%hhu, %hu, %u)" PAGE_HEADER_FMT,
                m_heapRel->m_pdbId, page->GetFileId(), page->GetBlockNum(), PAGE_HEADER_VAL(page)));
        }
        if (unlikely(page->GetType() != PageType::HEAP_PAGE_TYPE)) {
            char *clusterBufferInfo = m_bufMgr->GetClusterBufferInfo(m_heapRel->m_pdbId, selfPageId.m_fileId,
                selfPageId.m_blockId);
            if (likely(clusterBufferInfo != nullptr)) {
                ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("%s", clusterBufferInfo));
                DstorePfreeExt(clusterBufferInfo);
            }
            PrintBackTrace();
            ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("Page Confused! "
                "bufTag:(%hhu, %hu, %u)" PAGE_HEADER_FMT,
                m_heapRel->m_pdbId, page->GetFileId(), page->GetBlockNum(), PAGE_HEADER_VAL(page)));
        }
    }

    bool NeedWal()
    {
        return (!(m_isLob ? m_heapRel->lobTableSmgr->IsGlobalTempTable() :
                    m_heapRel->tableSmgr->IsGlobalTempTable()));
    }

    StorageInstance *m_instance;
    ThreadContext *m_thrd;
    BufMgrInterface *m_bufMgr;
    BufferDesc *m_bufferDesc;
    TDAllocContext m_tdcontext;
    bool            m_useRingBuf;   /* Indicates whether to use the buffer ring policy. */
    BufferRing      m_ringBuf;      /* Buffer ring object */
    bool m_isLob;
};

} /* namespace DSTORE */
#endif
