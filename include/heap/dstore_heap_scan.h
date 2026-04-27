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
 * dstore_heap_interface.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_scan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_SCAN_H
#define DSTORE_HEAP_SCAN_H

#include "index/dstore_scankey.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_struct.h"
#include "page/dstore_heap_page.h"
#include "tablespace/dstore_data_segment_context.h"

namespace DSTORE {

/* Note: If multiple inheritance is required in the future, virtual inheritance must be used. */
class HeapScanHandler : public HeapHandler {
public:
    HeapScanHandler(StorageInstance *instance, ThreadContext *thread,
                    StorageRelation heapRel, bool isLobOperation = false, bool isUseRingBuf = false);
    ~HeapScanHandler() override = default;
    HeapScanHandler(const HeapScanHandler &) = delete;
    HeapScanHandler &operator = (const HeapScanHandler &) = delete;

    RetStatus Begin(Snapshot snapshot, bool showAnyTupForDebug = false);
    void ReScan();
    void ResetController();
    RetStatus SetParallelController(ParallelWorkController *controller, int smpId = 1);
    void SetScanKey(TupleDesc desc, int nkeys, ScanKey key);
    void End() noexcept;

    inline void ReleaseLastBuffer() noexcept
    {
        ReleaseLastScanBuffer();
        ReleaseLastFetchBuffer();
    }

    HeapTuple *SeqScan();
    RetStatus SampleScan(HeapSampleScanContext* sampleScanContext);
    HeapTuple *FetchTuple(ItemPointerData &ctid, bool needCheckVisibility = true);
    RetStatus FetchAllVisiableTupleInPage(PageId pageId, OffsetNumber *offsetArray, uint16 arrayLen,
                                          uint16 &validLenlen);
    /* just for fetch tuple */
    inline void EndFetch()
    {
        ReleaseLastFetchBuffer();
    }
    DSTORE_LOCAL HeapTuple *FetchBigTuple(HeapTuple *tuple, bool needCheckVisibility);

    void DumpScanPage(Datum &fileId, Datum &blockId, Datum &data);

    void DumpPage(PageId pageId, Datum &data);

    inline bool IsUsingMvccSnapshot()
    {
        return m_snapshot.snapshotType == SnapshotType::SNAPSHOT_MVCC;
    }

    FailureInfo m_failureInfo;

private:
    inline void ReleaseLastScanBuffer()
    {
        if (m_crBufDesc != INVALID_BUFFER_DESC) {
            m_bufMgr->Release(m_crBufDesc);
            m_crBufDesc = INVALID_BUFFER_DESC;
        }
    }

    inline void ReleaseLastFetchBuffer()
    {
        if (unlikely(m_curPageDesc != INVALID_BUFFER_DESC)) {
            m_bufMgr->Release(m_curPageDesc);
            m_curPageDesc = INVALID_BUFFER_DESC;
            m_curPageId = INVALID_PAGE_ID;
        }
    }

    void ClearScanKey() noexcept;
    void ClearLastScanBigTuple() noexcept;
    HeapTuple *SeqScanNext();
    bool ExecMatchScanKey(HeapTuple* tuple);
    RetStatus PrepareValidCrPage(HeapPage *&crPage, PageId &oldPageId);

    HeapTuple *FetchNewestDiskTuple(ItemPointerData ctid);
    HeapTuple *FetchVisibleDiskTuple(ItemPointerData &ctid);

    RetStatus StatTupleByInplaceUpdate(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
        bool isInProgress, int &liveRows, int &numTuples);
    RetStatus StatVisibleTuple(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
        int &liveRows, int &numTuples);
    RetStatus StatEndtuple(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
        bool isInProgress, int &liveRows, int &numTuples);

    template<bool isInProgress>
    RetStatus TupleStatByLiveMode(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
        int &liveRows, int &deadRows, int &numTuples);
    RetStatus TupleNumStat(HeapSampleScanContext *sampleScanContext, HeapPage *curHeapPage,
        int &liveRows, int &deadRows, int &numTuples);

    /* for show any tuples */
    HeapTuple *SeqScanForShowAnyTup();
    RetStatus PreparePageForShowAnyTup(HeapPage *&curPage);
    HeapTuple *FetchHistoricTupleForDebug(ItemPointerData &ctid);

    /* for scan */
    bool m_startFlag;
    bool m_scanOver;
    DataSegmentScanContext *m_segScanContext;
    uint16 m_curOffset;
    HeapPage *m_localCrPage;
    BufferDesc *m_crBufDesc;
    HeapTuple m_resTuple;
    HeapTuple *m_bigTuple;

    /* for fetch */
    PageId m_curPageId;
    BufferDesc *m_curPageDesc;

    /* Scan key information */
    int m_numScanKey;      /* number of pushed down scan keys */
    ScanKey m_scanKey;     /* array of pushed down scan key descriptors */
    TupleDesc m_tupDesc;   /* heap tuple descriptor for rs_ctup */
    SnapshotData m_snapshot;

    /* for show any tuples */
    ShowAnyTupleContext *m_showAnyTupForDebug;
};

} /* namespace DSTORE */

#endif
