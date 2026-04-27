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
#include <gtest/gtest.h>
#include "heap/dstore_heap_vacuum.h"
#include "flashback/dstore_flashback_interface.h"
#include "ut_heap/ut_heap.h"
namespace DSTORE {
SnapshotData g_snapShotNow{SnapshotType::SNAPSHOT_NOW, MAX_COMMITSEQNO, FIRST_CID};
Snapshot SNAPSHOT_NOW = (Snapshot)&g_snapShotNow;
}
TEST_F(UTHeap, FlashbackTableDeltaTupleTest_TIER1_level0)
{
    std::vector<HeapTuple *> tuples;
    std::vector<ItemPointerData> ctids;

    /* Insert tuples 1-15 (offset in page 1-15, idx 0-14) */
    for (int i = 0; i < 15; ++i) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("insert_tuple_" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ctids.push_back(ctid);
    }

    /* Update tuples 6-10 */
    for (int i = 5; i < 10; ++i) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("1st_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuples[i]);
        tuples[i] = tuple;
    }

    /* Get a snapshot and save it */
    SnapshotData snp = ConstructCurSnapshot();

    /* Insert tuples 16-25 */
    for (int i = 15; i < 25; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("insert_tuple_" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ctids.push_back(ctid);
    }

    /* Update tuples 7-12 */
    for (int i = 6; i < 12; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("2nd_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuples[i]);
        tuples[i] = tuple;
    }

    /* Update tuples 14-15 */
    for (int i = 13; i < 15; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("3rd_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuples[i]);
        tuples[i] = tuple;
    }

    /* Update tuples 20-25 */
    for (int i = 19; i < 25; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("4th_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuples[i]);
        tuples[i] = tuple;
    }

    /* Start getting delta tuples */
    FlashbackTableHandler *handler =
        FlashbackInterface::CreateFlashbackTableHandler(m_utTableHandler->GetTableRel(), snp.GetCsn(), SNAPSHOT_NOW);

    /* Tuples 7-12, 14-25 should be delta tuples */
    std::vector<int> deltaIdxs;
    for (int i = 6; i < 12; ++i) {
        deltaIdxs.push_back(i);
    }
    for (int i = 13; i < 25; ++i) {
        deltaIdxs.push_back(i);
    }

    thrd->GetActiveTransaction()->Start();
    HeapTuple *deltaTuple;
    for (int i : deltaIdxs) {
        deltaTuple = FlashbackInterface::GetDeltaTuple(handler);
        StorageAssert(deltaTuple != nullptr);
        ItemPointerData *ctid = deltaTuple->GetCtid();
        StorageAssert(ctids[i].m_placeHolder == ctid->m_placeHolder);
        int32 dataSize = tuples[i]->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void*)deltaTuple->GetDiskTuple()->GetData(), tuples[i]->GetDiskTuple()->GetData(), dataSize), 0);
    }

    /* No delta tuples left */
    deltaTuple = FlashbackInterface::GetDeltaTuple(handler);
    EXPECT_EQ(deltaTuple, nullptr);
    FlashbackInterface::DestroyFlashbackHandler(handler);
    thrd->GetActiveTransaction()->Commit();

    for (int i = 0; i < 25; ++i) {
        DstorePfreeExt(tuples[i]);
    }
}


TEST_F(UTHeap, FlashbackTableLostTupleTest_level0)
{
    std::vector<HeapTuple *> tuples; /* tuples before the snapshot */
    std::vector<ItemPointerData> ctids;

    /* Insert tuples 1-15 (offset in page 1-15, idx 0-14) */
    for (int i = 0; i < 15; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("insert_tuple_" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ctids.push_back(ctid);
    }

    /* Update tuples 6-10 */
    for (int i = 5; i < 10; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("1st_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuples[i]);
        tuples[i] = tuple;
    }

    /* Get a snapshot and save it */
    SnapshotData snp = ConstructCurSnapshot();
        printf("%d cns \n", snp.GetCsn());

    /* Delete tuples 9-15 */
    for (int i = 8; i < 15; ++i) {
        RetStatus ret = DeleteTuple(&ctids[i]);
        ASSERT_EQ(ret, DSTORE_SUCC);
    }

    /* Update tuples 1-3 */
    for (int i = 0; i < 3; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("2nd_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuple);
    }

    /* Update tuples 6-7 */
    for (int i = 5; i < 7; ++i) {
        HeapTuple *tuple = UTTableHandler::GenerateSpecificHeapTuple("3rd_update_" + std::to_string(i));
        ItemPointerData ctid = UpdateTupAndCheckResult(&ctids[i], tuple);
        StorageAssert(ctids[i].m_placeHolder == ctid.m_placeHolder);
        DstorePfreeExt(tuple);
    }

    /*
     * Delete delta tuples first. This is needed because GetLostTuple() logic need all delta tuples to be deleted in
     * order to do its visibility check.
     * For instance, tuple A has version 8 and 15, snapshotCSN = 10. delta tuple tuple A (version 15) gets deleted, so
     * Lost tuple version 8 can be directly be treated as 'lost' when checking the base page.
     */
    {
        thrd->GetActiveTransaction()->Start();
        FlashbackTableHandler *handler =
            FlashbackInterface::CreateFlashbackTableHandler(m_utTableHandler->GetTableRel(), snp.GetCsn(), SNAPSHOT_NOW);
        HeapTuple *tuple = nullptr;
        int i = 0;
        while ((tuple = FlashbackInterface::GetDeltaTuple(handler)) != nullptr) {
            /* HeapInterface::Delete() has visibility check, so we need to delete the row from SNAPSHOT_NOW */
            SnapshotData snapshot = {DSTORE::SnapshotType::SNAPSHOT_MVCC, MAX_COMMITSEQNO, INVALID_CID};
            printf("deleting the %d th delta tuple\n", i++);
            RetStatus ret = DeleteTuple(&tuple->m_head.ctid, &snapshot, true);
            ASSERT_EQ(ret, DSTORE_SUCC);
        }
        FlashbackInterface::DestroyFlashbackHandler(handler);
        thrd->GetActiveTransaction()->Commit();
    }

    /* Start getting lost tuples */
    FlashbackTableHandler *handler =
        FlashbackInterface::CreateFlashbackTableHandler(m_utTableHandler->GetTableRel(), snp.GetCsn(), &snp);

    /* Tuples 1-3, 6-7, and 9-15 should be lost tuples */
    std::vector<int> lostIdxs;
    for (int i = 0; i < 3; ++i) {
        lostIdxs.push_back(i);
    }
    for (int i = 5; i < 7; ++i) {
        lostIdxs.push_back(i);
    }
    for (int i = 8; i < 15; ++i) {
        lostIdxs.push_back(i);
    }

    thrd->GetActiveTransaction()->Start();
    HeapTuple *lostTuple;
    for (int i : lostIdxs) {
        lostTuple = FlashbackInterface::GetLostTuple(handler);
        StorageAssert(lostTuple != nullptr);
        ItemPointerData *ctid = lostTuple->GetCtid();
        StorageAssert(ctids[i].m_placeHolder == ctid->m_placeHolder);
        int32 dataSize = tuples[i]->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void*)lostTuple->GetDiskTuple()->GetData(), tuples[i]->GetDiskTuple()->GetData(), dataSize), 0);
    }

    /* No lost tuples left */
    lostTuple = FlashbackInterface::GetLostTuple(handler);
    EXPECT_EQ(lostTuple, nullptr);
    FlashbackInterface::DestroyFlashbackHandler(handler);
    thrd->GetActiveTransaction()->Commit();

    for (int i = 0; i < 15; ++i) {
        DstorePfreeExt(tuples[i]);
    }
}
