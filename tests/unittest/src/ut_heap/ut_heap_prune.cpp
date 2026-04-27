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
#include "ut_heap/ut_heap.h"
#include "page/dstore_data_page.h"

TEST_F(UTHeap, HeapCompactTupleTest_TIER1_level0)
{
    HeapPage *page = new HeapPage();
    errno_t rc = memset_s(page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");

    page->Init(0, PageType::HEAP_PAGE_TYPE, INVALID_PAGE_ID);
    page->SetRecentDeadTupleMinCsn(INVALID_CSN);
    page->SetPotentialDelSize(0);
    page->SetDataHeaderSize(HEAP_PAGE_HEADER_SIZE);
    page->m_header.m_lower = page->DataHeaderSize();
    page->AllocateTdSpace();

    /* Step1: Generate disk tuples. */
    std::vector<std::string> heapDiskTups;
    constexpr int NUM_TUP = 26;
    char x = 'a';
    for (int i = 0; i < NUM_TUP; i++) {
        heapDiskTups.push_back(std::string(i + sizeof(HeapDiskTuple), x));
        x++;
    }

    /* Step2: Add disk tuples to page. */
    std::vector<OffsetNumber> heapDiskTupOffset;
    constexpr int INTERVAL = 3;
    ItemIdCompactData items[NUM_TUP];
    int cnt = 0;
    for (int i = 0; i < NUM_TUP; i++) {
        HeapDiskTuple *diskTuple = (HeapDiskTuple *)heapDiskTups[i].c_str();
        diskTuple->SetTdStatus(DSTORE::DETACH_TD);
        OffsetNumber offset = page->AddTuple(diskTuple, heapDiskTups[i].length());
        heapDiskTupOffset.push_back(offset);
        if (i % INTERVAL == 1) {
            ItemId *id = page->GetItemIdPtr(offset);
            items[cnt].itemOffnum = offset;
            items[cnt].tupOffset = id->GetOffset();
            items[cnt].itemLen = id->GetLen();
            cnt++;
        }
    }

    /* Step3: Sort items descendingly by offset. */
    qsort(items, cnt, sizeof(ItemIdCompactData), ItemIdCompactData::Compare);

    /* Step4: Compact tuples. */
    uint16 upper = page->CompactTuples(items, cnt);

    /* Step5: Check tuples are correct. */
    for (int i = 0; i < NUM_TUP; i++) {
        if (i % INTERVAL == 1) {
            char *tuple = (char *)page->GetRowData(heapDiskTupOffset[i]);
            EXPECT_EQ(memcmp(tuple, heapDiskTups[i].c_str(), heapDiskTups[i].length()), 0);
        }
    }

    /* Step6: Check tuples are compacted. */
    EXPECT_LT(page->m_header.m_upper, upper);

    delete page;
}

TEST_F(UTHeap, HeapInsertDeletePruneTest_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<ItemPointerData> heapTupCtids;

    /* Step1: Create insert data, for 26 * 512 > 13k data, there will be some point that prune happens. */
    constexpr int NUM_TUP = 26;
    constexpr int LEN_TUP = 512;
    char x = 'a';
    for (int i = 0; i < NUM_TUP; ++i) {
        HeapTuple *insertTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(LEN_TUP, x));
        heapTupsForInsert.push_back(insertTuple);
        x++;
    }

    /* Step2: Insert tuples, but delete tuple every other line to generate gaps between existing tuples. */
    constexpr int INTERVAL = 2;
    for (int i = 0; i < NUM_TUP; ++i) {
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTupsForInsert[i]);
        heapTupCtids.push_back(ctid);
        if (i % INTERVAL == 1) {
            int ret_val = DeleteTuple(&ctid);
            EXPECT_EQ(ret_val, DSTORE_SUCC);
        }
    }

    /* Step3: Check existing tuples are still correct after compaction. */
    for (int i = 0; i < NUM_TUP; ++i) {
        if (i % INTERVAL == 1) {
            continue;
        }
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&heapTupCtids[i]);
        EXPECT_EQ(tuple->GetDiskTupleSize(), heapTupsForInsert[i]->GetDiskTupleSize());
        EXPECT_EQ(memcmp((void *)tuple->GetDiskTuple()->GetData(),
                         (void *)heapTupsForInsert[i]->GetDiskTuple()->GetData(), LEN_TUP), 0);
        DstorePfreeExt(tuple);
    }

    /* Step4: Free memory. */
    for (int i = 0; i < NUM_TUP; ++i) {
        DstorePfreeExt(heapTupsForInsert[i]);
    }
}

TEST_F(UTHeap, HeapInsertUpdatePruneTest_TIER1_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<HeapTuple *> heapTupsForUpdate;
    std::vector<ItemPointerData> heapTupCtids;

    /* Step1: Create data for insert and update. */
    constexpr int NUM_TUP = 26;
    constexpr int LEN_TUP = 512;
    constexpr int NEW_LEN_TUP = 522;
    char x = 'a';
    for (int i = 0; i < NUM_TUP; ++i) {
        HeapTuple *insertTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(LEN_TUP, x));
        HeapTuple *updateTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(NEW_LEN_TUP, x));
        heapTupsForInsert.push_back(insertTuple);
        heapTupsForUpdate.push_back(updateTuple);
        x++;
    }

    /* Step2: Insert tuples, but update tuple every other line to generate gaps between existing tuples. */
    constexpr int INTERVAL = 3;
    for (int i = 0; i < NUM_TUP; ++i) {
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTupsForInsert[i]);
        if (i % INTERVAL == 1) {
            ctid = UpdateTupAndCheckResult(&ctid, heapTupsForUpdate[i]);
        }
        heapTupCtids.push_back(ctid);
    }

    /* Step3: Check tuples are correct after compaction. */
    for (int i = 0; i < NUM_TUP; ++i) {
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&heapTupCtids[i]);
        HeapTuple *compareTuple;
        int dataLen;

        if (i % INTERVAL == 1) {
            compareTuple = heapTupsForUpdate[i];
            dataLen = NEW_LEN_TUP;
        } else {
            compareTuple = heapTupsForInsert[i];
            dataLen = LEN_TUP;
        }

        EXPECT_EQ(tuple->GetDiskTupleSize(), compareTuple->GetDiskTupleSize());
        EXPECT_EQ(memcmp((void *)tuple->GetDiskTuple()->GetData(),
                         (void *)compareTuple->GetDiskTuple()->GetData(), dataLen), 0);
        DstorePfreeExt(tuple);
    }

    /* Step4: Free memory. */
    for (int i = 0; i < NUM_TUP; ++i) {
        DstorePfreeExt(heapTupsForInsert[i]);
        DstorePfreeExt(heapTupsForUpdate[i]);
    }
}
