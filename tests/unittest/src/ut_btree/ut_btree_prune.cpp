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
#include "heap/dstore_heap_scan.h"
#include "ut_undo/ut_undo_zone.h"
#include "ut_btree/ut_btree.h"
#include "index/dstore_index_interface.h"
#include "ut_btree/ut_btree_prune.h"

static void SetFakeFrozenXidForTD(TD *td)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    Xid xid;
    pdb->GetTransactionMgr()->AllocTransactionSlot(xid);
    pdb->GetTransactionMgr()->CommitTransactionSlot(xid);

    UndoZone *uzone = nullptr;
    pdb->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
    TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
    slot->SetTrxSlotStatus(TXN_STATUS_FROZEN);

    td->SetXid(INVALID_XID);
    td->SetCsn(INVALID_CSN);
    td->SetStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE);
    td->SetCsnStatus(IS_INVALID);
}

static void SetFakeCommittedXidForTD(TD *td)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    Xid xid;
    pdb->GetTransactionMgr()->AllocTransactionSlot(xid);
    pdb->GetTransactionMgr()->CommitTransactionSlot(xid);

    UndoZone *uzone = nullptr;
    pdb->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
    TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
    slot->SetTrxSlotStatus(TXN_STATUS_COMMITTED);

    td->SetXid(xid);
    td->SetUndoRecPtr(UndoRecPtr(1));
    td->SetCsn(recycleMinCsn + 1);
    td->SetStatus(TDStatus::OCCUPY_TRX_END);
    td->SetCsnStatus(IS_INVALID);
}

static void SetFakeAbortedXidForTD(TD *td, TdId tdId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    Xid xid;
    pdb->GetTransactionMgr()->AllocTransactionSlot(xid);
    pdb->GetTransactionMgr()->CommitTransactionSlot(xid);

    UndoZone *uzone = nullptr;
    pdb->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
    TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
    slot->SetTrxSlotStatus(TXN_STATUS_ABORTED);
    UndoRecord *record = UZTestUtil::CreateUndoRecord({{1, 1}, 1}, td, UNDO_BTREE_INSERT, tdId,
                                                      sizeof(UndoDataBtreeInsert) + 10);
    uzone->ExtendSpaceIfNeeded(record->GetRecordSize());
    thrd->m_walWriterContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = uzone->InsertUndoRecord(record);
    thrd->m_walWriterContext->EndAtomicWal();
    slot->SetCurTailUndoPtr(undoRecPtr);
    slot->SetSpaceTailUndoPtr(undoRecPtr);

    td->SetUndoRecPtr(undoRecPtr);
    td->SetXid(xid);
    td->SetCsn(recycleMinCsn + 1);
    td->SetStatus(TDStatus::OCCUPY_TRX_END);
    td->SetCsnStatus(IS_INVALID);
}

static void SetFakeInProgressXidForTD(TD *td)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    Xid xid;
    pdb->GetTransactionMgr()->AllocTransactionSlot(xid);
    pdb->GetTransactionMgr()->CommitTransactionSlot(xid);

    UndoZone *uzone = nullptr;
    pdb->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
    TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
    slot->SetTrxSlotStatus(TXN_STATUS_IN_PROGRESS);

    td->SetXid(xid);
    td->SetCsn(recycleMinCsn);
    td->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
    td->SetCsnStatus(IS_CUR_XID_CSN);
}

void UTBtree::GenerateFakeTDStates(BtrPage *page, int numTds)
{
    EXPECT_EQ(numTds, 8);
    for (int i = 0; i < numTds; i++) {
        TD *td = page->GetTd(i);
        SetFakeFrozenXidForTD(td);
    }
    int numTdIdCommited = 4;
    uint8 tdIdCommited[] = {1, 3, 5, 6};
    for (int i = 0; i < numTdIdCommited; i++) {
        TD *td = page->GetTd(tdIdCommited[i]);
        SetFakeCommittedXidForTD(td);
    }

    int numTdIdAborted = 2;
    uint8 tdIdAborted[] = {2, 4};
    for (int i = 0; i < numTdIdAborted; i++) {
        TD *td = page->GetTd(tdIdAborted[i]);
        SetFakeAbortedXidForTD(td, tdIdAborted[i]);
    }
}

void UTBtree::GenerateFakeTDStatesNoNeedPrune(BtrPage *page, int numTds)
{
    EXPECT_EQ(numTds, 8);
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    int numTdIdCommited = 3;
    uint8 tdIdCommited[] = {1, 3, 5};
    for (int i = 0; i < numTdIdCommited; i++) {
        TD *td = page->GetTd(tdIdCommited[i]);
        SetFakeCommittedXidForTD(td);
        td->SetCsn(recycleMinCsn + 100);
        td->SetCsnStatus(IS_CUR_XID_CSN);
    }
}

void UTBtree::GenerateFakeTDInProgress(BtrPage *page, int numTds)
{
    int numTdIdCommited = 4;
    uint8 tdIdCommited[] = {1, 3, 5, 6};
    for (int i = 0; i < numTdIdCommited; i++) {
        TD *td = page->GetTd(tdIdCommited[i]);
        SetFakeInProgressXidForTD(td);
    }
}

void UTBtree::GenerateFakeTuples(BtrPage *page, OffsetNumber minOff, OffsetNumber maxOff, int numTds)
{
    /* First insert a fake high key */
    ItemPointerData ctid{INVALID_PAGE_ID, 1};
    IndexTuple *hikey = m_utTableHandler->GenerateSpecificIndexTuple(std::string("Z-Hikey"));
    hikey->SetHeapCtid(&ctid);
    IndexTuple *tuple = m_utTableHandler->GenerateSpecificIndexTuple(std::string("IndexTuple"));
    ctid.SetOffset(2);
    tuple->SetHeapCtid(&ctid);
    BtreeInsert insert{m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                       m_utTableHandler->GetIndexScanKey()};
    IndexTuple *internalHikey = insert.CreateTruncateInternalTuple(tuple, hikey);
    page->AddTuple(internalHikey, BTREE_PAGE_HIKEY, INVALID_TD_SLOT);
 
    /* insert fake data tuples */
    for (OffsetNumber offnum = minOff; offnum <= maxOff; offnum++) {
        DstorePfree(tuple);
        ctid.SetOffset(offnum);
        tuple = m_utTableHandler->GenerateSpecificIndexTuple(std::string("IndexTuple"), ctid);
        page->AddTuple(tuple, offnum, offnum % numTds + 1);
        (page->GetIndexTuple(offnum))->SetTdStatus(DSTORE::ATTACH_TD_AS_NEW_OWNER);
    }
    DstorePfreeExt(tuple);
}

BtreePagePrune *UTBtree::GenerateEmptyPrunablePage(int numTuples)
{
    /* Step 1. Create a page with prunalbe number of TDs */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();

    int numTds = numTuples + 1;
    BtreePagePrune *btreePagePrune = CreateFakeBtreePrune(numTds);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();

    /* Step 2. Insert some tuples into the page */
    OffsetNumber minOff = OffsetNumberNext(BTREE_PAGE_HIKEY);
    OffsetNumber offnum = minOff;
    OffsetNumber maxOff = numTuples; /* Include high key */

    /* First insert a fake high key */
    IndexTuple *tuple = m_utTableHandler->GenerateSpecificIndexTuple(std::string("Hikey"));
    page->AddTuple(tuple, BTREE_PAGE_HIKEY, INVALID_TD_SLOT);

    /* insert fake data tuples */
    for (; offnum <= maxOff; offnum++) {
        tuple = m_utTableHandler->GenerateSpecificIndexTuple(std::string("IndexTuple"));
        page->AddTuple(tuple, offnum, offnum % numTds + 1);
        ((IndexTuple *)page->GetRowData(offnum))->SetTdStatus(DSTORE::ATTACH_TD_AS_NEW_OWNER);
    }

    /* Step 3. Mark tuples as deleted */
    for (offnum = minOff; offnum < numTuples; offnum++) {
        IndexTuple *tuple = (IndexTuple *)page->GetRowData(offnum);
        tuple->SetTdStatus(DSTORE::DETACH_TD);
        tuple->SetDeleted();
    }

    page->SetTuplePrunable(true);
    StorageAssert(page->CheckSanity());
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();
    pageMeta->InitPageMeta(INVALID_PAGE_ID, 0, false);
    PageId fakeRight = PageId{0, 1};
    pageMeta->SetRight(fakeRight);

    transaction->Commit();

    btreePagePrune->Init();

    return btreePagePrune;
}

BtreePagePrune *UTBtree::CreateFakeBtreePrune(int numTds)
{
    /* Build btree index */
    int indexCols[] = {1};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Generate btree page */
    BtreePagePayload payload;
    m_utTableHandler->GetBtreeSmgr()->GetNewPage(payload, numTds);

    return DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), payload.GetBuffDesc());
}

void UTBtree::CheckLiveItemsInfo(BtreePagePrune *btreePagePrune, BtrPage *page, int numLiveTuples)
{
    /* Check high key */
    ItemIdCompact itemIdCompact = btreePagePrune->GetLiveItemsInfo();
    IndexTuple *tuple = page->GetIndexTuple((itemIdCompact++)->itemOffnum);
    EXPECT_FALSE(tuple->IsDeleted());
    EXPECT_STREQ(tuple->GetValues(), "Z-Hikey");

    /* Check data tuples */
    for (int i = 1; i < numLiveTuples; i++) {
        ItemId *itemId = page->GetItemIdPtr(itemIdCompact->itemOffnum);
        StorageAssert(page->CheckItemIdSanity(itemId));
        tuple = page->GetIndexTuple(itemId);
        EXPECT_FALSE(tuple->IsDeleted());
        EXPECT_STREQ(tuple->GetValues(), "IndexTuple");
        itemIdCompact++;
    }
}

void UTBtree::CheckTuplesOnPage(BtrPage *page, OffsetNumber minoff, OffsetNumber maxoff, uint16 oldUpper,
                                int deltaTupleNum)
{
    uint16 newUpper = page->GetSpecialOffset();

    /* Check high key */
    ItemId *itemId = page->GetItemIdPtr(BTREE_PAGE_HIKEY);
    newUpper -= itemId->GetLen();
    EXPECT_STREQ((page->GetIndexTuple(BTREE_PAGE_HIKEY))->GetValues(), "Z-Hikey");

    /* Check data tuples */
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        itemId = page->GetItemIdPtr(offnum);
        EXPECT_FALSE(itemId->IsNoStorage());
        newUpper -= itemId->GetLen();
        EXPECT_EQ(newUpper, itemId->GetOffset());
        IndexTuple *tupleByOffset = (IndexTuple *)((char *)page + newUpper);
        IndexTuple *tupleByItemId = page->GetIndexTuple(itemId);
        EXPECT_EQ(tupleByOffset, tupleByItemId);
        EXPECT_STREQ(tupleByOffset->GetValues(), "IndexTuple");
    }

    /* all tuples have same length in the UT test */
    EXPECT_EQ(newUpper, oldUpper + deltaTupleNum * itemId->GetLen());

    EXPECT_EQ(newUpper, page->m_header.m_upper);
}

void UTBtree::GenerateFakeTupleStates(BtrPage *page, bool *isTupleLive, int numItems, int *numLiveItemIds)
{
    int numDeletedTuple = 3;
    OffsetNumber deletedTuple[] = {4, 9, 12};
    for (int i = 0; i < numDeletedTuple; i++) {
        IndexTuple *tuple = page->GetIndexTuple(deletedTuple[i]);
        tuple->SetTdStatus(DSTORE::DETACH_TD);
        tuple->SetDeleted();
        isTupleLive[deletedTuple[i]] = false;
    }
    int numCommittedTuple = 3;
    OffsetNumber committedTuple[] = {3, 10, 15};
    for (int i = 0; i < numCommittedTuple; i++) {
        IndexTuple *tuple = page->GetIndexTuple(committedTuple[i]);
        tuple->SetTdStatus(DSTORE::ATTACH_TD_AS_HISTORY_OWNER);
        tuple->SetTdId(6);
        tuple->SetDeleted();
        isTupleLive[committedTuple[i]] = false;
    }

    *numLiveItemIds = numItems - numDeletedTuple - numCommittedTuple; /* Include high key */
}

void UTBtree::GenerateFakeNoTuplePrunable(int numItems, int *numLiveItemIds)
{
    *numLiveItemIds = numItems;    /* Include high key */
}

void UTBtree::GenerateFakeTupleInprogress(BtrPage *page, bool *isTupleLive, int numItems, int *numLiveItemIds)
{
    int numUnusedItemId = 1;
    OffsetNumber unuseItemId[] = {5}; /* Never set high key used */
    for (int i = 0; i < numUnusedItemId; i++) {
        ItemId *itemId = page->GetItemIdPtr(unuseItemId[i]);
        itemId->SetUnused();
        isTupleLive[unuseItemId[i]] = false;
    }
    int numInprogressTuple = 3;
    OffsetNumber inprogressTuple[] = {2, 10, 15};
    for (int i = 0; i < numInprogressTuple; i++) {
        IndexTuple *tuple = page->GetIndexTuple(inprogressTuple[i]);
        tuple->SetTdStatus(DSTORE::ATTACH_TD_AS_NEW_OWNER);
        tuple->SetTdId(3);
        isTupleLive[inprogressTuple[i]] = true;
    }
    *numLiveItemIds = numItems - numUnusedItemId; /* Include high key */
}

BtreePagePrune *UTBtree::PrepareBtreePrune(int numItems, int *numLiveItemIds,
    bool *isTupleLive, bool needCompactTD, FakeTupleStatus stat)
{
     /* Step 1. Create a page with prunalbe number of TDs */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();

    int numTds = DEFAULT_TD_COUNT * (2 - (int)(!needCompactTD));
    BtreePagePrune *btreePagePrune = CreateFakeBtreePrune(numTds);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();

    /* Step 2. Insert some tuples into the page */
    OffsetNumber minOff = OffsetNumberNext(BTREE_PAGE_HIKEY);
    OffsetNumber maxOff = numItems; /* Include high key */

    /* Step 3. Mark TD as commited or frozen */
    if (needCompactTD) {
        switch (stat) {
            case FakeTupleStatus::FAKE_NORMAL:
                GenerateFakeTDStates(page, numTds);
                break;
            case FakeTupleStatus::FAKE_NO_PRUNEABLE:
                GenerateFakeTDStatesNoNeedPrune(page, numTds);
                break;
            case FakeTupleStatus::FAKE_IN_PROGRESS:
                GenerateFakeTDInProgress(page, numTds);
                break;
        }
    }

    GenerateFakeTuples(page, minOff, maxOff, numTds);
    for (OffsetNumber offnum = minOff; offnum <= maxOff; offnum++) {
        isTupleLive[offnum] = true;
    }
    /* Step 4. Mark some tuples as deleted */
    switch (stat) {
        case FakeTupleStatus::FAKE_NORMAL:
            GenerateFakeTupleStates(page, isTupleLive, numItems, numLiveItemIds);
            break;
        case FakeTupleStatus::FAKE_NO_PRUNEABLE:
            GenerateFakeNoTuplePrunable(numItems, numLiveItemIds);
            break;
        case FakeTupleStatus::FAKE_IN_PROGRESS:
            GenerateFakeTupleInprogress(page, isTupleLive, numItems, numLiveItemIds);
            break;
    }

    page->SetTuplePrunable(true);
    StorageAssert(page->CheckSanity());
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();
    pageMeta->InitPageMeta(INVALID_PAGE_ID, 0, false);
    PageId fakeRight = PageId{0, 1};
    pageMeta->SetRight(fakeRight);

    transaction->Commit();
    btreePagePrune->Init();

    return btreePagePrune;
}

TEST_F(UTBtree, BtreePruneTest_level0)
{
    /* Step 1. Create a page with prunable tuples */
    int numItems = 20;
    int numLiveItemIds = numItems;
    bool isTupleLive[numItems + 1];
    BtreePagePrune *btreePagePrune = PrepareBtreePrune(numItems, &numLiveItemIds,
        isTupleLive, true, FakeTupleStatus::FAKE_NORMAL);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();
    uint8 tdCount = page->GetTdCount();
    uint16 oldLower = page->m_header.m_lower;
    uint16 oldUpper = page->m_header.m_upper;

    /* Step 2. Prune page */
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);

    /* Step 3. Check pruned page */
    EXPECT_EQ(numItems, btreePagePrune->GetTupleCount());
    EXPECT_EQ(numLiveItemIds, btreePagePrune->GetLiveTupleCount());

    OffsetNumber minoff = btreePagePrune->GetPagePayload()->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        EXPECT_EQ(isTupleLive[offnum], btreePagePrune->GetTupleLiveStatus()[offnum]);
    }

    CheckLiveItemsInfo(btreePagePrune, page, numLiveItemIds);

    /* Check lower */
    uint16 newLower = oldLower - (tdCount - btreePagePrune->GetFixedTdCount()) * sizeof(TD) -
                      (numItems - numLiveItemIds) * sizeof(ItemId);
    EXPECT_EQ(page->GetDataBeginOffset(), newLower);

    CheckTuplesOnPage(page, minoff, maxoff, oldUpper, numItems - numLiveItemIds);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
}

TEST_F(UTBtree, BtreePruneFailTest_level0)
{
    /* Step 1. Create a page with prunable tuples */
    int numItems = 20;
    int numLiveItemIds = numItems;
    bool isTupleLive[numItems + 1];
    BtreePagePrune *btreePagePrune = PrepareBtreePrune(numItems, &numLiveItemIds,
        isTupleLive, false, FakeTupleStatus::FAKE_NO_PRUNEABLE);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();
    uint8 tdCount = page->GetTdCount();
    uint16 oldLower = page->m_header.m_lower;
    uint16 oldUpper = page->m_header.m_upper;

    /* Step 2. Prune page */
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_FALSE(btreePagePrune->IsPagePrunable());

    /* Step 3. Check pruned page */
    EXPECT_EQ(numItems, btreePagePrune->GetTupleCount());
    EXPECT_EQ(numLiveItemIds, btreePagePrune->GetLiveTupleCount());

    OffsetNumber minoff = btreePagePrune->GetPagePayload()->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        EXPECT_EQ(isTupleLive[offnum], btreePagePrune->GetTupleLiveStatus()[offnum]);
    }
    /* Check lower */
    uint16 newLower = oldLower - (tdCount - btreePagePrune->GetFixedTdCount()) * sizeof(TD) -
                      (numItems - numLiveItemIds) * sizeof(ItemId);
    EXPECT_EQ(page->GetDataBeginOffset(), newLower);

    CheckTuplesOnPage(page, minoff, maxoff, oldUpper, numItems - numLiveItemIds);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
}

TEST_F(UTBtree, BtreePruneInprogressTest_level0)
{
    /* Step 1. Create a page with prunable tuples */
    int numItems = 20;
    int numLiveItemIds = numItems;
    bool isTupleLive[numItems + 1];
    BtreePagePrune *btreePagePrune = PrepareBtreePrune(numItems, &numLiveItemIds,
        isTupleLive, true, FakeTupleStatus::FAKE_IN_PROGRESS);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();
    pageMeta->SetType(BtrPageType::INTERNAL_PAGE);
    uint8 tdCount = page->GetTdCount();
    uint16 oldLower = page->m_header.m_lower;
    uint16 oldUpper = page->m_header.m_upper;
    /* Step 2. Prune page */
    RetStatus ret = btreePagePrune->Prune(5);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    /* Step 3. Check pruned page */
    EXPECT_EQ(numItems, btreePagePrune->GetTupleCount());
    EXPECT_EQ(numLiveItemIds, btreePagePrune->GetLiveTupleCount());
    OffsetNumber minoff = btreePagePrune->GetPagePayload()->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    for (OffsetNumber offnum = minoff; offnum <= maxoff; offnum++) {
        EXPECT_EQ(isTupleLive[offnum], btreePagePrune->GetTupleLiveStatus()[offnum]);
    }
    /* Check lower */
    uint16 newLower = oldLower - (tdCount - btreePagePrune->GetFixedTdCount()) * sizeof(TD) -
                      (numItems - numLiveItemIds) * sizeof(ItemId);
    EXPECT_EQ(page->GetDataBeginOffset(), newLower);
    CheckTuplesOnPage(page, minoff, maxoff, oldUpper, numItems - numLiveItemIds);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
}

TEST_F(UTBtree, BtreePruneSetAndGetCurrCsnTest_level0)
{
    /* Step 1. Create a page with prunable tuples */
    int numTuples = 20;
    BtreePagePrune *btreePagePrune = GenerateEmptyPrunablePage(numTuples);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    CommitSeqNo maxCsn = INVALID_CSN;

    /* Step 2. Set TD csn and TD status for deleted tuples */
    OffsetNumber minoff = btreePagePrune->GetPagePayload()->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxoff = page->GetMaxOffset();
    OffsetNumber offnum;
    for (offnum = minoff; offnum <= maxoff; offnum++) {
        IndexTuple *tuple = (IndexTuple *)page->GetRowData(offnum);
        if (tuple->IsDeleted()) {
            maxCsn = recycleMinCsn + offnum;
            TD *td = page->GetTd(page->GetTupleTdId(offnum));
            td->SetCsn(maxCsn);
            tuple->SetTdStatus(DSTORE::ATTACH_TD_AS_NEW_OWNER);
        }
    }
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(btreePagePrune->GetPagePayload()->GetBuffDesc());

    /* Step 3. IsPageEmpty set the current max csn of BtreePagePrune*/
    btreePagePrune->IsPageEmpty(false);
    EXPECT_EQ(btreePagePrune->GetCurrMaxCsn(), maxCsn);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
}

/*
 * Fetch undo record to judge if one tuple can be pruned.
 */
TEST_F(UTBtree, BtreePruneJudgeTupleInPageTest_level0)
{
    /* Build empty btree index */
    int indexCols[] = {1};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Insert and delete 4(=td num) tuples, then insert 4 tuples. */
    int rowNum = 4;
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;

        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow);
        DeleteIndexTuple(indexTuple);
        DstorePfreeExt(indexTuple);
    }
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow);
        DstorePfreeExt(indexTuple);
    }

    /* The btree only has one root page which is also leaf page. */
    PageId metaPageId = m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId();
    PageId rootPageId = TestBtreeMetaPage(metaPageId);
    BufferDesc *rootBuf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, rootPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePrune = 
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), rootBuf);

    EXPECT_EQ(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID), 1);
    EXPECT_FALSE(btreePrune->IsPageEmpty(false));

    g_storageInstance->GetBufferMgr()->UnlockAndRelease(rootBuf);
    delete btreePrune;
}

TEST_F(UTBtree, BtreeScanAfterPruneTest_level0)
{
    HeapScanHandler *heapScan = nullptr;
    Transaction *txn = thrd->GetActiveTransaction();

    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    IndexTuple *deleteTuple = nullptr;
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();

    /* Step 1. Insert (1,1)(2,2)... into btree. */
    int insertRowCount = 283;
    int delNum = 5;
    IndexTuple *indexTuple = nullptr;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        if (i == delNum) {
            deleteTuple = indexTuple;
        }
        txn->Commit();
    }

    /* Step 2. Normal scan every tuple, inserted tuple should  be visible. */
    int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
    txn->Start();
    txn->SetSnapshotCsn();
    IndexInterface::IndexScanSetSnapshot(&indexScan, txn->GetSnapshot());

    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    heapScan->Begin(txn->GetSnapshot());
    indexScan.BeginScan();
    IndexInterface::IndexScanSetSnapshot(&indexScan, txn->GetSnapshot());
    IndexScanDesc indexScanDesc = indexScan.GetScanDesc();
    ASSERT_NE(indexScanDesc, nullptr);
    ScanKeyData keyInfos[nkeys];
    int foundNum = 0;
    for (int i = 1; i <= insertRowCount; ++i) {
        InitScanKeyInt24(keyInfos, i, SCAN_ORDER_EQUAL, i, SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 1);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();

    /* Step 3. Delete tuple */
    GsAtomicFetchAddU64(&(g_storageInstance->GetCsnMgr()->m_nextCsn), 1);
    CommitSeqNo deleteCsn = INVALID_CSN;
    txn->Start();
    txn->SetSnapshotCsn();
    deleteCsn = txn->GetSnapshotCsn();
    RetStatus status = DeleteIndexTuple(deleteTuple, true);
    PageId delPageId = m_utTableHandler->GetLastWorkingBtrPageId();
    EXPECT_EQ(status, DSTORE_SUCC);
    txn->Commit();

    /* Step 4. Normal scan every tuple, deleted tuple should be invisible. */
    txn->Start();
    txn->SetSnapshotCsn();

    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    heapScan->Begin(txn->GetSnapshot());
    indexScan.BeginScan();
    IndexInterface::IndexScanSetSnapshot(&indexScan, txn->GetSnapshot());
    indexScanDesc = indexScan.GetScanDesc();
    ASSERT_NE(indexScanDesc, nullptr);
    foundNum = 0;
    for (int i = 1; i <= insertRowCount; i++) {
        InitScanKeyInt24(keyInfos, i, SCAN_ORDER_EQUAL, i, SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        if (i == delNum) {
            EXPECT_EQ(foundNum, 0);
        } else {
            EXPECT_EQ(foundNum, 1);
        }
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();

    /* Step 5. Btree page prune. */
    EXPECT_TRUE(delPageId != INVALID_PAGE_ID);
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, delPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), bufferDesc);
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
    btreePagePrune = nullptr;

    /* Step 6. Insert again to call PruneNonItemIdTuples when rollback. */
    int insertRowCountAfterPrune = 1;
    for (int i = 1 + insertRowCount; i <= insertRowCountAfterPrune + insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        txn->Commit();
    }

    /* Step 7. Flashback scan every tuple, deleted tuple should be visible. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(deleteCsn - 1);
    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    heapScan->Begin(txn->GetSnapshot());
    indexScan.BeginScan();
    IndexInterface::IndexScanSetSnapshot(&indexScan, txn->GetSnapshot());
    indexScanDesc = indexScan.GetScanDesc();
    ASSERT_NE(indexScanDesc, nullptr);
    foundNum = 0;
    for (int i = 1; i <= insertRowCount; i++) {
        InitScanKeyInt24(keyInfos, i, SCAN_ORDER_EQUAL, i, SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 1);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();
}

/* Delete and prune the tuple that can not find in the function BinarySearch(). */
TEST_F(UTBtree, BtreeScanAfterDeleteTest_level0)
{
    HeapScanHandler *heapScan = nullptr;
    Transaction *txn = thrd->GetActiveTransaction();

    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    IndexTuple *deleteTuple = nullptr;
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();

    /* Step 1. Insert (1,1)(2,2)... into btree. */
    int insertRowCount = 20;
    int delNum = 20;
    IndexTuple *indexTuple = nullptr;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        if (i == delNum) {
            deleteTuple = indexTuple;
        }
        txn->Commit();
    }

    /* Step 2. Delete tuple */
    GsAtomicFetchAddU64(&(g_storageInstance->GetCsnMgr()->m_nextCsn), 1);
    CommitSeqNo deleteCsn = INVALID_CSN;
    txn->Start();
    txn->SetSnapshotCsn();
    deleteCsn = txn->GetSnapshotCsn();
    RetStatus status = DeleteIndexTuple(deleteTuple, true);
    PageId delPageId = m_utTableHandler->GetLastWorkingBtrPageId();
    EXPECT_EQ(status, DSTORE_SUCC);
    txn->Commit();

    /* Step 3. Btree page prune. */
    EXPECT_TRUE(delPageId != INVALID_PAGE_ID);
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, delPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), bufferDesc);
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
    btreePagePrune = nullptr;

    /* Step 5. Flashback scan every tuple, deleted tuple should be visible. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(deleteCsn - 1);

    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    heapScan->Begin(txn->GetSnapshot());
    IndexScanDesc indexScanDesc = indexScan.GetScanDesc();
    ASSERT_NE(indexScanDesc, nullptr);
    int foundNum = 0;
    ScanKeyData keyInfos[nkeys];
    for (int i = 1; i <= insertRowCount; i++) {
        InitScanKeyInt24(keyInfos, i, SCAN_ORDER_EQUAL, i, SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 1);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();
}

TEST_F(UTBtree, BtreePruneOnlyTupleTest_level0)
{
    /* Step 1. Create a page */
    int numItems = 10;
    int numLiveItemIds = numItems;
    bool isTupleLive[numItems + 1];
    BtreePagePrune *btreePagePrune = PrepareBtreePrune(numItems, &numLiveItemIds,
        isTupleLive, false, FakeTupleStatus::FAKE_NO_PRUNEABLE);
    BtrPage *page = btreePagePrune->GetPagePayload()->GetPage();
    uint16 oldLower = page->m_header.m_lower;
    uint16 oldUpper = page->m_header.m_upper;
    /* Step 2. change lower to make some ItemId invalid */
    page->m_header.m_lower -= 8 * sizeof(ItemId);
    btreePagePrune->SetTupleCount(2);
    /* Step 3. Prune page */
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);

    /* Step 4. Check new lower and upper */
    EXPECT_EQ(oldLower - 8 * sizeof(ItemId), page->m_header.m_lower);
    EXPECT_EQ(oldUpper + 8 * (page->GetItemIdPtr(2))->GetLen(), page->m_header.m_upper);

    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
}

TEST_F(UTBtree, BtreePruneCrPageTest_level0)
{
    HeapScanHandler *heapScan = nullptr;
    Transaction *txn = thrd->GetActiveTransaction();

    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    IndexTuple *deleteTuple = nullptr;
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();

    /* Step 1. Insert shuffled values into btree. */
    int insertRowCount = 283;
    std::vector<int> values;
    for (int i = 1; i <= insertRowCount; i++) {
        values.push_back(i);
    }
    std::random_shuffle(values.begin(), values.end());
    int delNum = 3;
    IndexTuple *indexTuple = nullptr;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(values[i - 1]);
        insertRowTemp.column_int32 = (int32)(values[i - 1]);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        if (i == delNum) {
            deleteTuple = indexTuple;
        }
        txn->Commit();
    }

    /* Step 2. Delete tuple */
    GsAtomicFetchAddU64(&(g_storageInstance->GetCsnMgr()->m_nextCsn), 1);
    CommitSeqNo deleteCsn = INVALID_CSN;
    txn->Start();
    txn->SetSnapshotCsn();
    deleteCsn = txn->GetSnapshotCsn();
    RetStatus status = DeleteIndexTuple(deleteTuple, true);
    PageId delPageId = m_utTableHandler->GetLastWorkingBtrPageId();
    EXPECT_EQ(status, DSTORE_SUCC);
    txn->Commit();

    /* Step 3. Btree page prune. */
    EXPECT_TRUE(delPageId != INVALID_PAGE_ID);
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, delPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), bufferDesc);
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
    btreePagePrune = nullptr;

    /* Step 4. Insert again to call PruneNonItemIdTuples when rollback. */
    int insertRowCountAfterPrune = 1;
    for (int i = 1 + insertRowCount; i <= insertRowCountAfterPrune + insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        txn->Commit();
    }

    /* Step 5. Flashback scan every tuple, deleted tuple should be visible. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(deleteCsn - 1);
    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    heapScan->Begin(txn->GetSnapshot());
    indexScan.BeginScan();
    int foundNum = 0;
    ScanKeyData keyInfos[nkeys];
    for (int i = 1; i <= insertRowCount; i++) {
        InitScanKeyInt24(keyInfos, values[i - 1], SCAN_ORDER_EQUAL, values[i - 1], SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 1);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();
}

/* Rollback btreeinsert after page prune moved TD */
TEST_F(UTBtree, BtreePruneTDMoveTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    /* 1. Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    bufMgr->UnlockAndRelease(btrMetaBuf);
    PageId rootPageId = DSTORE::INVALID_PAGE_ID;
    BufferDesc *rootBuf = nullptr;
    BtrPage *rootPage = nullptr;
    /* 2. Insert tuples into btree. */
    int insertRowCount = 7;
    bool *nullbitmap = DefaultNullBitMap;
    IndexTuple *indexTuples[7];
    Xid rollbackXid = INVALID_XID;
    std::vector<Xid> xidVec;
    for (int i = 1; i <= insertRowCount; ++i) {
        if (i == 3) {
            rootPageId = btrMeta->GetRootPageId();
            rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
            rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
            bufMgr->UnlockAndRelease(rootBuf);
            TDAllocContext tdContext;
            tdContext.Init(g_defaultPdbId, true);
            tdContext.Begin(csnMgr->GetRecycleCsnMin(INVALID_PDB_ID));
            TdId tdId = rootPage->ExtendTd(tdContext);
        }
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        txn->SetSnapshotCsn();

        indexTuples[i - 1] = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuples[i - 1] , nullptr);
        if (i == 2) {
            rollbackXid = txn->GetCurrentXid();
        }
        txn->Commit();
    }
    EXPECT_EQ(rootPage->GetTdCount(), 6);
    
    /* 3. Delete tuples */
    txn->Start();
    txn->SetSnapshotCsn();
    EXPECT_EQ(DeleteIndexTuple(indexTuples[1], true), DSTORE_SUCC);
    Xid deleteXid = txn->GetCurrentXid();
    txn->Commit();

    for (int i = 0; i < rootPage->GetTdCount(); i++) {
        if (i == 1 || i == 2) {
            continue;
        }
        TD *td = rootPage->GetTd(i);
        td->Reset();
    }

    /* 4. Btree page prune. */
    BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId, rootPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), bufferDesc);
    EXPECT_TRUE(btreePagePrune->Prune() == DSTORE_SUCC);
    btreePagePrune->GetPagePayload()->Drop(bufMgr);
    delete btreePagePrune;
    btreePagePrune = nullptr;
    EXPECT_EQ(rootPage->GetTdCount(), 4);

    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();

    /* 5. Rollback delete and scan data on page now. */
    BackupConsistencyPoint *consistencyPoint =
        static_cast<BackupConsistencyPoint *>(DstorePalloc0(sizeof(BackupConsistencyPoint)));
    g_storageInstance->SetBackupRestoreConsistencyPoint(g_defaultPdbId, consistencyPoint);

    TD *td = nullptr;
    for (TdId tdId = 0; tdId < rootPage->GetTdCount(); tdId++) {
        td = rootPage->GetTd(tdId);
        if (td->GetXid() == deleteXid) {
            break;
        }
    }
    UndoRecord undoRecord;
    EXPECT_EQ(transactionMgr->FetchUndoRecord(deleteXid, &undoRecord, td->GetUndoRecPtr()), DSTORE_SUCC);

    rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtreeUndoContext *btrUndoContext = nullptr;
    bool needFreeOutside = true;
    btrUndoContext = BtreeUndoContext::FindUndoRecRelatedPage(btrUndoContext, g_defaultPdbId, rootBuf, deleteXid,
        &undoRecord, bufMgr, needFreeOutside, td->GetUndoRecPtr(), true);
    EXPECT_EQ(rootPage->RollbackByUndoRec(&undoRecord, btrUndoContext), DSTORE_SUCC);
    bufMgr->UnlockAndRelease(rootBuf);

    txn->Start();
    txn->SetSnapshotCsn();
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    indexScan.ReScan(nullptr);
    int visTupleCnt = 0;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCnt++;
    }
    EXPECT_EQ(visTupleCnt, 7);
    txn->Commit();

    /* 6. RollbackTransactionSlot and scan data on page now. */
    transactionMgr->RollbackTransactionSlot(rollbackXid);
    txn->Start();
    txn->SetSnapshotCsn();
    indexScan.ReScan(nullptr);
    visTupleCnt = 0;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCnt++;
    }
    EXPECT_EQ(visTupleCnt, 6);
    txn->Commit();
}