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
 * Description: BtreeWal unittest
 */
#include <queue>
#include "ut_btree/ut_btree_wal.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "ut_mock/ut_mock.h"
#include "ut_btree/ut_btree.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_prune.h"
#include "ut_undo/ut_undo_zone.h"
#include "ut_btree/ut_btree_prune.h"

void UTBtreeWal::RedoAndCheckSplitPage(BtrPage &splitPage, PageId leftPageId, Xid xid, uint64 plsn, bool isLeaf)
{
    /* Read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    int walNum = 0;
    WalType walType;
    if (isLeaf) {
        walType = WAL_BTREE_PAGE_PRUNE;
        EXPECT_EQ(ReadRecordsAfterPlsn(plsn, walType, recordList), DSTORE_SUCC);
        walNum = (int)recordList.size();
        walType = WAL_BTREE_SPLIT_LEAF;
    } else {
        walType = WAL_BTREE_SPLIT_INTERNAL;
    }
    while (recordList.size() == walNum) {
        EXPECT_EQ(ReadRecordsAfterPlsn(plsn, walType, recordList), DSTORE_SUCC);
        walType = isLeaf ? WAL_BTREE_SPLIT_INSERT_LEAF : WAL_BTREE_SPLIT_INSERT_INTERNAL;
    }
    EXPECT_EQ(recordList.size(), ++walNum);
    walType = isLeaf ? WAL_BTREE_NEW_LEAF_RIGHT : WAL_BTREE_NEW_INTERNAL_RIGHT;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, walType, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), ++walNum);

    /* Get Changed Pages */
    BtrPage leftPage;
    BtrPage rightPage;
    FetchPage(leftPage, leftPageId);
    PageId rightPageId = leftPage.GetLinkAndStatus()->GetRight();
    FetchPage(rightPage, rightPageId);

    /* Restore old pages */
    BtrPage testLeft;
    BtrPage testRight;
    ReadDataFileImage(leftPageId, (char *)&testLeft, BLCKSZ);
    ReadDataFileImage(rightPageId, (char *)&testRight, BLCKSZ);
    RestorePage(testLeft, leftPageId);
    RestorePage(testRight, rightPageId);
    PageId oldRightPageId = testLeft.GetLinkAndStatus()->GetRight();

    /* Apply redo */
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        auto *record = static_cast<WalRecordIndex *>(&recordInfo->walRecord);
        WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
    }

    /* Fetch pages after redo */
    FetchPage(testLeft, leftPageId);
    FetchPage(testRight, rightPageId);

    /* Check split left */
    ComparePageHeader(&testLeft, &leftPage, false);
    CompareTDs(&testLeft, &leftPage);
    CompareItems(&testLeft, &leftPage);
    BtrPageLinkAndStatus *leftLinkStat = testLeft.GetLinkAndStatus();
    BtrPageLinkAndStatus *origLinkStat = splitPage.GetLinkAndStatus();
    EXPECT_FALSE(leftLinkStat->IsSplitComplete());
    EXPECT_EQ(leftLinkStat->GetLeft(), origLinkStat->GetLeft());
    EXPECT_EQ(leftLinkStat->GetRight(), rightPageId);

    /* Check split new right */
    CompareTwoPageMembers(&testRight, &rightPage);
    /* Check original right page */
    if (oldRightPageId.IsValid()) {
        RedoAndCheckOrigRightPage(rightPage, oldRightPageId, plsn);
    }
}

void UTBtreeWal::RedoAndCheckOrigRightPage(BtrPage &newRightPage, PageId oldRightPageId, uint64 plsn)
{
    /* Read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_LEFT_SIB_LINK, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    WalRecordRedoInfo *rightRecordInfo = recordList.back();

    /* Get Changed Pages */
    BtrPage oldRightPage;
    FetchPage(oldRightPage, oldRightPageId);

    /* Restore old pages */
    BtrPage testOldRight;
    ReadDataFileImage(oldRightPageId, (char *)&testOldRight, BLCKSZ);
    RestorePage(testOldRight, oldRightPageId);

    /* Apply redo */
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    auto *rightRecord= static_cast<WalRecordIndex *>(&rightRecordInfo->walRecord);
    WalRecordIndex::DumpIndexRecord(rightRecord, m_waldump_fp);
    RedoIndexRecord(&redoCtx, rightRecord, rightRecordInfo->endPlsn);
    /* Fetch pages after redo */
    FetchPage(testOldRight, oldRightPageId);

    CompareTwoPageMembers(&testOldRight, &oldRightPage);
    /* Check links */
    BtrPageLinkAndStatus *oldRightLinkStat = testOldRight.GetLinkAndStatus();
    BtrPageLinkAndStatus *newRightLinkStat = newRightPage.GetLinkAndStatus();
    EXPECT_EQ(oldRightLinkStat->GetLeft(), newRightPage.GetSelfPageId());
    EXPECT_EQ(newRightLinkStat->GetRight(), oldRightPageId);
}

void UTBtreeWal::RedoAndCheckRoot(Xid xid, uint64 plsn)
{
    /* Read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_NEW_INTERNAL_ROOT, recordList), DSTORE_SUCC);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_SPLITSTATUS, recordList), DSTORE_SUCC);
    EXPECT_GE(recordList.size(), 2);
    WalRecordRedoInfo *rootRecordInfo = recordList.front();

    /* Get new root info from btree meta */
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId rootPageId =  btrMeta->GetRootPageId();
    uint32 rootLevel = btrMeta->GetRootLevel();
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(btrMetaBuf);

    /* Record newest root */
    BtrPage rootPage;
    FetchPage(rootPage, rootPageId);
    EXPECT_TRUE(rootPage.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    EXPECT_TRUE(rootPage.GetLinkAndStatus()->IsRoot());
    EXPECT_EQ(rootLevel, rootPage.GetLinkAndStatus()->GetLevel());

    /* Restore old root */
    BtrPage testPage;
    ReadDataFileImage(rootPageId, (char *)&testPage, BLCKSZ);
    RestorePage(testPage, rootPageId);

    /* Apply redo */
    PageId oldRootPageId = rootPageId;
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        auto *record = static_cast<WalRecordIndex *>(&recordInfo->walRecord);
        if (record->m_type == WAL_BTREE_NEW_INTERNAL_ROOT) {
            oldRootPageId = static_cast<WalRecordBtreeNewInternalRoot *>(record)->origRoot;
        }
        WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
    }

    /* Fetch pages after redo */
    FetchPage(testPage, rootPageId);
    EXPECT_TRUE(testPage.GetLinkAndStatus()->IsRoot());
    CompareTwoPageMembers(&rootPage, &testPage);

    /* Check split flag on original root */
    FetchPage(testPage, oldRootPageId);
    EXPECT_TRUE(testPage.GetLinkAndStatus()->IsSplitComplete());
    EXPECT_FALSE(testPage.GetLinkAndStatus()->IsRoot());
}

TEST_F(UTBtreeWal, BtreePruneWalTest_level0)
{
    int numItems = 20;
    int numLiveTuples;
    bool isTupleLive[numItems + 1];
    BtreePagePrune *btreePagePrune = PrepareBtreePrune(numItems, &numLiveTuples, isTupleLive, true);
    BufferDesc *bufferDesc = btreePagePrune->GetPagePayload()->GetBuffDesc();
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(bufferDesc);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    (void)g_storageInstance->GetBufferMgr()->FlushAll(false);

    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    /* step 3: do prune */
    g_storageInstance->GetBufferMgr()->PinAndLock(bufferDesc, LW_EXCLUSIVE);
    btreePagePrune->Prune();
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* step 4: read walFile to Get WalRecord of WAL_TBS_INIT_UNDO_SEGMENT_META type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_BTREE_PAGE_PRUNE, recordList);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ASSERT(0);
    }
    EXPECT_EQ(recordList.size(), 1);
    /* step 5: use WalRecord to redo for Page in Image */
    BtrPage redoPage;
    WalRecordRedoContext redoCtx = {DSTORE::INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        WalRecordBtreePagePrune *walRecordBtreePagePrune =
                static_cast<WalRecordBtreePagePrune *>(&(recordInfo->walRecord));
        ReadDataFileImage(walRecordBtreePagePrune->m_pageId, (char *)&redoPage, BLCKSZ);
        RestorePage(redoPage, walRecordBtreePagePrune->m_pageId);
        WalRecordIndex::DumpIndexRecord(walRecordBtreePagePrune, m_waldump_fp);
        RedoIndexRecord(&redoCtx, walRecordBtreePagePrune, recordInfo->endPlsn);
        FetchPage(redoPage, walRecordBtreePagePrune->m_pageId);
        redoPage.SetPlsn(recordInfo->endPlsn);
    }
    /* step 6: compare redoPage and prunePage */
    g_storageInstance->GetBufferMgr()->PinAndLock(bufferDesc, LW_SHARED);
    BtrPage *prunePage = static_cast<BtrPage *>(bufferDesc->GetPage());
    CompareTwoPageMembers(prunePage, &redoPage, true);
    uint32 offset = OFFSETOF(BtrPage, m_data) + prunePage->TdDataSize();
    EXPECT_TRUE(memcmp((char *)&redoPage + offset, (char *)bufferDesc->GetPage() + offset, BLCKSZ - offset) == 0);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
}

TEST_F(UTBtreeWal, BtreeBuildWalTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 3: build index (two layer) */
    int rowNum = 100;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    /* wait Wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: use WalRecord to redo, where we can get a new btree. */
    std::vector<WalRecordRedoInfo *> recordList;
    /* redo meta page */
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_INIT_META_PAGE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    char* origPage[BLCKSZ];
    FetchPage(*((BtrPage *)origPage), m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());   /* copy original page */
    auto record = static_cast<WalRecordBtreeInitMetaPage *>(&(recordList[0]->walRecord));
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoIndexRecord(&redoCtx, record, recordList[0]->endPlsn);
    recordList.clear();

    /* redo btree */
    auto origBtree = GetBtreeAllPages(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_BUILD, recordList), DSTORE_SUCC);
    std::priority_queue<BtrPage*, std::vector<BtrPage*>, BtrPageCmp> redoBtree;
    EXPECT_EQ(recordList.size(), origBtree.size());
    char* redoPages[origBtree.size()][BLCKSZ];
    int pageCount = 0;
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordBtreeBuild *record = static_cast<WalRecordBtreeBuild *>(&(recordInfo->walRecord));
        record->Redo((BtrPage*)&redoPages[pageCount], INVALID_XID);
        redoBtree.push((BtrPage*)&redoPages[pageCount++]);
    }

    /* step 5: compare. */
    /* comapre redo meta page with original meta page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf = bufMgr->
        Read(g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(), LW_SHARED);
    BtrPage *redoPage = (BtrPage*)btrMetaBuf->GetPage();
    CompareBtrMeta((BtrPage *)origPage, redoPage);
    CompareLinkAndStatus(((BtrPage *)origPage)->GetLinkAndStatus(), redoPage->GetLinkAndStatus());
    bufMgr->UnlockAndRelease(btrMetaBuf);

    /* compare redo btree with original btree */
    while (!origBtree.empty() && !redoBtree.empty()) {
        BtrPage* origPage = redoBtree.top();
        BtrPage* redoPage = redoBtree.top();
        uint32 offset = OFFSETOF(BtrPage, m_data);
        EXPECT_TRUE(memcmp((char *)origPage + offset, (char *)redoPage + offset, BLCKSZ - offset) == 0);
        origBtree.pop();
        redoBtree.pop();
    }
    ASSERT_TRUE(origBtree.empty());
    ASSERT_TRUE(redoBtree.empty());
}

TEST_F(UTBtreeWal, BtreeInsertWalTest_level0)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    bufMgr->FlushAll(false);

    /* Step 1. copy dataFile as Image */
    CopyDataFile();
    /* Step 2. get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 3. insert & create a root */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->InsertRandomIndexTuple(true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* Step 4: read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_META_ROOT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_NEW_LEAF_ROOT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_INSERT_ON_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 3);

    /* Step 5: use WalRecord to redo for Page in Image */
    WalRecordRedoInfo *insertRecordInfo = recordList.back();
    recordList.pop_back();
    WalRecordRedoInfo *metaRecordInfo = recordList.front();
    WalRecordRedoInfo *rootRecordInfo = recordList.back();

    /* Step 6. Check */
    BtrPage testPage;

    /* Check Btree Meta page */
    PageId btrMetaPageId = m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId();
    BtrPage metaPage;
    FetchPage(metaPage, btrMetaPageId); /* fetch meta page from disk(buffer pool) after changed*/
    ReadDataFileImage(btrMetaPageId, (char *)&testPage, BLCKSZ); /* Read page before changing */
    RestorePage(testPage, btrMetaPageId); /* restore the unchanged page to disk(buffer pool) so that we can apply redo */

    auto *metaRecord = static_cast<WalRecordIndex *>(&metaRecordInfo->walRecord);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordIndex::DumpIndexRecord(metaRecord, m_waldump_fp);
    RedoIndexRecord(&redoCtx, metaRecord, metaRecordInfo->endPlsn);
    FetchPage(testPage, btrMetaPageId);
    CompareBtrMeta(&testPage, &metaPage);

    /* Check root & insert */
    PageId leafRoot = ((BtrMeta *)testPage.GetData())->GetRootPageId();
    BtrPage rootPage;
    FetchPage(rootPage, leafRoot); /* fetch root page from disk(buffer pool) after changed*/
    ReadDataFileImage(leafRoot, (char *)&testPage, BLCKSZ); /* Read page before changing */
    RestorePage(testPage, leafRoot); /* restore the unchanged page to disk(buffer pool) so that we can apply redo */

    auto *rootRecord = static_cast<WalRecordBtreeNewLeafRoot *>(&rootRecordInfo->walRecord);
    auto *insertRecord = static_cast<WalRecordBtreeInsertOnLeaf *>(&insertRecordInfo->walRecord);
    WalRecordIndex::DumpIndexRecord(rootRecord, m_waldump_fp);
    WalRecordIndex::DumpIndexRecord(insertRecord, m_waldump_fp);
    RedoIndexRecord(&redoCtx, rootRecord, rootRecordInfo->endPlsn);
    RedoIndexRecord(&redoCtx, insertRecord,insertRecordInfo->endPlsn);
    FetchPage(testPage, leafRoot);
    CompareTwoPageMembers(&testPage, &rootPage);
}

TEST_F(UTBtreeWal, BtreeSplitLeafRootWalTest_level0)
{
    /* Step 1. Prepare an index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    /* Insert random data */
    m_utTableHandler->InsertRandomIndexTuple();

    /* Step 2. Prepare a to-be-split page */
    /* Get leaf page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);

    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_SHARED);
    auto *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Prepare insert data */
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Fulfill leaf page */
    while (targetPage->GetFreeSpaceForInsert() >= insertContext->indexTuple->GetSize()) {
        bufMgr->UnlockContent(targetBuf);
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid), DSTORE_SUCC);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
        bufMgr->LockContent(targetBuf, LW_SHARED);
        targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    }
    /* Now leaf page is full and will split at next insertion */
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    BtrPage splitPage;
    memcpy((void *)&splitPage, targetPage, BLCKSZ);
    bufMgr->UnlockAndRelease(targetBuf);
    txn->Commit();
    bufMgr->FlushAll(false);

    /* Step 3. copy dataFile as Image */
    CopyDataFile();
    /* Step 4. get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 5. Make page split */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
    BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                            m_utTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid), DSTORE_SUCC);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* Step 6. Redo and check */
    RedoAndCheckSplitPage(splitPage, leafPageId, xid, plsn, true);
    RedoAndCheckRoot(xid, plsn);
}

TEST_F(UTBtreeWal, BtreeSplitLeafWalTest_level0)
{
    /* Step 1. Prepare an index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);
    /* insert data with the same key */
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    for (int i = 0; i < 400; i++) {
        m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
    }
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
    btreeBuild.BuildIndex();
    txn->Commit();

    /* Step 2. Prepare a to-be-split page */
    /* Get leftmost leaf */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId rootPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);
    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    auto *root = static_cast<BtrPage *>(rootBuf->GetPage());
    EXPECT_TRUE(root->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    EXPECT_EQ(root->GetLinkAndStatus()->GetLevel(), 1);
    PageId leftPageId = root->GetIndexTuple(1)->GetLowlevelIndexpageLink();
    bufMgr->UnlockAndRelease(rootBuf);
    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, leftPageId, LW_SHARED);
    auto *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());

    /* Fulfill leaf page with fake ctid */
    ItemPointerData fakeCtid{{0, 0}, 0};
    while (targetPage->GetFreeSpaceForInsert() >= insertContext->indexTuple->GetSize()) {
        bufMgr->UnlockContent(targetBuf);
        txn->Start();
        txn->SetSnapshotCsn();
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
        txn->Commit();
        bufMgr->LockContent(targetBuf, LW_SHARED);
        targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    }
    /* Now leaf page is full and will split at next insertion */
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    bufMgr->UnlockContent(targetBuf);

    /* Set xid' status in TD slots to in-progress to simulate concurrent transactions */
    bufMgr->LockContent(targetBuf, LW_EXCLUSIVE);
    uint8 tdNum = targetPage->GetTdCount();
    bool hasValidXid = false;
    for (int i = 0; i < tdNum; i++) {
        Xid xid = targetPage->GetTd(i)->GetXid();
        if (xid == INVALID_XID) {
            continue;
        }
        hasValidXid = true;
        UndoZone *uzone = nullptr;
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
        TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->m_txnInfoCache->
            m_cachedEntry[xid.m_zoneId][xid.m_logicSlotId % CACHED_SLOT_NUM_PER_ZONE].txnInfo.status = TXN_STATUS_IN_PROGRESS;
        slot->SetTrxSlotStatus(TXN_STATUS_IN_PROGRESS);
        targetPage->GetTd(i)->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
        targetPage->GetTd(i)->SetCsnStatus(IS_INVALID);
    }
    EXPECT_TRUE(hasValidXid);
    bufMgr->MarkDirty(targetBuf, false);

    BtrPage splitPage;
    memcpy((void *)&splitPage, targetPage, BLCKSZ);
    bufMgr->UnlockAndRelease(targetBuf);
    bufMgr->FlushAll(false);

    /* Step 3. copy dataFile as Image */
    CopyDataFile();
    /* Step 4. get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 5. Make page split */
    txn->Start();
    txn->SetSnapshotCsn();
    BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                            m_utTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Check targetPage again to ensure that TD space has been extended */
    targetBuf = bufMgr->Read(g_defaultPdbId, leftPageId, LW_SHARED);
    targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_EQ(targetPage->GetTdCount(), tdNum + 2);
    bufMgr->UnlockAndRelease(targetBuf);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* Step 6. Redo and check split */
    RedoAndCheckSplitPage(splitPage, leftPageId, xid, plsn, true);

    /* Step 7. Redo and check parent */
    /* Read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_INSERT_ON_INTERNAL, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    WalRecordRedoInfo *insertRecordInfo = recordList.front();

    /* Record newest root */
    BtrPage rootPage;
    FetchPage(rootPage, rootPageId);

    /* Restore old root */
    BtrPage testPage;
    ReadDataFileImage(rootPageId, (char *)&testPage, BLCKSZ);
    RestorePage(testPage, rootPageId);

    /* Apply redo */
    PageId oldRootPageId;
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    auto *insertRecord= static_cast<WalRecordBtreeInsertOnInternal *>(&insertRecordInfo->walRecord);
    WalRecordIndex::DumpIndexRecord(insertRecord, m_waldump_fp);
    RedoIndexRecord(&redoCtx, insertRecord, insertRecordInfo->endPlsn);

    /* Fetch pages after redo */
    FetchPage(testPage, rootPageId);
    EXPECT_TRUE(testPage.GetLinkAndStatus()->IsRoot());
    CompareTwoPageMembers(&rootPage, &testPage);

    /* Check split flag on split page */
    FetchPage(testPage, leftPageId);
    EXPECT_FALSE(testPage.GetLinkAndStatus()->IsSplitComplete());

    /* Redo and check split status */
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_SPLITSTATUS, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);
    WalRecordRedoInfo *splitRecordInfo = recordList.back();
    auto *walRecord = static_cast<WalRecordIndex *>(&splitRecordInfo->walRecord);
    WalRecordIndex::DumpIndexRecord(walRecord, m_waldump_fp);
    RedoIndexRecord(&redoCtx, walRecord, splitRecordInfo->endPlsn);

    /* Check split flag on split page */
    FetchPage(testPage, leftPageId);
    EXPECT_TRUE(testPage.GetLinkAndStatus()->IsSplitComplete());
}

TEST_F(UTBtreeWal, DISABLED_BtreeSplitInternalWalTest_level0)
{
    /* Step 1. Prepare an index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);
    /* insert data */
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    for (int i = 0; i < 40000; i++) {
        m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
    }
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexBuildInfo(),
                          m_utTableHandler->GetIndexScanKey());
    btreeBuild.BuildIndex();
    txn->Commit();

    /* Step 2. Prepare a to-be-split root */
    /* Get root page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId targetPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);
    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, targetPageId, LW_SHARED);
    auto *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    EXPECT_EQ(targetPage->GetLinkAndStatus()->GetLevel(), 1);

    txn->Start();
    txn->SetSnapshotCsn();
    /* Fulfill root page */
    while (targetPage->GetFreeSpaceForInsert() >= insertContext->indexTuple->GetSize() + sizeof(ItemPointerData)) {
        bufMgr->UnlockContent(targetBuf);
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid), DSTORE_SUCC);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
        bufMgr->LockContent(targetBuf, LW_SHARED);
        targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    }
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->IsRoot());

    /* Fulfill leftmost leaf using fake ctid */
    PageId leftmostPageId = targetPage->GetIndexTuple(targetPage->GetLinkAndStatus()->GetFirstDataOffset())->GetLowlevelIndexpageLink();
    bufMgr->UnlockContent(targetBuf);
    BufferDesc *leftBuf = bufMgr->Read(g_defaultPdbId, leftmostPageId, LW_EXCLUSIVE);
    auto leftPage = static_cast<BtrPage *>(leftBuf->GetPage());
    ItemPointerData fakeCtid{{0, 0}, 0};
    while (leftPage->GetFreeSpaceForInsert() >= insertContext->indexTuple->GetSize()) {
        bufMgr->UnlockContent(leftBuf);
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
        bufMgr->LockContent(leftBuf, LW_SHARED);
        leftPage = static_cast<BtrPage *>(leftBuf->GetPage());
    }
    /* Now leaf page is full and will split at next insertion */
    bufMgr->UnlockAndRelease(leftBuf);
    bufMgr->LockContent(targetBuf, LW_SHARED);
    targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->IsRoot());

    BtrPage splitRoot;
    memcpy((void *)&splitRoot, targetPage, BLCKSZ);
    bufMgr->UnlockAndRelease(targetBuf);
    txn->Commit();
    bufMgr->FlushAll(false);

    /* Step 3. copy dataFile as Image */
    CopyDataFile();
    /* Step 4. get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 5. Make page split */
    txn->Start();
    txn->SetSnapshotCsn();
    BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                            m_utTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* Step 6. Redo and check */
    RedoAndCheckSplitPage(splitRoot, targetPageId, xid, plsn, false);
    RedoAndCheckRoot(xid, plsn);
}

TEST_F(UTBtreeWal, BtreeUnlinkWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    g_storageInstance->GetBufferMgr()->FlushAll(false);
    PageId rootPageId = recyclableBtree->GetBtrStorageMgr()->GetRootPageIdFromMetaCache();

    /* Copy dataFile as Image */
    CopyDataFile();
    m_walStream = m_walStreamManager->GetWritingWalStream();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Recycle a "skinny subtree". */
    /* ---- Since we have 4 leaves in total, according to the rule we built this recyclable Btree, the leftmost leaf
     * page belongs to a "skinny subtree" that we can recycle 3 pages from level 0 to level2.*/
    recyclableBtree->RecyclePageAndCheck(1);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_RIGHT_SIB_LINK, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_LEFT_SIB_LINK, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 4);

    /* Redo and check link changes */
    BtrPage testPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        auto *record = static_cast<WalRecordIndex *>(&(recordInfo->walRecord));
        WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
        FetchPage(afterPage, record->m_pageId);
        ReadDataFileImage(record->m_pageId, (char *)&testPage, BLCKSZ);
        RestorePage(testPage, record->m_pageId);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
        FetchPage(testPage, record->m_pageId);
        CompareTwoPageMembers(&testPage, &afterPage);
    }
    recordList.clear();

    /* Redo and check internal delete */
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_DOWNLINK, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 3);
    WalRecordRedoInfo *rootRecordInfo = nullptr;
    WalRecordRedoInfo *leafRecordInfo1 = nullptr;
    WalRecordRedoInfo *leafRecordInfo2 = nullptr;
    for (WalRecordRedoInfo *recordInfo : recordList) {
        auto *record = static_cast<WalRecordIndex *>(&(recordInfo->walRecord));
        if (record->m_pageId == rootPageId) {
            rootRecordInfo = recordInfo;
        } else if (leafRecordInfo1 == nullptr) {
            leafRecordInfo1 = recordInfo;
        } else {
            leafRecordInfo2 = recordInfo;
        }
    }
    recordList.clear();
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_DELETE_ON_INTERNAL, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_PAGE_PRUNE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 3);
    WalRecordRedoInfo *deleteRecordInfo = recordList.front();
    WalRecordRedoInfo *pruneRecordInfo = recordList.back();

    auto *deleteRecord = static_cast<WalRecordIndex *>(&(deleteRecordInfo->walRecord));
    auto *pruneRecord = static_cast<WalRecordIndex *>(&(pruneRecordInfo->walRecord));
    auto *downlinkRecord = static_cast<WalRecordIndex *>(&(rootRecordInfo->walRecord));
    WalRecordIndex::DumpIndexRecord(deleteRecord, m_waldump_fp);
    WalRecordIndex::DumpIndexRecord(pruneRecord, m_waldump_fp);
    WalRecordIndex::DumpIndexRecord(downlinkRecord, m_waldump_fp);
    FetchPage(afterPage, deleteRecord->m_pageId);
    ReadDataFileImage(deleteRecord->m_pageId, (char *)&testPage, BLCKSZ);
    RestorePage(testPage, deleteRecord->m_pageId);
    RedoIndexRecord(&redoCtx, deleteRecord, deleteRecordInfo->endPlsn);
    RedoIndexRecord(&redoCtx, pruneRecord, pruneRecordInfo->endPlsn);
    RedoIndexRecord(&redoCtx, downlinkRecord, rootRecordInfo->endPlsn);
    FetchPage(testPage, deleteRecord->m_pageId);
    CompareTwoPageMembers(&testPage, &afterPage);
    recordList.clear();

    /* Redo and check leaf downlink change */
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_DOWNLINK, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 3);
    for (WalRecordRedoInfo *recordInfo : recordList) {
        auto *record = static_cast<WalRecordIndex *>(&(recordInfo->walRecord));
        if (record->m_pageId == rootPageId) {
            continue;
        }
        WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
        FetchPage(afterPage, record->m_pageId);
        if (afterPage.GetLinkAndStatus()->TestLiveStatus(DSTORE::BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB)) {
            continue;
        }
        ReadDataFileImage(record->m_pageId, (char *)&testPage, BLCKSZ);
        RestorePage(testPage, record->m_pageId);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
        FetchPage(testPage, record->m_pageId);
        CompareTwoPageMembers(&testPage, &afterPage);
    }
}

TEST_F(UTBtreeWal, BtreeChangeLowestSingleWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    (void)g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* Copy dataFile as Image */
    CopyDataFile();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Recycle a "skinny subtree". and the lowest single page will go down */
    recyclableBtree->RecyclePageAndCheck(0);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    /* Redo and check */
    BtrPage testPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    auto *record = static_cast<WalRecordIndex *>(&(recordInfo->walRecord));
    WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&testPage, BLCKSZ);
    RestorePage(testPage, record->m_pageId);
    RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(testPage, record->m_pageId);
    CompareBtrMeta(&testPage, &afterPage);
}

TEST_F(UTBtreeWal, BtreeDeleteWALTest_level0)
{
    /* Build btree index. */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Insert specific data for later deletion */
    /* We would insert tdNum times to use up all TD slots on page */
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    ItemPointerData fakeCtid{{0, 0}, 0};
    Transaction *txn = thrd->GetActiveTransaction();
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        fakeCtid.SetOffset(fakeCtid.GetOffset() + 1);
        txn->Start();
        txn->SetSnapshotCsn();
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
        txn->Commit();
    }

    /* Get leaf */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);
    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_SHARED);
    BtrPage *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    EXPECT_EQ(targetPage->GetLinkAndStatus()->GetLevel(), 0);
    uint8 tdNum = targetPage->GetTdCount();
    bufMgr->UnlockContent(targetBuf);

    /* Now TD space is full and will extent at next operation */
    bufMgr->LockContent(targetBuf, LW_EXCLUSIVE);
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    EXPECT_EQ(targetPage->GetLinkAndStatus()->GetLevel(), 0);

    /* Set some of xids' status in TD slots to in-progress to simulate concurrent transactions */
    bool hasValidXid = false;
    for (int i = 0; i < tdNum - 1; i++) {
        Xid xid = targetPage->GetTd(i)->GetXid();
        if (xid == INVALID_XID) {
            continue;
        }
        hasValidXid = true;
        UndoZone *uzone = nullptr;
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
        TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
        slot->SetTrxSlotStatus(TXN_STATUS_IN_PROGRESS);
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->WriteTxnInfoToCache(xid, *slot, slot->csn, true);
        targetPage->GetTd(i)->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
        targetPage->GetTd(i)->SetCsnStatus(IS_INVALID);
    }
    EXPECT_TRUE(hasValidXid);
    bufMgr->MarkDirty(targetBuf, false);
    bufMgr->UnlockAndRelease(targetBuf);

    BtrPage redoPage;
    BtrPage afterPage;
    uint64 plsn;

    /* only leaf, since internal wal will be tested in unlink wal */
    g_storageInstance->GetBufferMgr()->FlushAll(false);
    CopyDataFile();
	m_walStream = m_walStreamManager->GetWritingWalStream();
    plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    FetchPage(afterPage, leafPageId);
    ReadDataFileImage(leafPageId, (char *)&redoPage, BLCKSZ);
    CompareTwoPageMembers(&redoPage, &afterPage, true);

    txn->Start();
    txn->SetSnapshotCsn();
    BtreeDelete leafDelete(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                           m_utTableHandler->GetIndexScanKey());
    /* Delete the last inserted tuple */
    EXPECT_EQ(leafDelete.DeleteTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_BTREE_DELETE_ON_LEAF, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    EXPECT_EQ(recordList.size(), 1);

    auto *record = static_cast<WalRecordIndex *>(&(recordList.front()->walRecord));
    WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
    FetchPage(afterPage, record->m_pageId);
    /* Check target page to ensure TD has been reused */
    EXPECT_EQ(afterPage.GetTdCount(), tdNum);
    /* Check the last TD slot is exactly which we used for deletion */
    EXPECT_EQ(afterPage.GetTd(tdNum -1)->GetXid(), xid);

    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoIndexRecord(&redoCtx, record, recordList.front()->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    CompareTwoPageMembers(&redoPage, &afterPage);
    recordList.clear();
}

TEST_F(UTBtreeWal, BtreePageRollbackWALTest_level0)
{
    /* Step 1. insert an index tuple and get leaf page */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->InsertRandomIndexTuple(true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    g_storageInstance->GetBufferMgr()->FlushAll(false);
    /* get leaf page id*/
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    BtrPage tmpPage;
    FetchPage(tmpPage, leafPageId);
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: do page rollback and generate page rollback wal */
    txn->Start();
    txn->SetSnapshotCsn();
    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_EXCLUSIVE);
    auto *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    BtreeUndoContext btrUndoContext(g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
        m_utTableHandler->GetIndexInfo(), bufMgr, m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid());
    btrUndoContext.InitWithBtrPage(targetPage, targetBuf);
    RetStatus ret = targetPage->RollbackByXid(m_pdbId, xid, bufMgr, targetBuf, &btrUndoContext);
    bufMgr->UnlockAndRelease(targetBuf);
    ASSERT_EQ(ret, DSTORE_SUCC);
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_UNDO_BTREE_PAGE_ROLL_BACK */
    std::vector<WalRecordRedoInfo *> btreePageRollbackList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_BTREE_PAGE_ROLL_BACK, btreePageRollbackList);
    ASSERT_TRUE(btreePageRollbackList.size() == 1);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordUndo *record = static_cast<WalRecordUndo *>(&(btreePageRollbackList[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(record, m_waldump_fp);
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), g_defaultPdbId, m_walStream->GetMaxAppendedPlsn()};
    targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_EXCLUSIVE);
    WalRecordUndo::RedoUndoRecord(&redoCtx, record, targetBuf);
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(targetBuf);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(targetBuf);
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    ASSERT_TRUE(CmpPages(&redoPage, &afterPage, BLCKSZ));
}

/* this case to keep that page is same in primary and standby node when btree insert extend undo space fail */
TEST_F(UTBtreeWal, BtreePageInsertExtendUndoSpaceFailTest_level0)
{
    /* Step 1. insert four index tuple to occupy all td and get leaf page */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    const int nIndexTuple = 4;
    for (int i = 0; i < nIndexTuple; i++) {
        m_utTableHandler->InsertRandomIndexTuple();
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);
    /* get leaf page id*/
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    BtrPage tmpPage;
    FetchPage(tmpPage, leafPageId);
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: insert one index tuple and extend undo space fail, only record alloctd wal */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(insertContext->heapTuple, true);
    BtreeInsert btreeInsert(m_utTableHandler->m_btreeTestContext->indexRel,
        &m_utTableHandler->m_btreeTestContext->indexBuildInfo->baseInfo,
        m_utTableHandler->m_btreeTestContext->scanKeyInfo);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, 0, FI_GLOBAL, 0, 1);
    RetStatus status = btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &ctid);
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, FI_GLOBAL);
    StorageAssert(status == DSTORE_FAIL);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());


    /* step 5: read walFile to Get WalRecord of WAL_BTREE_ALLOC_TD */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_BTREE_ALLOC_TD, recordList);
    ASSERT_TRUE(recordList.size() == 1);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordIndex *record = static_cast<WalRecordIndex *>(&(recordList[0]->walRecord));
    ASSERT_TRUE(record->m_pageId == leafPageId);
    WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), g_defaultPdbId, m_walStream->GetMaxAppendedPlsn()};
    RedoIndexRecord(&redoCtx, record, recordList.front()->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    ASSERT_TRUE(afterPage.m_header.m_flags == PAGE_TUPLE_PRUNABLE);
    ASSERT_TRUE(redoPage.m_header.m_flags == 0);
    redoPage.m_header.m_flags = afterPage.m_header.m_flags;
    ComparePageHeader(&afterPage, &redoPage, false);
    CompareTDs(&afterPage, &redoPage);
    CompareItems(&afterPage, &redoPage);
    ASSERT_TRUE(CmpPages(static_cast<char *>(static_cast<void *>(&redoPage)) + redoPage.m_header.m_upper,
        static_cast<char *>(static_cast<void *>(&afterPage)) + redoPage.m_header.m_upper,
        BLCKSZ - redoPage.m_header.m_upper));
}

/* this case to keep that page is same in primary and standby node when btree delete insert undo record fail */
TEST_F(UTBtreeWal, BtreePageDeleteInsertUndoRecordFailTest_level0)
{
    /* Step 1. insert four index tuple to occupy all td and get leaf page */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    ItemPointerData fakeCtid{{0, 0}, 0};
    Transaction *txn = thrd->GetActiveTransaction();
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        fakeCtid.SetOffset(fakeCtid.GetOffset() + 1);
        txn->Start();
        txn->SetSnapshotCsn();
        BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                m_utTableHandler->GetIndexScanKey());
        EXPECT_EQ(btreeInsert.InsertTuple(insertContext->values, insertContext->isnull, &fakeCtid), DSTORE_SUCC);
        txn->Commit();
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);
    /* get leaf page id*/
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    BtrPage tmpPage;
    FetchPage(tmpPage, leafPageId);
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: delete last inserted index tuple and insert undo record fail, only record alloctd wal */
    txn->Start();
    txn->SetSnapshotCsn();
    BtreeDelete leafDelete(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                           m_utTableHandler->GetIndexScanKey());
    /* Delete the last inserted tuple */
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, 0, FI_GLOBAL, 0, 1);
    RetStatus status = leafDelete.DeleteTuple(insertContext->values, insertContext->isnull, &fakeCtid);
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, FI_GLOBAL);
    StorageAssert(status == DSTORE_FAIL);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_BTREE_ALLOC_TD */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_BTREE_ALLOC_TD, recordList);
    ASSERT_TRUE(recordList.size() == 1);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordIndex *record = static_cast<WalRecordIndex *>(&(recordList[0]->walRecord));
    ASSERT_TRUE(record->m_pageId == leafPageId);
    WalRecordIndex::DumpIndexRecord(record, m_waldump_fp);
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), g_defaultPdbId, m_walStream->GetMaxAppendedPlsn()};
    RedoIndexRecord(&redoCtx, record, recordList.front()->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    ComparePageHeader(&afterPage, &redoPage, false);
    CompareTDs(&afterPage, &redoPage);
    CompareItems(&afterPage, &redoPage);
    ASSERT_TRUE(CmpPages(static_cast<char *>(static_cast<void *>(&redoPage)) + redoPage.m_header.m_upper,
        static_cast<char *>(static_cast<void *>(&afterPage)) + redoPage.m_header.m_upper,
        BLCKSZ - redoPage.m_header.m_upper));
}

TEST_F(UTBtreeWal, BtreeDuplicateRollbackAfterCommitTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* 1. Build btree index using empty table */
    int indexCols[] = {TEXT_IDX};
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, false);
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2610));
    ItemPointerData fakeHeapCtid = {{1, 1}, 1};

    /* 2. Insert and delete within the same transaction to avoid page prune before splitting */
    txn->Start();
    txn->SetSnapshotCsn();
    CommitSeqNo flashbackCsn = txn->GetSnapshotCsn();
    int insertRowCount = 4;
    std::vector<CommandId> cidVec;
    for (int i = 1; i <= insertRowCount; ++i) {
        EXPECT_EQ(m_utTableHandler->InsertIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        cidVec.push_back(txn->GetCurCid());
        txn->IncreaseCommandCounter();

        EXPECT_EQ(m_utTableHandler->DeleteIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        cidVec.push_back(txn->GetCurCid());
        txn->IncreaseCommandCounter();
    }
    Xid insertXid = txn->GetCurrentXid();
    bool insCommit = true;
    if ((uint32)std::rand() % 2 == 0) {
        txn->Commit();
    } else {
        txn->Abort();
        insCommit = false;
    }
    XidStatus xidStatus(insertXid, txn);
    CommitSeqNo commitcsn = xidStatus.GetCsn();

    /* 3. Scan data on page now. */
    txn->Start();
    txn->SetSnapshotCsn();
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    indexScan.ReScan(nullptr);
    int visTupleCnt = 0;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::BACKWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCnt++;
    }
    EXPECT_EQ(visTupleCnt, 0);

    /* 4. Flashback to flashbackCsn should be empty.*/
    txn->SetSnapshotCsnForFlashback(flashbackCsn);
    for (const CommandId& cid : cidVec) {
        txn->SetTransactionSnapshotCid(cid);
        indexScan.InitSnapshot(txn->GetSnapshot());
        indexScan.ReScan(nullptr);
        visTupleCnt = 0;
        while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
            visTupleCnt++;
        }
        EXPECT_EQ(visTupleCnt, 0);
    }
    indexScan.EndScan();
    txn->Commit();

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    EXPECT_EQ(btrMeta->GetRootLevel(), 1);
    PageId rootPageId =  btrMeta->GetRootPageId();
    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    PageId leftmostLeafPageId =
        rootPage->GetIndexTuple(rootPage->GetLinkAndStatus()->GetFirstDataOffset())->GetLowlevelIndexpageLink();
    BufferDesc *leftmostLeafPageBuf = bufMgr->Read(g_defaultPdbId, leftmostLeafPageId, LW_EXCLUSIVE);
    BtrPage *leftmostLeafPage = static_cast<BtrPage *>(leftmostLeafPageBuf->GetPage());
    
    PageId rightmostLeafPageId = leftmostLeafPage->GetRight();
    BufferDesc *rightmostLeafPageBuf = bufMgr->Read(g_defaultPdbId, rightmostLeafPageId, LW_EXCLUSIVE);
    BtrPage *rightmostLeafPage = static_cast<BtrPage *>(rightmostLeafPageBuf->GetPage());
    if (insCommit) {
        /* 5. Rollback leftmost page. */
        EXPECT_EQ(leftmostLeafPage->GetMaxOffset(), 3);
        txn->Start();
        txn->SetSnapshotCsn();
        BtreeUndoContext btrUndoContext(g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId(),
            m_utTableHandler->GetIndexInfo(), bufMgr, m_utTableHandler->GetBtreeSmgr()->GetMetaCreateXid());
        btrUndoContext.InitWithBtrPage(leftmostLeafPage, leftmostLeafPageBuf);
        RetStatus ret = leftmostLeafPage->RollbackByXid(m_pdbId, insertXid, bufMgr,
                                                        leftmostLeafPageBuf, &btrUndoContext);
        EXPECT_EQ(ret, DSTORE_SUCC);
        txn->Commit();

        /* 6. Rollback rightmost page. */
        EXPECT_EQ(rightmostLeafPage->GetMaxOffset(), 2);
        txn->Start();
        txn->SetSnapshotCsn();
        btrUndoContext.InitWithBtrPage(rightmostLeafPage, rightmostLeafPageBuf);
        ret = rightmostLeafPage->RollbackByXid(m_pdbId, insertXid, bufMgr, rightmostLeafPageBuf, &btrUndoContext);
        EXPECT_EQ(ret, DSTORE_SUCC);
        txn->Commit();
    }

    bufMgr->UnlockAndRelease(leftmostLeafPageBuf);
    bufMgr->UnlockAndRelease(rightmostLeafPageBuf);
    bufMgr->UnlockAndRelease(rootBuf);
    bufMgr->UnlockAndRelease(btrMetaBuf);

    EXPECT_TRUE(leftmostLeafPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    EXPECT_TRUE(leftmostLeafPage->IsLeftmost());
    EXPECT_EQ(leftmostLeafPage->GetMaxOffset(), 2);
    EXPECT_EQ(leftmostLeafPage->GetNonDeletedTupleNum(), 0);

    EXPECT_TRUE(rightmostLeafPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    EXPECT_TRUE(rightmostLeafPage->IsRightmost());
    EXPECT_EQ(rightmostLeafPage->GetMaxOffset(), 1);
    EXPECT_EQ(rightmostLeafPage->GetNonDeletedTupleNum(), 0);
}

TEST_F(UTBtreeWal, BtreeDuplicateRollbackBeforeCommitTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* 1. Build btree index using empty table */
    int indexCols[] = {TEXT_IDX};
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, false);
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2610));
    ItemPointerData fakeHeapCtid = {{1, 1}, 1};

    /* 2. Insert and delete whthin the same transaction to avoid page prune before splitting */
    txn->Start();
    txn->SetSnapshotCsn();
    int insertRowCount = 4;
    std::vector<CommandId> cidVec; /* 0 - 7 */
    for (int i = 1; i <= insertRowCount; ++i) {
        EXPECT_EQ(m_utTableHandler->InsertIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        cidVec.push_back(txn->GetCurCid());
        txn->IncreaseCommandCounter();
        EXPECT_EQ(m_utTableHandler->DeleteIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        cidVec.push_back(txn->GetCurCid());
        txn->IncreaseCommandCounter();
    }
    Xid insertXid = txn->GetCurrentXid();

    /* 3. Scan data on page now. */
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    indexScan.ReScan(nullptr);
    int visTupleCnt = 0;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::BACKWARD_SCAN_DIRECTION, &found)) && found) {
        visTupleCnt++;
    }
    EXPECT_EQ(visTupleCnt, 0);

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    EXPECT_EQ(btrMeta->GetRootLevel(), 1);
    PageId rootPageId =  btrMeta->GetRootPageId();
    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    PageId leftmostLeafPageId =
        rootPage->GetIndexTuple(rootPage->GetLinkAndStatus()->GetFirstDataOffset())->GetLowlevelIndexpageLink();
    bufMgr->UnlockAndRelease(rootBuf);

    /* 4. Rollback to cid and scan visible tuple.*/
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    for (auto cid = cidVec.rbegin(); cid != cidVec.rend(); cid++) {
        BufferDesc *pageBuf = bufMgr->Read(g_defaultPdbId, leftmostLeafPageId, LW_EXCLUSIVE);
        BtrPage *page = static_cast<BtrPage *>(pageBuf->GetPage());
        PageId nextPageId = page->GetRight();
        TD *td = nullptr;
        for (TdId tdId = 0; tdId < page->GetTdCount(); tdId++) {
            td = page->GetTd(tdId);
            if (td->GetXid() == insertXid) {
                break;
            }
        }
        UndoRecord undoRecord;
        for (;;) {
            EXPECT_EQ(transactionMgr->FetchUndoRecord(td->GetXid(), &undoRecord, td->GetUndoRecPtr()), DSTORE_SUCC);
            if (*cid == undoRecord.GetCid()) {
                break;
            } else {
                td->RollbackTdInfo(&undoRecord);
            }
        }

        BtreeUndoContext *btrUndoContext = nullptr;
        bool needFreeOutside = true;
        btrUndoContext = BtreeUndoContext::FindUndoRecRelatedPage(btrUndoContext, g_defaultPdbId, pageBuf, insertXid,
            &undoRecord, bufMgr, needFreeOutside, td->GetUndoRecPtr(), false);
        if (pageBuf != btrUndoContext->m_currBuf) {
            /* The undo recored related index tuple is not on pageBuf and FindUndoRecRelatedPage has already
             * stepped to the correct page for us */
            EXPECT_TRUE(btrUndoContext->m_currBuf->GetPageId() == nextPageId);
            /* No need to release pageBuf, FindUndoRecRelatedPage has released it before step right */
            pageBuf = btrUndoContext->m_currBuf;
            page = static_cast<BtrPage *>(pageBuf->GetPage());
        }
        EXPECT_EQ(page->RollbackByUndoRec(&undoRecord, btrUndoContext), DSTORE_SUCC);
        bufMgr->UnlockAndRelease(pageBuf);

        int numTuple = undoRecord.GetUndoType() == UNDO_BTREE_DELETE ? 1 : 0;
        indexScan.InitSnapshot(txn->GetSnapshot());
        indexScan.ReScan(nullptr);
        visTupleCnt = 0;
        while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
            visTupleCnt++;
        }
        EXPECT_EQ(visTupleCnt, numTuple);
    }
    indexScan.EndScan();
    bufMgr->UnlockAndRelease(btrMetaBuf);
    if ((uint32)std::rand() % 2 == 0) {
        txn->Commit();
    } else {
        txn->Abort();
    }
}

TEST_F(UTBtreeWal, BtreeDuplicateRollbackSingleTransTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* 1. Build btree index using empty table */
    int indexCols[] = {TEXT_IDX};
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, false);
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2610));
    ItemPointerData fakeHeapCtid = {{1, 1}, 1};

    /* 2. Insert and delete. */
    std::vector<CommitSeqNo> csnVec;
    int insertRowCount = 10;
    for (int i = 1; i <= insertRowCount; ++i) {
        txn->Start();
        txn->SetSnapshotCsn();
        EXPECT_EQ(m_utTableHandler->InsertIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        Xid insertXid = txn->GetCurrentXid();
        txn->Commit();
        XidStatus insertXidStatus(insertXid, txn);
        csnVec.push_back(insertXidStatus.GetCsn());
        
        txn->Start();
        txn->SetSnapshotCsn();
        EXPECT_EQ(m_utTableHandler->DeleteIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        Xid deleteXid = txn->GetCurrentXid();
        txn->Commit();
        XidStatus deleteXidStatus(deleteXid, txn);
        csnVec.push_back(deleteXidStatus.GetCsn());
    }

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    EXPECT_EQ(btrMeta->GetRootLevel(), 0);

    /* 3. Scan data on page now. */
    txn->Start();
    txn->SetSnapshotCsn();
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    indexScan.ReScan(nullptr);
    int visTupleCnt = 0;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::BACKWARD_SCAN_DIRECTION, &found)) && found){
        visTupleCnt++;
    }
    EXPECT_EQ(visTupleCnt, 0);

    /* 4. Flashback to commit csn.*/
    bool visible = false;
    for (const CommitSeqNo& csn : csnVec) {
        txn->SetSnapshotCsnForFlashback(csn);
        indexScan.InitSnapshot(txn->GetSnapshot());
        indexScan.ReScan(nullptr);
        visTupleCnt = 0;
        while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
            visTupleCnt++;
        }
        EXPECT_EQ(visTupleCnt, visible ? 1 : 0);
        visible = !visible;
    }
    indexScan.EndScan();
    txn->Commit();
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(btrMetaBuf);
}

TEST_F(UTBtreeWal, BtreeDuplicateRedoSplitTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* 1. Build btree index using empty table */
    int indexCols[] = {TEXT_IDX};
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, false);
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2610));
    ItemPointerData fakeHeapCtid = {{1, 1}, 1};

    /* 2. Insert and delete whthin the same transaction to avoid page prune before splitting */
    txn->Start();
    txn->SetSnapshotCsn();
    int insertRowCount = 4;
    BtrPage beforePage;
    PageId beforePageId;
    uint64 redoStartPlsn;
    for (int i = 1; i <= insertRowCount; ++i) {
        if (i == insertRowCount) {
            redoStartPlsn = m_walStream->GetMaxAppendedPlsn();
            m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), redoStartPlsn);
            
            BufferDesc *btrMetaBuf;
            BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
            beforePageId =  btrMeta->GetRootPageId();
            EXPECT_EQ(btrMeta->GetRootLevel(), 0);
            bufMgr->UnlockAndRelease(btrMetaBuf);
            FetchPage(beforePage, beforePageId);
        }
        EXPECT_EQ(m_utTableHandler->InsertIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        txn->IncreaseCommandCounter();
        EXPECT_EQ(m_utTableHandler->DeleteIndexTupleOnly(&insertRow, DefaultNullBitMap, &fakeHeapCtid), DSTORE_SUCC);
        txn->IncreaseCommandCounter();
    }
    Xid insertXid = txn->GetCurrentXid();
    txn->Commit();

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    EXPECT_EQ(btrMeta->GetRootLevel(), 1);
    PageId rootPageId =  btrMeta->GetRootPageId();
    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    bufMgr->UnlockAndRelease(rootBuf);
    PageId leftmostLeafPageId =
        rootPage->GetIndexTuple(rootPage->GetLinkAndStatus()->GetFirstDataOffset())->GetLowlevelIndexpageLink();
    BufferDesc *leftmostLeafPageBuf = bufMgr->Read(g_defaultPdbId, leftmostLeafPageId, LW_EXCLUSIVE);
    BtrPage *leftmostLeafPage = static_cast<BtrPage *>(leftmostLeafPageBuf->GetPage());
    PageId rightmostLeafPageId = leftmostLeafPage->GetRight();
    bufMgr->UnlockAndRelease(leftmostLeafPageBuf);
    BtrPage leftPageBackup;
    BtrPage rightPageBackup;
    FetchPage(leftPageBackup, leftmostLeafPageId);
    FetchPage(rightPageBackup, rightmostLeafPageId);

    /* 3. Get redo records and redo page split. */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_SPLIT_INSERT_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_NEW_LEAF_RIGHT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_DELETE_ON_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 3);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_UPDATE_SPLITSTATUS, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 4);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_UPDATE_LIVESTATUS, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 5);

    RestorePage(beforePage, beforePageId);
    WalRecordRedoContext redoCtx = {insertXid, m_walWriter->GetWalId()};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        WalRecordIndex *record = static_cast<WalRecordIndex *>(&recordInfo->walRecord);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
    }
    BtrPage leftRedoPage;
    BtrPage rightRedoPage;
    FetchPage(leftRedoPage, leftmostLeafPageId);
    FetchPage(rightRedoPage, rightmostLeafPageId);

    EXPECT_EQ(rightRedoPage.HasPrunableTuple(), leftRedoPage.HasPrunableTuple());

    /* 4. Compare redo page and backup page. */
    CompareTwoPageMembers(&leftPageBackup, &leftRedoPage);
    CompareTwoPageMembers(&rightPageBackup, &rightRedoPage);

    bufMgr->UnlockAndRelease(btrMetaBuf);
}

TEST_F(UTBtreeWal, BtreeSplitLeafWithUnreadableItemWalTest_level0)
{
    /* Step 1. Prepare an index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_int16 = 1;
    insertRow.column_int32 = 1;
    bool *nullbitmap = DefaultNullBitMap;

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
    EXPECT_NE(indexTuple, nullptr);
    /* first item flag is ITEM_ID_UNREADABLE_RANGE_HOLDER */
    txn->Abort();

    /* Step 2. Prepare a to-be-split page */
    /* Get leaf page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    PageId leafPageId =  btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(btrMetaBuf);

    BufferDesc *targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_SHARED);
    BtrPage *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    ItemId *firstItem = targetPage->GetItemIdPtr(targetPage->GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_TRUE(firstItem->IsRangePlaceholder());

    /* Prepare insert data */
    BtreeTestInsertContext *insertContext = m_utTableHandler->GenerateRandomIndexTuple();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Fulfill leaf page */
    while (targetPage->GetFreeSpaceForInsert() >= insertContext->indexTuple->GetSize()) {
        bufMgr->UnlockContent(targetBuf);
        insertRow.column_int16 = 2;
        insertRow.column_int32 = 2;
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        thrd->GetActiveTransaction()->IncreaseCommandCounter();
        /* keep first item flag is  ITEM_ID_UNREADABLE_RANGE_HOLDER */
        ItemId *firstItem = targetPage->GetItemIdPtr(targetPage->GetLinkAndStatus()->GetFirstDataOffset());
        EXPECT_TRUE(firstItem->IsRangePlaceholder());
        bufMgr->LockContent(targetBuf, LW_SHARED);
        targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    }
    /* Now leaf page is full and will split at next insertion */
    EXPECT_TRUE(targetPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    BtrPage splitPage;
    memcpy((void *)&splitPage, targetPage, BLCKSZ);
    bufMgr->UnlockAndRelease(targetBuf);
    txn->Commit();

    /* Btree page prune, otherwise prune will happen when split to make RedoAndCheckSplitPage check TD failed. */
    targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), targetBuf);
    (void)btreePagePrune->Prune();
    btreePagePrune->GetPagePayload()->Drop(bufMgr);
    delete btreePagePrune;
    btreePagePrune = nullptr;

    bufMgr->FlushAll(false);

    /* Step 3. copy dataFile as Image */
    CopyDataFile();
    /* Step 4. get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 5. Make page split */
    txn->Start();
    txn->SetSnapshotCsn();

    insertRow.column_int16 = 2;
    insertRow.column_int32 = 2;
    indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
    EXPECT_NE(indexTuple, nullptr);

    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    targetBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_SHARED);
    targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    firstItem = targetPage->GetItemIdPtr(targetPage->GetLinkAndStatus()->GetFirstDataOffset());
    EXPECT_TRUE(firstItem->IsRangePlaceholder());
    bufMgr->UnlockAndRelease(targetBuf);

    /* Step 6. Redo and check */
    RedoAndCheckSplitPage(splitPage, leafPageId, xid, plsn, true);
}

TEST_F(UTBtreeWal, BtreePruneByEmptyCheck_level0)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* 1. Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* 2. Insert tuples into btree. */
    int insertRowCount = 7;
    IndexTuple *indexTuples[MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE];
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    PageId firstLeafPageId = INVALID_PAGE_ID;
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    int tupleCounter = 0;
    /* Fistly, make two leaf pages */
    while (true) {
        insertRow.column_int16 = static_cast<int16>(tupleCounter);
        insertRow.column_int32 = static_cast<int32>(tupleCounter);
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow, DefaultNullBitMap, true);
        if (m_utTableHandler->GetLastWorkingBtrPageId().IsValid()) {
            if (firstLeafPageId.IsValid()) {
                EXPECT_EQ(firstLeafPageId, m_utTableHandler->GetLastWorkingBtrPageId());
            } else {
                firstLeafPageId = m_utTableHandler->GetLastWorkingBtrPageId();
            }
        } else {
            DstorePfree(indexTuple);
            break;
        }
        EXPECT_LE(tupleCounter, MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE);
        indexTuples[tupleCounter++] = indexTuple;
    }

    /* Secondly, delete all from the first page */
    int counter = 0;
    PageId lastOperPageId = firstLeafPageId;
    while (lastOperPageId == firstLeafPageId) {
        EXPECT_EQ(DeleteIndexTuple(indexTuples[counter++], true), DSTORE_SUCC);
        lastOperPageId = m_utTableHandler->GetLastWorkingBtrPageId();
    }
    txn->Commit();

    csnMgr->UpdateLocalCsnMin();

    uint64 redoStartPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), redoStartPlsn);
    BtrPage beforePage;
    BtrPage afterPage;
    FetchPage(beforePage, firstLeafPageId);

    /* Set right sibling spliting to make current page unrecycable */
    PageId rightPageId = beforePage.GetRight();
    BufferDesc *rightPageBuf = bufMgr->Read(m_pdbId, rightPageId, LW_EXCLUSIVE);
    BtrPage *rightPage = static_cast<BtrPage *>(rightPageBuf->GetPage());
    rightPage->GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    bufMgr->MarkDirty(rightPageBuf);
    bufMgr->UnlockAndRelease(rightPageBuf);

    /* Try recycle the empty page. It would fail because we  */
    BtreePageRecycle recycle(m_utTableHandler->GetIndexRel());
    recycle.BatchRecycleBtreePage(m_utTableHandler->GetIndexInfo(), m_utTableHandler->GetIndexScanKey());

    /* Insert after recycle failed */
    uint32 textLen = MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE - sizeof(int16) - sizeof(int32) - VARHDRSZ;
    insertRow.column_int16 = static_cast<int16>(0);
    insertRow.column_int32 = static_cast<int32>(0);
    txn->Start();
    lastOperPageId = firstLeafPageId;
    counter = 0;
    while (lastOperPageId == firstLeafPageId) {
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow, DefaultNullBitMap, true);
        EXPECT_NE(indexTuple, nullptr);
        lastOperPageId = m_utTableHandler->GetLastWorkingBtrPageId();
        DstorePfreeExt(indexTuple);
        counter++;
    }
    Xid insertXid = txn->GetCurrentXid();
    txn->Commit();
    BtrPage testPage;
    FetchPage(testPage, firstLeafPageId);

    RestorePage(beforePage, firstLeafPageId);
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_PAGE_PRUNE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_INSERT_ON_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), counter);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_SPLIT_INSERT_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_SPLIT_LEAF, recordList), DSTORE_SUCC);
    EXPECT_EQ(ReadRecordsAfterPlsn(redoStartPlsn, WAL_BTREE_UPDATE_SPLITSTATUS, recordList), DSTORE_SUCC);

    WalRecordRedoContext redoCtx = {insertXid, m_walWriter->GetWalId()};
    for (WalRecordRedoInfo *recordInfo : recordList) {
        WalRecordIndex *record = static_cast<WalRecordIndex *>(&recordInfo->walRecord);
        RedoIndexRecord(&redoCtx, record, recordInfo->endPlsn);
    }
    FetchPage(afterPage, firstLeafPageId);
    CompareTwoPageMembers(&testPage, &afterPage);
}