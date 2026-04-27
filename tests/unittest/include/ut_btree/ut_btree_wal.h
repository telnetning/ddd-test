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
#include <queue>
#include "ut_utilities/ut_dstore_framework.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_mock/ut_mock.h"
#include "ut_btree/ut_btree.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_recycle_wal.h"
#include "index/dstore_btree_prune.h"


/* define a simple rule of sorting pages to help compare redo page and original page with a correct . */
struct BtrPageCmp {
    bool operator()(BtrPage* a, BtrPage* b) {
        return a->GetSelfPageId() < b->GetSelfPageId();
    }
};

class UTBtreeWal : public WALBASICTEST, public UTBtree {
protected:
    void RedoAndCheckRoot(Xid xid, uint64 plsn);
    void RedoAndCheckSplitPage(BtrPage &splitPage, PageId leftPageId, Xid xid, uint64 plsn, bool isLeaf);
    void RedoAndCheckOrigRightPage(BtrPage &newRightPage, PageId oldRightPageId, uint64 plsn);
    void SetUp() override
    {
        UTBtree::SetUp();
        WALBASICTEST::Prepare();

        /* enable Wal flushing */
        PrepareControlFileContent();
        NodeId selfNode = 0; /* todo selfNode must be assigned a correct value */
        ASSERT_EQ(m_walStreamManager->Init(m_walControlFile), DSTORE_SUCC);
        m_walStream = m_walStreamManager->GetWritingWalStream();
        WalReaderConf readerConf = {0, 0, m_walStream, nullptr,
                                    static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize),
                                    DSTORE::WalReadSource::WAL_READ_FROM_DISK};
        ASSERT_EQ(WalRecordReader::AllocateWalReader(readerConf, &m_walRecordReader, m_ut_memory_context),
                  DSTORE_SUCC);
#ifdef ENABLE_FAULT_INJECTION
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, false, nullptr),
            FAULT_INJECTION_ENTRY(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, false, nullptr)};
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
#endif
        m_waldump_fp = fopen("waldump.txt", "w+");
    }

    void TearDown() override
    {
        fclose(m_waldump_fp);
        UTBtree::TearDown();
    }

    void RedoIndexRecord(WalRecordRedoContext *redoCtx, const WalRecordIndex *indexRecord, uint64 plsn)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId, indexRecord->m_pageId, LW_EXCLUSIVE);
        redoCtx->recordEndPlsn = m_walStream->GetMaxAppendedPlsn();
        WalRecordIndex::RedoIndexRecord(redoCtx, indexRecord, bufferDesc);
        (void)bufMgr->MarkDirty(bufferDesc);
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void RedoBtrRecycleRecord(WalRecordRedoContext *redoCtx, const WalRecordBtrRecycle *btrRecycleRecord, uint64 plsn)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId, btrRecycleRecord->m_pageId, LW_EXCLUSIVE);
        redoCtx->recordEndPlsn = m_walStream->GetMaxAppendedPlsn();
        WalRecordBtrRecycle::RedoBtrRecycleRecord(redoCtx, btrRecycleRecord, bufferDesc);
        (void)bufMgr->MarkDirty(bufferDesc);
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void RestorePage(BtrPage &page, PageId pageId)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId, pageId, DSTORE::LW_EXCLUSIVE);
        errno_t rc = memcpy_s(bufferDesc->GetPage(), BLCKSZ, &page, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        bufMgr->MarkDirty(bufferDesc);
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void FetchPage(BtrPage &page, PageId pageId)
    {
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *bufferDesc = bufMgr->Read(g_defaultPdbId, pageId, DSTORE::LW_SHARED);
        errno_t rc = memcpy_s(&page, BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        bufMgr->UnlockAndRelease(bufferDesc);
    }

    void ComparePageHeader(BtrPage *page1, BtrPage *page2, bool checkPlsn = true)
    {
        /* ignore glsn, checksum & cid */
        if (checkPlsn) {
            EXPECT_EQ(page1->GetPlsn(), page2->GetPlsn());
        }
        EXPECT_EQ(page1->GetWalId(), page2->GetWalId());
        EXPECT_EQ(page1->m_header.m_flags, page2->m_header.m_flags);
        EXPECT_EQ(page1->GetLower(), page2->GetLower());
        EXPECT_EQ(page1->GetUpper(), page2->GetUpper());
        EXPECT_EQ(page1->GetSpecialOffset(), page2->GetSpecialOffset());
        EXPECT_EQ(page1->GetType(), page2->GetType());
        EXPECT_EQ(page1->GetSelfPageId().m_fileId, page2->GetSelfPageId().m_fileId);
        EXPECT_EQ(page1->GetSelfPageId().m_blockId, page2->GetSelfPageId().m_blockId);

        EXPECT_EQ(page1->GetTdCount(), page2->GetTdCount());
    }

    void CompareTDs(BtrPage *page1, BtrPage *page2)
    {
        /* TD info */
        EXPECT_EQ(page1->TdDataSize(), page2->TdDataSize());
        for (PdbId tdid = 0; tdid < page1->GetTdCount(); tdid++) {
            TD *td1 = page1->GetTd(tdid);
            TD *td2 = page2->GetTd(tdid);
            EXPECT_EQ(td1->m_xid, td2->m_xid);
            EXPECT_EQ(td1->m_undoRecPtr, td2->m_undoRecPtr);
            EXPECT_EQ(td1->m_lockerXid, td2->m_lockerXid);
            EXPECT_EQ(td1->m_status, td2->m_status);
            EXPECT_EQ(td1->m_pad, td2->m_pad);
        }
    }

    void CompareItems(BtrPage *page1, BtrPage *page2)
    {
        EXPECT_EQ(page1->GetMaxOffset(), page2->GetMaxOffset());
        for (OffsetNumber offset = 1; offset <= page2->GetMaxOffset(); offset++) {
            ItemId *itemId1 = page1->GetItemIdPtr(offset);
            ItemId *itemId2 = page2->GetItemIdPtr(offset);
            EXPECT_EQ(itemId1->m_placeHolder, itemId2->m_placeHolder);
            if (itemId1->IsNoStorage()) {
                continue;
            }
            IndexTuple *tuple1 = (IndexTuple *)(page1->GetRowData(itemId1));
            IndexTuple *tuple2 = (IndexTuple *)(page2->GetRowData(itemId2));
            EXPECT_EQ(tuple1->GetSize(), tuple2->GetSize());
            int ret = memcmp(tuple1, tuple2, tuple1->GetSize());
            EXPECT_EQ(ret, 0);
        }
    }

    void CompareLinkAndStatus(BtrPageLinkAndStatus* linkStat1, BtrPageLinkAndStatus* linkStat2)
    {
        EXPECT_EQ(linkStat1->prev, linkStat2->prev);
        EXPECT_EQ(linkStat1->next, linkStat2->next);
        EXPECT_EQ(linkStat1->level, linkStat2->level);
        EXPECT_EQ(linkStat1->btrMetaPageId, linkStat2->btrMetaPageId);
        EXPECT_EQ(linkStat1->status.bitVal.type, linkStat2->status.bitVal.type);
        EXPECT_EQ(linkStat1->status.bitVal.isRoot, linkStat2->status.bitVal.isRoot);
        EXPECT_EQ(linkStat1->status.bitVal.liveStat, linkStat2->status.bitVal.liveStat);
        EXPECT_EQ(linkStat1->status.bitVal.splitStat, linkStat2->status.bitVal.splitStat);
    }

    /* detail compare */
    void CompareTwoPageMembers(BtrPage *page1, BtrPage *page2, bool checkPlsn = false)
    {
        /* header */
        ComparePageHeader(page1, page2, checkPlsn);
        /* TDs */
        CompareTDs(page1, page2);
        /* itemid and tuple */
        CompareItems(page1, page2);
        /* links and status */
        CompareLinkAndStatus(page1->GetLinkAndStatus(), page2->GetLinkAndStatus());
    }

    void CompareBtrMeta(BtrPage *metaPage1, BtrPage *metaPage2)
    {
        /* header */
        ComparePageHeader(metaPage1, metaPage2, false);
        /* root pointers */
        BtrMeta *meta1 = (BtrMeta *)metaPage1->GetData();
        BtrMeta *meta2 = (BtrMeta *)metaPage2->GetData();
        EXPECT_EQ(meta1->GetRootPageId(), meta2->GetRootPageId());
        EXPECT_EQ(meta1->GetLowestSinglePage(), meta2->GetLowestSinglePage());
        EXPECT_EQ(meta1->GetRootLevel(), meta2->GetRootLevel());
        EXPECT_EQ(meta1->GetLowestSinglePageLevel(), meta2->GetLowestSinglePageLevel());
        /* attributes info */
        EXPECT_EQ(meta1->GetNatts(), meta2->GetNatts());
        EXPECT_EQ(meta1->GetNkeyatts(), meta2->GetNkeyatts());
        for (int i = 0; i < meta1->GetNatts(); i++) {
            EXPECT_EQ(meta1->GetIndoption(i), meta2->GetIndoption(i));
        }
    }

    std::priority_queue<BtrPage*, std::vector<BtrPage*>, BtrPageCmp> GetBtreeAllPages(PageId metaPageId)
    {
        std::priority_queue<BtrPage*, std::vector<BtrPage*>, BtrPageCmp> btreePages;
        if (metaPageId == INVALID_PAGE_ID) {
            return btreePages;
        }
        BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
        BufferDesc *btrMetaBuf = bufMgr->Read(g_defaultPdbId, metaPageId, DSTORE::LW_SHARED);
        BtrPage *btrMetaPage = (BtrPage*)btrMetaBuf->GetPage();
        PageId btrRootPageId = ((BtrMeta *)btrMetaPage->GetData())->GetLowestSinglePage();
        BtrDeepSearch(btrRootPageId, btreePages);
        bufMgr->UnlockAndRelease(btrMetaBuf);
        return btreePages;
    }

    void BtrDeepSearch(PageId curPageId, std::priority_queue<BtrPage*, std::vector<BtrPage*>, BtrPageCmp> &pageCollector)
    {
        if (curPageId == INVALID_PAGE_ID) {
            return;
        }
        BufferDesc *bufDesc = g_storageInstance->GetBufferMgr()->
            Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *curPage = (BtrPage*)bufDesc->GetPage();
        pageCollector.push(curPage);
        if (!curPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
            for (OffsetNumber n = curPage->GetLinkAndStatus()->GetFirstDataOffset(); n <= curPage->GetMaxOffset(); n++) {
                IndexTuple *tuple = (IndexTuple*)curPage->GetRowData(n);
                BtrDeepSearch(tuple->GetLowlevelIndexpageLink(), pageCollector);
            }
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufDesc);
        return;
    }

    FILE *m_waldump_fp;
};