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
 * dstore_btr_recycle_root_meta_page.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/page/dstore_btr_recycle_root_meta_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "index/dstore_btree_recycle_wal.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "wal/dstore_wal_write_context.h"
#include "page/dstore_btr_recycle_root_meta_page.h"

namespace DSTORE {

RetStatus BtrRecycleRootMeta::GetRecyclePartitionMetaPageId(NodeId nodeId, PageId &pageId, Xid &createdXid)
{
    if (STORAGE_FUNC_FAIL(AcquireRecycleRootMetaBuf(LW_SHARED, createdXid))) {
        /* We've unlocked the metaBuf if fail */
        pageId = INVALID_PAGE_ID;
        return DSTORE_FAIL;
    }
    BtrRecycleRootMetaPage *recycleRootMetaPage = GetRecycleRootMetaPage();

    uint16 recyclePartitionId = nodeId % MAX_BTR_RECYCLE_PARTITION;
    PageId recyclePartitionMetaPageId = recycleRootMetaPage->GetRecyclePartitionMetaPageId(recyclePartitionId);

    ReleaseRecycleRootMetaBuf();
    pageId = recyclePartitionMetaPageId;
    return DSTORE_SUCC;
}

RetStatus BtrRecycleRootMeta::InitRecyclePartitionMeta(NodeId nodeId, PageId &pageId)
{
    pageId = INVALID_PAGE_ID;
    if (STORAGE_VAR_NULL(segment)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"segment\".", __FUNCTION__));
        PrintBackTrace();
        return DSTORE_FAIL;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    Xid createdXid = INVALID_XID;
    if (STORAGE_FUNC_FAIL(AcquireRecycleRootMetaBuf(LW_EXCLUSIVE, createdXid))) {
        return DSTORE_FAIL;
    }

    BtrRecycleRootMetaPage *recycleRootMetaPage = GetRecycleRootMetaPage();
    uint16 recyclePartitionId = nodeId % MAX_BTR_RECYCLE_PARTITION;
    PageId recyclePartitionMeta = recycleRootMetaPage->GetRecyclePartitionMetaPageId(recyclePartitionId);
    if (!recyclePartitionMeta.IsValid()) {
        recyclePartitionMeta = segment->GetNewPageFromUnassignedPages();
        if (unlikely(!recyclePartitionMeta.IsValid())) {
            ReleaseRecycleRootMetaBuf();
            storage_set_error(INDEX_ERROR_FAIL_CREATE_NEW_PAGE,
                              segment->GetSegmentMetaPageId().m_fileId, segment->GetSegmentMetaPageId().m_blockId);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("[%s] Failed to get new page from GetNewPageFromUnassignedPages.", __FUNCTION__));
            PrintBackTrace();
            return DSTORE_FAIL;
        }
        BufferDesc *recyclePartitionMetaBuf = GetBufMgr()->Read(this->GetPdbId(), recyclePartitionMeta, LW_EXCLUSIVE,
            BufferPoolReadFlag::CreateNewPage());
        if (unlikely(recyclePartitionMetaBuf == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Faild to read page (%hu, %u) in function"
                " InitRecyclePartitionMeta.", recyclePartitionMeta.m_fileId, recyclePartitionMeta.m_blockId));
            pageId = INVALID_PAGE_ID;
            ReleaseRecycleRootMetaBuf();
            return DSTORE_FAIL;
        }

        BtrRecyclePartitionMetaPage *partMetaPage =
            static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
        if (STORAGE_VAR_NULL(partMetaPage)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("page content in buf(%u, %hu, %u) is nullptr", GetPdbId(),
                recyclePartitionMetaBuf->GetPageId().m_fileId, recyclePartitionMetaBuf->GetPageId().m_blockId));
            PrintBackTrace();
            GetBufMgr()->UnlockAndRelease(recyclePartitionMetaBuf);
            ReleaseRecycleRootMetaBuf();
            return DSTORE_FAIL;
        }
        BtrRecyclePartitionMeta btrRecyclePartitionMeta(this->GetPdbId(), recyclePartitionMeta, recyclePartitionMetaBuf,
                                                        segment->IsTempSegment());
        bool needWal = !segment->IsTempSegment();
        if (needWal) {
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        }

        partMetaPage->InitRecyclePartitionMetaPage(recyclePartitionMeta, createdXid);
        recycleRootMetaPage->SetRecyclePartitionMeta(recyclePartitionId, recyclePartitionMeta);
        (void)GetBufMgr()->MarkDirty(recyclePartitionMetaBuf);
        (void)GetBufMgr()->MarkDirty(recycleRootMetaBuf);

        if (needWal) {
            btrRecyclePartitionMeta.GenerateWalForRecyclePartitionMetaInit();
            GenerateWalForRecycleRootMetaSetPartitionMeta(recyclePartitionId, recyclePartitionMeta);
            (void)walWriterContext->EndAtomicWal();
        }
        GetBufMgr()->UnlockAndRelease(recyclePartitionMetaBuf);
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Initializing BtreeRecyclePartition id %hu at page (%hu, %u) for datanode %u.",
                  recyclePartitionId, recyclePartitionMeta.m_fileId, recyclePartitionMeta.m_blockId, nodeId));
#endif
    ReleaseRecycleRootMetaBuf();
    pageId = recyclePartitionMeta;
    return DSTORE_SUCC;
}

BufMgrInterface *BtrRecycleRootMeta::GetBufMgr()
{
    if (bufMgr == nullptr) {
        bufMgr = (segment->IsTempSegment() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr());
    }
    return bufMgr;
}

RetStatus BtrRecycleRootMeta::AcquireRecycleRootMetaBuf(LWLockMode access, Xid &createdXid)
{
    StorageAssert((access != LW_EXCLUSIVE) || (recycleRootMetaBuf == INVALID_BUFFER_DESC));
    if (recycleRootMetaBuf == INVALID_BUFFER_DESC) {
        recycleRootMetaBuf = GetBufMgr()->Read(this->GetPdbId(), recycleRootMeta, access);
        if (unlikely(recycleRootMetaBuf == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("failed to read recycle root meta page (%hu, %u).",
                recycleRootMeta.m_fileId, recycleRootMeta.m_blockId));
            return DSTORE_FAIL;
        }
    }
    BtrRecycleRootMetaPage *page = static_cast<BtrRecycleRootMetaPage *>(recycleRootMetaBuf->GetPage());
    if (unlikely(!page->TestType(PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE))) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("page (%hu, %u) is no longer recycle root meta.",
            recycleRootMetaBuf->GetPageId().m_fileId, recycleRootMetaBuf->GetPageId().m_blockId));
        GetBufMgr()->UnlockAndRelease(recycleRootMetaBuf);
        return DSTORE_FAIL;
    }

    if (unlikely(createdXid != INVALID_XID && page->GetCreatedXid() != createdXid)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("recycle root meta page (%hu, %u) was created by xid(%d, %lu), request xid(%d, %lu)",
            recycleRootMetaBuf->GetPageId().m_fileId, recycleRootMetaBuf->GetPageId().m_blockId,
            static_cast<int32>(page->GetCreatedXid().m_zoneId), page->GetCreatedXid().m_logicSlotId,
            static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId));
        GetBufMgr()->UnlockAndRelease(recycleRootMetaBuf);
        return DSTORE_FAIL;
    }

    createdXid = page->GetCreatedXid();
    return DSTORE_SUCC;
}

void BtrRecycleRootMeta::ReleaseRecycleRootMetaBuf()
{
    assert(recycleRootMetaBuf != INVALID_BUFFER_DESC);
    GetBufMgr()->UnlockAndRelease(recycleRootMetaBuf);
    recycleRootMetaBuf = nullptr;
}

BtrRecycleRootMetaPage *BtrRecycleRootMeta::GetRecycleRootMetaPage()
{
    assert(recycleRootMetaBuf != INVALID_BUFFER_DESC);
    return static_cast<BtrRecycleRootMetaPage *>(recycleRootMetaBuf->GetPage());
}

void BtrRecycleRootMeta::GenerateWalForRecycleRootMetaSetPartitionMeta(
    uint16 recyclePartitionId, const PageId recyclePartitionMeta)
{
    if (segment->IsTempSegment()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(recycleRootMetaBuf);
    Page *metaPage = recycleRootMetaBuf->GetPage();
    uint16 redoDataSize = sizeof(WalRecordBtrRecycleRootMetaSetPartitionMeta);
    WalRecordBtrRecycleRootMetaSetPartitionMeta redoData;

    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META,
                                .size = redoDataSize,
                                .pageId = recycleRootMetaBuf->GetPageId(),
                                .preWalId = metaPage->GetWalId(),
                                .prePlsn = metaPage->GetPlsn(),
                                .preGlsn = metaPage->GetGlsn(),
                                .glsnChangedFlag = metaPage->GetWalId() != walWriterContext->GetWalId(),
                                .preVersion = recycleRootMetaBuf->GetFileVersion()};
    redoData.SetHeader(header, recyclePartitionMeta, recyclePartitionId);
    walWriterContext->PutNewWalRecord(&redoData);
}

char* BtrRecycleRootMetaPage::Dump()
{
    StringInfoData str;
    str.init();

    str.append("Page Header: \n");
    Page::DumpHeader(&str);
    str.append("\n");

    str.append("CreatedXid:(%d, %lu)\n", static_cast<int32>(GetCreatedXid().m_zoneId), GetCreatedXid().m_logicSlotId);
    int numValidPartition = 0;
    str.append("Number of valid recycle partitions: %hu \n", MAX_BTR_RECYCLE_PARTITION);
    for (uint16 i = 0; i < MAX_BTR_RECYCLE_PARTITION; i++) {
        PageId metaPageId = GetRecyclePartitionMetaPageId(i);
        if (metaPageId.IsValid()) {
            numValidPartition++;
        }
        str.append("Recycle partition meta [partition %hu]: [page id] (%hu, %u) \n",
            i, metaPageId.m_fileId, metaPageId.m_blockId);
    }
    str.append("Number of valid recycle partitions: %hu \n", numValidPartition);
    return str.data;
}
}
