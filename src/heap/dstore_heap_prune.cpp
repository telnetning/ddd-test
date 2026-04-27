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
 * dstore_heap_prune.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_prune.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "heap/dstore_heap_prune.h"
#include "buffer/dstore_buf_mgr.h"
#include "common/log/dstore_log.h"
#include "heap/dstore_heap_wal_struct.h"
#include "transaction/dstore_transaction.h"
#include "wal/dstore_wal_write_context.h"

namespace DSTORE {

TableStorageMgr *HeapPruneHandler::GetTableSmgr() const
{
    return m_isLob ? m_heapRel->lobTableSmgr : m_heapRel->tableSmgr;
}

/*
 * Prune the page and modify the FSM page if needed.
 */
void HeapPruneHandler::TryPrunePage(uint16 needSize, bool fullPrune)
{
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));

    /* Step 1: Check corrupt page here to avoid further error. */
    StorageAssert(m_bufferDesc != INVALID_BUFFER_DESC);
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    StorageAssert(page->CheckHeapPageSanity());

    /* Step 2: Check if page can be pruned. */
    if (!CheckPagePrunable(page, needSize)) {
        return;
    }

    /* for vacuum */
    if (fullPrune) {
        TDAllocContext tdContext;
        tdContext.Init(m_heapRel->m_pdbId, NeedWal());
        tdContext.Begin(m_recycleMinCsn);
        (void)page->TryReuseTdSlots<PageType::HEAP_PAGE_TYPE>(tdContext);
    }

    /* Step 3: prune itemids. */
    uint16 maxItemIdNum = page->GetMaxOffset();
    ItemIdDiff *itemIdDiff = static_cast<ItemIdDiff *>(DstorePalloc(sizeof(ItemIdDiff) * maxItemIdNum));
    if (unlikely(itemIdDiff == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) when prune page.",
            static_cast<uint32>(sizeof(ItemIdDiff) * maxItemIdNum)));
        return;
    }
    CommitSeqNo recentDeadMinCsn = INVALID_CSN;
    uint16 diffNum = TryPruneItems(page, itemIdDiff, maxItemIdNum, recentDeadMinCsn);

    /* Step 4: prune tuples. */
    page->TryCompactTuples();
    (void)m_bufMgr->MarkDirty(m_bufferDesc);
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Prune heap page (%hu, %u)",
        page->GetSelfPageId().m_fileId, page->GetSelfPageId().m_blockId));
#endif
    /* Step 5: Generate redo record. */
    GeneratePruneWal(itemIdDiff, diffNum, recentDeadMinCsn);
    DstorePfree(itemIdDiff);

    /* Step 6: Update FSM. */
    RetStatus ret = GetTableSmgr()->UpdateFSM(page->GetFsmIndex(), page->GetFreeSpaceForInsert());
    StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_HEAP, ErrMsg("UpdateFSM failed."));
}

uint16 HeapPruneHandler::ScanPrunableItems(HeapPage *page, ItemIdDiff *itemIdDiff, uint16 nItems, CommitSeqNo &minCsn)
{
    uint16 diffIdx = 0;

    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= nItems; ++offset) {
        ItemId *itemId = page->GetItemIdPtr(offset);

        /* Skip itemId that already clean. */
        if (itemId->IsUnused()) {
            continue;
        }

        /* If the item contain an open transaction, it shouldn't be pruned. */
        TdId tdId = page->GetTupleTdId(offset);
        TD *td = page->GetTd(tdId);
        if (page->TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER) &&
            td->TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS)) {
            continue;
        }

        /* Only deleted/updated tuple can be pruned. */
        if (!page->IsDiskTupleDeleted(offset)) {
            continue;
        }
        if (page->TestTupleTdStatus(offset, DETACH_TD)) {
            itemIdDiff[diffIdx].offNum = offset;
            itemIdDiff[diffIdx].newState = ITEM_ID_UNUSED;
            diffIdx++;
        } else {
            if (itemId->IsNoStorage()) {
                /* If the item had been pruned before, skip it. */
                continue;
            }
            itemIdDiff[diffIdx].offNum = offset;
            itemIdDiff[diffIdx].newState = ITEM_ID_NO_STORAGE;
            diffIdx++;
            CommitSeqNo deleteCsn =
                (page->TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER) && td->TestCsnStatus(IS_CUR_XID_CSN))
                    ? td->GetCsn()
                    : COMMITSEQNO_FIRST_NORMAL;
            minCsn = (minCsn == INVALID_CSN) ? deleteCsn : DstoreMin(minCsn, deleteCsn);
        }
    }

    return diffIdx;
}

bool HeapPruneHandler::CheckPagePrunable(HeapPage *page, uint16 needSize)
{
    /* Step 1: Bypass check for potentially prunable items. */
    CommitSeqNo recentDeadTupleMinCsn = page->GetRecentDeadTupleMinCsn();
    bool itemIdPrunable = page->HasPrunableItem() && (recentDeadTupleMinCsn < m_recycleMinCsn);
    /* Page is not prunable if there are not deleted tuples. */
    if (!page->HasPrunableTuple() && !itemIdPrunable) {
        return false;
    }

    /* Step 2: If page free space is more than factor% of max tuple space, still not prunable. */
    float maxTupSpaceSize = static_cast<float>(HeapPage::MaxPossibleTupleSpace());
    bool hasMorePotentialFreeSpace = static_cast<float>(page->GetPotentialDelSize()) >
                                     maxTupSpaceSize * m_potentialFreeSpaceFactor;
    bool hasLessFreeSpace = static_cast<float>(page->GetFreeSpaceForInsert()) < maxTupSpaceSize * m_freeSpaceFactor;
    return hasMorePotentialFreeSpace ||
           (hasLessFreeSpace && page->GetPotentialDelSize() > 0) ||
           (needSize > 0 && needSize < page->GetPotentialDelSize());
}

uint16 HeapPruneHandler::TryPruneItems(HeapPage *page, ItemIdDiff *itemIdDiff, uint16 maxDiffSize,
                                       CommitSeqNo &recentDeadMinCsn)
{
    uint16 diffCnts = ScanPrunableItems(page, itemIdDiff, maxDiffSize, recentDeadMinCsn);
    /* If there is no item prunable on page,  we only do tuple compaction. */
    if (diffCnts != 0) {
        page->PruneItems(itemIdDiff, diffCnts);
        page->SetRecentDeadTupleMinCsn(recentDeadMinCsn);
        page->SetItemPrunable(recentDeadMinCsn != INVALID_CSN);
    }
    return diffCnts;
}

void HeapPruneHandler::GeneratePruneWal(ItemIdDiff *itemIdDiff, uint16 diffNum, CommitSeqNo recentDeadMinCsn)
{
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    if (NeedWal()) {
        AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
        StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
        walContext->BeginAtomicWal(m_thrd->GetActiveTransaction()->GetCurrentXid());
        walContext->RememberPageNeedWal(m_bufferDesc);

        uint32 size = sizeof(WalRecordHeapPrune) + static_cast<uint16>(sizeof(ItemIdDiff) * diffNum);
        WalRecordHeapPrune *walData = static_cast<WalRecordHeapPrune *>(walContext->GetTempWalBuf());
        StorageReleasePanic(size > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
        "the walDataSize is (%u)", size));
        bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
        uint64 fileVersion = m_bufferDesc->GetFileVersion();
        walData->SetHeader({WAL_HEAP_PRUNE, size, m_bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
            page->GetGlsn(), glsnChangedFlag, fileVersion}, diffNum, recentDeadMinCsn);
        walData->SetData(static_cast<char *>(static_cast<void *>(itemIdDiff)), diffNum * sizeof(ItemIdDiff));
        walContext->PutNewWalRecord(walData);

        (void)walContext->EndAtomicWal();
    }
}

} /* namespace DSTORE */
