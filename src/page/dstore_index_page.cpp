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
 * dstore_index_page.cpp
 *
 * IDENTIFICATION
 *        src/page/dstore_index_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "page/dstore_index_page.h"
#include "common/log/dstore_log.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "catalog/dstore_typecache.h"
#include "page/dstore_itemptr.h"
#include "errorcode/dstore_index_error_code.h"
#include "errorcode/dstore_tuple_error_code.h"
#include "errorcode/dstore_page_error_code.h"
#include "transaction/dstore_transaction.h"

namespace DSTORE {

static const char *STR_TD_TUPLE_STATUS[3] = {
    "is attached as new owner",
    "is attached as history owner",
    "is detached",
};

/* Only for index page dump, due to g_storageInstance is nullptr */
inline static void GetTypeCache(TypeCache &typeCache, Oid typOid)
{
    for (uint32 t = 0; t < sizeof(TYPE_CACHE_TABLE) / sizeof(TypeCache); ++t) {
        if (TYPE_CACHE_TABLE[t].type == typOid) {
            typeCache = TYPE_CACHE_TABLE[t];
            return;
        }
    }
    storage_set_error(CATALOG_ERROR_UNKOWN_ATT_ALIGN);
}

void BtrPage::InitBtrPageInner(const PageId &selfPageId)
{
    uint16 specialSize = MAXALIGN(sizeof(BtrPageLinkAndStatus));
    Page::Init(specialSize, PageType::INDEX_PAGE_TYPE, selfPageId);

    SetDataHeaderSize(RESERVED_BTR_PAGE_HEADER_SIZE);
    m_header.m_lower = DataHeaderSize();
}

void BtrPage::InitBtrPage(BufferDesc *bufDesc, const PageId &selfPageId, UNUSE_PARAM const FsmIndex &fsmIndex)
{
    BtrPage *btrPage = static_cast<BtrPage *>(bufDesc->GetPage());
    if (STORAGE_VAR_NULL(btrPage)) {
        return;
    }
    btrPage->InitBtrPageInner(selfPageId);
}

bool BtrPage::IsBtrPageValid(Page *page, Xid checkingXid)
{
    if (STORAGE_VAR_NULL(page)) {
        return false;
    }
    if (unlikely(!page->TestType(PageType::INDEX_PAGE_TYPE))) {
        return false;
    }
    BtrPage *btrPage = static_cast<BtrPage *>(page);
    uint16 btrPageType = static_cast<uint16>(btrPage->GetLinkAndStatus()->status.bitVal.type);
    if (unlikely(btrPageType <= 0 || btrPageType > static_cast<uint16>(BtrPageType::META_PAGE))) {
        return false;
    }
    return btrPage->GetBtrMetaCreateXid() == checkingXid;
}

bool BtrPage::IsBtrPageValid(Page *page, Xid checkingXid, BtrPageType checkingType, PageId btrMetaPageId)
{
    if (unlikely(!BtrPage::IsBtrPageValid(page, checkingXid))) {
        return false;
    }
    BtrPage *btrPage = static_cast<BtrPage *>(page);
    if (btrPage->GetBtrMetaPageId() != btrMetaPageId) {
        return false;
    }
    BtrPageLinkAndStatus *linkStat = btrPage->GetLinkAndStatus();
    if (!linkStat->TestType(checkingType)) {
        return false;
    }
    return true;
}

void BtrPage::Reset(const PageId &selfPageId)
{
    uint64 glsn = m_header.m_glsn;
    uint64 plsn = m_header.m_plsn;
    WalId walId = m_header.m_walId;
    BtrPage::InitBtrPageInner(selfPageId);
    m_header.m_glsn = glsn;
    m_header.m_plsn = plsn;
    m_header.m_walId = walId;
}

void BtrPage::InitNewPageForBuild(const PageId &btrMetaPageId, Xid createdXid, uint32 level, bool isRoot, uint8 tdCount)
{
    /* Initialize page meta */
    GetLinkAndStatus()->InitPageMeta(btrMetaPageId, level, isRoot);
    SetBtrMetaCreateXid(createdXid);

    /* Alloc td space only for leaf pages */
    if (tdCount > 0) {
        AllocateTdSpace(tdCount);
    }

    /* Make the P_HIKEY line pointer appear allocated */
    m_header.m_lower += sizeof(ItemId);
}

uint8 BtrPage::AllocTd(TDAllocContext &context)
{
    /* pivot page do not need td slot */
    if (unlikely(!GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE))) {
        return INVALID_TD_SLOT;
    }
    return DataPage::AllocTd<PageType::INDEX_PAGE_TYPE>(context);
}

OffsetNumber BtrPage::AddTuple(IndexTuple *tuple, OffsetNumber offset, uint8 tdID)
{
    tuple->SetTdId(tdID);
    if (tdID == INVALID_TD_SLOT) {
        tuple->SetTdStatus(DETACH_TD);
    }
    return AddTupleData(tuple, offset);
}

/* Add a data item to a particular page during split. */
OffsetNumber BtrPage::AddTupleWhenSplit(const ItemId *itemId, const IndexTuple *tuple, OffsetNumber offset)
{
    IndexTuple minusInfTuple;
    if (unlikely(GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE) &&
        offset == GetLinkAndStatus()->GetFirstDataOffset())) {
        /* The first data item on an internal page should have a key value of "minus infinity" that's reliably less
         * than any real key value that could appear in the left page. */
        minusInfTuple = *tuple;
        tuple = IndexTuple::CreateMinusinfPivotTuple(&minusInfTuple);
        if (unlikely(tuple == nullptr)) {
            return INVALID_ITEM_OFFSET_NUMBER;
        }
    }
    OffsetNumber insertOffset = AddTupleData(tuple, offset);
    if (itemId != nullptr && unlikely(itemId->IsRangePlaceholder())) {
        GetItemIdPtr(insertOffset)->MarkUnreadableAndRangeholder();
    }
    return insertOffset;
}

RetStatus BtrPage::CopyItemsFromSplitPage(OffsetNumber &targetOff, BtrPage *newPage, const IndexTuple *insertTuple,
                                          bool isLeft, SplitContext &splitContext, bool isSameWithLastLeft)
{
    OffsetNumber maxOffOnSplit = GetMaxOffset();
    bool insertOnCurrPage = (isLeft == splitContext.insertOnLeft);

    /*
     * Before insert new Item:
     *      Total number of items = GetMaxOffset() - GetLinkAndStatus()->GetFirstDataOffset() + 1;
     *      Number of items on left = firstRightOff - GetLinkAndStatus()->GetFirstDataOffset();
     *      Number of items on right = Total number of items - Number of items on left
                                     = GetMaxOffset() - firstRightOff + 1;
     */
    uint16 numItemsOnPage;
    if (isLeft) {
        numItemsOnPage = static_cast<uint16>(splitContext.firstRightOff - GetLinkAndStatus()->GetFirstDataOffset());
    } else {
        numItemsOnPage = static_cast<uint16>(maxOffOnSplit - splitContext.firstRightOff) + 1U;
    }

    OffsetNumber minNewPageOff = newPage->GetLinkAndStatus()->GetFirstDataOffset();
    OffsetNumber maxNewPageOff =
        OffsetNumberPrev(minNewPageOff + numItemsOnPage) + static_cast<uint16>(insertOnCurrPage);
    for (OffsetNumber off = minNewPageOff; off <= maxNewPageOff; off = OffsetNumberNext(off)) {
        const IndexTuple *currIndexTuple = nullptr;
        ItemId *currItemId = nullptr;
        if (unlikely(insertOnCurrPage && targetOff == splitContext.insertOff)) {
            insertOnCurrPage = false;
            currIndexTuple = insertTuple;
            targetOff = OffsetNumberPrev(targetOff);
            /* Convert insert offset from original page's offset to split new page's offset */
            splitContext.insertOff = off;
        } else {
            StorageAssert(targetOff <= maxOffOnSplit);
            currItemId = GetItemIdPtr(targetOff);
            currIndexTuple = GetIndexTuple(targetOff);
        }

        OffsetNumber insertedOff = newPage->AddTupleWhenSplit(currItemId, currIndexTuple, off);
        if (unlikely(insertedOff != off)) {
            /* Outer caller should set error code */
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to insert left when split btrPage(%hu, %u), supposed to insert %hu but inserted %hu",
                GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, off, insertedOff));
            return DSTORE_FAIL;
        }
        targetOff = OffsetNumberNext(targetOff);
    }
    if (!isLeft && isSameWithLastLeft) {
        IndexTuple *firstRight = newPage->GetIndexTuple(minNewPageOff);
        firstRight->SetSameWithLastLeft();
    }
    StorageAssert(newPage->CheckSanity());
    return DSTORE_SUCC;
}

/*
 * Replace a specified tuple on an index page.
 *
 * The new tuple is placed exactly where the old one had been, shifting
 * other tuples' data up or down as needed to keep the page compacted.
 * This is better than deleting and reinserting the tuple, because it
 * avoids any data shifting when the tuple size doesn't change; and
 * even when it does, we avoid moving the line pointers around.
 * This could be used by an index AM that doesn't want to unset the
 * LP_DEAD bit when it happens to be set.  It could conceivably also be
 * used by an index AM that cares about the physical order of tuples as
 * well as their logical/ItemId order.
 *
 * If there's insufficient space for the new tuple, return false.  Other
 * errors represent data-corruption problems, so we just elog.
 */
RetStatus BtrPage::OverwriteTuple(OffsetNumber offnum, IndexTuple *newtup)
{
    ItemId *tupid;
    OffsetNumber itemcount;
    uint16 oldsize;
    OffsetNumber offset;
    int16 sizeDiff;
    errno_t rc;
    uint32 newsize = newtup->GetSize();
    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    if (m_header.m_lower < sizeof(PageHeader) ||
        m_header.m_lower > m_header.m_upper ||
        m_header.m_upper > m_header.m_special.m_offset ||
        m_header.m_special.m_offset > BLCKSZ ||
        m_header.m_special.m_offset != MAXALIGN(m_header.m_special.m_offset)) {
        storage_set_error(TUPLE_ERROR_CORRUPTED_PAGE_POINTERS,
                          m_header.m_lower, m_header.m_upper, m_header.m_special.m_offset);
        return DSTORE_FAIL;
    }

    itemcount = GetMaxOffset();
    if (offnum == 0 || offnum > itemcount) {
        storage_set_error(PAGE_ERROR_INVALID_INDEX_OFFNUM, offnum);
        return DSTORE_FAIL;
    }

    tupid = GetItemIdPtr(offnum);
    StorageAssert(tupid->HasStorage());
    oldsize = tupid->GetLen();
    offset = tupid->GetOffset();
    if (offset < m_header.m_upper || (offset + oldsize) > m_header.m_special.m_offset) {
        storage_set_error(TUPLE_ERROR_CORRUPTED_LINE_POINTER, offset, oldsize);
        return DSTORE_FAIL;
    }

    /*
     * Determine actual change in space requirement, check for page overflow.
     */
    if (newsize > static_cast<uint32>(oldsize + (m_header.m_upper - m_header.m_lower))) {
        storage_set_error(INDEX_ERROR_OVERWRITE_HIGHKEY);
        return DSTORE_FAIL;
    }

    /*
     * Relocate existing data and update line pointers, unless the new tuple
     * is the same size as the old (after alignment), in which case there's
     * nothing to do.  Notice that what we have to relocate is data before the
     * target tuple, not data after, so it's convenient to express sizeDiff
     * as the amount by which the tuple's size is decreasing, making it the
     * delta to add to m_upper and affected line pointers.
     */
    sizeDiff = static_cast<int16>(oldsize - static_cast<uint16>(newsize));
    if (sizeDiff != 0) {
        char *addr = PageHeaderPtr() + m_header.m_upper;

        /* relocate all tuple data before the target tuple */
        uint16 moveSize = static_cast<uint16>(offset - m_header.m_upper);
        if (moveSize != 0) {
            rc = memmove_s(addr + sizeDiff, moveSize, addr, moveSize);
            storage_securec_check(rc, "\0", "\0");
        }

        /* adjust free space boundary pointer */
        StorageAssert(m_header.m_upper < UINT16_MAX - sizeDiff);
        m_header.m_upper = static_cast<uint16>(static_cast<int16>(m_header.m_upper) + sizeDiff);

        /* adjust affected line pointers too */
        for (uint16 i = FIRST_ITEM_OFFSET_NUMBER; i <= itemcount; i++) {
            ItemId *ii = GetItemIdPtr(i);

            /* Allow items without storage; currently only BRIN needs that */
            if (ii->HasStorage() && ii->GetOffset() <= offset) {
                ii->SetOffset(static_cast<uint16>(ii->GetOffset() + sizeDiff));
            }
        }
    }

    /* Update the item's tuple length without changing its lp_flags field */
    tupid->SetOffset(static_cast<uint16>(offset + sizeDiff));
    tupid->SetLen(newsize);

    /* Copy new tuple data onto page */
    rc = memmove_s(GetRowData(tupid), newsize, static_cast<void *>(newtup), newsize);
    storage_securec_check(rc, "\0", "\0");

    StorageAssert(CheckSanity());

    return DSTORE_SUCC;
}

void BtrPage::InitMemLeftForSplit(BtrPage *splitPage)
{
    StorageAssert(splitPage != nullptr);

    /* Note: InitMemLeftForSplit is called by temp BtrPage, the pageHeader values are random */
    /* Page::Init will retain the old lsn which is random. So we memset header 0 first. */
    errno_t ret = memset_sp(PageHeaderPtr(), BLCKSZ, 0, BLCKSZ);
    storage_securec_check(ret, "", "");
    /* Zero the page and set up standard page header info */
    InitBtrPageInner(splitPage->GetSelfPageId());
    SetLsn(splitPage->GetWalId(), splitPage->GetPlsn(), splitPage->GetGlsn());
    SetBtrMetaCreateXid(splitPage->GetBtrMetaCreateXid());

    /* Alloc td space only for leaf pages */
    if (splitPage->GetTdCount() > 0) {
        AllocateTdSpace(splitPage->GetTdCount());
    }

    /* Copy necessary page info to temporary left page */
    BtrPageLinkAndStatus *splitLinkStat = splitPage->GetLinkAndStatus();
    BtrPageLinkAndStatus *leftLinkStat = GetLinkAndStatus();
    leftLinkStat->InitPageMeta(splitPage->GetBtrMetaPageId(), splitLinkStat->GetLevel(), splitLinkStat->IsRoot());
    leftLinkStat->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
    leftLinkStat->SetLiveStatus(static_cast<BtrPageLiveStatus>(splitLinkStat->GetLiveStatus()));
    leftLinkStat->SetLeft(splitLinkStat->GetLeft());

    /* Copy original TD slots if splitting target is leaf */
    if (splitLinkStat->TestType(BtrPageType::LEAF_PAGE)) {
        /* Copy original TD slots from splitting target page */
        StorageAssert(splitPage->GetTdCount() > 0);
        errno_t rc = memcpy_s(GetTd(0), sizeof(TD) * GetTdCount(),
                              splitPage->GetTd(0), sizeof(TD) * splitPage->GetTdCount());
        storage_securec_check(rc, "\0", "\0");
    }

    StorageAssert(CheckSanity());
}

void BtrPage::InitNewRightForSplit(BtrPage *splitPage, BtrPage *leftPage)
{
    if (STORAGE_VAR_NULL(splitPage)) {
        ErrLog(DSTORE_PANIC, MODULE_PAGE, ErrMsg("SplitPage is nullptr."));
    }
    if (STORAGE_VAR_NULL(leftPage)) {
        ErrLog(DSTORE_PANIC, MODULE_PAGE, ErrMsg("LeftPage is nullptr."));
    }
    BtrPageLinkAndStatus *splitLinkStat = splitPage->GetLinkAndStatus();
    BtrPageLinkAndStatus *rightLinkStat = GetLinkAndStatus();
    /* Initialize right page and set links */
    SetTuplePrunable(splitPage->HasPrunableTuple());
    SetBtrMetaCreateXid(splitPage->GetBtrMetaCreateXid());
    rightLinkStat->InitPageMeta(splitPage->GetBtrMetaPageId(), splitLinkStat->GetLevel(), false);
    rightLinkStat->SetLeft(splitPage->GetSelfPageId());
    rightLinkStat->SetRight(splitLinkStat->GetRight());
    /* Also update left page's right link to point to new right page */
    leftPage->GetLinkAndStatus()->SetRight(GetSelfPageId());

    /* For leaf page, copy TD slots from temporary left page that contains the new allocated TD slot */
    if (rightLinkStat->TestType(BtrPageType::LEAF_PAGE)) {
        StorageAssert(leftPage->GetTdCount() > 0);
        errno_t rc = memcpy_s(GetTd(0), sizeof(TD) * GetTdCount(),
                              leftPage->GetTd(0), sizeof(TD) * leftPage->GetTdCount());
        storage_securec_check(rc, "\0", "\0");
    }

    StorageAssert(CheckSanity());
}

/*
 * Get Btree index info from meta page.
 * We only fill mandatory params for tuple comparing.
 */
IndexInfo *BtrPage::GetIndexInfoFromMetaPage(BufferDesc *bufDesc)
{
    char *data = static_cast<BtrPage *>(bufDesc->GetPage())->GetData();
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(data));
    TupleDesc attr = btrMeta->ConstructTupleDesc();
    if (STORAGE_VAR_NULL(attr)) {
        return nullptr;
    }

    uint16 nAttr = btrMeta->GetNatts();
    uint16 nKeyAttr = btrMeta->GetNkeyatts();
    Size size = sizeof(IndexInfo) + sizeof(int16) * nKeyAttr + sizeof(Oid) * nKeyAttr;
    IndexInfo *info = static_cast<IndexInfo *>(DstorePalloc0(size));
    if (STORAGE_VAR_NULL(info)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", size));
        DstorePfreeExt(attr);
        return nullptr;
    }
    info->attributes = attr;
    info->indexAttrsNum = nAttr;
    info->indexKeyAttrsNum = nKeyAttr;
    info->relKind = btrMeta->GetRelKind();
    info->tableOidAtt = btrMeta->GetTableOidAtt();

    char *infoPtr = static_cast<char *>(static_cast<void *>(info));
    info->indexOption = static_cast<int16 *>(static_cast<void *>(infoPtr + sizeof(IndexInfo)));
    errno_t rc = memcpy_s(info->indexOption, nKeyAttr * sizeof(int16), btrMeta->indexOption, nKeyAttr * sizeof(int16));
    storage_securec_check(rc, "\0", "\0");
    info->opcinType = static_cast<Oid *>(static_cast<void *>(infoPtr + sizeof(IndexInfo) + sizeof(int16) * nKeyAttr));
    rc = memcpy_s(info->opcinType, nKeyAttr * sizeof(Oid), btrMeta->opcinTypes, nKeyAttr * sizeof(Oid));
    storage_securec_check(rc, "\0", "\0");
#ifndef UT
#ifndef DSTORE_TEST_TOOL
    int16_t numSupportProc = btrMeta->GetNumSupportProc();
    if (numSupportProc != 0) {
        info->m_indexSupportProcInfo = static_cast<IndexSupportProcInfo *>(DstorePalloc0(sizeof(IndexSupportProcInfo)));
        if (STORAGE_VAR_NULL(info->m_indexSupportProcInfo)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(IndexSupportProcInfo)));
            DstorePfreeExt(info);
            DstorePfreeExt(attr);
            return nullptr;
        }
        info->m_indexSupportProcInfo->numKeyAtts = nKeyAttr;
        info->m_indexSupportProcInfo->numSupportProc = numSupportProc;
        info->m_indexSupportProcInfo->supportProcs =
            static_cast<Oid *>(DstorePalloc0(sizeof(Oid) * numSupportProc * nKeyAttr));
        if (STORAGE_VAR_NULL(info->m_indexSupportProcInfo->supportProcs)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("Failed to alloc size:%lu", sizeof(Oid) * numSupportProc * nKeyAttr));
            DstorePfreeExt(info->m_indexSupportProcInfo);
            DstorePfreeExt(info);
            DstorePfreeExt(attr);
            return nullptr;
        }
        rc = memcpy_s(info->m_indexSupportProcInfo->supportProcs, numSupportProc * nKeyAttr * sizeof(Oid),
            btrMeta->functionOids, numSupportProc * nKeyAttr * sizeof(Oid));
        storage_securec_check(rc, "\0", "\0");
    } else {
        info->m_indexSupportProcInfo = nullptr;
    }
#endif
#endif

    return info;
}

/*
 * Find correct offset of the tuple in this leaf page.
 */
OffsetNumber BtrPage::BinarySearch(IndexTuple *tuple, IndexInfo *indexInfo, bool *isEqual)
{
    StorageAssert(GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));
    StorageAssert(isEqual != nullptr);
    *isEqual = false;

    BtrPageLinkAndStatus *btrPageMeta = GetLinkAndStatus();
    OffsetNumber low = btrPageMeta->GetFirstDataOffset();
    OffsetNumber high = GetMaxOffset();
    /*
     * Here we can make sure that the tuple belongs to this page,
     * so the page must be not empty.
     */
    if (unlikely(high < low)) {
        return low;
    }

    high++;                     /* establish the loop invariant for high */

    /*
     * Maybe there are more than one tuple having same key and same heap ctid in the page,
     * so we should find the first matched tuple which is fresh.
     */
    int cmpval = 1;

    while (high > low) {
        OffsetNumber mid = low + static_cast<uint16>((high - low) / 2);
        IndexTuple *itup = GetIndexTuple(mid);

        /* We have low <= mid < high, so mid points at a real slot */
        int result = IndexTuple::Compare(tuple, itup, indexInfo);
        if (result >= cmpval) {
            low = mid + 1;
            continue;
        }
        if (result == 0) {
            /* We've found the equal value on current page */
            *isEqual = true;
        }
        high = mid;
    }
    return low;
}

RetStatus BtrPage::RemoveItemId(OffsetNumber offset)
{
    OffsetNumber maxOff = GetMaxOffset();
    if (unlikely(offset == INVALID_ITEM_OFFSET_NUMBER || offset > maxOff)) {
        storage_set_error(PAGE_ERROR_INSERT_UNDO_NOT_FOUND);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to remove %hu, %hu on page", offset, maxOff));
        return DSTORE_FAIL;
    }

    ItemId *item = GetItemIdPtr(offset);
    if (offset < maxOff) {
        ItemId *nextItem = GetItemIdPtr(OffsetNumberNext(offset));
        uint32 size = static_cast<uint32>((maxOff - offset) * static_cast<uint16>(sizeof(ItemId)));
        errno_t rc = memmove_s(item, size, nextItem, size);
        storage_securec_check(rc, "\0", "\0");
    }
    GetItemIdPtr(maxOff)->SetUnused();
    RemoveLastItem();
    return DSTORE_SUCC;
}

/*
 * Execute undo for index insert.
 *
 * When undoRec page id is matched with current page id:
 * 1. If the page doesn't split, or if the page splited when it isn't the
 *    rightest page, the offset is the correct position.
 * 2. If the page splited when it is the the rightest page, all items were moved
 *    backward one item for adding highkey, so correct position is offset + 1.
 * 3. If the page splited when it isn't the rightest page, but due to prune
 *    it became the rightest page, so all items were moved ahead one item,
 *    so correct position is offset - 1.
 * 4. If page is pruned, the offset is shuffled, we must use binary search to
 *    find correct position.
 *
 * When not matched:
 * 5. the undoRec must be generated by a left spilted page, we should use
 *    binary search to find correct position.
 *
 * As above, we use binary search in all cases.
 */
RetStatus BtrPage::UndoBtreeInsert(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage)
{
    /* Search the target offset */
#ifdef DSTORE_USE_ASSERT_CHECKING
    IndexTuple *tuple = btrUndoContext->m_undoTuple;
#endif
    OffsetNumber offset = btrUndoContext->m_offset;
    StorageAssert(IndexTuple::Compare(tuple, GetIndexTuple(offset), btrUndoContext->m_indexInfo) == 0);

    /* TO_BE_FIXED: set itemid unused instead of remove directly */
    if (offset == GetLinkAndStatus()->GetFirstDataOffset() && !btrUndoContext->m_undoWithNoWal) {
        ItemId *undoTupleId = GetItemIdPtr(offset);
        undoTupleId->MarkUnreadableAndRangeholder();
        IndexTuple *tupleOnPage = GetIndexTuple(undoTupleId);
        tupleOnPage->SetTdStatus(TupleTdStatus::DETACH_TD);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("[UNDO_BTREE_INSERT]set ({%hu, %u}, %hu) rangeholder, heap ctid = ({%hu, %u}, %hu)",
            GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, offset, tuple->GetHeapCtid().GetPageId().m_fileId,
            tuple->GetHeapCtid().GetPageId().m_blockId, tuple->GetHeapCtid().GetOffset()));
#endif
    } else if (STORAGE_FUNC_FAIL(RemoveItemId(offset))) {
        return DSTORE_FAIL;
    }
    SetTuplePrunable(true);

    /* rollback current td to the old one whose info recorded in undo. */
    TD *td = (tdOnPage == nullptr) ? GetTd(undoRec->GetTdId()) : tdOnPage;
    td->RollbackTdInfo(undoRec);
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Index insert rollback in ctid = ({%hu, %u}, %hu), heap ctid = ({%hu, %u}, %hu)",
               GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, offset, tuple->GetHeapCtid().GetPageId().m_fileId,
               tuple->GetHeapCtid().GetPageId().m_blockId, tuple->GetHeapCtid().GetOffset()));
#endif
    return DSTORE_SUCC;
}

RetStatus BtrPage::PruneNonItemIdTuples(uint32 &spacePruned, uint32 spaceNeeded, bool needRecycleTd)
{
    OffsetNumber maxOff = GetMaxOffset();
    OffsetNumber minOff = BTREE_PAGE_HIKEY;
    OffsetNumber firstDataOff = GetLinkAndStatus()->GetFirstDataOffset();
    ItemId *curItem = GetItemIdPtr(minOff);
    uint16 upper = GetSpecialOffset();
    uint32 origFreeSpace = GetFreeSpace<FreeSpaceCondition::RAW>();
    uint32 freeSpace = BLCKSZ - sizeof(BtrPageLinkAndStatus);
    uint8 recycleTdCnt = 0;

    /* Step 1: Collect alive Item IDs */
    ItemId **sortItemIds = static_cast<ItemId **>(DstorePalloc(sizeof(ItemId *) * maxOff));
    if (unlikely(sortItemIds == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when PruneNonItemIdTuples."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    uint16 numOfValidItems = 0;
    for (OffsetNumber offset = minOff; offset <= maxOff; offset++) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsNormal() || offset == firstDataOff) {
            *curItem = *itemId;
            sortItemIds[numOfValidItems++] = curItem;
            if (likely(offset < maxOff)) {
                curItem++;
            }
            freeSpace -= itemId->GetLen();
        } else {
            m_header.m_lower -= sizeof(ItemId);
        }
    }
    freeSpace -= GetLower(); /* This is the new Lower that we've just re-calculated */
    if (unlikely(freeSpace < spaceNeeded)) {
        if (unlikely(!needRecycleTd)) {
            DstorePfreeExt(sortItemIds);
            return DSTORE_FAIL;
        }
        /* Step 2. Still lack of space, try recycle td space
         * If xid == INVALID_XID, the td is newly allocated or frozen. Transaction is not able to
         * visited it during CR page construction, it is safe to be recycled. */
        for (int tdId = GetTdCount() - 1; tdId >= 0; tdId--) {
            TD *td = GetTd(static_cast<TdId>(tdId));
            if (td->GetXid() != INVALID_XID) {
                break;
            }
            StorageAssert(td->GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR);
            recycleTdCnt++;
            freeSpace += sizeof(TD);
        }
        if (unlikely(freeSpace < spaceNeeded)) {
            DstorePfreeExt(sortItemIds);
            return DSTORE_FAIL;
        }
    }

    spacePruned = freeSpace - origFreeSpace;
    /* Step 3. Sort Item IDs by tuple offset then compact page by removing rollbacked-tuple-holes */
    qsort(sortItemIds, numOfValidItems, sizeof(ItemId *), ItemId::DescendingSortByOffsetCompare);
    for (uint16 i = 0; i < numOfValidItems; i++) {
        ItemId *item = sortItemIds[i];
        upper -= item->GetLen();
        if (item->GetOffset() != upper) {
            errno_t rc = memmove_s(static_cast<char *>(static_cast<void *>(this)) + upper, item->GetLen(),
                static_cast<char *>(static_cast<void *>(this)) + item->GetOffset(), item->GetLen());
            storage_securec_check(rc, "\0", "\0");
            item->SetOffset(upper);
        }
    }
    DstorePfree(sortItemIds);
    if (unlikely(recycleTdCnt > 0)) {
        RecycleTd(recycleTdCnt);
    }
    if (upper != GetUpper()) {
        SetUpper(upper);
    }
    StorageAssert(CheckSanity());

    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Pruned page when undo. %d tds pruned, saved space %u."
           BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, recycleTdCnt, spacePruned,
           BTR_PAGE_HEADER_VAL(this), BTR_PAGE_LINK_AND_STATUS_VAL(GetLinkAndStatus())));
    return DSTORE_SUCC;
}

OffsetNumber BtrPage::UndoInsertPrunedTupleBack(IndexTuple *tuple, uint32 tupleSize,
    bool needRecycleTd, IndexInfo *indexInfo)
{
    uint32 spaceNeeded = tupleSize + sizeof(ItemId);
    if (spaceNeeded > GetFreeSpace<FreeSpaceCondition::RAW>()) {
        uint32 spacePruned = 0U;
        if (unlikely(PruneNonItemIdTuples(spacePruned, spaceNeeded, needRecycleTd) == DSTORE_FAIL) ||
                        spacePruned == 0U) {
            ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("PruneNonItemIdTuples fail when execute undo for delete."
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    BTR_PAGE_HEADER_VAL(this), BTR_PAGE_LINK_AND_STATUS_VAL(GetLinkAndStatus())));
        }
        if (spaceNeeded > GetFreeSpace<FreeSpaceCondition::RAW>()) {
            StorageReleasePanic(!needRecycleTd, MODULE_INDEX, ErrMsg("No space for rollback index deletion on"
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, BTR_PAGE_HEADER_VAL(this),
                    BTR_PAGE_LINK_AND_STATUS_VAL(GetLinkAndStatus())));
            ExtendCrPage();
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                    ErrMsg("CrPage is full and can not do UndoBtreeDelete, Extend the CrPage."
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    BTR_PAGE_HEADER_VAL(this), BTR_PAGE_LINK_AND_STATUS_VAL(GetLinkAndStatus())));
        }
    }

    UNUSE_PARAM bool isEqual;
    /* Find the inserting offset of the pruned tuple. */
    OffsetNumber offset = BinarySearch(tuple, indexInfo, &isEqual);

    uint16 pageSize = PAGE_SIZE;
    if (unlikely(GetIsCrExtend())) {
        pageSize = EXTEND_PAGE_SIZE;
    }
    offset = AddTupleData(tuple, offset, pageSize);
    StorageAssert(offset != INVALID_ITEM_OFFSET_NUMBER);
    return offset;
}

/*
 * Execute undo for index delete.
 *
 * Same as BtrPage::UndoBtreeInsert.
 * When undoRec page id is matched with current page id:
 * 1. If the page doesn't split, or if the page splited when it isn't the
 *    rightest page, the offset is the correct position.
 * 2. If the page splited when it is the the rightest page, all items were moved
 *    backward one item for adding highkey, so correct position is offset + 1.
 * 3. If the page splited when it isn't the rightest page, but due to prune
 *    it became the rightest page, so all items were moved ahead one item,
 *    so correct position is offset - 1.
 * 4. If page is pruned, the offset is shuffled, we must use binary search to
 *    find correct position.
 *
 * When not matched:
 * 5. the undoRec must be generated by a left spilted page, we should use
 *    binary search to find correct position.
 *
 * As above, we use binary search in all cases.
 */
RetStatus BtrPage::UndoBtreeDelete(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage)
{
    StorageAssert(undoRec == btrUndoContext->m_undoRec);
    IndexTuple *tuple = btrUndoContext->m_undoTuple;

    /* Search the target offset */
    UNUSE_PARAM OffsetNumber maxOff = GetMaxOffset();
    OffsetNumber offset = btrUndoContext->m_offset;

    bool isTdFound = true;
    IndexTuple *tupleOnPage = nullptr;
    if (!btrUndoContext->m_isDeletionPruned) {
        /* Case 1. The deleted tuple is still on the page */
        ItemId *itemId = GetItemIdPtr(offset);
        tupleOnPage = GetIndexTuple(itemId);
        StorageReleasePanic((!itemId->IsNormal() || tupleOnPage->GetHeapCtid() != tuple->GetHeapCtid()), MODULE_INDEX,
            ErrMsg("Failed to undoBtreeDelete indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu), "
                   "original BtrPage in UndoRec was indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu)",
            GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, offset, tupleOnPage->GetHeapCtid().GetFileId(),
            tupleOnPage->GetHeapCtid().GetBlockNum(), tupleOnPage->GetHeapCtid().GetOffset(),
            undoRec->GetCtid().GetFileId(), undoRec->GetCtid().GetBlockNum(), undoRec->GetCtid().GetOffset(),
            tuple->GetHeapCtid().GetFileId(), tuple->GetHeapCtid().GetBlockNum(), tuple->GetHeapCtid().GetOffset()));
        TdId tdId = tuple->GetTdId();
        tupleOnPage->SetTdId(tdId < GetTdCount() ? tdId : INVALID_TD_SLOT);
        tupleOnPage->SetNotDeleted();
        tupleOnPage->SetCcindexStatus(tuple->GetCcindexStatus());
    } else {
        /* Case 2. for CRCunstruction and Rollback to consistent point when recovery,
         * the index tuple might have been pruned, need to insert it back */
        bool forRecovery = g_storageInstance->IsInBackupRestore(btrUndoContext->m_pdbId);
        offset = UndoInsertPrunedTupleBack(tuple, tuple->GetSize(), !forRecovery, btrUndoContext->m_indexInfo);
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Index delete rollback indexTuple(%hu, %u, %hu) failed, actual offset: %hu, "
                       "heapCtid(%hu, %u, %hu)",
                GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, btrUndoContext->m_offset, offset,
                tuple->GetHeapCtid().GetFileId(), tuple->GetHeapCtid().GetBlockNum(),
                tuple->GetHeapCtid().GetOffset()));
            /* compare function failed comparing */
            return DSTORE_FAIL;
        }
        tupleOnPage = GetIndexTuple(offset);
        isTdFound = (GetTd(btrUndoContext->m_undoRec->GetTdId())->GetXid() == btrUndoContext->m_xid);
        btrUndoContext->m_offset = offset;
    }

    /*
    * The old TD may have been reused, even though it was not reused when generating undo record.
    * So we set tdId ATTACH_TD_AS_HISTORY_OWNER for consistency even if the TD is not reused in fact.
    */
    if (unlikely(tupleOnPage->GetSize() != tuple->GetSize())) {
        Btree::DumpDamagedTuple(tupleOnPage, this, offset);
        Btree::DumpDamagedTuple(tuple);
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
            ErrMsg("[%s]tuple size on page is %u but in undoRec is %u.",
            __FUNCTION__, tupleOnPage->GetSize(), tuple->GetSize()));
    }
    /*
     * The TD linked to the tuple might have been pruned or reset after the original undo record wrote
     * the undo record. Thus we must recheck the TD's status, and unlink it from tuple if it has already been pruned
     * or frozen.
     */
    if (!isTdFound || !IsTdValidAndOccupied(tupleOnPage->GetTdId()) || btrUndoContext->m_isIns4Del) {
        tupleOnPage->SetTdStatus(DETACH_TD);
    }
    if (!tupleOnPage->TestTdStatus(DETACH_TD)) {
        tupleOnPage->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
    }

    if (isTdFound) {
        TD *td = (tdOnPage == nullptr) ? GetTd(undoRec->GetTdId()) : tdOnPage;
        td->RollbackTdInfo(undoRec);
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Index delete rollback in ctid = ({%hu, %u}, record offset: %hu, actual offset: %hu), heap ctid = "
               "({%hu, %u}, %hu)",
        GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, btrUndoContext->m_offset, offset,
        tuple->GetHeapCtid().GetPageId().m_fileId, tuple->GetHeapCtid().GetPageId().m_blockId,
        tuple->GetHeapCtid().GetOffset()));
#endif
    return DSTORE_SUCC;
}

OffsetNumber BtrPage::AddTupleData(const IndexTuple *indexTuple, OffsetNumber offset, uint16 pageSize)
{
    StorageAssert(DstoreOffsetNumberIsValid(offset));
    StorageAssert(CheckSanity());

    /* Step 1. Check is shuffle needed */
    OffsetNumber maxAfterInsert = OffsetNumberNext(GetMaxOffset());
    if (unlikely(offset > maxAfterInsert)) {
        /* It not allowed to leave a hole on page. Items must be arranged one by one */
        return INVALID_ITEM_OFFSET_NUMBER;
    }
    bool needShuffle = offset < maxAfterInsert;

    /* Step 2. Get index tuple size and check correctness */
    uint32 origSize = indexTuple->GetSize();
    StorageAssert(origSize > 0 && origSize < UINT16_MAX);
    uint16 tupleSize = static_cast<uint16>(origSize);

    /* Step 3. Check and calculate new lower and upper */
    StorageAssert((GetLower() <= (UINT16_MAX - static_cast<uint16>(sizeof(ItemId)))));
    uint16 newLower = GetLower() + static_cast<uint16>(sizeof(ItemId));
    StorageAssert(GetUpper() > tupleSize);
    uint16 newUpper = static_cast<uint16>(GetUpper() - tupleSize);
    if (unlikely(newLower > newUpper)) {
        return INVALID_ITEM_OFFSET_NUMBER;
    }

    /* Step 4. Shuffle ItemIDs if needed */
    ItemId *itemID = GetItemIdPtr(offset);
    if (needShuffle) {
        uint16 size = static_cast<uint16>((maxAfterInsert - offset) * static_cast<uint16>(sizeof(ItemId)));
        error_t rc = memmove_s(itemID + 1, size, itemID, size);
        storage_securec_check(rc, "\0", "\0");
    }

    /* Step 5. Record index tuple size in ItemID */
    /* be careful we should not set the align size as this is the real size for tuple */
    itemID->SetNormal(newUpper, tupleSize);

    /* Step 6. Update lower and upper in page header */
    SetLower(newLower);
    SetUpper(newUpper);

    /* Step 7. Copy index tuple onto page */
    error_t rc = memcpy_s(static_cast<char *>(static_cast<void *>(this)) + newUpper,
        static_cast<uint16>(pageSize - newUpper), static_cast<const void *>(indexTuple), tupleSize);
    storage_securec_check(rc, "\0", "\0");
    StorageAssert(CheckSanity());
    return offset;
}

RetStatus BtrPage::UndoBtree(UndoRecord *undoRec, BtreeUndoContext *btrUndoContext, TD *tdOnPage)
{
    RetStatus status = DSTORE_FAIL;
    UndoType undoType = undoRec->GetUndoType();
    if (undoType == UNDO_BTREE_DELETE || undoType == UNDO_BTREE_DELETE_TMP || btrUndoContext->m_isIns4Del) {
        status = UndoBtreeDelete(undoRec, btrUndoContext, tdOnPage);
    } else if (undoType == UNDO_BTREE_INSERT || undoType == UNDO_BTREE_INSERT_TMP) {
        status = UndoBtreeInsert(undoRec, btrUndoContext, tdOnPage);
    } else {
        storage_set_error(PAGE_ERROR_UNDO_TYPE_NOT_FOUND);
        return DSTORE_FAIL;
    }

    StorageAssert(CheckSanity());
    return status;
}

RetStatus BtrPage::RollbackBtrForRecovery(UndoRecord *undoRec)
{
    StorageAssert(undoRec->GetPageId() == GetSelfPageId());
    TdId tdId = undoRec->GetTdId();
    StorageAssert(tdId < GetTdCount());
    StorageAssert(CheckSanity());

    OffsetNumber offset = undoRec->GetCtid().GetOffset();
    UndoType undoType = undoRec->GetUndoType();
    bool isDelete = (undoType == UNDO_BTREE_DELETE || undoType == UNDO_BTREE_DELETE_TMP);
    bool isInsert4Del = (!isDelete && static_cast<UndoDataBtreeInsert*>(undoRec->GetUndoData())->m_ins4Del);
    /* We have a special case when the index is under concurrently building that the
     * insertion undo record is for an InsertionForDeleting case.
     * For this case, the undo tuple is UNDO_BTREE_INSERT while the deleted flag on tuple is true */
    if (!isDelete && !isInsert4Del) {
        /* rollback current td to the old one whose info recorded in undo. */
        GetTd(tdId)->RollbackTdInfo(undoRec);

        /* TO_BE_FIXED: set itemid unused instead of remove directly */
        /* For the first data tuple on page, set it range holder instead of remove it directly to keep a low key on
         * page, which is exactly the same with rollback process.
         * For other tuples, just remove the item from page for now. */
        if (offset == GetLinkAndStatus()->GetFirstDataOffset()) {
            ItemId *undoTupleId = GetItemIdPtr(offset);
            undoTupleId->MarkUnreadableAndRangeholder();
            IndexTuple *tupleOnPage = GetIndexTuple(undoTupleId);
            tupleOnPage->SetTdStatus(TupleTdStatus::DETACH_TD);
        } else if (STORAGE_FUNC_FAIL(RemoveItemId(offset))) {
            return DSTORE_FAIL;
        }
        SetTuplePrunable(true);
        StorageAssert(CheckSanity());
        return DSTORE_SUCC;
    }

    if (isDelete || isInsert4Del) {
        /* rollback current td to the old one whose info recorded in undo. */
        GetTd(tdId)->RollbackTdInfo(undoRec);

        IndexTuple *tupleUndo = GetIndexTupleFromUndoRec(undoRec);
        if (STORAGE_VAR_NULL(tupleUndo)) {
            return DSTORE_FAIL;
        }
        TdId oldTdId = tupleUndo->GetTdId();
        BtrCcidxStatus ccindexStatus = tupleUndo->GetCcindexStatus();
        uint32 tupleSize = tupleUndo->GetSize();

        StorageAssert(offset <= GetMaxOffset());
        ItemId *itemId = GetItemIdPtr(offset);
        IndexTuple *tupleOnPage = GetIndexTuple(itemId);
        ItemPointerData undoHeapCtid = tupleUndo->GetHeapCtid();
        ItemPointerData pageHeapCtid = tupleOnPage->GetHeapCtid();
        if (isInsert4Del) {
            DstorePfreeExt(tupleUndo);
        }
        if (likely(itemId->IsNormal() && tupleOnPage->GetSize() == tupleSize && undoHeapCtid == pageHeapCtid)) {
            /* We're recovering a rollback that was generated by transaction aborted/failed, meaning this deletion has
             * never been committed and the deleting tuple has gotten no chance to be pruned. Thus the tuple must be
             * still on the page with a deleted flag. */
            /* undo deleting by remove the deleted flag if the tuple is still on the page. */
            tupleOnPage->SetNotDeleted();
            tupleOnPage->SetTdId(oldTdId);
            tupleOnPage->SetCcindexStatus(ccindexStatus);
            StorageAssert(CheckSanity());

            if (!IsTdValidAndOccupied(tupleOnPage->GetTdId()) || isInsert4Del) {
                tupleOnPage->SetTdStatus(DETACH_TD);
            }
            if (!tupleOnPage->TestTdStatus(DETACH_TD)) {
                tupleOnPage->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
            }
            return DSTORE_SUCC;
        }
        storage_set_error(PAGE_ERROR_DELETE_UNDO_NOT_FOUND);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("undo delete indexTuple(%hu, %u, %hu) not found, itemFlag:%u, "
            "size:%u, heapctid(%hu, %u, %hu). tuple on page has size %u, heapctid(%hu, %u, %hu)",
            GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, offset, itemId->GetFlags(), tupleSize,
            undoHeapCtid.GetFileId(), undoHeapCtid.GetBlockNum(), undoHeapCtid.GetOffset(), tupleOnPage->GetSize(),
            pageHeapCtid.GetFileId(), pageHeapCtid.GetBlockNum(), pageHeapCtid.GetOffset()));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("unknow undo type %hhu", static_cast<uint8>(undoType)));
    storage_set_error(PAGE_ERROR_UNDO_TYPE_NOT_FOUND);
    return DSTORE_FAIL;
}

RetStatus BtrPage::ConstructCR(Transaction *transaction, CRContext *crCtx, BtreeUndoContext *btrUndoContext,
                               BufMgrInterface *bufMgr)
{
    if (GetLinkAndStatus()->IsUnlinked()) {
        crCtx->useLocalCr = true;
        return DSTORE_SUCC;
    }

    bool needFreeUndoContext = false;
    if (btrUndoContext == nullptr) {
        needFreeUndoContext = true;
        btrUndoContext = DstoreNew(g_dstoreCurrentMemoryContext) BtreeUndoContext(crCtx->pdbId, bufMgr);
    }
    if (STORAGE_VAR_NULL(btrUndoContext)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreNew failed when ConstructCR."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(btrUndoContext->InitWithBtrPage(this))) {
        delete btrUndoContext;
        return DSTORE_FAIL;
    }
    StorageAssert(btrUndoContext->m_indexInfo != nullptr);

    btrUndoContext->m_undoType = BtreeUndoContextType::CONSTRUCT_CR;
    RetStatus ret = DataPage::ConstructCR(transaction, crCtx, btrUndoContext);
    if (needFreeUndoContext) {
        btrUndoContext->Destroy();
        delete btrUndoContext;
    }
    return ret;
}

char* BtrPage::Dump(Page *metaPage, bool showData)
{
    StringInfoData str;
    str.init();
    BtrPageType pageType = static_cast<BtrPageType>(GetLinkAndStatus()->GetType());
    if (pageType != BtrPageType::META_PAGE && !IsInitialized()) {
        str.append("Uninitialized Page.");
        return str.data;
    }

    switch (pageType) {
        case BtrPageType::INVALID_BTR_PAGE:
            str.append("Invalid BtrPage (maybe uninitialized).");
            break;
        case BtrPageType::LEAF_PAGE:
            DumpForLeaf(&str, metaPage, showData);
            break;
        case BtrPageType::INTERNAL_PAGE:
            DumpForPivot(&str, metaPage, showData);
            break;
        case BtrPageType::META_PAGE:
        default:
            StorageAssert(pageType == BtrPageType::META_PAGE);
            DumpForMeta(&str);
    }
    return str.data;
}

char *BtrPage::DumpLeafPageForLogs()
{
    if (unlikely(GetType() != PageType::INDEX_PAGE_TYPE || !GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE))) {
        return nullptr;
    }
    StringInfoData strInfo;
    if (unlikely(!strInfo.init())) {
        return nullptr;
    }
        /* Step 1. Get page meta */
    DumpForPageMeta(&strInfo);
    strInfo.append("BtrMeta create xid: (%d, %lu)\n", static_cast<int32>(GetBtrMetaCreateXid().m_zoneId),
            GetBtrMetaCreateXid().m_logicSlotId);

    /* Step 2. Print header */
    strInfo.append("IndexPageHeader\n");
    DataPage::DumpDataPageHeader(&strInfo);

    /* Step 3. Collect itemIDs & tuples */
    for (OffsetNumber offNum = 1; offNum <= GetMaxOffset(); offNum++) {
        ItemId *itemId = GetItemIdPtr(offNum);
        strInfo.append("ItemId %hu: [offset] %hu, [len] %hu, [flag] %u\n",
                       offNum, itemId->GetOffset(), itemId->GetLen(), itemId->GetFlags());
        IndexTuple *indexTup = GetIndexTuple(itemId);
        ItemPointerData heapCtid = indexTup->GetHeapCtid();
        strInfo.append("   [heap ctid] (%hu, %u, %hu)\n",
                       heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset());
        strInfo.append("   [td id] %d %s, tuple is deleted: %d \n", indexTup->GetTdId(),
                       STR_TD_TUPLE_STATUS[indexTup->GetTdStatus()], static_cast<int>(indexTup->IsDeleted()));
        strInfo.append("   [ins4del] %s  [ccindexStat] %d  [sameWithLastLeft] %s \n",
                       indexTup->IsInsertDeletedForCCindex() ? "true" : "false",
                       static_cast<int>(indexTup->GetCcindexStatus()),
                       indexTup->IsSameWithLastLeft() ? "true" : "false");
    }
    return strInfo.data;
}

void BtrPage::DumpForPageMeta(StringInfo str)
{
    str->append("glsn %lu | plsn %lu | walId %lu | headerSize %hu | "
                "lower %hu | upper %hu | tdCount %hhu | itemCount(nonDel/total) %hu/%hu\n",
                GetGlsn(), GetPlsn(), GetWalId(), DataHeaderSize(), GetLower(), GetUpper(), GetTdCount(),
                GetNonDeletedTupleNum(), GetMaxOffset());
    BtrPageLinkAndStatus *pageMeta = GetLinkAndStatus();
    str->append("Left Sib: (%hu, %u)\t", pageMeta->GetLeft().m_fileId, pageMeta->GetLeft().m_blockId);
    str->append("Right Sib: (%hu, %u)\t", pageMeta->GetRight().m_fileId, pageMeta->GetRight().m_blockId);
    str->append("BtrMeta PageId: (%hu, %u)\n", pageMeta->btrMetaPageId.m_fileId, pageMeta->btrMetaPageId.m_blockId);
    str->append("Level: %u\t", pageMeta->GetLevel());
    pageMeta->IsRoot() ? str->append("Root page.\t") : str->append("Not root page.\t");
    str->append("Live status: %hu\t", pageMeta->status.bitVal.liveStat);
    str->append("Split status: %hu\n", pageMeta->status.bitVal.splitStat);
}

void BtrPage::DumpForLeaf(StringInfo str, Page *metaPage, bool showData)
{
    /* Step 1. Get page meta */
    DumpForPageMeta(str);
    str->append("BtrMeta create xid: (%d, %lu)\n", static_cast<int32>(GetBtrMetaCreateXid().m_zoneId),
        GetBtrMetaCreateXid().m_logicSlotId);

    /* Step 2. Print header */
    str->append("IndexPageHeader\n");
    DataPage::DumpDataPageHeader(str);

    if (HasPrunableItem()) {
        str->append(" PD_ITEM_PRUNABLE ");
    }
    if (HasPrunableTuple()) {
        str->append(" PD_TUPLE_PRUNABLE ");
    }
    str->append("\n");

    /* Step 3. Get tuple descriptor */
    TupleDesc tupleDesc = static_cast<BtrMeta *>(static_cast<void *>(
        static_cast<BtrPage *>(metaPage)->GetData()))->ConstructTupleDesc();

    /* Step 4. Collect itemIDs & tuples */
    for (OffsetNumber offNum = 1; offNum <= GetMaxOffset(); offNum++) {
        ItemId *itemId = GetItemIdPtr(offNum);
        str->append("ItemId %hu: [offset] %hu, [len] %hu, [flag] %u\n",
                    offNum, itemId->GetOffset(), itemId->GetLen(), itemId->GetFlags());
        IndexTuple *indexTup = GetIndexTuple(itemId);
        if (showData) {
            CollectTupleKeys(indexTup, tupleDesc, str);
        }
        ItemPointerData heapCtid = indexTup->GetHeapCtid();
        if (heapCtid != INVALID_ITEM_POINTER) {
            str->append("   [heap ctid] {%hu, %u}, %hu\n",
                        heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset());
        }
        str->append("   [td id] %d %s, tuple is deleted: %d \n", indexTup->GetTdId(),
                    STR_TD_TUPLE_STATUS[indexTup->GetTdStatus()], static_cast<int>(indexTup->IsDeleted()));
        str->append("   [ins4del] %s  [ccindexStat] %d  [sameWithLastLeft] %s \n",
                    indexTup->IsInsertDeletedForCCindex() ? "true" : "false",
                    static_cast<int>(indexTup->GetCcindexStatus()),
                    indexTup->IsSameWithLastLeft() ? "true" : "false");
    }
    DstorePfree(tupleDesc);
}

void BtrPage::DumpForPivot(StringInfo str, Page *metaPage, bool showData)
{
    /* Step 1. Get page meta */
    DumpForPageMeta(str);

    /* Step 2. Get tuple descriptor */
    if (STORAGE_VAR_NULL(metaPage)) {
        ErrLog(DSTORE_PANIC, MODULE_PAGE, ErrMsg("MetaPage is nullptr."));
    }
    TupleDesc tupleDesc = static_cast<BtrMeta *>(static_cast<void *>(
        static_cast<BtrPage *>(metaPage)->GetData()))->ConstructTupleDesc();

    /* Step 3. Collect itemIDs & tuples */
    for (OffsetNumber offNum = 1; offNum <= GetMaxOffset(); offNum++) {
        ItemId *itemId = GetItemIdPtr(offNum);
        str->append("ItemId %hu: [offset] %hu, [len] %hu, [flag] %u\n",
                    offNum, itemId->GetOffset(), itemId->GetLen(), itemId->GetFlags());
        IndexTuple *indexTup = GetIndexTuple(itemId);
        if (showData) {
            CollectTupleKeys(indexTup, tupleDesc, str);
        }
        ItemPointerData heapCtid = indexTup->GetHeapCtid();
        if (heapCtid != INVALID_ITEM_POINTER) {
            str->append("   [heap ctid] (%hu, %u, %hu)\n",
                        heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset());
        }
        PageId downLink = indexTup->GetLowlevelIndexpageLink();
        str->append("   [downlink] (%hu, %u), tuple is deleted: %d\n", downLink.m_fileId, downLink.m_blockId,
                    static_cast<int>(indexTup->IsDeleted()));
    }
    DstorePfree(tupleDesc);
}

void BtrPage::DumpForMeta(StringInfo str)
{
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(GetData()));
    str->append("Root: [page id] (%hu, %u) [level] %u\n", btrMeta->GetRootPageId().m_fileId,
                btrMeta->GetRootPageId().m_blockId, btrMeta->GetRootLevel());
    str->append("Lowest single: [page id] (%hu, %u) [level] %u\n", btrMeta->GetLowestSinglePage().m_fileId,
                btrMeta->GetLowestSinglePage().m_blockId, btrMeta->GetLowestSinglePageLevel());
    str->append("RelKind: %c\n", btrMeta->GetRelKind());
    str->append("TableOidAtt: %d\n", btrMeta->GetTableOidAtt());
    str->append("Number of attributes: %hu\n", btrMeta->GetNatts());
    str->append("Number of key attributes: %hu\n", btrMeta->GetNkeyatts());
    for (int i = 0; i < btrMeta->natts; i++) {
        str->append("  [att %d] %u\n", i, btrMeta->attTypeIds[i]);
    }
    for (int i = 0; i < btrMeta->nkeyAtts; i++) {
        str->append("  [opt %d] %hd\n", i, btrMeta->indexOption[i]);
    }
    for (int i = 0; i < btrMeta->nkeyAtts; i++) {
        str->append("  [colid %d] %u\n", i, btrMeta->attColIds[i]);
    }
    str->append("[xid] (%d, %lu)\n", static_cast<int32>(btrMeta->createXid.m_zoneId), btrMeta->createXid.m_logicSlotId);
}

void BtrPage::CollectTupleKeys(IndexTuple *tuple, TupleDesc tupleDesc, StringInfo str)
{
    int numAttrs = static_cast<int>(tuple->GetKeyNum(static_cast<uint16>(tupleDesc->natts)));
    Form_pg_attribute  *attrs = tupleDesc->attrs;

    bool isAttrNull = false;
    str->append("   [keys]");
    for (int attNum = 1; attNum <= numAttrs; attNum++) {
        str->append(" [%d] ", attNum);
        Datum datum = tuple->GetAttr(attNum, tupleDesc, &isAttrNull);
        if (isAttrNull) {
            str->append("null");
        } else {
            AppendAttrToStrByType(attrs[attNum - 1]->atttypid, datum, str);
        }
    }

    if (tuple->IsPivot() && tuple->m_link.val.hasTableOid) {
        str->append(" [tableOid] %d", tuple->GetTableOid(NULL));
    }

    str->append("\n");
}

void BtrPage::AppendAttrToStrByType(Oid typeOid, Datum attr, StringInfo str)
{
    switch (typeOid) {
        case BOOLOID:
            str->append("%s", DatumGetBool(attr) ? "true" : "false");
            break;
        case INT1OID:
            str->append("%hd/%d", DATUM_GET_UINT8(attr), DATUM_GET_INT8(attr));
            break;
        case INT2OID:
            str->append("%d", DatumGetInt16(attr));
            break;
        case INT4OID:
        case OIDOID:
            str->append("%d", DatumGetInt32(attr));
            break;
        case INT8OID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            str->append("%ld", DatumGetInt64(attr));
            break;
        case CHAROID:
            str->append("%c", DatumGetChar(attr));
            break;
        case CSTRINGOID:
        case NAMEOID:
            str->append("%s", static_cast<DstoreNameData *>(static_cast<void *>(DatumGetPointer(attr)))->data);
            break;
        case FLOAT4OID:
            str->append("%f", DatumGetFloat32(attr));
            break;
        case FLOAT8OID:
            str->append("%lf", DatumGetFloat64(attr));
            break;
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID: {
            void *textPtr = static_cast<void *>(DatumGetText(attr));
            str->append("%s", VarDataAny(textPtr));
            break;
        }
        case OIDVECTOROID: {
            OidVector *vectorPtr = static_cast<OidVector *>(static_cast<void *>(DatumGetPointer(attr)));
            StorageAssert(vectorPtr != nullptr);
            for (int i = 0; i < vectorPtr->dim1; i++) {
                str->append("%u,", vectorPtr->values[i]);
            }
            break;
        }
        case INT2VECTOROID: {
            int2vector *vectorPtr = static_cast<int2vector *>(static_cast<void *>(DatumGetPointer(attr)));
            StorageAssert(vectorPtr != nullptr);
            for (int i = 0; i < vectorPtr->dim1; i++) {
                str->append("%d, ", vectorPtr->values[i]);
            }
            break;
        }
        case INT1ARRAYOID:
        case INT2ARRAYOID:
        case INT4ARRAYOID:
        case INT8ARRAYOID:
        case BOOLARRAYOID:
        case CHARARRAYOID:
        case NAMEARRAYOID:
        case BPCHARARRAYOID:
        case TEXTARRAYOID:
        case VARCHARARRAYOID:
        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID:
        case INETARRAYOID:
        case ANYARRAYOID: {
            AppendArrAttrToStr(attr, str);
            break;
        }
        default:
            break;
    }
}

void BtrPage::AppendArrAttrToStr(Datum attr, StringInfo str)
{
    ArrayType *arrayPtr = DatumGetArray(attr);
    if (STORAGE_VAR_NULL(arrayPtr)) {
        return;
    }
    int ndims = ARR_NDIM(arrayPtr);
    int *dims = ARR_DIMS(arrayPtr);
    int numItems = ArrayGetNItems(ndims, dims);
    if (numItems == 0) {
        str->append("{}");
        return;
    }

    char *data = (ARR_DATA_PTR(arrayPtr));
    bits8 *bitmap = ARR_NULLBITMAP(arrayPtr);
    uint32 bitmask = 1;
    Datum datum;

    TypeCache typeCache;
    typeCache.type = DSTORE_INVALID_OID;
    GetTypeCache(typeCache, ARR_ELEMTYPE(arrayPtr));
    if (typeCache.type == DSTORE_INVALID_OID) {
        str->append("Unsupported type %u", arrayPtr->elemtype);
        return;
    }

    int *brackets = static_cast<int *>(DstorePalloc(sizeof(int) * static_cast<uint32>(ndims)));
    if (unlikely(brackets == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when AppendArrAttrToStr."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return;
    }
    /* Skip memory check for pagedump */
    for (int i = 0; i < ndims; i++) {
        brackets[i] = 0;
        str->append("{");
    }

    int itemCount = 0;
    while (itemCount < numItems) {
        itemCount++;
        /* Get data, checking for NULL */
        if (bitmap != nullptr && (*bitmap & bitmask) == 0) {
            datum = static_cast<Datum>(0);
        } else {
            datum = FetchAtt(data, typeCache.attbyval, typeCache.attlen);
            data = AttAddLength(data, typeCache.attlen, data);
            data = AttAlignNominal(data, typeCache.attalign);
        }
        /* advance bitmap pointers if any */
        bitmask <<= 1;
        if (bitmask == 0x100) {
            bitmap = bitmap != nullptr ? bitmap + 1 : bitmap;
            bitmask = 1;
        }

        /* Print data */
        if (datum == 0 && !typeCache.attbyval) {
            str->append("NULL");
        } else {
            AppendAttrToStrByType(arrayPtr->elemtype, datum, str);
        }

        /* Print brackets */
        StorageAssert(ndims > INT_MIN);
        int dimPos = ndims - 1;
        while (dimPos >= 0) {
            brackets[dimPos]++;
            if (brackets[dimPos] != dims[dimPos]) {
                break;
            }
            str->append("}");
            brackets[dimPos] = 0;
            dimPos--;
        }
        if (itemCount == numItems) {
            break;
        }
        str->append(", ");
        dimPos = ndims - 1;
        while (dimPos >= 0 && brackets[dimPos] == 0) {
            str->append("{");
            dimPos--;
        }
    }
    DstorePfree(brackets);
}

bool BtrPage::HasGarbageSpace()
{
    uint16 tupleTotalLen = 0;
    for (OffsetNumber offset = 1; offset <= GetMaxOffset(); offset = OffsetNumberNext(offset)) {
        tupleTotalLen += GetItemIdPtr(offset)->GetLen();
    }
    return tupleTotalLen != (GetSpecialOffset() - GetUpper());
}

TupleDesc BtrMeta::ConstructTupleDesc()
{
    uint16 nAtts = GetNatts();

    Size tupleDescSize = MAXALIGN(sizeof(TupleDescData));
    Size attrsPointerSize = MAXALIGN(nAtts * sizeof(Form_pg_attribute));
    Size attrsDataSize = MAXALIGN(nAtts * sizeof(FormData_pg_attribute));
    TupleDesc tupleDesc = (TupleDesc)DstorePalloc(tupleDescSize + attrsPointerSize + attrsDataSize);
    if (unlikely(tupleDesc == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when ConstructTupleDesc."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    char *tupleDescPtr = static_cast<char *>(static_cast<void *>(tupleDesc));
    Form_pg_attribute *attrsPointer =
        static_cast<Form_pg_attribute *>(static_cast<void *>(tupleDescPtr + tupleDescSize));
    FormData_pg_attribute *attrsData =
        static_cast<FormData_pg_attribute *>(static_cast<void *>((tupleDescPtr + tupleDescSize + attrsPointerSize)));
    for (uint16 i = 0; i < nAtts; i++) {
        attrsPointer[i] = &attrsData[i];
    }
    tupleDesc->natts = nAtts;
    tupleDesc->attrs = attrsPointer;

    TypeCache typeCache;
    for (int i = 0; i < nAtts; ++i) {
        tupleDesc->attrs[i]->atttypid = GetAttTypids(i);
        tupleDesc->attrs[i]->attlen = GetAttlen(i);
        tupleDesc->attrs[i]->attbyval = GetAttbyval(i);
        tupleDesc->attrs[i]->attalign = GetAttalign(i);
        tupleDesc->attrs[i]->attcacheoff = -1;
        tupleDesc->attrs[i]->attcollation = GetAttCollation(i);
    }
    return tupleDesc;
}
}  // namespace DSTORE
