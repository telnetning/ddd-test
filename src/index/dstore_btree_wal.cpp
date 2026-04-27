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
 * This file impliments wal related functions for btree.
 *
 * IDENTIFICATION
 *        src/index/dstore_btree_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <functional>
#include "index/dstore_btree_wal.h"

namespace DSTORE {

struct BtreeWalRedoItem {
    WalType type;
    std::function<void(const WalRecordIndex *, BtrPage *, WalRecordRedoContext *)> redo;

    BtreeWalRedoItem(WalType walType,
        std::function<void(const WalRecordIndex *, BtrPage *, WalRecordRedoContext *)> redoFunc) noexcept
        : type(walType), redo(std::move(redoFunc))
    {}
};

static const BtreeWalRedoItem BTREE_WAL_REDO_TABLE[] { /* NOLINT */
    {WAL_BTREE_BUILD,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeBuild *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_INIT_META_PAGE,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeInitMetaPage *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_META_ROOT,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateMetaPage *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateMetaPage *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_NEW_INTERNAL_ROOT,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeNewInternalRoot *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_NEW_LEAF_ROOT,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeNewLeafRoot *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_INSERT_ON_INTERNAL,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeInsertOnInternal *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_INSERT_ON_LEAF,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeInsertOnLeaf *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_SPLIT_INTERNAL,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeSplitInternal *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_SPLIT_LEAF,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeSplitLeaf *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_SPLIT_INSERT_INTERNAL,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeSplitInsertInternal *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_SPLIT_INSERT_LEAF,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeSplitInsertLeaf *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_NEW_INTERNAL_RIGHT,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeNewRight *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_NEW_LEAF_RIGHT,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeNewRight *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_DELETE_ON_LEAF,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeDeleteOnLeaf *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_DELETE_ON_INTERNAL,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeDeleteOnInternal *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_PAGE_PRUNE,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreePagePrune *>(self))->Redo(redoCtx->pdbId, page, redoCtx->xid); }},
    {WAL_BTREE_ALLOC_TD,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeAllocTd *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_LIVESTATUS,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateLiveStatus *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_SPLITSTATUS,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateSplitStatus *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_LEFT_SIB_LINK,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateSibLink *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_RIGHT_SIB_LINK,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateSibLink *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_UPDATE_DOWNLINK,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeUpdateDownlink *>(self))->Redo(page, redoCtx->xid); }},
    {WAL_BTREE_ERASE_INS_FOR_DEL_FLAG,
     [](const WalRecordIndex *self, BtrPage *page, WalRecordRedoContext *redoCtx) {
         (static_cast<const WalRecordBtreeEraseInsForDelFlag *>(self))->Redo(page, redoCtx->xid); }}
};

struct BtreeWalDescItem {
    WalType type;
    std::function<void(const WalRecordIndex *, FILE *)> desc;

    BtreeWalDescItem(WalType walType, std::function<void(const WalRecordIndex *, FILE *)> descFunc) noexcept
        : type(walType), desc(std::move(descFunc)) {}
};

static const BtreeWalDescItem BTREE_WAL_DESC_TABLE[]{
    /* NOLINT */
    {WAL_BTREE_BUILD,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreeBuild *>(self))->Describe(fp); }},
    {WAL_BTREE_INIT_META_PAGE, [](const WalRecordIndex *self,
                                  FILE *fp) { (static_cast<const WalRecordBtreeInitMetaPage *>(self))->Describe(fp); }},
    {WAL_BTREE_UPDATE_META_ROOT,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateMetaPage *>(self))->Describe(fp);
     }},
    {WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateMetaPage *>(self))->Describe(fp);
     }},
    {WAL_BTREE_NEW_INTERNAL_ROOT,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeNewInternalRoot *>(self))->Describe(fp);
     }},
    {WAL_BTREE_NEW_LEAF_ROOT, [](const WalRecordIndex *self,
                                 FILE *fp) { (static_cast<const WalRecordBtreeNewLeafRoot *>(self))->Describe(fp); }},
    {WAL_BTREE_INSERT_ON_LEAF, [](const WalRecordIndex *self,
                                  FILE *fp) { (static_cast<const WalRecordBtreeInsertOnLeaf *>(self))->Describe(fp); }},
    {WAL_BTREE_INSERT_ON_INTERNAL,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeInsertOnInternal *>(self))->Describe(fp);
     }},
    {WAL_BTREE_SPLIT_INTERNAL,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeSplitInternal *>(self))->Describe(fp);
     }},
    {WAL_BTREE_SPLIT_LEAF,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreeSplitLeaf *>(self))->Describe(fp); }},
    {WAL_BTREE_SPLIT_INSERT_INTERNAL,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeSplitInsertInternal *>(self))->Describe(fp);
     }},
    {WAL_BTREE_SPLIT_INSERT_LEAF,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeSplitInsertLeaf *>(self))->Describe(fp);
     }},
    {WAL_BTREE_NEW_INTERNAL_RIGHT,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreeNewRight *>(self))->Describe(fp); }},
    {WAL_BTREE_NEW_LEAF_RIGHT,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreeNewRight *>(self))->Describe(fp); }},
    {WAL_BTREE_DELETE_ON_LEAF, [](const WalRecordIndex *self,
                                  FILE *fp) { (static_cast<const WalRecordBtreeDeleteOnLeaf *>(self))->Describe(fp); }},
    {WAL_BTREE_DELETE_ON_INTERNAL,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeDeleteOnInternal *>(self))->Describe(fp);
     }},
    {WAL_BTREE_PAGE_PRUNE,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreePagePrune *>(self))->Describe(fp); }},
    {WAL_BTREE_ALLOC_TD,
     [](const WalRecordIndex *self, FILE *fp) { (static_cast<const WalRecordBtreeAllocTd *>(self))->Describe(fp); }},
    {WAL_BTREE_UPDATE_LIVESTATUS,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateLiveStatus *>(self))->Describe(fp);
     }},
    {WAL_BTREE_UPDATE_SPLITSTATUS,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateSplitStatus *>(self))->Describe(fp);
     }},
    {WAL_BTREE_UPDATE_LEFT_SIB_LINK,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateSibLink *>(self))->Describe(fp);
     }},
    {WAL_BTREE_UPDATE_RIGHT_SIB_LINK,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateSibLink *>(self))->Describe(fp);
     }},
    {WAL_BTREE_UPDATE_DOWNLINK,
     [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeUpdateDownlink *>(self))->Describe(fp);
     }},
    {WAL_BTREE_ERASE_INS_FOR_DEL_FLAG, [](const WalRecordIndex *self, FILE *fp) {
         (static_cast<const WalRecordBtreeEraseInsForDelFlag *>(self))->Describe(fp);
     }}};

void WalRecordIndex::RedoIndexRecord(WalRecordRedoContext *redoCtx, const WalRecordIndex *indexRecord,
                                     BufferDesc *bufferDesc)
{
    StorageAssert(indexRecord != nullptr);
    PageId pageId = indexRecord->m_pageId;
    STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_INDEX, pageId);
    BtrPage *page = static_cast<BtrPage *>(bufferDesc->GetPage());

    for (uint32 i = 0; i < sizeof(BTREE_WAL_REDO_TABLE) / sizeof(BTREE_WAL_REDO_TABLE[0]); i++) {
        if (BTREE_WAL_REDO_TABLE[i].type == indexRecord->m_type) {
            BTREE_WAL_REDO_TABLE[i].redo(indexRecord, page, redoCtx);
            break;
        }
    }

    const uint64 glsn = indexRecord->m_pagePreWalId != redoCtx->walId ? indexRecord->m_pagePreGlsn + 1
                                                                      : indexRecord->m_pagePreGlsn;
    page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, glsn);
}

void WalRecordIndex::DumpIndexRecord(const WalRecordIndex *indexRecord, FILE *fp)
{
    indexRecord->Dump(fp);
    for (uint32 i = 0; i < sizeof(BTREE_WAL_DESC_TABLE) / sizeof(BTREE_WAL_DESC_TABLE[0]); i++) {
        if (BTREE_WAL_DESC_TABLE[i].type == indexRecord->m_type) {
            BTREE_WAL_DESC_TABLE[i].desc(indexRecord, fp);
            return;
        }
    }
}

void WalRecordBtreeBuild::SetData(BtrPage *page)
{
    CopyData(pageData, BLCKSZ, static_cast<char *>(static_cast<void *>(page)), BLCKSZ);
}

void WalRecordBtreeBuild::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    CopyData(static_cast<char *>(static_cast<void *>(page)), BLCKSZ, pageData, BLCKSZ);
}

void WalRecordBtreeBuild::Describe(FILE *fp) const
{
    BtrPage *page = reinterpret_cast<BtrPage *>(const_cast<char *>(pageData));
    (void)fprintf(fp, BTR_PAGE_HEADER_FMT, BTR_PAGE_HEADER_VAL(page));
    (void)fprintf(fp, " ");
    BtrPageLinkAndStatus *linkAndStat = page->GetLinkAndStatus();
    (void)fprintf(fp, BTR_PAGE_LINK_AND_STATUS_FMT, BTR_PAGE_LINK_AND_STATUS_VAL(linkAndStat));
    if (!linkAndStat->TestType(BtrPageType::LEAF_PAGE)) {
        return;
    }
    /* show heapctids of all tuples for leaf page */
    bool isLeaf = linkAndStat->TestType(BtrPageType::LEAF_PAGE);
    for (OffsetNumber i = BTREE_PAGE_HIKEY; i <= page->GetMaxOffset(); i++) {
        ItemId *ii = page->GetItemIdPtr(i);
        (void)fprintf(fp, " [%hu]flag(%u)", i, ii->GetFlags());
        if (unlikely(!ii->IsNormal() && !ii->IsRangePlaceholder())) {
            continue;
        }
        IndexTuple *iTuple = page->GetIndexTuple(ii);
        ItemPointer downlink = &iTuple->m_link.heapCtid;
        (void)fprintf(fp, "pivot(%d)", static_cast<int>(iTuple->IsPivot()));
        if (isLeaf) {
            (void)fprintf(fp, "heapCtid(%hu, %u, %hu)", downlink->GetFileId(), downlink->GetBlockNum(),
                          downlink->GetOffset());
        } else {
            (void)fprintf(fp, "downlink(%hu, %u) keyNum(%hu)", downlink->GetFileId(), downlink->GetBlockNum(),
                          iTuple->m_link.val.num);
        }
    }
}

void WalRecordBtreeInitMetaPage::SetData(BtrPage *page)
{
    btrCreateXid = page->GetBtrMetaCreateXid();
    uint32 metaSize = sizeof(BtrMeta);
    CopyData(btrMeta, m_size - sizeof(btrCreateXid), page->GetData(), metaSize);
}

void WalRecordBtreeInitMetaPage::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    page->InitBtrPageInner(m_pageId);
    uint32 lower = page->GetLower();
    uint32 metaSize = BTR_META_SIZE;
    uint32 remainSize = page->GetUpper() - lower;
    StorageReleasePanic(remainSize < metaSize, MODULE_INDEX,
                        ErrMsg("Unexpected BtrMeta page size, upper:%u, lower:%u", page->GetUpper(), page->GetLower()));

    CopyData(page->GetData(), remainSize, btrMeta, metaSize);
    page->SetLower(lower + metaSize);
    page->SetBtrMetaCreateXid(btrCreateXid);
    page->GetLinkAndStatus()->SetType(BtrPageType::META_PAGE);
}

void WalRecordBtreeInitMetaPage::Describe(FILE *fp) const
{
    const BtrMeta *meta = reinterpret_cast<const BtrMeta *>(btrMeta);
    (void)fprintf(fp, "CreatedXid:(%u, %lu), Root(%hu, %u), RootLevel(%u), "
                  "LowestSingle(%hu, %u), LowestSingleLevel(%u), ",
                  static_cast<uint32>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                  meta->GetRootPageId().m_fileId, meta->GetRootPageId().m_blockId, meta->GetRootLevel(),
                  meta->GetLowestSinglePage().m_fileId, meta->GetLowestSinglePage().m_blockId,
                  meta->GetLowestSinglePageLevel());
    (void)fprintf(fp, "nKeyAttrs(%hu), nAttrs(%hu), ", meta->GetNkeyatts(), meta->GetNatts());
    for (uint16 i = 0; i < meta->GetNatts(); i++) {
        (void)fprintf(fp, "[%hu]type(%u)oper(%hd) ", i, meta->GetAttTypids(i), meta->GetIndoption(i));
    }
}

void WalRecordBtreeUpdateMetaPage::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    BtrMeta *meta = static_cast<BtrMeta *>(static_cast<void *>(page->GetData()));
    StorageAssert(page->GetSelfPageId() == m_pageId);
    StorageAssert(rootPageId != INVALID_PAGE_ID);
    StorageReleasePanic(!page->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE), MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u) is not btree meta. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, BTR_PAGE_HEADER_VAL(page),
        BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));

    if (m_type == WAL_BTREE_UPDATE_META_ROOT) {
        meta->SetBtreeMetaInfo(rootPageId, rootPageId, level, level);
    } else {
        StorageAssert(m_type == WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE);
        meta->SetLowestSinglePage(rootPageId);
        meta->SetLowestSinglePageLevel(level);
    }
}

void WalRecordBtreeUpdateMetaPage::Describe(FILE *fp) const
{
    (void)fprintf(fp, "rootPage(%hu, %u), level(%u)", rootPageId.m_fileId, rootPageId.m_blockId, level);
}

void WalRecordBtreeNewLeafRoot::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    page->Reset(m_pageId);
    page->AllocateTdSpace(DEFAULT_TD_COUNT);
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    page->SetBtrMetaCreateXid(metaCreateXid);
    linkStat->InitPageMeta(btrMeta, 0, true);
}

void WalRecordBtreeNewLeafRoot::Describe(FILE *fp) const
{
    (void)fprintf(fp, "btrMetaPageId{%hu, %u}", btrMeta.m_fileId, btrMeta.m_blockId);
    (void)fprintf(fp, "metaCreateXid{%d, %lu}", static_cast<int32>(metaCreateXid.m_zoneId),
                  metaCreateXid.m_logicSlotId);
}

void WalRecordBtreeNewInternalRoot::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    IndexTuple *leftDownlink = IndexTuple::CreateMinusinfPivotTuple();
    StorageReleasePanic((leftDownlink == nullptr),
                        MODULE_INDEX, ErrMsg("CreatePivotTuple failed: %s", thrd->GetErrorMessage()));
    leftDownlink->SetLowlevelIndexpageLink(origRoot);
    leftDownlink->SetTdId(INVALID_TD_SLOT);
    leftDownlink->SetTdStatus(DETACH_TD);
    const IndexTuple *rightDownlink = static_cast<const IndexTuple *>(static_cast<const void *>(rawData));

    page->Reset(m_pageId);
    page->SetBtrMetaCreateXid(btrMetaCreateXid);
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    linkStat->InitPageMeta(btrMeta, rootLevel, true);
    StorageReleasePanic((page->AddTupleData(leftDownlink, BTREE_PAGE_HIKEY) != BTREE_PAGE_HIKEY ||
        page->AddTupleData(rightDownlink, OffsetNumberNext(BTREE_PAGE_HIKEY)) != OffsetNumberNext(BTREE_PAGE_HIKEY)),
        MODULE_INDEX, ErrMsg("Failed to insert downlinks into root page."));
    StorageAssert(page->CheckSanity());
    DstorePfree(leftDownlink);
}

void WalRecordBtreeNewInternalRoot::Describe(FILE *fp) const
{
    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(rawData));
    (void)fprintf(fp, "origRoot{%hu, %u} btrMeta{%hu, %u}, rootLevel(%u) ", origRoot.m_fileId, origRoot.m_blockId,
                btrMeta.m_fileId, btrMeta.m_blockId, rootLevel);
    (void)fprintf(
        fp, "rightDownlink{size(%u), link(%lu): downlink(%hu, %u), keyNum(%hu), info(%u)}, metaCreateXid(%d, %lu)",
        tuple->GetSize(), tuple->m_link.heapCtid.m_placeHolder, tuple->GetLowlevelIndexpageLink().m_fileId,
        tuple->GetLowlevelIndexpageLink().m_blockId, tuple->GetKeyNum(0), tuple->m_info.m_info,
        static_cast<int32>(btrMetaCreateXid.m_zoneId), btrMetaCreateXid.m_logicSlotId);
}

void WalRecordBtreeInsertOnInternal::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);
    StorageReleasePanic(!page->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE), MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u) is not internal. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, BTR_PAGE_HEADER_VAL(page),
        BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));

    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    StorageReleasePanic((page->AddTupleData(tuple, m_offset) != m_offset), MODULE_INDEX,
        ErrMsg("Failed to redo insert on internal page." BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
}

void WalRecordBtreeInsertOnInternal::Describe(FILE *fp) const
{
    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    (void)fprintf(fp, "IndexTuple{size(%u), link(%lu): downlink(%hu, %u), keyNum(%hu), info(%u)}",
                tuple->GetSize(), tuple->m_link.heapCtid.m_placeHolder, tuple->GetLowlevelIndexpageLink().m_fileId,
                tuple->GetLowlevelIndexpageLink().m_blockId, tuple->GetKeyNum(0), tuple->m_info.m_info);
}

void WalRecordBtreeInsertOnLeaf::Redo(BtrPage *page, Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);
    StorageReleasePanic(!page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE), MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u) is not leaf. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, BTR_PAGE_HEADER_VAL(page),
        BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));

    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    RedoAllocTdWal(page, PageType::INDEX_PAGE_TYPE, m_rawData + tuple->GetSize(),
                   m_size - (sizeof(WalRecordBtreeInsertOnLeaf) + tuple->GetSize()));
    StorageReleasePanic(page->AddTupleData(tuple, m_offset) != m_offset, MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u), %hu, size(%u) insert failed." BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, m_offset, tuple->GetSize(),
        BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
    if (likely(tuple->GetTdId() != INVALID_TD_SLOT)) {
        page->SetTd(tuple->GetTdId(), xid, UndoRecPtr(m_undoRecPtr), INVALID_CID);
    }
}
void WalRecordBtreeInsertOnLeaf::Describe(FILE *fp) const
{
    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    const ItemPointerData undoRecPtr = ItemPointerData(m_undoRecPtr);
    (void)fprintf(fp,
                  "offset(%hu), IndexTuple{td(%hhu), size(%u), heapCtid(%hu, %u, %hu), del(%u), "
                  "ins4Del(%u), ccindexStat(%u)}, undoRecPtr(%hu, %u, %u)",
                  m_offset, tuple->GetTdId(), tuple->GetSize(), tuple->GetHeapCtid().GetFileId(),
                  tuple->GetHeapCtid().GetBlockNum(), tuple->GetLinkOffset(), tuple->m_info.val.m_isDeleted,
                  tuple->m_info.val.m_ccindexInsForDelFlag, tuple->m_info.val.m_ccindexStatus,
                  undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset());
    if (m_size > sizeof(WalRecordBtreeInsertOnLeaf) + tuple->GetSize()) {
        DescAllocTd(fp, m_rawData + tuple->GetSize(),
                    m_size - (sizeof(WalRecordBtreeInsertOnLeaf) + tuple->GetSize()));
    }
}

void WalRecordBtreeSplit::RedoSplitOnly(BtrPage *page, const char *walData, uint32 hikeyFrom) const
{
    /* Create temporary left page in memory */
    BtrPage tempLeft;
    tempLeft.InitMemLeftForSplit(page);
    /* Update link to right sibling */
    tempLeft.GetLinkAndStatus()->SetRight(newRightBlockNumber);

    const IndexTuple *leftHikey = static_cast<const IndexTuple *>(static_cast<const void *>(walData + hikeyFrom));

    /* Recover AllocTD */
    BtrPageLinkAndStatus *linkAndStat = page->GetLinkAndStatus();
    if (linkAndStat->TestType(BtrPageType::LEAF_PAGE)) {
        RedoAllocTdWal(&tempLeft, PageType::INDEX_PAGE_TYPE, walData + hikeyFrom + leftHikey->GetSize(),
                       m_size - (sizeof(WalRecordBtreeSplitLeaf) + hikeyFrom + leftHikey->GetSize()));
    }

    /* Recover left hikey */
    StorageReleasePanic(tempLeft.AddTupleData(leftHikey, BTREE_PAGE_HIKEY) != BTREE_PAGE_HIKEY, MODULE_INDEX,
                        ErrMsg("Failed to redo split when add hikey on leaf {%hu, %u}, size(%u).",
                               page->GetFileId(), page->GetBlockNum(), leftHikey->GetSize()));

    /* Copy index tuples before split point to memory page */
    OffsetNumber newOff = BTREE_PAGE_FIRSTKEY;
    for (OffsetNumber off = linkAndStat->GetFirstDataOffset(); off < firstRightOffNum; off++) {
        ItemId *currItemId = page->GetItemIdPtr(off);
        IndexTuple *tuple = page->GetIndexTuple(currItemId);
        StorageReleasePanic(tempLeft.AddTupleWhenSplit(currItemId, tuple, newOff) != newOff, MODULE_INDEX,
                            ErrMsg("Failed to redo split when add indexTuple(%hu, %u, %hu), size(%u).",
                                   page->GetFileId(), page->GetBlockNum(), off, tuple->GetSize()));
        newOff = OffsetNumberNext(newOff);
    }

    /* Replace splitting target with new left */
    page->Clone(&tempLeft);
    /* Update split status */
    linkAndStat->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
}

uint8 WalRecordBtreeSplit::RedoSplitInsert(BtrPage *page, OffsetNumber insertOff, const char *walData) const
{
    /* Create temporary left page in memory */
    BtrPage tempLeft;
    tempLeft.InitMemLeftForSplit(page);
    /* Update link to right sibling */
    tempLeft.GetLinkAndStatus()->SetRight(newRightBlockNumber);

    /* Read tuples from wal record */
    const IndexTuple *leftHikey = static_cast<const IndexTuple *>(static_cast<const void *>(walData));
    uint32 readSize = leftHikey->GetSize();
    const IndexTuple *insertTuple = static_cast<const IndexTuple *>(static_cast<const void *>(walData + readSize));
    readSize += insertTuple->GetSize();

    /* Recover AllocTD */
    BtrPageLinkAndStatus *linkAndStat = page->GetLinkAndStatus();
    if (linkAndStat->TestType(BtrPageType::LEAF_PAGE)) {
        RedoAllocTdWal(&tempLeft, PageType::INDEX_PAGE_TYPE, walData + readSize,
                       m_size - (sizeof(WalRecordBtreeSplitInsertLeaf) + readSize));
    }

    /* Recover left hikey */
    StorageReleasePanic(tempLeft.AddTupleData(leftHikey, BTREE_PAGE_HIKEY) != BTREE_PAGE_HIKEY, MODULE_INDEX,
                        ErrMsg("Failed to redo split when add hikey on internal {%hu, %u}, size(%u).",
                               page->GetFileId(), page->GetBlockNum(), leftHikey->GetSize()));

    /* Recover split and insertion */
    OffsetNumber startOff = linkAndStat->GetFirstDataOffset();
    SplitContext splitContext{insertOff, firstRightOffNum, true};
    StorageReleasePanic(
        STORAGE_FUNC_FAIL(page->CopyItemsFromSplitPage(startOff, &tempLeft, insertTuple, true, splitContext)),
        MODULE_INDEX, ErrMsg("Failed to redo split when copy tuples to new left."));

    /* Replace splitting target with new left */
    page->Clone(&tempLeft);
    /* Update split status */
    linkAndStat->SetSplitStatus(BtrPageSplitStatus::SPLIT_INCOMPLETE);
    return insertTuple->GetTdId();
}

void WalRecordBtreeSplitInternal::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    RedoSplitOnly(page, m_rawData);
}

void WalRecordBtreeSplitInternal::Describe(FILE *fp) const
{
    WalRecordBtreeSplit::DescSplit(fp);
    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    (void)fprintf(fp, "leftHikey{size(%u), info(%x)}", tuple->GetSize(), tuple->m_info.m_info);
}

uint32 WalRecordBtreeSplitLeaf::SetTdData(uint32 from, BtrPage *page, uint8 tdID)
{
    needSetTD = true;

    StorageAssert(tdID != INVALID_TD_SLOT);
    uint64 undoRecPtr = page->GetTd(tdID)->GetUndoRecPtr().m_placeHolder;
    uint32 total = m_size - sizeof(WalRecordBtreeSplitLeaf);
    uint32 used = from;
    char *buf = static_cast<char *>(m_rawData);

    CopyData(buf + used, total - used, static_cast<char *>(static_cast<void *>(&tdID)), sizeof(uint8));
    used += sizeof(uint8);
    CopyData(buf + used, total - used,
             static_cast<char *>(static_cast<void *>(&undoRecPtr)), sizeof(uint64));
    used += sizeof(uint64);
    /* Check length */
    StorageAssert(used <= total);
    StorageAssert(buf + used <= static_cast<char *>(static_cast<void *>(this)) + m_size);
    return used;
}

void WalRecordBtreeSplitLeaf::Redo(BtrPage *page, Xid xid) const
{
    uint32 hikeyFrom =  needSetTD ? (sizeof(uint8) + sizeof(uint64)) : 0U;
    /* Recover AllocTd & tuples */
    RedoSplitOnly(page, m_rawData, hikeyFrom);

    /* Set TD from new right insert if needed(details see BtreeSplit::GenerateSplitUndoAndWalRecord) */
    if (needSetTD) {
        const uint8 *tdID = static_cast<const uint8 *>(static_cast<const void *>(m_rawData));
        const uint64 *placeHolder = static_cast<const uint64 *>(static_cast<const void *>(m_rawData + sizeof(uint8)));
        page->SetTd(*tdID, xid, UndoRecPtr(*placeHolder), INVALID_CID);
    }
}

void WalRecordBtreeSplitLeaf::Describe(FILE *fp) const
{
    WalRecordBtreeSplit::DescSplit(fp);
    uint32 start = 0U;
    if (needSetTD) {
        const uint8 *tdID = static_cast<const uint8 *>(static_cast<const void *>(m_rawData));
        const uint64 *placeHolder = static_cast<const uint64 *>(static_cast<const void *>(m_rawData + sizeof(uint8)));
        ItemPointerData undoPtr(*placeHolder);
        (void)fprintf(fp, "td(%hhu), undoRecPtr(%hu, %u, %hu), ", *tdID, undoPtr.GetFileId(), undoPtr.GetBlockNum(),
                      undoPtr.GetOffset());
        start += sizeof(uint8) + sizeof(uint64);
    }
    const IndexTuple *tuple = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData + start));
    (void)fprintf(fp, "leftHikey{size(%u), info(%x)} ", tuple->GetSize(), tuple->m_info.m_info);
    start += tuple->GetSize();
    if (m_size > sizeof(WalRecordBtreeSplitLeaf) + start) {
        DescAllocTd(fp, m_rawData + start, m_size - (sizeof(WalRecordBtreeSplitLeaf) + start));
    }
}

void WalRecordBtreeSplitInsertInternal::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    UNUSE_PARAM uint8 tdId = RedoSplitInsert(page, m_insertOff, m_rawData);
    StorageAssert(tdId == INVALID_TD_SLOT);
}

void WalRecordBtreeSplitInsertInternal::Describe(FILE *fp) const
{
    WalRecordBtreeSplit::DescSplit(fp);
    const IndexTuple *hikey = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    (void)fprintf(fp, "leftHikey{size(%u), info(%x)} ", hikey->GetSize(), hikey->m_info.m_info);

    const IndexTuple *insert = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData + hikey->GetSize()));
    (void)fprintf(fp, "IndexTuple{size(%u), link(%lu): downlink((%hu, %u), keyNum(%hu), info(%u)} on offset(%hu)",
                insert->GetSize(), insert->m_link.heapCtid.m_placeHolder, insert->GetLowlevelIndexpageLink().m_fileId,
                insert->GetLowlevelIndexpageLink().m_blockId, insert->GetKeyNum(0), insert->m_info.m_info, m_insertOff);
}

void WalRecordBtreeSplitInsertLeaf::Redo(BtrPage *page, Xid xid) const
{
    /* Recover AllocTd & tuples */
    uint8 tdId = RedoSplitInsert(page, m_insertOff, m_rawData);
    /* Recover TD slot */
    if (likely(tdId != INVALID_TD_SLOT)) {
        page->SetTd(tdId, xid, UndoRecPtr(m_undoRecPtr), INVALID_CID);
    }
}

void WalRecordBtreeSplitInsertLeaf::Describe(FILE *fp) const
{
    WalRecordBtreeSplit::DescSplit(fp);
    const IndexTuple *hikey = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData));
    const ItemPointerData hikeyHeapCtid = hikey->GetHeapCtid();
    const ItemPointerData undoRecPtr = ItemPointerData(m_undoRecPtr);
    (void)fprintf(fp, "leftHikey{size(%u), heapCtid(%hu, %u, %u)}, tdId(%u), isDeleted(%u), sameWithLastLeft(%u)",
        hikey->GetSize(), hikeyHeapCtid.GetFileId(), hikeyHeapCtid.GetBlockNum(), hikeyHeapCtid.GetOffset(),
        hikey->m_info.val.m_tdId, hikey->m_info.val.m_isDeleted, hikey->m_info.val.m_isSameWithLastLeft);

    const IndexTuple *insert = static_cast<const IndexTuple *>(static_cast<const void *>(m_rawData + hikey->GetSize()));
    (void)fprintf(fp, "offset(%hu), IndexTuple{td(%hhu), size(%u), heapCtid(%hu, %u, %hu), info(%x)}, "
                "undoRecPtr(%hu, %u, %u)",
                m_insertOff, insert->GetTdId(), insert->GetSize(),  insert->GetHeapCtid().GetFileId(),
                insert->GetHeapCtid().GetBlockNum(), insert->GetLinkOffset(), insert->m_info.m_info,
                undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset());
    if (m_size > sizeof(WalRecordBtreeSplitInsertLeaf) + hikey->GetSize() + insert->GetSize()) {
        DescAllocTd(fp, m_rawData + hikey->GetSize() + insert->GetSize(),
                    m_size - (sizeof(WalRecordBtreeSplitInsertLeaf) + hikey->GetSize() + insert->GetSize()));
    }
}

void WalRecordBtreeNewRight::SetPage(BtrPage *page)
{
    uint32 total = m_size - sizeof(WalRecordBtreeNewRight);
    uint32 used = 0U;
    char *buf = static_cast<char *>(m_rawData);

    /* TD slots(if any) and ItemIds */
    CopyData(buf, total, page->GetData(), static_cast<uint32>(page->GetLower() - page->DataHeaderSize()));
    used += static_cast<uint32>(page->GetLower() - page->DataHeaderSize());

    /* record tuples and BtrLinksAndStatus */
    CopyData(buf + used, static_cast<uint32>(total - used),
             static_cast<char *>(static_cast<void *>(page)) + upper, static_cast<uint32>(BLCKSZ - upper));
    used += static_cast<uint32>((BLCKSZ - upper));

    btrCreatedXid = page->GetBtrMetaCreateXid();

    /* Check length */
    StorageAssert(used <= total);
    StorageAssert(buf + used <= static_cast<char *>(static_cast<void *>(this)) + m_size);
}

void WalRecordBtreeNewRight::Redo(BtrPage *page, UNUSE_PARAM Xid xid, UNUSE_PARAM bool forDump) const
{
    page->Reset(m_pageId);
    page->SetLower(lower);
    page->SetUpper(upper);
    page->m_header.m_flags = pageFlags;
    page->dataHeader.tdCount = tdCount;
    page->SetBtrMetaCreateXid(btrCreatedXid);

    /* TD slots(if any) and ItemIds */
    uint32 singleReadSize = static_cast<uint32>(lower - page->DataHeaderSize());
    CopyData(page->GetData(), singleReadSize, m_rawData, singleReadSize);
    uint32 readSize = singleReadSize;
    /* We have no idea if it's a leaf page for now. We'll check later */

    /* tuples and BtrLinksAndStatus */
    singleReadSize = static_cast<uint32>(BLCKSZ - upper);
    CopyData(static_cast<char *>(static_cast<void *>(page)) + upper, singleReadSize,
             m_rawData + readSize, singleReadSize);
    readSize += singleReadSize;

    /* Now we can check TD status */
    StorageReleasePanic(unlikely(page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE) && tdCount == 0),
        MODULE_INDEX, ErrMsg("Failed to redo new right leaf for no TD slots."));
    /* Check data size */
    StorageReleasePanic(unlikely(readSize + sizeof(WalRecordBtreeNewRight) != m_size), MODULE_INDEX,
        ErrMsg("Failed to redo new right. read %u, total %hu.", readSize, m_size));
    /* Check page status */
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    if (unlikely(!linkStat->TestType(BtrPageType::LEAF_PAGE) && !linkStat->TestType(BtrPageType::INTERNAL_PAGE))) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX,
            ErrMsg("Failed to redo new right. data damaged. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(linkStat)));
    }
}

void WalRecordBtreeNewRight::Describe(FILE *fp) const
{
    uint32 specialSize = MAXALIGN(sizeof(BtrPageLinkAndStatus));
    uint32 pageConentSize = (lower - RESERVED_BTR_PAGE_HEADER_SIZE) + (BLCKSZ - upper);
    char *linkAndStatusOffset = const_cast<char *>(m_rawData) + pageConentSize - specialSize;
    BtrPageLinkAndStatus *linkAndStatus = reinterpret_cast<BtrPageLinkAndStatus *>(linkAndStatusOffset);
    char *firstKeyOffset = const_cast<char *>(m_rawData) +
                           (tdCount * sizeof(TD)) + ((linkAndStatus->GetFirstDataOffset() - 1) * sizeof(ItemId));
    ItemId *firstItem = reinterpret_cast<ItemId *>(firstKeyOffset);
    if (likely(firstItem->GetOffset() >= upper && firstItem->GetOffset() < BLCKSZ - specialSize)) {
        char *firstDataOffset = const_cast<char *>(m_rawData) +
                                (lower - RESERVED_BTR_PAGE_HEADER_SIZE) + (firstItem->GetOffset() - upper);
        IndexTuple *firstData = reinterpret_cast<IndexTuple *>(firstDataOffset);
        (void)fprintf(fp, "lower(%hu), upper(%hu), tdCount(%hhu), Btr create xid: (%d, %lu), "
            "firstItem{flag(%u), offset(%hu), len(%hu)}, firstDataKey{size(%u), heapCtid(%hu, %u, %u)}, "
            "tdId(%u), isDeleted(%u), ins4del(%u), ccindexStatus(%u), sameWithLastLeft(%u)}, ",
            lower, upper, tdCount, static_cast<int32>(btrCreatedXid.m_zoneId), btrCreatedXid.m_logicSlotId,
            firstItem->GetFlags(), firstItem->GetOffset(), firstItem->GetLen(),
            firstData->GetSize(), firstData->GetHeapCtid().GetFileId(), firstData->GetHeapCtid().GetBlockNum(),
            firstData->GetHeapCtid().GetOffset(), firstData->GetTdId(), firstData->m_info.val.m_isDeleted,
            firstData->m_info.val.m_ccindexInsForDelFlag, firstData->m_info.val.m_ccindexStatus,
            firstData->m_info.val.m_isSameWithLastLeft);
    } else {
        (void)fprintf(fp, "lower(%hu), upper(%hu), tdCount(%hhu), Btr create xid: (%d, %lu) "
            "firstItem{flag(%u), offset(%hu), len(%hu)} damaged while dump! ",
            lower, upper, tdCount, static_cast<int32>(btrCreatedXid.m_zoneId), btrCreatedXid.m_logicSlotId,
            firstItem->GetFlags(), firstItem->GetOffset(), firstItem->GetLen());
    }
    (void)fprintf(fp, "LinkAndStatus{prev(%hu, %u), next(%hu, %u), level(%hu), stat(%u/%u/%u/%u)},",
        linkAndStatus->prev.m_fileId, linkAndStatus->prev.m_blockId,
        linkAndStatus->next.m_fileId, linkAndStatus->next.m_blockId,
        linkAndStatus->level, linkAndStatus->status.bitVal.type, linkAndStatus->status.bitVal.isRoot,
        linkAndStatus->status.bitVal.liveStat, linkAndStatus->status.bitVal.splitStat);
}

void WalRecordBtreeDeleteOnInternal::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);
    StorageReleasePanic(!page->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE), MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u) is not internal. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, BTR_PAGE_HEADER_VAL(page),
        BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));

    IndexTuple *indexTuple = page->GetIndexTuple(m_offset);
    indexTuple->SetDeleted();
    page->SetTuplePrunable(true);
}

void WalRecordBtreeDeleteOnInternal::Describe(FILE *fp) const
{
    (void)fprintf(fp, "offset(%hu).", m_offset);
}

void WalRecordBtreeDeleteOnLeaf::Redo(BtrPage *page, Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);
    StorageReleasePanic(!page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE), MODULE_INDEX,
        ErrMsg("[%s]page(%hu, %u) is not leaf. " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        __FUNCTION__, m_pageId.m_fileId, m_pageId.m_blockId, BTR_PAGE_HEADER_VAL(page),
        BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));

    RedoAllocTdWal(page, PageType::INDEX_PAGE_TYPE, m_rawData, m_size - sizeof(WalRecordBtreeDeleteOnLeaf));

    IndexTuple *indexTuple = page->GetIndexTuple(m_offset);
    indexTuple->SetDeleted();
    indexTuple->SetTdId(m_tdId);
    page->SetTuplePrunable(true);

    if (m_tdId != INVALID_TD_SLOT) {
        indexTuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
        page->SetTd(m_tdId, xid, UndoRecPtr(m_undoRecPtr), INVALID_CID);
    } else {
        indexTuple->SetTdStatus(DETACH_TD);
    }
}

void WalRecordBtreeDeleteOnLeaf::Describe(FILE *fp) const
{
    ItemPointerData delHeapCtid = ItemPointerData(m_heapCtid);
    (void)fprintf(fp, "offset(%hu), td(%hhu), undoRecPtr(%hu, %u, %hu), heapCtid(%hu, %u, %hu).", m_offset, m_tdId,
                  UndoRecPtr(m_undoRecPtr).GetFileId(), UndoRecPtr(m_undoRecPtr).GetBlockNum(),
                  UndoRecPtr(m_undoRecPtr).GetOffset(), delHeapCtid.GetFileId(), delHeapCtid.GetBlockNum(),
                  delHeapCtid.GetOffset());
    if (m_size == sizeof(WalRecordBtreeDeleteOnLeaf)) {
        return;
    }
    DescAllocTd(fp, m_rawData, m_size - sizeof(WalRecordBtreeDeleteOnLeaf));
}

uint32 WalRecordBtreePagePrune::SetData(const bool *isTupleLive, uint16 numTotalItemIds)
{
    pruneStatusDataLen = AppendData(0, static_cast<const char *>(static_cast<const void *>(isTupleLive)),
                         static_cast<uint32>(sizeof(bool)) * (numTotalItemIds + 1));
    return pruneStatusDataLen;
}

uint32 WalRecordBtreePagePrune::AppendData(uint32 fromPos, const char *srcData, uint32 srcSize)
{
    StorageAssert(fromPos + srcSize <= m_size);
    char *buf = static_cast<char *>(m_rawData) + fromPos;
    CopyData(buf, m_size - fromPos, srcData, srcSize);
    StorageAssert(buf + srcSize <= static_cast<char *>(m_rawData) + m_size);

    return fromPos + srcSize;
}

void WalRecordBtreePagePrune::Redo(PdbId pdbId, BtrPage *page, UNUSE_PARAM Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);
    const bool *isTupleLive = nullptr;
    uint32 offset = 0;
    uint8 origTdCount = page->GetTdCount();
    isTupleLive = static_cast<const bool *>(static_cast<const void *>(m_rawData + offset));
    offset += static_cast<uint32>(sizeof(bool)) * (page->GetMaxOffset() + 1);

    BtreePagePrune redoPrune(pdbId, page);
    redoPrune.SetFixedTdCount(fixedTdCount);
    RetStatus ret = redoPrune.SetLiveTupleCountAndAllocLiveItems(numLiveTuples);
    StorageReleasePanic(ret == DSTORE_FAIL, MODULE_INDEX, ErrMsg("Redo BtrPrune failed: %s", thrd->GetErrorMessage()));
    redoPrune.SetTupleLiveStatus(const_cast<bool *>(isTupleLive));

    /* Start to write page. No error is acceptable now! */
    RedoAllocTdWal(page, PageType::INDEX_PAGE_TYPE, m_rawData + offset,
                   m_size - (sizeof(WalRecordBtreePagePrune) + offset));
    if (fixedTdCount < origTdCount) {
        redoPrune.CompactTdSpace();
    }
    redoPrune.CompactItems();
    redoPrune.CompactTuples();

    page->SetTuplePrunable(prunable);
    StorageAssert(page->CheckSanity());

    /* To avoid repeated memory release. */
    redoPrune.SetTupleLiveStatus(nullptr);
}

void WalRecordBtreePagePrune::Describe(FILE *fp) const
{
    (void)fprintf(fp, "pagePrunable(%s), m_fixedTdCount(%hhu), m_numLiveTuples(%hu).",
                prunable ? "true" : "false", fixedTdCount, numLiveTuples);
    if (m_size == sizeof(WalRecordBtreePagePrune)) {
        return;
    }
    if (m_size > sizeof(WalRecordBtreePagePrune) + pruneStatusDataLen) {
        DescAllocTd(fp, m_rawData + pruneStatusDataLen,
                    m_size - (sizeof(WalRecordBtreePagePrune) + pruneStatusDataLen));
    } else {
        StorageAssert(m_size == sizeof(WalRecordBtreePagePrune) + pruneStatusDataLen);
    }
}

void WalRecordBtreeUpdateSibLink::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    StorageAssert(page->GetSelfPageId() == m_pageId);

    if (m_type == WAL_BTREE_UPDATE_LEFT_SIB_LINK) {
        linkStat->SetLeft(sibLinkPageId);
    } else {
        linkStat->SetRight(sibLinkPageId);
    }
}

void WalRecordBtreeUpdateSibLink::Describe(FILE *fp) const
{
    (void)fprintf(fp, "sibLink{%hu, %u}", sibLinkPageId.m_fileId, sibLinkPageId.m_blockId);
}

void WalRecordBtreeUpdateDownlink::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
    StorageAssert(page->GetSelfPageId() == m_pageId);

    IndexTuple *tuple = page->GetIndexTuple(tupleOffset);
    tuple->SetLowlevelIndexpageLink(downlinkPageId);
    if (linkStat->TestType(BtrPageType::LEAF_PAGE)) {
        linkStat->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    }
}
void WalRecordBtreeUpdateDownlink::Describe(FILE *fp) const
{
    (void)fprintf(fp, "offset(%hu), downlink(%hu, %u)", tupleOffset, downlinkPageId.m_fileId, downlinkPageId.m_blockId);
}

void WalRecordBtreeEraseInsForDelFlag::Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == m_pageId);

    IndexTuple *tuple = page->GetIndexTuple(tupleOffset);
    tuple->SetNotInsertDeletedForCCindex();
}

}