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
 * dstore_index_diagnose.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_index_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "diagnose/dstore_index_diagnose.h"
#include "buffer/dstore_buf_mgr.h"
#include "common/algorithm/dstore_string_info.h"
#include "common/memory/dstore_mctx.h"
#include "common/memory/dstore_memory_allocator.h"
#include "framework/dstore_instance.h"
#include "index/dstore_btree.h"
#include "page/dstore_index_page.h"
#include "table/dstore_table_interface.h"
#include "transaction/dstore_csn_mgr.h"

namespace DSTORE {

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
static const char *BtrPageRecycleOperTypeText[] = {
    [BTR_PUT_INTO_PENDING_QUEUE] = "Put into Pending Queue",
    [BTR_GET_FROM_PENDING_QUEUE] = "Get from Pending Queue",
    [BTR_PUT_INTO_FREE_QUEUE] = "Put into Free Queue",
    [BTR_GET_FROM_FREE_QUEUE] = "Get from Free Queue"
};

static const char *BtrPageRecycleFailReasonText[] = {
    [BTR_PAGE_NOT_EMPTY] = "Not Empty",
    [BTR_PAGE_DEL_NOT_VIS_TO_ALL] = "Delete Status Not Visible To All",
    [BTR_PAGE_LEFT_SIB_SPLITTING] = "Left Sib is Splitting",
    [BTR_PAGE_LEFT_SIB_CHANGED] = "Left Sib is Changed",
    [BTR_PAGE_RIGHT_SIB_UNLINKED] = "Right Sib is Unlinked",
    [BTR_GET_PARENT_FAIL] = "Get Parent Fail",
    [BTR_PAGE_RIGHTMOST_CHILD_OF_PARENT] = "Rightmost Child of Parent",
    [BTR_PAGE_PIVOT_CHANGED] = "Pivot is Changed",
    [BTR_DEL_PIVOT_FAIL] = "Delete Pivot Tuple Fail",
    [BTR_PAGE_RECYCLED_BY_OTHERS] = "Recycled by Others"
};
#endif

char *IndexDiagnose::PrintIndexInfo(PdbId pdbId, FileId fileId, BlockNumber blockId)
{
    StringInfoData dumpInfo;
    if (!dumpInfo.init()) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate memory for dumpInfo"));
        return nullptr;
    }

    BufMgrInterface* bufMgr = g_storageInstance->GetBufferMgr();
    PageId metaPageId = {fileId, blockId + NUM_PAGES_FOR_SEGMENT_META};
    BufferDesc *metaBuf = bufMgr->Read(pdbId, metaPageId, LW_SHARED);
    if (unlikely(metaBuf == INVALID_BUFFER_DESC)) {
        dumpInfo.append("Failed to read Btree Index Meta.");
        return dumpInfo.data;
    }
    BtrPage *metaPage = static_cast<BtrPage *>(metaBuf->GetPage());
    if (unlikely(!metaPage->TestType(PageType::INDEX_PAGE_TYPE)) ||
        unlikely(!metaPage->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE))) {
        bufMgr->UnlockAndRelease(metaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        dumpInfo.append("Invalid File ID or Block Number for Index Segment.");
        return dumpInfo.data;
    }
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));

    dumpInfo.append("Meta page id: {%hu, %u}\n", metaPageId.m_fileId, metaPageId.m_blockId);
    dumpInfo.append("Root page id: {%hu, %u}, ", btrMeta->rootPage.m_fileId, btrMeta->rootPage.m_blockId);
    dumpInfo.append("level: %u\n", btrMeta->rootLevel);
    dumpInfo.append("Fast root page id: {%hu, %u}, ",
                    btrMeta->lowestSinglePage.m_fileId, btrMeta->lowestSinglePage.m_blockId);
    dumpInfo.append("level: %u \n", btrMeta->lowestSinglePageLevel);
    dumpInfo.append("key atts num: %d, atts num: %d\n", btrMeta->GetNkeyatts(), btrMeta->GetNatts());
    dumpInfo.append("relKind: %c, tableOidAtt: %d\n", btrMeta->GetRelKind(), btrMeta->GetTableOidAtt());
    for (int i = 0; i < btrMeta->GetNatts(); i++) {
        dumpInfo.append(" %d Att type oid: %u\n", i + 1, btrMeta->GetAttTypids(i));
    }

    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        dumpInfo.append("Split times in each level:\n");
        for (uint32 level = 0; level <= btrMeta->rootLevel; level++) {
            dumpInfo.append("  level %u: split %lu times when build, %lu times when insert \n", level,
                            btrMeta->operCount[static_cast<int>(BtreeOperType::BTR_OPER_SPLIT_WHEN_BUILD)][level],
                            btrMeta->operCount[static_cast<int>(BtreeOperType::BTR_OPER_SPLIT_WHEN_INSERT)][level]);
        }
        dumpInfo.append("Recycle times in each level:\n");
        for (uint32 level = 0; level <= btrMeta->rootLevel; level++) {
            dumpInfo.append("  level %u: mark recyclable %lu times, recycled %lu times\n", level,
                            btrMeta->operCount[static_cast<int>(BtreeOperType::BTR_OPER_MARK_RECYCLABLE)][level],
                            btrMeta->operCount[static_cast<int>(BtreeOperType::BTR_OPER_RECYCLED)][level]);
        }
    }

    bufMgr->UnlockAndRelease(metaBuf, BufferPoolUnlockContentFlag::DontCheckCrc());

    return dumpInfo.data;
}
}
