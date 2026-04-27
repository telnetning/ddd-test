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
 * dstore_tablespace_utils.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_tablespace_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TABLESPACE_UTILS_H
#define DSTORE_TABLESPACE_UTILS_H

#include "common/algorithm/dstore_ilist.h"
#include "common/memory/dstore_mctx.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

struct PageIdNode {
    dlist_node node;
    PageId pageId;
};

inline RetStatus AppendToPageIdList(dlist_head &pageIdsList, const PageId &pageId)
{
    PageIdNode *pageIdNode = static_cast<PageIdNode*>(DstorePalloc0(sizeof(PageIdNode)));
    if (pageIdNode == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to alloc PageIdNode, pageId (%hu, %u).", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    pageIdNode->pageId = pageId;
    DListPushTail(&pageIdsList, &pageIdNode->node);
    return DSTORE_SUCC;
}

inline void TransferPageIdListToArray(dlist_head *pageIdsList, PageId *resultArr)
{
    Size index = 0;
    dlist_iter iter;
    dlist_foreach(iter, pageIdsList)
    {
        PageIdNode *pageIdNode = dlist_container(PageIdNode, node, iter.cur);
        resultArr[index].m_fileId = pageIdNode->pageId.m_fileId;
        resultArr[index].m_blockId = pageIdNode->pageId.m_blockId;
        DstorePfreeExt(pageIdNode);
        index += 1;
    }
}

inline RetStatus GetPagesFromPageIdList(dlist_head *pageIdsList, PageId **pageIds, Size *length, char **errInfo)
{
    dlist_iter iter;
    Size numOfPages = 0;
    StringInfoData dumpInfo;
    PageId *resultArr = nullptr;
    dlist_foreach(iter, pageIdsList)
    {
        numOfPages += 1;
    }

    StorageAssert(numOfPages != 0);
    resultArr = static_cast<PageId*>(DstorePalloc0(sizeof(PageId) * numOfPages));
    if (resultArr == nullptr) {
        *length = 0;    /* no output */
        if (unlikely(!dumpInfo.init())) {
            ErrLog(DSTORE_ERROR, MODULE_PAGE, ErrMsg("cannot allocate memory for dump info."));
            return DSTORE_FAIL;
        }
        dumpInfo.append("Allocating result array for page ids failed.");
        *errInfo = dumpInfo.data;
        return DSTORE_FAIL;
    }

    TransferPageIdListToArray(pageIdsList, resultArr);

    *length = numOfPages;
    *pageIds = resultArr;
    return DSTORE_SUCC;
}
} /* namespace DSTORE */
#endif