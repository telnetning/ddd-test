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
#ifndef DSTORE_UT_BTREE_PRUNE_H
#define DSTORE_UT_BTREE_PRUNE_H

#include "index/dstore_btree_prune.h"

using namespace DSTORE;

class UTBtreePrune : public BtreePagePrune {
public:
    UTBtreePrune(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scankeyInfo, BufferDesc *buf)
        : BtreePagePrune(indexRel, indexInfo, scankeyInfo, buf)
    {}

    void SetPage(BtrPage *page)
    {
        StorageAssert(page != nullptr);
        if (m_pagePayload.page != nullptr) {
            DstorePfree(m_pagePayload.page);
        }
        m_pagePayload.page = page;
        m_pagePayload.linkAndStatus = page->GetLinkAndStatus();
        Init();
    }
};

#endif
