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
 * dstore_heap_diagnose.h
 *
 * IDENTIFICATION
 *        dstore/interface/diagnose/dstore_heap_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_HEAP_DIAGNOSE_H
#define DSTORE_DSTORE_HEAP_DIAGNOSE_H

#include "common/dstore_common_utils.h"
#include "diagnose/dstore_diagnose.h"
#include "page/dstore_page_struct.h"
#include "systable/dstore_relation.h"

namespace DSTORE {

#pragma GCC visibility push(default)

struct PageFreespace : public DiagnoseItem {
public:
    PageId m_pageId;        /* PageId */
    uint16_t m_freespace;   /* page's freespace in bytes */
    uint16_t m_spaceline;   /* page's freespace line */

    PageFreespace() = default;

    void SetPageId(PageId pageId)
    {
        m_pageId = pageId;
    }

    PageId GetPageId()
    {
        return m_pageId;
    }

    void SetFreespace(uint16_t freespace)
    {
        m_freespace = freespace;
    }

    uint16_t GetFreespace()
    {
        return m_freespace;
    }

    void SetSpaceline(uint16_t spaceline)
    {
        m_spaceline = spaceline;
    }

    uint16_t GetSpaceline()
    {
        return m_spaceline;
    }
};

class HeapDiagnose {
public:
    static PageFreespace *GetPageFreespace(PdbId pdbId, FileId fileId, BlockNumber blockNumber);
    static DiagnoseIterator *GetHeapFreespaceByFsm(StorageRelation storageRel);
};

#pragma GCC visibility pop

}  // namespace DSTORE
#endif  // DSTORE_DSTORE_HEAP_DIAGNOSE_H
