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
#ifndef UT_TABLESPACE_MOCK_H
#define UT_TABLESPACE_MOCK_H

#include "tablespace/dstore_tablespace.h"

namespace DSTORE {
class MockTableSpace : public TableSpaceInterface {
public:
    RetStatus FreeExtent(ExtentSize extent_size, const PageId& page_id) override
    {
        m_current_page.m_blockId -= extent_size;
        return DSTORE_SUCC;
    }

    RetStatus AllocExtent(ExtentSize extent_size, PageId *new_extent, bool *isReUseFlag, bool *continueTryAlloc) override
    {
        PageId head_page = m_current_page;
        m_current_page.m_blockId += extent_size;
        if (m_current_page.m_blockId > 100000000) {
            m_current_page.m_fileId += 1;
            m_current_page.m_blockId = 1;
        }
        *new_extent = head_page;
        return DSTORE_SUCC;
    }

    ~MockTableSpace() override = default;

protected:
    MockTableSpace(TablespaceId tablespaceId, BufMgrInterface *bufMgr) : m_bufMgr(bufMgr)
    {
        m_current_page.m_fileId = tablespaceId;
        m_current_page.m_blockId = 1;
    }

private:
    PageId m_current_page{};
    BufMgrInterface *m_bufMgr;
    friend class UtMockModule;
};
};  // namespace DSTORE

#endif
