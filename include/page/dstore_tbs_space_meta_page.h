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
 * dstore_tbs_space_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_tbs_space_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_TBS_SPACE_META_PAGE_H
#define DSTORE_DSTORE_TBS_SPACE_META_PAGE_H

#include "control/dstore_control_file.h"
#include "page/dstore_page.h"
#include "wal/dstore_wal_struct.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

struct TbsSpaceMetaPage : public Page {
public:
    uint8 pageVersion;

    void InitTbsSpaceMetaPage(const PageId &tbsSpaceMetaPageId)
    {
        Page::Init(0, PageType::TBS_SPACE_META_PAGE_TYPE, tbsSpaceMetaPageId);
        pageVersion = 0;
    }

    char *Dump()
    {
        StringInfoData str;
        str.init();
        Page::DumpHeader(&str);
        str.append("Space Meta Page Info\n");
        str.append("  version = %u \n", pageVersion);
        return str.data;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(TbsSpaceMetaPage);

static_assert(sizeof(TbsSpaceMetaPage) <= BLCKSZ);

}  // namespace DSTORE
#endif  // DSTORE_DSTORE_TBS_SPACE_META_PAGE_H