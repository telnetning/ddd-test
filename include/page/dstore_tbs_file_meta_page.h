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
 * dstore_tbs_file_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_tbs_file_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_TBS_FILE_META_PAGE_H
#define DSTORE_DSTORE_TBS_FILE_META_PAGE_H

#include "control/dstore_control_file.h"
#include "page/dstore_page.h"
#include "wal/dstore_wal_struct.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

struct TbsFileMetaPage : public Page {
public:
    uint32 hwm;  /* high watermark in the file, the pages before it have been allocated at least one times. */
    WalId prevShrinkWalId;
    uint64 pageBaseGlsn;
    /*
     * The first OID to assign in the next batch. (Allocate DSTORE_PREFETCH_OID_COUNT per batch.)
     * Only the OID in the metapage of the system tablespace gs_catalog is meaningful.
     * The OID in the metapage of other tablespaces is meaningless and is always the initial value
     * FIRST_BOOTSTRAP_OBJECT_ID.
     */
    uint32 oid;
    uint64 m_reuseVersion; /* indicates the reused version of the fileId */
    Xid m_ddlXid;          /* the xid of the transaction that created the file */
    uint8 reserved[128];

    void InitTbsFileMetaPage(const PageId &tbsFileMetaPageId, uint64 reuseVersion, Xid ddlXid)
    {
        Page::Init(0, PageType::TBS_FILE_META_PAGE_TYPE, tbsFileMetaPageId);
        hwm = 0;
        prevShrinkWalId = INVALID_WAL_ID;
        pageBaseGlsn = INVALID_WAL_GLSN;
        oid = FIRST_BOOTSTRAP_OBJECT_ID;
        m_reuseVersion = reuseVersion;
        m_ddlXid = ddlXid;
    }

    char *Dump()
    {
        StringInfoData str;
        str.init();
        Page::DumpHeader(&str);
        str.append("File Meta Page Info\n");
        str.append("  hwm = %u \n", hwm);
        str.append("  prevShrinkWalId = %lu \n", prevShrinkWalId);
        str.append("  pageBaseGlsn = %lu \n", pageBaseGlsn);
        str.append("  oid = %u \n", oid);
        str.append("  reuseVersion = %lu \n", m_reuseVersion);
        str.append("  ddlXid = %lu \n", m_ddlXid.m_placeHolder);
        return str.data;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(TbsFileMetaPage);

static_assert(sizeof(TbsFileMetaPage) <= BLCKSZ);

}  // namespace DSTORE
#endif  // DSTORE_DSTORE_TBS_FILE_META_PAGE_H
