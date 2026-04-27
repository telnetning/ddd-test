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
#ifndef DSTORE_UT_TABLESPACE_UTIL_H
#define DSTORE_UT_TABLESPACE_UTIL_H

#include "tablespace/dstore_tablespace.h"

namespace DSTORE {

const uint64 UT_MAX_FILE_SIZE = (uint64)1024 * 1024 * 1024 * 128;
const uint64 UT_INITIAL_FILE_SIZE = (uint64)1024 * 8 * (BITMAP_PAGES_PER_GROUP + 3);

class UtTbsUtil {
public:
    static TableSpaceInterface *UtGetTablespace(BufMgrInterface *bufMgr, TablespaceId tbsId,
                                                LockMode lockMode = DSTORE::DSTORE_NO_LOCK)
    {
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        return (TableSpaceInterface *)tablespaceMgr->OpenTablespace(tbsId, lockMode);
    }

    static void UtDropTablespace(TableSpaceInterface *tablespace, LockMode lockMode = DSTORE::DSTORE_NO_LOCK)
    {
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        tablespaceMgr->CloseTablespace((TableSpace *)tablespace, lockMode); 
    }

    static uint64 ConvertPageId(const PageId &pageId)
    {
        return ((uint64)pageId.m_fileId << 32) + pageId.m_blockId;
    }
    static bool CheckTwoSetHasSameElement(const std::set<uint64> &set1, const std::set<uint64> &set2)
    {
        std::set<uint64>::iterator it;
        for (it = set1.begin(); it != set1.end(); it++) {
            if (set2.find(*it) != set2.end()) {
                return true;
            }
        }
        return false;
    }
};

} /* namespace DSTORE */

#endif //DSTORE_UT_TABLESPACE_UTIL_H
