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
 * dstore_undo_mgr_diagnose.h
 *
 * Description:
 * Provide diagnose function for undo page and slot status
 * ---------------------------------------------------------------------------------------
 *
 */

#ifndef DSTORE_UNDO_MGR_DIAGNOSE_H
#define DSTORE_UNDO_MGR_DIAGNOSE_H

#include <cstdint>
#include "common/dstore_common_utils.h"

namespace DSTORE {
#pragma GCC visibility push(default)
class UndoMgrDiagnose {
public:
    static char* GetUndoZoneInfoByZid(PdbId pdbId, int32 zoneId);

    static char* GetUndoZoneStatusByZid(PdbId pdbId, int32 zoneId);
    static char *ReadUndoRecord(char *page, int startingByte, ItemPointerData &nextPtr,
                                std::function<void(PageId &, char *, int, void *)> readPageFunc, void *readPageArg);

    static char* GetUndoSlotStatusByXid(PdbId pdbId, int128 transactionId);

    static char* GetUndoPageHeader(PdbId pdbId, int32 fileId, int64 blockNum);
};

#pragma GCC visibility pop
}  /* namespace DSTORE */
#endif  /* DSTORE_UNDO_MGR_DIAGNOSE_H */