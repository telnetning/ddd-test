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
 * ---------------------------------------------------------------------------------
 *
 * dstore_undo_fault_injection.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/dstore_datatype.h"
#include "common/fault_injection/dstore_undo_fault_injection.h"

namespace DSTORE {

void SetUndoZoneFreePageNumZero(UNUSE_PARAM const FaultInjectionEntry *entry, PageId &needCheckPageId,
                                const PageId recyclePageId)
{
    needCheckPageId.m_fileId = recyclePageId.m_fileId;
    needCheckPageId.m_blockId = recyclePageId.m_blockId;
}

}  // namespace DSTORE