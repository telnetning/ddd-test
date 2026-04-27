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
 * dstore_index_diagnose.h
 *
 * IDENTIFICATION
 *        dstore/interface/diagnose/dstore_index_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_INDEX_DIAGNOSE_H
#define DSTORE_DSTORE_INDEX_DIAGNOSE_H

#include "common/dstore_common_utils.h"

namespace DSTORE {

#pragma GCC visibility push(default)
class IndexDiagnose {
public:
    static char *PrintIndexInfo(PdbId pdbId, FileId fileId, BlockNumber blockId);
};
#pragma GCC visibility pop

}  // namespace DSTORE
#endif  // DSTORE_STORAGE_INDEX_DIAGNOSE_H
