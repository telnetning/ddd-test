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

#ifndef DSTORE_CHECKPOINTER_DIAGNOSE
#define DSTORE_CHECKPOINTER_DIAGNOSE

#include "common/dstore_common_utils.h"

namespace DSTORE {

#pragma GCC visibility push(default)
class CheckpointMgr;

class CheckpointerDiagnose {
public:
    explicit CheckpointerDiagnose(uint32_t pdbId);
    ~CheckpointerDiagnose() = default;

    char *GetLocalCheckpointStatInfo();
    char *GetGlobalCheckpointStatInfo();

private:
    CheckpointMgr *m_checkpointMgr = nullptr;
    uint32_t m_pdbId = INVALID_PDB_ID;
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif
