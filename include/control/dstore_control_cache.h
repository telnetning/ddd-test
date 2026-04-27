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
 * dstore_control_cache.h
 *  control file cache for dstore
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_cache.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_CACHE_H
#define DSTORE_CONTROL_CACHE_H

#include "dstore_control_struct.h"
#include "dstore_control_file.h"

namespace DSTORE {

using InvalidCallback = RetStatus (*)(ControlFile *configFile, const void *privateData, uint32 len);

struct ControlCacheInvalidHandle {
    ControlGroupType groupType;
    InvalidCallback callback;
};

struct ControlCacheInvalidCtx {
    BlockNumber *dirtyBlocks;
    BlockNumber blockNum;
    PdbId pdbId;
    void *privateData;
    uint32 privateDataLen;
    BlockNumber pageCount;
};

class ControlCache : public BaseObject {
public:
    ControlCache();
    ~ControlCache();
};
} // namespace DSTORE
#endif // DSTORE_CONTROL_CACHE_H