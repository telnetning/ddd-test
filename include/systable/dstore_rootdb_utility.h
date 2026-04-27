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
 * Description:
 * The dstore_rootdb_utility.h create all bootstrap systables and insert buildin datas to them.
 */

#ifndef DSTORE_ROOTDB_UTILITY
#define DSTORE_ROOTDB_UTILITY

#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "tuple/dstore_memheap_tuple.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_scan.h"
#include "systable/dstore_systable_struct.h"
#include "systable/systable_callback_param.h"

namespace DSTORE {
struct SysTypeTupDefCache;
struct SysTablePageIdMap;

struct BootStrapSystable {
    SysTableCreateStmt createStmt;
    const char **presetDataRows;
    int presetDataRowCnt;
    bool hasOid;
};
class RootDBUtility : public BaseObject {
public:
    RootDBUtility() : m_insertfunc(nullptr) {}
    DISALLOW_COPY_AND_MOVE(RootDBUtility);
    virtual ~RootDBUtility() {};
    RetStatus DropRootPDB() const;
    RetStatus CopyTemplateDatabase(PdbId pdbId, SysTablePageIdMap *newPdbPageArray, TablespaceId tblSpcId);
    static void PreAllocPdbIdWhenBootstrap(PdbId templatePdbId);
#ifndef UT
private:
#endif
    PreSetFunc m_insertfunc = nullptr;
    CountFunc m_countfunc = nullptr;
    BuildRelCache m_buildrelcache = nullptr;

    void LoadSysType(void);
};
}  // namespace DSTORE

#endif
