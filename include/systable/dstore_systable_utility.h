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
 * The storage_systable_utility difines all management operations on system tables.
 */

#ifndef DSTORE_SYSTABLE_UTILITY_H
#define DSTORE_SYSTABLE_UTILITY_H

#include "systable/dstore_systable_struct.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "systable/dstore_relation.h"
#include "systable/systable_type.h"
#include "systable/systable_func.h"
#include "index/dstore_index_struct.h"
#include "lock/dstore_lock_struct.h"
#include "systable/systable_type.h"

namespace DSTORE {
constexpr int PG_CATALOG_NAMESPACE = 11;
constexpr int BOOTSTRAP_SUPERUSERID = 10;
constexpr Oid BOOTSTRAP_SYSTABLE_OIDS[] = {SYSTABLE_RELATION_OID, SYSTABLE_ATTRIBUTE_OID, SYSTABLE_TYPE_OID,
                                           SYSTABLE_FUNC_OID};
constexpr int BOOTSTRAP_SYSTABLE_CNT = sizeof(BOOTSTRAP_SYSTABLE_OIDS) / sizeof(BOOTSTRAP_SYSTABLE_OIDS[0]);
constexpr int MAX_HEAP_ATTR_NUMBER = 1600;

struct SysTypeTupDef;
struct SysTablePageIdMap;
TupleDesc CreateTupleDesc(uint natts, bool hasOid, const SysAttributeTupDef *col);
TupleDesc CreateTemplateTupleDesc(int attrNum, bool hasOid);
TupleDesc CreateLobTupleDesc();
RetStatus LockRelationId(PdbId pdbId, Oid relOid, LockMode mode);
void UnlockRelationId(PdbId pdbId, Oid relOid, LockMode mode);
class SystableUtility : public BaseObject {
public:
    SystableUtility();
    DISALLOW_COPY_AND_MOVE(SystableUtility);
    ~SystableUtility() = default;

    struct SysCompositeType : public BaseObject {
        dlist_node nodeInList;
        Oid typid;
        bool operator==(const SysCompositeType &type) const
        {
            return typid == type.typid;
        }
        bool operator!=(const SysCompositeType &type) const
        {
            return typid == type.typid;
        }
        SysCompositeType()
        {
            DListNodeInit(&nodeInList);
            typid = DSTORE_INVALID_OID;
        }
        ~SysCompositeType() = default;
        dlist_node *GetNodeInList()
        {
            return &nodeInList;
        }
        SysCompositeType *GetSystableUtilityFromNodeInList(dlist_node *node) const
        {
            return static_cast<SysCompositeType *>(dlist_container(SysCompositeType, nodeInList, node));
        }
    };

    /*
     * Create core system table.
     * Note:
     *    The core system table is the system table required by the DStore.
     */
    static RetStatus CreateBootStrapSystable(PdbId pdbId, Oid relOid, TablespaceId tableSpaceId, PageId &segmentId);

    /*
     * Get segmentId for bootstrap systable.
     */
    static RetStatus GetBootSystableSegmentId(PdbId pdbId, Oid sysTableOid, DSTORE::PageId &segmentId);

    static RetStatus AddRelationMap(PdbId pdbId, Oid relOid, PageId &segmentId);

private:
    dlist_head m_comtype;
    SysCompositeType sysCompositeType;
};

}  // namespace DSTORE

#endif