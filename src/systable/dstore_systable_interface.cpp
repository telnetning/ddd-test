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
 * dstore_systable_interface.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/systable/dstore_systable_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "systable/dstore_systable_interface.h"
#include "systable/dstore_systable.h"
#include "systable/dstore_systable_utility.h"
#include "catalog/dstore_typecache.h"
#include "common/dstore_common_utils.h"
#include "transaction/dstore_transaction_interface.h"
#include "transaction/dstore_transaction.h"
#include "tuple/dstore_memheap_tuple.h"
#include "tuple/dstore_tuple_interface.h"
#include "systable/systable_attribute.h"
#include "framework/dstore_instance.h"

using namespace DSTORE;
namespace SystableInterface {
/*
 * Scan sys_relation.
 * Note:
 *    If successed, the return value needs to be free..
 */
HeapTuple *ScanSysRelation(StorageRelation sysRel, Oid targetRelOid)
{
    if (unlikely(!DstoreRelationIsValid(sysRel))) {
        return nullptr;
    }
    /* Free the key in EndScan. */
    ScanKey key = HeapInterface::CreateScanKey(1);
    if (unlikely(key == nullptr)) {
        return nullptr;
    }

    g_storageInstance->GetCacheHashMgr()->GenerateScanKey(
        SYS_OIDOID, targetRelOid, DSTORE::SCAN_ORDER_EQUAL, key,
        static_cast<AttrNumber>(HeapTupleSystemAttr::DSTORE_OBJECT_ID_ATTRIBUTE_NUMBER));

    (void)TransactionInterface::SetSnapShot(true);

    /* Prepare the scan here */
    Systable sysTable(sysRel);
    Snapshot snapshot = thrd->GetActiveTransaction()->GetSnapshot();
    auto scan = sysTable.BeginScan(1, key, snapshot);
    if (unlikely(scan == nullptr)) {
        return nullptr;
    }

    /* Do the scan now */
    HeapTuple *sysRelTuple = sysTable.GetNext(scan);
    /* Must copy tuple before releasing buffer. */
    if (sysRelTuple != nullptr) {
        sysRelTuple = TupleInterface::CopyHeapTuple(sysRelTuple, nullptr);
        if (sysRelTuple == nullptr) {
            sysTable.EndScan(scan);
            ErrLog(DSTORE_ERROR, MODULE_CATALOG, ErrMsg("CopyHeapTuple fail when scan sys relation."));
            return nullptr;
        }
        StorageAssert(targetRelOid == sysRelTuple->GetOid());
    }

    /* End scan. Release resources */
    sysTable.EndScan(scan);
    return sysRelTuple;
}

DSTORE::RetStatus GetCoreSystableSegmentId(PdbId pdbId, Oid sysTableOid, DSTORE::PageId &segmentId)
{
    if (unlikely(sysTableOid == DSTORE_INVALID_OID)) {
        return RetStatus::DSTORE_FAIL;
    }
    return SystableUtility::GetBootSystableSegmentId(pdbId, sysTableOid, segmentId);
}

DSTORE::RetStatus CreateCoreSystable(PdbId pdbId, Oid relOid, TablespaceId tableSpaceId, PageId &segmentId)
{
    if (unlikely(relOid == DSTORE_INVALID_OID || tableSpaceId == DSTORE_INVALID_OID)) {
        return RetStatus::DSTORE_FAIL;
    }
    return SystableUtility::CreateBootStrapSystable(pdbId, relOid, tableSpaceId, segmentId);
}

DSTORE::RetStatus AddRelationMap(PdbId pdbId, Oid relOid, PageId &segmentId)
{
    if (unlikely(relOid == DSTORE_INVALID_OID)) {
        return RetStatus::DSTORE_FAIL;
    }
    return SystableUtility::AddRelationMap(pdbId, relOid, segmentId);
}

DSTORE::Oid GetNewObjectId(DSTORE::PdbId pdbId, bool isInitDb, bool isInplaceUpgrade)
{
    DSTORE::Oid oid = DSTORE::DSTORE_INVALID_OID;
    DSTORE::StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_FRAMEWORK, ErrMsg("Internal error, get pdb(%u) failed.", pdbId));
    
    RetStatus ret = pdb->GetNewObjectId(isInitDb, oid, isInplaceUpgrade);
    StorageReleasePanic(ret != RetStatus::DSTORE_SUCC, MODULE_FRAMEWORK,
                        ErrMsg("Internal error, failed to allocate new oid for pdb(%u).", pdbId));
    return oid;
}
}  // namespace SystableInterface