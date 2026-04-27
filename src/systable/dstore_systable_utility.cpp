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
 * The dstore_systable_utility.cpp implements the management operation interface of the systable.
 */

#include "systable/dstore_systable_utility.h"
#include "common/log/dstore_log.h"
#include "common/error/dstore_error.h"
#include "control/dstore_control_file.h"
#include "errorcode/dstore_systable_error_code.h"
#include "page/dstore_heap_page.h"
#include "tablespace/dstore_heap_segment.h"
#include "systable/systable_relation.h"
#include "catalog/dstore_fake_relation.h"
#include "systable/systable_type.h"
#include "systable/systable_attribute.h"
#include "systable/systable_func.h"
#include "systable/sys_database.h"
#include "catalog/dstore_fake_class.h"
#include "tuple/dstore_tuple_struct.h"
#include "catalog/dstore_fake_type.h"
#include "lock/dstore_lock_struct.h"
#include "systable/systable_bootstrap_buildin_data.h"
#include "lock/dstore_lock_datatype.h"
#include "common/dstore_datatype.h"
#include "table/dstore_table_interface.h"
#include "systable/dstore_rootdb_utility.h"
#include "systable/systable_index.h"
#include "lock/dstore_lock_interface.h"


namespace DSTORE {

SystableUtility::SystableUtility()
{
    DListInit(&m_comtype);
}

RetStatus SystableUtility::CreateBootStrapSystable(PdbId pdbId, Oid relOid,
                                                   TablespaceId tableSpaceId, PageId &segmentId)
{
    if (g_storageInstance->GetPdb(pdbId) == nullptr) {
        return DSTORE_FAIL;
    }
    ControlFile *controlFile = g_storageInstance->GetPdb(pdbId)->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_SYSTABLE, ErrMsg("internal error, can not get control file"));

    UNUSE_PARAM PageId pageId = INVALID_PAGE_ID;
    if (controlFile->GetSysTableItem(relOid, pageId) == DSTORE_SUCC) {
        storage_set_error(SYSTABLE_ERROR_BOOTSTRAP_TALBE_EXISTS, relOid);
        return DSTORE_FAIL;
    }

    /* step1: allocate segment for this bootStrap systable */
    segmentId = HeapNormalSegment::AllocHeapNormalSegment(pdbId, tableSpaceId, g_storageInstance->GetBufferMgr());
    StorageReleasePanic(segmentId == INVALID_PAGE_ID, MODULE_SYSTABLE,
        ErrMsg("internal error, can not get control file"));

    /* step2: save segment id to control file */
    return controlFile->AddSysTableItem(relOid, segmentId);
}

TupleDesc CreateTupleDesc(uint natts, bool hasOid, const SysAttributeTupDef *col)
{
    Size descSize = MAXALIGN(sizeof(TupleDescData) + natts * sizeof(SysAttributeTupDef *));
    Size attrsSize = natts * MAXALIGN(sizeof(SysAttributeTupDef));
    char *offset = (char *)DstorePalloc0(descSize + attrsSize);
    if (unlikely(offset == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to allocate memory for desc."));
        return nullptr;
    }
    TupleDesc desc = static_cast<TupleDesc>(static_cast<void *>(offset));

    desc->natts = static_cast<int16_t>(natts);
    desc->tdisredistable = false;
    desc->attrs = static_cast<SysAttributeTupDef **>(static_cast<void *>(offset + sizeof(TupleDescData)));
    desc->initdefvals = nullptr;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasOid;
    desc->tdrefcount = -1;
    desc->tdhasuids = false;

    offset += descSize;
    for (uint i = 0; i < natts; i++) {
        desc->attrs[i] = static_cast<SysAttributeTupDef*>(static_cast<void *>(offset));
        offset += MAXALIGN(static_cast<uint32>(sizeof(SysAttributeTupDef)));
        *desc->attrs[i] = col[i];
    }
    return desc;
}

TupleDesc CreateTemplateTupleDesc(int attrNum, bool hasOid)
{
    Size descSize = MAXALIGN(sizeof(TupleDescData) + static_cast<uint32>(attrNum) * sizeof(SysAttributeTupDef));
    Size attrsSize = static_cast<uint32>(attrNum) * MAXALIGN(sizeof(SysAttributeTupDef));
    char *offset = (char *)DstorePalloc0(descSize + attrsSize);
    if (unlikely(offset == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("Failed to allocate memory for desc."));
        return nullptr;
    }
    TupleDesc desc = static_cast<TupleDesc>(static_cast<void *>(offset));

    desc->natts = attrNum;
    desc->tdisredistable = false;
    desc->attrs = static_cast<SysAttributeTupDef **>(static_cast<void *>(offset + sizeof(TupleDescData)));
    desc->initdefvals = nullptr;
    desc->tdtypeid = RECORDOID;
    desc->tdtypmod = -1;
    desc->tdhasoid = hasOid;
    desc->tdrefcount = -1;
    desc->tdhasuids = false;
    desc->tdhaslob = false;

    offset += descSize;
    for (int i = 0; i < attrNum; i++) {
        desc->attrs[i] = static_cast<SysAttributeTupDef*>(static_cast<void *>(offset));
        offset += MAXALIGN(static_cast<uint32>(sizeof(SysAttributeTupDef)));
    }
    return desc;
}

TupleDesc CreateLobTupleDesc()
{
    TupleDesc tupDesc = CreateTemplateTupleDesc(1, false);
    if (unlikely(tupDesc == nullptr)) {
        return nullptr;
    }
    Form_pg_attribute lobAttr = tupDesc->attrs[0];
    lobAttr->attrelid = 0;
    errno_t rc = strcpy_s(lobAttr->attname.data, NAME_DATA_LEN, "lob_value");
    storage_securec_check(rc, "\0", "\0");
    lobAttr->atttypid = SYS_BYTEAOID;
    lobAttr->attstattarget = -1;
    lobAttr->attlen = -1;
    lobAttr->attnum = 1;
    lobAttr->attndims = 0;
    lobAttr->attcacheoff = -1;
    lobAttr->atttypmod = -1;
    lobAttr->attbyval = false;
    lobAttr->attstorage = 'p';
    lobAttr->attalign = 'i';
    lobAttr->attnotnull = false;
    lobAttr->atthasdef = false;
    lobAttr->attisdropped = false;
    lobAttr->attislocal = true;
    lobAttr->attcmprmode = 0x7f;
    lobAttr->attinhcount = 0;
    lobAttr->attcollation = 0;
    return tupDesc;
}

RetStatus SystableUtility::GetBootSystableSegmentId(PdbId pdbId, Oid sysTableOid, PageId &segmentId)
{
    /* Do not check bootstrap systables. */
    StorageAssert((pdbId <= PDB_MAX_ID) && pdbId != 0);
    /* for bootstrap systable, get segment id from control file */
    ControlFile *controlFile = g_storageInstance->GetPdb(pdbId)->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_SYSTABLE,
                        ErrMsg("internal error, Oid(%u) can not get control file", sysTableOid));
    return controlFile->GetSysTableItem(sysTableOid, segmentId);
}

RetStatus SystableUtility::AddRelationMap(PdbId pdbId, Oid relOid, PageId &segmentId)
{
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    ControlFile *controlFile = g_storageInstance->GetPdb(pdbId)->GetControlFile();
    StorageReleasePanic(controlFile == nullptr, MODULE_SYSTABLE, ErrMsg("internal error, can not get control file"));

    /* step1: Check whether the value of relid exists. */
    UNUSE_PARAM PageId pageId = INVALID_PAGE_ID;
    if (controlFile->GetSysTableItem(relOid, pageId) == DSTORE_SUCC) {
        storage_set_error(SYSTABLE_ERROR_BOOTSTRAP_TALBE_EXISTS, relOid);
        return DSTORE_FAIL;
    }

    /* step2: save segment id to control file */
    RetStatus ret = controlFile->AddSysTableItem(relOid, segmentId);
    return ret;
}
}  // namespace DSTORE
