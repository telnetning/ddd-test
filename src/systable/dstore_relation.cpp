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
 * dstore_relation.cpp
 *
 * IDENTIFICATION
 *        src/systable/dstore_relation.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "table/dstore_table_interface.h"
#include "systable/dstore_systable_utility.h"
#include "systable/sys_database.h"
#include "systable/dstore_systable_perf_unit.h"
#include "heap/dstore_heap_handler.h"
#include "wal/dstore_wal.h"
#include "index/dstore_btree.h"
#include "systable/dstore_relation.h"

namespace DSTORE {

PageId StorageRelationData::GetSegmentPageId()
{
    if (btreeSmgr != nullptr) {
        return btreeSmgr->GetSegMetaPageId();
    }
    if (tableSmgr != nullptr) {
        return tableSmgr->GetSegMetaPageId();
    }
    return INVALID_PAGE_ID;
}

void StorageRelationData::SetTableSmgrSegment(HeapSegment *segment)
{
    tableSmgr->m_segment = segment;
}

void StorageRelationData::SetBtreeSmgrSegment(IndexSegment *segment)
{
    btreeSmgr->m_segment = segment;
}

SegmentInterface *StorageRelationData::GetTableSmgrSegment()
{
    StorageAssert(tableSmgr != nullptr);
    return tableSmgr->m_segment;
}

SegmentInterface *StorageRelationData::GetBtreeSmgrSegment()
{
    return btreeSmgr->m_segment;
}

SegmentInterface *StorageRelationData::GetLobSmgrSegment()
{
    return lobTableSmgr->m_segment;
}

RetStatus StorageRelationData::Construct(Oid pdbId, Oid relid, SysClassTupDef *classTuple, TupleDesc tupDesc,
    int fillFactor, TablespaceId tablespaceId, bool enableLsc, bool is_nailed)
{
    LatencyStat::Timer timer(&SystablePerfUnit::GetInstance().m_relDataConstructLatency);
    RetStatus ret = DSTORE_FAIL;
    relOid = relid;
    rel = nullptr;
    attr = tupDesc;
    tableSmgr = nullptr;
    lobTableSmgr = nullptr;
    btreeSmgr = nullptr;
    m_pdbId = (pdbId) ? pdbId : DSTORE::g_defaultPdbId;
    
    PageId segmentId = {classTuple->relfileid, classTuple->relblknum};
    PageId lobSegmentId = {classTuple->rellobfileid, classTuple->rellobblknum};

    switch (static_cast<RelationKind>(classTuple->relkind)) {
        case SYS_RELKIND_VIEW:
        case SYS_RELKIND_COMPOSITE_TYPE:
        case SYS_RELKIND_FOREIGN_TABLE:
        case SYS_RELKIND_STREAM:
        case SYS_RELKIND_CONTQUERY:
            break;
        case SYS_RELKIND_INDEX:
        case SYS_RELKIND_GLOBAL_INDEX:
            btreeSmgr = StorageTableInterface::CreateBtreeSmgr(m_pdbId, tablespaceId, segmentId, fillFactor,
                                                               classTuple->relpersistence, enableLsc);
            /*
             * This is used only to avoid the scenario where the old segment ID fails to be used to build the SMG
             * during the relcache processing.
             */
            if (unlikely(btreeSmgr == nullptr) && is_nailed &&
                StorageGetErrorCode() == INDEX_ERROR_FAIL_CREATE_BTREE_SMGR) {
                ErrLog(DSTORE_WARNING, MODULE_SYSTABLE,
                       ErrMsg("Failed to create nailed table BtreeSmgr, relid(%u), relname(%s), tableSpaceId(%d), "
                              "segmentId(%u, %u)",
                              relid, classTuple->relname.data, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
                break;
            }
            if (unlikely(btreeSmgr == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                       ErrMsg("Failed to create BtreeSmgr, relid(%u), relname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                              relid, classTuple->relname.data, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
                return DSTORE_FAIL;
            }
            break;
        case SYS_RELKIND_RELATION:
        case SYS_RELKIND_SEQUENCE:
        case SYS_RELKIND_LARGE_SEQUENCE:
        case SYS_RELKIND_MATVIEW:
            /* Try to make each sequence tuple occupy one page */
            if (unlikely(relOid == SEQUENCE_RELATION_ID || relOid == SEQUENCE_INDEX_RELATION_ID)) {
                fillFactor = 1;
                ErrLog(DSTORE_DEBUG1, MODULE_SYSTABLE,
                    ErrMsg("gs_sequence relation %u fillFactor %d", relOid, fillFactor));
            }
            tableSmgr = StorageTableInterface::CreateTableSmgr(m_pdbId, tablespaceId, segmentId, fillFactor, tupDesc,
                                                               classTuple->relpersistence, enableLsc);
            if (unlikely(tableSmgr == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                       ErrMsg("Failed to create TableSmgr, relid(%u), relname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                              relid, classTuple->relname.data, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
                return DSTORE_FAIL;
            }
            tableSmgr->SetTableOid(relOid);
            ret = ConstructLobTableSmgr(tablespaceId, lobSegmentId, fillFactor, classTuple->relpersistence, enableLsc);
            if (unlikely(ret == DSTORE_FAIL)) {
                StorageTableInterface::DestroyTableSmgr(tableSmgr);
                tableSmgr = nullptr;
                ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                    ErrMsg("Failed to create LobTableSmgr, relid(%u), relname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                        relid, classTuple->relname.data, tablespaceId, lobSegmentId.m_fileId, lobSegmentId.m_blockId));
                return DSTORE_FAIL;
            }
            break;
        default:
            ErrLog(DSTORE_WARNING, MODULE_SYSTABLE, ErrMsg("invalid relkind: %c", classTuple->relkind));
            return DSTORE_FAIL;
    }
    xact_seqno = 0;
    return DSTORE_SUCC;
}

RetStatus StorageRelationData::Construct(Oid pdbId, Oid partid, SysPartitionDef *partTuple,
    TupleDesc tupDesc, const int fillFactor, TablespaceId tablespaceId, bool isThrdMemCtx)
{
    LatencyStat::Timer timer(&SystablePerfUnit::GetInstance().m_partDataConstructLatency);

    StorageAssert(static_cast<TBS_ID>(tablespaceId) != TBS_ID::TEMP_TABLE_SPACE_ID);
    RetStatus ret = DSTORE_FAIL;
    relOid = partid;
    rel = nullptr;
    attr = tupDesc;
    tableSmgr = nullptr;
    lobTableSmgr = nullptr;
    btreeSmgr = nullptr;
    m_pdbId = pdbId;
    PageId segmentId = {partTuple->relfileid, partTuple->relblknum};
    PageId lobSegmentId = {partTuple->rellobfileid, partTuple->rellobblknum};

    if (partTuple->parttype == SYS_PART_OBJ_TYPE_INDEX_PARTITION) {
        btreeSmgr = StorageTableInterface::CreateBtreeSmgr(m_pdbId, tablespaceId, segmentId, fillFactor,
                                                           SYS_RELPERSISTENCE_PERMANENT, isThrdMemCtx);
        if (unlikely(btreeSmgr == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                   ErrMsg("Failed to create BtreeSmgr, partid(%u), partname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                          partid, partTuple->relname.data, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
            return DSTORE_FAIL;
        }
    } else {
        tableSmgr = StorageTableInterface::CreateTableSmgr(m_pdbId, tablespaceId, segmentId, fillFactor, tupDesc,
                                                           SYS_RELPERSISTENCE_PERMANENT, isThrdMemCtx);
        if (unlikely(tableSmgr == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                   ErrMsg("Failed to create TableSmgr, partid(%u), partname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                          partid, partTuple->relname.data, tablespaceId, segmentId.m_fileId, segmentId.m_blockId));
            return DSTORE_FAIL;
        }
        ret = ConstructLobTableSmgr(tablespaceId, lobSegmentId, fillFactor, SYS_RELPERSISTENCE_PERMANENT, isThrdMemCtx);
        if (unlikely(ret == DSTORE_FAIL)) {
            StorageTableInterface::DestroyTableSmgr(tableSmgr);
            tableSmgr = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE,
                ErrMsg("Failed to create LobTableSmgr, partid(%u), partname(%s), tableSpaceId(%d), segmentId(%u, %u)",
                       partid, partTuple->relname.data, tablespaceId, lobSegmentId.m_fileId, lobSegmentId.m_blockId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus StorageRelationData::ConstructLobTableSmgr(TablespaceId tablespaceId, PageId lobSegmentId,
                                                     const int fillFactor, char relpersistence, bool isThrdMemCtx)
{
    if (lobSegmentId != INVALID_PAGE_ID && lobSegmentId.m_fileId != 0 && lobSegmentId.m_blockId != 0) {
        TupleDesc lobTupleDesc = CreateLobTupleDesc();
        if (unlikely(lobTupleDesc == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("failed to create lobTupleDesc."));
            return DSTORE_FAIL;
        }
        lobTableSmgr = StorageTableInterface::CreateTableSmgr(
            m_pdbId, tablespaceId, lobSegmentId, fillFactor, lobTupleDesc, relpersistence, isThrdMemCtx);
        if (unlikely(lobTableSmgr == nullptr)) {
            DstorePfreeExt(lobTupleDesc);
            ErrLog(DSTORE_ERROR, MODULE_SYSTABLE, ErrMsg("failed to create LobTableSmgr."));
            return DSTORE_FAIL;
        }
        DstorePfreeExt(lobTupleDesc);
    }
    return DSTORE_SUCC;
}

void StorageRelationData::SetTupleDesc(bool isThrdMemCtx)
{
    StorageAssert(tableSmgr != nullptr);
    DstorePfreeExt(tableSmgr->m_tupDesc);

    DstoreMemoryContext context = ((!isThrdMemCtx) ?
        thrd->GetSessionMemoryCtx() : thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    AutoMemCxtSwitch autoSwitch(context);
    tableSmgr->m_tupDesc = attr->Copy();
}

void StorageRelationData::Destroy()
{
    if (tableSmgr) {
        StorageTableInterface::DestroyTableSmgr(tableSmgr);
        tableSmgr = nullptr;
    }

    if (lobTableSmgr) {
        StorageTableInterface::DestroyTableSmgr(lobTableSmgr);
        lobTableSmgr = nullptr;
    }

    if (btreeSmgr) {
        StorageTableInterface::DestroyBtreeSmgr(btreeSmgr);
        btreeSmgr = nullptr;
    }
    xact_seqno = 0;
}

uint64_t StorageRelationData::GetSmgrSeqno()
{
    return xact_seqno;
}
void StorageRelationData::SetSmgrSeqno(uint64_t newSeqno)
{
    xact_seqno = newSeqno;
}

void StorageRelationData::Init()
{
    relOid = DSTORE_INVALID_OID;
    rel = nullptr;
    attr = nullptr;
    index = nullptr;
    indKey = nullptr;
    indexInfo = nullptr;
    tableSmgr = nullptr;
    lobTableSmgr = nullptr;
    btreeSmgr = nullptr;
    xact_seqno = 0;
}

} /* namespace DSTORE */
