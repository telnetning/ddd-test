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
 * dstore_heap_segment.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_heap_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_heap_segment.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_datatype.h"
#include "lock/dstore_table_lock_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "common/algorithm/dstore_binaryheap.h"
#include "tablespace/dstore_tablespace_utils.h"
#include "tablespace/dstore_table_space_perfunit.h"
#include "tablespace/dstore_tablespace.h"

using namespace DSTORE;

RetStatus HeapSegment::InitSegment()
{
    if (STORAGE_FUNC_FAIL(Init())) {
        return DSTORE_FAIL;
    }

    /* FreeSpaceMapList initialization will be delayed till first Free page request */
    m_fsmList = nullptr;
    m_isFsmInitialized = false;

    m_isInitialized = true;
    return DSTORE_SUCC;
}

RetStatus HeapSegment::InitFreeSpaceMap()
{
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    if (likely(m_isFsmInitialized)) {
        return DSTORE_SUCC;
    }
    /* Create PartitionFreeSpaceMap in saved memory context */
    AutoMemCxtSwitch autoSwitch {m_ctx};

    /* Check if we have FSM trees allocated for me */
    PageId fsmMetaPageIds[MAX_FSM_TREE_PER_RELATION];
    int numFsm = 0;
    FindFsmForCurrentNode(fsmMetaPageIds, numFsm);

    /* No FSM is assigned to current node. We need to either create a new FSM or take a FSM from other node */
    while (numFsm == 0) {
        /* Global temp table no need check again */
        if (IsTempSegment()) {
            PageId newFsmMetaPageId = INVALID_PAGE_ID;
            if (STORAGE_FUNC_FAIL(InitFreeSpaceMapInternal(newFsmMetaPageId))) {
                return DSTORE_FAIL;
            }
            
            /* Record info of new FSM into our array */
            fsmMetaPageIds[0] = newFsmMetaPageId;
            numFsm = 1;
            break;
        }
        /* Lock the Extension lock since we are going to update the segment meta page */
        LockTag tag;
        LockErrorInfo errorInfo = {0};
        LockMgr *lockMgr = g_storageInstance->GetLockMgr();
        tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
        if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
            return DSTORE_FAIL;
        }
        
        /* Double check here in case someone already assign one FSM for me */
        FindFsmForCurrentNode(fsmMetaPageIds, numFsm);
        
        /* Still no FSM assigned to current node, proceed to create or reassign one */
        if (numFsm == 0) {
            PageId newFsmMetaPageId = INVALID_PAGE_ID;
            if (STORAGE_FUNC_FAIL(InitFreeSpaceMapInternal(newFsmMetaPageId))) {
                lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
                return DSTORE_FAIL;
            }
        
            /* Record info of new FSM into our array */
            fsmMetaPageIds[0] = newFsmMetaPageId;
            numFsm = 1;
        }
        
        /* We should have at least one valid fsmMetaPageId here */
        StorageAssert(numFsm != 0);
        StorageAssert(fsmMetaPageIds[0] != INVALID_PAGE_ID);
        
        /* Now, release the extension lock */
        lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        break;
    }

    /* Initialize the FreeSpaceMapList based on collected fsmMetaPageId */
    if (STORAGE_FUNC_FAIL(InitFreeSpaceMapList(fsmMetaPageIds, numFsm))) {
        return DSTORE_FAIL;
    }

    /* Now, mark the PartitionFreeSpaceMap to be initialized */
    m_isFsmInitialized = true;
    return DSTORE_SUCC;
}

RetStatus HeapSegment::InitFreeSpaceMapInternal(PageId &fsmMetaPageId)
{
    uint32 nodeCount = 1;
    bool needNewFsm = true;
    BufferDesc *segMetaPageBuf = nullptr;
    HeapSegmentMetaPage *segMetaPage = nullptr;
    segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    if (unlikely(segMetaPage == nullptr)) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }
    /*
     * DRTODO: needNewFsm needs to consider FSM being set to a node that does not exist
     * in this cluster. This happens after cluster rebuild for standby, and writes fail
     * to reusing existing FSM after failover/switchover/alterPdbStatus.
     *
     * when reusing such FSMs, save clusterId in ownership info.
     *
     * Add a custer ID field to better track ownership
     * 1. modify FsmInfo to have clusterId field
     * 2. Modify the FSM recycle logic to reassign FSMs with different clusterId
     * 3. Add clusterId into HeapSegmentMetaPage to indicate which cluster used it for the last time.
     *    (this will help us decide if we need new fsm)
     * 4. if clusterId on HeapSegmentMetaPage is different from current cluster id,
     *    recycle ALL FSMs and assign them to current node.
     */
    needNewFsm = ((segMetaPage->numFsms < MAX_FSM_TREE_PER_RELATION) &&
                  (segMetaPage->numFsms < nodeCount));
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    /* Step 2. Create new FSM or Reassign existing FSMs based on needNewFsm */
    if (needNewFsm) {
        /* Create new FSM for current node */
        if (STORAGE_FUNC_FAIL(AllocateNewFsmTree(fsmMetaPageId))) {
            return DSTORE_FAIL;
        }
    } else {
        /*
         * We either reached the maximum number of FSM a relation could have or we have more FSMs than
         * the number of nodes. Therefore, we have to reassign an existing FSM tree from other nodes.
         */
        if (STORAGE_FUNC_FAIL(ReuseExistingFsmTree(fsmMetaPageId))) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

/* Scan through fsmInfos in HeapSegmentMetaPage and find FSMs assigned to current node */
void HeapSegment::FindFsmForCurrentNode(PageId *fsmMetaPageIds, int &numFsm)
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    for (int i = 0; i < segMetaPage->numFsms; i++) {
        if (segMetaPage->fsmInfos[i].assignedNodeId == selfNodeId) {
            fsmMetaPageIds[numFsm++] = segMetaPage->fsmInfos[i].fsmMetaPageId;
        }
    }
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
}

/* Allocate and initialize FreeSpaceMapList based on given fsmMetaPageIds */
RetStatus HeapSegment::InitFreeSpaceMapList(PageId *fsmMetaPageIds, int numFsm)
{
    StorageAssert(m_fsmList == nullptr);
    m_fsmList = DstoreNew(g_dstoreCurrentMemoryContext)FreeSpaceMapList();
    if (m_fsmList == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%hu, %u) failed to allocate FreeSpaceMapList.",
                GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId));
        return DSTORE_FAIL;
    }

    /* Load information based on given fsmMetaPageIds */
    for (int i = 0; i < numFsm; i++) {
        if (STORAGE_FUNC_FAIL(m_fsmList->LoadNewFreeSpaceMap(
            m_segmentId, fsmMetaPageIds[i], m_bufMgr, GetTablespaceId(), m_pdbId))) {
            delete m_fsmList;
            m_fsmList = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("Heap data segment (%hu, %u) failed to initialize PartitionFreeSpaceMap.",
                    GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

/* This function will create a new FSM and assign it to current node */
RetStatus HeapSegment::AllocateNewFsmTree(PageId &fsmMetaPageId)
{
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;

    return AllocateNewFsmTree(selfNodeId, fsmMetaPageId);
}

RetStatus HeapSegment::AllocateNewFsmTree(NodeId assignedNodeId, PageId &fsmMetaPageId)
{
    /* Get 2 pages from unassigned page list. 1 for FSM meta page and 1 for new FSM root */
    PageId newPageIdForFSM[INIT_FSM_NEED_PAGE_COUNT];
    uint16 fsmId;
    BufferDesc *segMetaPageBuf = nullptr;
    HeapSegmentMetaPage *segMetaPage = nullptr;

    /* Get two pages for the current Node */
    if (STORAGE_FUNC_FAIL(GetNewPagesForFSM(newPageIdForFSM, INIT_FSM_NEED_PAGE_COUNT))) {
        /* Cannot get new pages from data segment for FSM tree */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%hu, %u) get new page for FSM fail, tablespace has no new space",
                GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId));
        return DSTORE_FAIL;
    }

    /* Initialize new FSM tree here */
    fsmMetaPageId = newPageIdForFSM[0];
    PageId fsmRootPageId = newPageIdForFSM[1];
    if (STORAGE_FUNC_FAIL(
        HeapSegment::InitNewFsmTree(this->GetPdbId(), fsmMetaPageId, fsmRootPageId, m_bufMgr, NeedWal()))) {
        /* Failed to read out FSM root page buffer */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%hu, %u) failed to Initialize new fsm tree."
                   "Fsm meta page (%hu, %u), Fsm root page (%hu, %u).",
                GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
                fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId,
                fsmRootPageId.m_fileId, fsmRootPageId.m_blockId));
        return DSTORE_FAIL;
    }

    segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    StorageReleasePanic(segMetaPage == nullptr, MODULE_SEGMENT,
        ErrMsg("AllocateNewFsmTree: Get segment meta page from buffer failed."));
    fsmId = segMetaPage->numFsms;
    segMetaPage->fsmInfos[fsmId].fsmMetaPageId = fsmMetaPageId;
    segMetaPage->fsmInfos[fsmId].assignedNodeId = assignedNodeId;
    segMetaPage->numFsms++;
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);

    if (NeedWal()) {
        /* Record this FSM meta page in DataSegmentMetaPage */
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        /* Write WAL for adding a new Fsm */
        GenerateWalForAllocNewFsmTree(segMetaPageBuf, fsmId);
        (void)walWriterContext->EndAtomicWal();
    }
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    return DSTORE_SUCC;
}

void HeapSegment::GenerateWalForAllocNewFsmTree(BufferDesc *segMetaPageBuf, uint16 fsmId)
{
    WalRecordTbsSegMetaAddFsmTree segMetaAddFsmWalData;
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    PageId fsmMetaPageId = segMetaPage->fsmInfos[fsmId].fsmMetaPageId;
    NodeId assignedNodeId = segMetaPage->fsmInfos[fsmId].assignedNodeId;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
    segMetaAddFsmWalData.SetHeader({WAL_TBS_SEG_META_ADD_FSM_TREE, sizeof(segMetaAddFsmWalData),
                                    Segment::GetSegmentMetaPageId(), segMetaPage->GetWalId(), segMetaPage->GetPlsn(),
                                    segMetaPage->GetGlsn(), glsnChangedFlag, segMetaPageBuf->GetFileVersion()});
    segMetaAddFsmWalData.SetData(fsmMetaPageId, assignedNodeId, fsmId);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&segMetaAddFsmWalData);
}

#ifdef UT
/* Logic of this function should be the same with AllocateNewFsmTree except we pass in assignedNodeId */
RetStatus HeapSegment::UtAllocNewFsmTree(NodeId assignedNodeId, bool fsmExpired)
{
    PageId fsmMetaPageId = INVALID_PAGE_ID;
    if (STORAGE_FUNC_FAIL(AllocateNewFsmTree(assignedNodeId, fsmMetaPageId))) {
        return DSTORE_FAIL;
    }

    StorageAssert(fsmMetaPageId != INVALID_PAGE_ID);
    /* Mark FSM as expired if needed */
    if (fsmExpired) {
        return PartitionFreeSpaceMap::MarkFreeSpaceMapExpired(m_bufMgr, fsmMetaPageId, m_pdbId);
    }

    return DSTORE_SUCC;
}

bool HeapSegment::IsFsmInitInitialized()
{
    return this->m_isFsmInitialized;
}
#endif

/*
 * In this function, we will scan the fsmInfos array in HeapSegmentMetaPage and calculate how many
 * FSMs each node is assigned. These information will be recorded in NodeFsmInfo array and return out
 * for later use.
 */
void HeapSegment::CollectNumFsmsForEachNode(HeapSegmentMetaPage *segMetaPage, NodeFsmInfo *nodeFsmInfo,
    int &numNodeFsmInfo) const
{
    for (int i = 0; i < segMetaPage->numFsms; i++) {
        /* We should not have any FSMs assigned to the current node */
        StorageAssert(segMetaPage->fsmInfos[i].assignedNodeId != g_storageInstance->GetGuc()->selfNodeId);

        /* Check if this NodeId is already in our nodeFsmInfo array */
        bool found = false;
        for (int j = 0; j < numNodeFsmInfo; j++) {
            if (nodeFsmInfo[j].nodeId == segMetaPage->fsmInfos[i].assignedNodeId) {
                /* Increase numFsms of corresponding nodeFsmInfo and continue */
                nodeFsmInfo[j].numFsms += 1;
                found = true;
                break;
            }
        }

        /* Add this NodeId to nodeFsmInfo array if not found */
        if (!found) {
            nodeFsmInfo[numNodeFsmInfo].nodeId = segMetaPage->fsmInfos[i].assignedNodeId;
            nodeFsmInfo[numNodeFsmInfo].numFsms = 1;
            numNodeFsmInfo++;
        }
    }
}

void HeapSegment::GenerateWalForRecycleFsmTree(BufferDesc *segMetaPageBuf, uint16 fsmId)
{
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    WalRecordTbsSegMetaRecycleFsmTree segMetaRecycleFsmWalData;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
    PageId fsmMetaPageId = segMetaPage->fsmInfos[fsmId].fsmMetaPageId;
    NodeId newNodeId = segMetaPage->fsmInfos[fsmId].assignedNodeId;
    segMetaRecycleFsmWalData.SetHeader({WAL_TBS_SEG_META_RECYCLE_FSM_TREE, sizeof(segMetaRecycleFsmWalData),
                                        Segment::GetSegmentMetaPageId(), segMetaPage->GetWalId(),
                                        segMetaPage->GetPlsn(), segMetaPage->GetGlsn(), glsnChangedFlag,
                                        segMetaPageBuf->GetFileVersion()});
    segMetaRecycleFsmWalData.SetData(fsmMetaPageId, newNodeId, fsmId);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&segMetaRecycleFsmWalData);
}

/*
 * This function will find the coldest FSM of a given node and reassign this FSM to the current node.
 * Information of this fsm is saved in candidateFsmId which will be returned out for later use.
 */
RetStatus HeapSegment::ReassignColdestFsmFromNode(BufferDesc *segMetaBuf, NodeFsmInfo *candidateNode,
    uint16 &candidateFsmId)
{
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaBuf->GetPage());
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;

    /* Find the coldest FSM of the given Node */
    TimestampTz minAccessTimstamp = TIMESTAMPTZ_MAX;
    for (uint16 i = 0; i < segMetaPage->numFsms; i++) {
        if (segMetaPage->fsmInfos[i].assignedNodeId == candidateNode->nodeId) {
            /* Read info of this FSM and get its accessTimestamp */
            BufferDesc *fsmMetaBuf = m_bufMgr->Read(this->GetPdbId(), segMetaPage->fsmInfos[i].fsmMetaPageId,
                LW_SHARED);
            if (fsmMetaBuf == INVALID_BUFFER_DESC) {
                return DSTORE_FAIL;
            }

            FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
            if (fsmMetaPage->accessTimestamp < minAccessTimstamp) {
                minAccessTimstamp = fsmMetaPage->accessTimestamp;
                candidateFsmId = i;
            }

            m_bufMgr->UnlockAndRelease(fsmMetaBuf);
        }
    }
    if (candidateFsmId >= INVALID_FSM_ID) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Invalid candidateFsmId %hu, numFsms %hu, pdbId %u, tablespaceId %hu.", candidateFsmId,
                      segMetaPage->numFsms, this->m_pdbId, this->m_tablespaceId));
        return DSTORE_FAIL;
    }

    /* Reassign this FSM to the current node */
    NodeId oldNodeId = segMetaPage->fsmInfos[candidateFsmId].assignedNodeId;
    segMetaPage->fsmInfos[candidateFsmId].assignedNodeId = selfNodeId;
    (void)m_bufMgr->MarkDirty(segMetaBuf);

    if (NeedWal()) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());

        /* Write WAL about this reassignment here */
        GenerateWalForRecycleFsmTree(segMetaBuf, candidateFsmId);
        (void)walWriterContext->EndAtomicWal();
    }

    /* Update the accessTimestamp of the reassigned FSM to avoid reassignment again */
    PageId curFsmMetaPageId = segMetaPage->fsmInfos[candidateFsmId].fsmMetaPageId;
    BufferDesc *fsmMetaBuf = m_bufMgr->Read(this->GetPdbId(), curFsmMetaPageId, LW_EXCLUSIVE);
    if (fsmMetaBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }
    PartitionFreeSpaceMap::UpdateFsmAccessTimestamp(fsmMetaBuf, m_bufMgr);
    m_bufMgr->UnlockAndRelease(fsmMetaBuf);

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Heap segment (%hu, %u) reassign FSM (%hu, %u) from Node %u to Node %u successfully",
                  GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId, curFsmMetaPageId.m_fileId,
                  curFsmMetaPageId.m_blockId, oldNodeId, selfNodeId));

    return DSTORE_SUCC;
}


/*
 * In this function, we will try to reuse existing FSM tree who is assigned to a node,
 * who has the most FSMs and is the coldest FSM among them, for the current node. This
 * function should be called under the promise that we are not allowed to create new
 * FSMs for current HeapSegment. Please check InitFreeSpaceMapInternal for detail.
 */
RetStatus HeapSegment::ReuseExistingFsmTree(PageId &fsmMetaPageId)
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());

    NodeFsmInfo *nodeFsmInfo = nullptr;
    int numNodeFsmInfo = 0;

    /* Step 1. Scan through the fsmInfo array and collect numFsms of each node */
    nodeFsmInfo = (NodeFsmInfo *)DstorePalloc(sizeof(NodeFsmInfo) * MAX_FSM_TREE_PER_RELATION);
    if (STORAGE_VAR_NULL(nodeFsmInfo)) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }
    CollectNumFsmsForEachNode(segMetaPage, nodeFsmInfo, numNodeFsmInfo);

    /* Step 2: Find the candidate Node with the most FSMs and check its numFsms */
    StorageAssert(numNodeFsmInfo > 0 && numNodeFsmInfo <= MAX_FSM_TREE_PER_RELATION);
    NodeFsmInfo *candidateNode = nullptr;
    uint64 maxNumFsms = 0;
    for (int i = 0; i < numNodeFsmInfo; i++) {
        if (nodeFsmInfo[i].numFsms > maxNumFsms) {
            maxNumFsms = nodeFsmInfo[i].numFsms;
            candidateNode = &nodeFsmInfo[i];
        }
    }
    StorageAssert(candidateNode != nullptr);

    /* Check the numFsms of candidate Node to decide if we need to reassign a FSM tree */
    uint16 candidateFsmId = INVALID_FSM_ID;
    if (candidateNode->numFsms > 1) {
        /* Candidate Node has multiple FSM. Find and reassign the coldest one. */
        if (STORAGE_FUNC_FAIL(ReassignColdestFsmFromNode(segMetaPageBuf, candidateNode, candidateFsmId))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            DstorePfreeExt(nodeFsmInfo);
            return DSTORE_FAIL;
        }
    } else {
        /*
         * In this case, all FSMs are used and each node only have 1 FSMs assigned. This implicit
         * means all nodes have steady access to their own FSMs and no spare FSM for current node
         * to use. Otherwise, some cold FSM should be already reassigned and some nodes should have
         * multiple FSM to use. Therefore, we just randomly pick one FSM and use it directly. Note
         * that we do not reassign this FSM, so this FSM will be shared by two nodes.
         */
        uint8 randomNum = 0;
        if (STORAGE_FUNC_FAIL(DstoreGetRandomNum(randomNum))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            DstorePfreeExt(nodeFsmInfo);
            return DSTORE_FAIL;
        }

        candidateFsmId = static_cast<uint16>(randomNum % segMetaPage->numFsms);
    }

    if (candidateFsmId >= INVALID_FSM_ID) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Invalid candidateFsmId %hu, numFsms %hu, pdbId %u, tablespaceId %hu.", candidateFsmId,
                      segMetaPage->numFsms, this->m_pdbId, this->m_tablespaceId));
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        DstorePfreeExt(nodeFsmInfo);
        return DSTORE_FAIL;
    }
    /* Step 3. We have already get the id of a usable FSM for current Node. Return corresponding fsmMetaPageId. */
    fsmMetaPageId = segMetaPage->fsmInfos[candidateFsmId].fsmMetaPageId;
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    DstorePfreeExt(nodeFsmInfo);

    return DSTORE_SUCC;
}

HeapSegment::~HeapSegment()
{
    if (m_fsmList != nullptr) {
        delete m_fsmList;
        m_fsmList = nullptr;
        m_isFsmInitialized = false;
    }
}

RetStatus HeapSegment::DropSegment()
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    LockTag tag;
    tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
    LockErrorInfo error = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK, false, &error))) {    // Critical-Point
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(DropHeapSegmentInternal())) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        return DSTORE_FAIL;
    }
    m_isDrop = true;
    lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
    return DSTORE_SUCC;
}

PageId HeapSegment::GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime)
{
    return GetPageFromFsm(spaceNeeded, retryTime, nullptr);
}

PageId HeapSegment::GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime, uint32 *spaceInFsm)
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return INVALID_PAGE_ID;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return INVALID_PAGE_ID;
    }
    if (unlikely(spaceNeeded > BLCKSZ)) {
        storage_set_error(TBS_ERROR_PAGE_REQUEST_SIZE_INVALID);
        return INVALID_PAGE_ID;
    }

    if (unlikely(!m_isFsmInitialized)) {
        if (STORAGE_FUNC_FAIL(InitFreeSpaceMap())) {
            storage_set_error(TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED);
            return INVALID_PAGE_ID;
        }
        StorageAssert(m_isFsmInitialized);
    }

    /* Get the first usable PartitionFreeSpaceMap in m_fsmList for free space */
    FreeSpaceMapNode *fsmNode = m_fsmList->GetFreeSpaceMapForSpace();
    if (fsmNode == nullptr) {
        return INVALID_PAGE_ID;
    }

    PartitionFreeSpaceMap *fsm = fsmNode->m_fsm;

    /* Update the access timestamp of selected FSM before accessing it */
    if (unlikely(fsm->ConditionalUpdateFsmAccessTimestamp())) {
        return INVALID_PAGE_ID;
    }

    /* Now, we are ready to fetch page from this FSM */
    bool needExtensionTask = false;
    PageId heapSegMetaPageId = Segment::GetSegmentMetaPageId();
    PageId resultPage = fsm->GetPage(heapSegMetaPageId, spaceNeeded, retryTime, spaceInFsm, &needExtensionTask);

    /* If an extension task is registered for this FSM, move it to the end of FsmList */
    if (needExtensionTask) {
        m_fsmList->MoveFreeSpaceMapToEnd(fsmNode);
    }

    return resultPage;
}

RetStatus HeapSegment::CheckAndInitFreeSpaceMap()
{
    /* Check if HeapSegment is initialized */
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }

    /* Check if HeapSegment is dropped */
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }

    /* check if we have initialized PartitionFreeSpaceMap */
    if (unlikely(!m_isFsmInitialized)) {
        if (STORAGE_FUNC_FAIL(InitFreeSpaceMap())) {
            storage_set_error(TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED);
            return DSTORE_FAIL;
        }
        StorageAssert(m_isFsmInitialized);
    }

    return DSTORE_SUCC;
}

RetStatus HeapSegment::UpdateFsm(const PageId &dataPageId, uint32 remainSpace)
{
    /* Read fsm index in pageId */
    BufferDesc *pageBuf = m_bufMgr->Read(this->GetPdbId(), dataPageId, LW_SHARED);    // Critical-Point
    if (unlikely(pageBuf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }

    HeapPage *dataPage = static_cast<HeapPage*>(pageBuf->GetPage());
    FsmIndex fsmIndex = dataPage->GetFsmIndex();
    m_bufMgr->UnlockAndRelease(pageBuf);

    return UpdateFsm(fsmIndex, remainSpace);
}

RetStatus HeapSegment::UpdateFsm(const FsmIndex &fsmIndex, uint32 remainSpace)
{
    /* Initialize PartitionFreeSpaceMap if needed */
    if (STORAGE_FUNC_FAIL(CheckAndInitFreeSpaceMap())) {
        return DSTORE_FAIL;
    }
    if (unlikely(remainSpace > BLCKSZ)) {
        storage_set_error(TBS_ERROR_PAGE_REQUEST_SIZE_INVALID);
        return DSTORE_FAIL;
    }

    bool fsmIsUpdated = false;
    FsmUpdateSpaceContext context = {
        fsmIndex, remainSpace, &fsmIsUpdated, 0, UINT32_MAX, nullptr, m_bufMgr, nullptr, nullptr, NeedWal(), m_pdbId};
    return UpdateFsmInternal(context);
}

RetStatus HeapSegment::UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded,
    uint16 retryTimes, PageId *pageId, uint32* spaceInFsm)
{
    /* Initialize PartitionFreeSpaceMap if needed */
    if (STORAGE_FUNC_FAIL(CheckAndInitFreeSpaceMap())) {
        return DSTORE_FAIL;
    }
    if (unlikely(remainSpace > BLCKSZ || spaceNeeded > BLCKSZ)) {
        storage_set_error(TBS_ERROR_PAGE_REQUEST_SIZE_INVALID);
        return DSTORE_FAIL;
    }

    bool fsmIsUpdated = false;
    FsmUpdateSpaceContext context = {fsmIndex, remainSpace, &fsmIsUpdated, retryTimes, spaceNeeded, pageId,
                                     m_bufMgr, nullptr, spaceInFsm, NeedWal(), m_pdbId};
    return UpdateFsmInternal(context);
}

RetStatus HeapSegment::UpdateFsmInternal(FsmUpdateSpaceContext context)
{
    PageId fsmMetaPageId = INVALID_PAGE_ID;
    FsmIndex fsmIndex = context.fsmIndex;
    uint32 remainSpace = context.space;
    context.fsmMetaPageId = &fsmMetaPageId;
    context.pdbId = m_pdbId;
    
    if (STORAGE_FUNC_FAIL(PartitionFreeSpaceMap::UpdateSpace(context))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Heap data segment (%hu, %u) attempt to update page (%d, %u) size %u fail",
                      GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId, fsmIndex.page.m_fileId,
                      fsmIndex.page.m_blockId, remainSpace));
        return DSTORE_FAIL;
    }

    /* Check if returned page here is a new page and update fsm stat accordingly */
    if (context.pageId != nullptr && *(context.pageId) != INVALID_PAGE_ID) {
        StorageReleasePanic(fsmMetaPageId == INVALID_PAGE_ID, MODULE_SEGMENT, ErrMsg("fsmMetaPageId is invalid."));
        FreeSpaceMapNode *fsmNode = m_fsmList->SearchFreeSpaceMap(fsmMetaPageId);
        if (unlikely(fsmNode == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Search FSM failed, fsmMetaPageId(%hu-%u).",
                fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        PartitionFreeSpaceMap *fsm = fsmNode->m_fsm;
        StorageReleasePanic(fsm == nullptr, MODULE_SEGMENT, ErrMsg("fsm is invalid."));
        /* Update numUsedPages of this FSM based on if page is a new page */
        if (STORAGE_FUNC_FAIL(fsm->UpdateNumUsedPagesIfNeeded(*(context.pageId)))) {
            return DSTORE_FAIL;
        }
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Heap data segment (%hu, %u) attempt to update page (%d, %u) size %u success",
                  GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId, fsmIndex.page.m_fileId,
                  fsmIndex.page.m_blockId, remainSpace));
#endif
    return DSTORE_SUCC;
}

RetStatus HeapSegment::GetFsmPageIds(PageId **pageIds, Size *length, char **errInfo)
{
    *errInfo = nullptr;
    dlist_head pageIdsList;
    DListInit(&pageIdsList);
    StringInfoData dumpInfo;
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());

    /* Loop through all valid FSMs for this HeapSegment */
    for (int index = 0; index < MAX_FSM_TREE_PER_RELATION; index++) {
        PageId curFsmMetaPageId = segMetaPage->fsmInfos[index].fsmMetaPageId;
        if (curFsmMetaPageId == INVALID_PAGE_ID) {
            continue;
        }

        /* Record the PageId of the fsm meta page */
        if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, curFsmMetaPageId))) {
            *length = 0;    /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }
            dumpInfo.append("Failed to record FSM meta pageId.");
            *errInfo = dumpInfo.data;
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }

        /* Record the PageId of the first fsm root page */
        BufferDesc *fsmMetaBufferDesc = m_bufMgr->
            Read(this->GetPdbId(), curFsmMetaPageId, LW_SHARED);
        if (unlikely(fsmMetaBufferDesc == INVALID_BUFFER_DESC)) {
            *length = 0;    /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }
            dumpInfo.append("Invalid FSM meta buffer");
            *errInfo = dumpInfo.data;
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }
        FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBufferDesc->GetPage());
        PageId firstFsmRootPageId = fsmMetaPage->GetfirstFsmRoot();
        if (unlikely(firstFsmRootPageId == INVALID_PAGE_ID)) {
            m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
            *length = 0;    /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }
            dumpInfo.append("Invalid first FSM root pageId");
            *errInfo = dumpInfo.data;
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, firstFsmRootPageId))) {
            m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
            *length = 0;    /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }
            dumpInfo.append("Failed to record first FSM root pageId.");
            *errInfo = dumpInfo.data;
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }

        /* Then, traverse FSM extents to get the PageId of the rest pages */
        PageId curFsmExtMetaPageId = fsmMetaPage->fsmExtents.first;
        while (curFsmExtMetaPageId != INVALID_PAGE_ID) {
            BufferDesc *bufDesc = m_bufMgr->Read(this->GetPdbId(), curFsmExtMetaPageId, LW_SHARED);
            if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
                m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
                *length = 0;    /* no output */
                if (unlikely(!dumpInfo.init())) {
                    ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                    return DSTORE_FAIL;
                }
                dumpInfo.append("Invalid FSM extent meta buffer");
                *errInfo = dumpInfo.data;
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }

            SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(bufDesc->GetPage());
            for (uint32 extentIndex = 0; extentIndex < extMetaPage->GetSelfExtentSize(); extentIndex++) {
                if (STORAGE_FUNC_FAIL(AppendToPageIdList(
                        pageIdsList, {curFsmExtMetaPageId.m_fileId, curFsmExtMetaPageId.m_blockId + extentIndex}))) {
                    m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
                    m_bufMgr->UnlockAndRelease(bufDesc);
                    *length = 0; /* no output */
                    if (unlikely(!dumpInfo.init())) {
                        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                        return DSTORE_FAIL;
                    }
                    dumpInfo.append("Failed to record FSM extent meta pageId.");
                    *errInfo = dumpInfo.data;
                    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                    return DSTORE_FAIL;
                }
            }

            curFsmExtMetaPageId = extMetaPage->extentMeta.nextExtMetaPageId;
            m_bufMgr->UnlockAndRelease(bufDesc);
        }

        m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
    }

    /* Prepare the result PageId array for return */
    if (STORAGE_FUNC_FAIL(GetPagesFromPageIdList(&pageIdsList, pageIds, length, errInfo))) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    /* Unlock the buffer here */
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    return DSTORE_SUCC;
}

PageId HeapSegment::GetNewPage(const PageId fsmMetaPageId)
{
    LatencyStat::Timer timer(&TableSpacePerfUnit::GetInstance().m_heapNewPageLatency);

    /* Initialize PartitionFreeSpaceMap if needed */
    if (STORAGE_FUNC_FAIL(CheckAndInitFreeSpaceMap())) {
        return INVALID_PAGE_ID;
    }

    bool isBgExtension = (fsmMetaPageId != INVALID_PAGE_ID);
    PageId targetPageId = INVALID_PAGE_ID;

    /* Find the designated FSM we want to do extension to first */
    FreeSpaceMapNode *fsmNode = m_fsmList->SearchFreeSpaceMap(fsmMetaPageId);
    if (isBgExtension && fsmNode == nullptr) {
        /* This means designated FSM was reassigned to other node. Stop this background extension here */
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
            ErrMsg("Cannot find PartitionFreeSpaceMap {%hu, %u} to extend for segment {%hu, %u}",
                fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        return INVALID_PAGE_ID;
    }
    StorageAssert(fsmNode != nullptr);
    PartitionFreeSpaceMap *fsm = fsmNode->m_fsm;
    StorageAssert(fsm != nullptr);

    /* Read out current numTotalPage of the designated FSM */
    BufferDesc *fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_SHARED);    // Critical-Point
    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    StorageAssert(fsmMetaPage);
    uint64 oldNumTotalPage = fsmMetaPage->GetNumTotalPages();

    /* Get the extension lock here */
    LockTag tag;
    LockErrorInfo error = {0};
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    tag.SetTableExtensionLockTag(this->GetPdbId(), fsmMetaPageBuf->GetPageId());
    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);
    // Critical-Point
    if (!IsTempSegment() && STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &error))) {
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("Table (segmentId={%d, %u}, fsmMetaId = {%d, %u}) attempt to get new page, "
               "isBgExtension = %d, pdbId = %u.",
            GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
            fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId, isBgExtension, this->GetPdbId()));

    /* Check if anyone has done the extension for this FSM while waiting for lock */
    fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_SHARED);    // Critical-Point
    fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    StorageAssert(fsmMetaPage);
    uint64 numTotalPage = fsmMetaPage->GetNumTotalPages();
    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);
    if (unlikely(!isBgExtension && numTotalPage != oldNumTotalPage)) {
        targetPageId = GetPageFromFsm(BLCKSZ, 0);
        if (targetPageId != INVALID_PAGE_ID) {
            if (!IsTempSegment()) {
                lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
            }
            return targetPageId;
        }
    }

    /* Now, we are ready to do extension */
    if (STORAGE_FUNC_FAIL(GetNewPageInternal(fsm, &targetPageId, isBgExtension))) {
        if (!IsTempSegment()) {
            ErrorCode errCode = StorageGetErrorCode();
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
            storage_set_error(errCode);
        }
        return INVALID_PAGE_ID;
    }

    if (!isBgExtension) {
        if (targetPageId != INVALID_PAGE_ID && STORAGE_FUNC_FAIL(fsm->UpdateNumUsedPagesIfNeeded(targetPageId))) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
                ErrMsg("update numUsedPages failed. pdbId(%u)segmentId(%hu, %u)fsmMetaId(%hu, %u)targetPageId(%hu, %u)",
                    this->GetPdbId(), GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
                    fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId, targetPageId.m_fileId, targetPageId.m_blockId));
            return INVALID_PAGE_ID;
        }
        /* Move this FSM to front for upcoming free page request
         * if it is not a background extension to balance workload */
        m_fsmList->MoveFreeSpaceMapToFront(fsmNode);
    }
    if (!IsTempSegment()) {
        lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    }
    return targetPageId;
}

RetStatus HeapSegment::GetNewPageInternal(PartitionFreeSpaceMap *fsm, PageId *targetPageId, bool isBgExtension)
{
    bool needExtension = true;
    uint16 numNewPagesNeeded = static_cast<uint16>(fsm->GetExtendCoefficient() * PAGES_ADD_TO_FSM_PER_TIME);

    while (needExtension) {
        /* Step 1: Prepare free slots in leaf of fsm tree for dataPages, extend fsm if current free slots count is 0 */
        uint16 freeSlotCount = 0;
        if (STORAGE_FUNC_FAIL(PrepareFreeSlots(fsm, &freeSlotCount))) {
            return DSTORE_FAIL;
        }

        /* Step 2: Prepare free dataPages in segment, extend segment if current free dataPage count is 0 */
        uint16 freeDataPageCount;
        BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);    // Critical-Point
        if (STORAGE_FUNC_FAIL(PrepareFreeDataPages(&freeDataPageCount, segMetaPageBuf))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }
        if (unlikely(freeDataPageCount == 0)) {
            storage_set_error(TBS_ERROR_SEGMENT_HAS_NO_SPACE);
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }

        /* Step 3: init free dataPages, then add them to fsm */
        PageId addPageList[PAGES_ADD_TO_FSM_PER_TIME];
        uint16 addPageCount;
        bool needUpdateExtendCoeff;
        if (STORAGE_FUNC_FAIL(AddDataPagesToMetaPages(freeSlotCount, freeDataPageCount, isBgExtension, segMetaPageBuf,
                                                      addPageList, addPageCount, needUpdateExtendCoeff))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }
        bool pagesIsReused = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage())->lastExtentIsReused;
        if (STORAGE_FUNC_FAIL(AddDataPagesToFsm(fsm, addPageList, addPageCount,
            needUpdateExtendCoeff, pagesIsReused))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        *targetPageId = addPageList[0];
        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
            ErrMsg("Heap data segment (%d, %u) get new page success, returned new page id (%d, %u), page count %d",
                GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
                targetPageId->m_fileId, targetPageId->m_blockId, freeDataPageCount));

        /*
         * Step 4: Check if we have enough free pages during background extension. If not, we need another extension.
         * Foreground extension only do once at a time.
         */
        needExtension = isBgExtension && !fsm->HasEnoughUnusedPages(numNewPagesNeeded);
    }

    PageId fsmMetaPageId = fsm->GetFsmMetaPageId();
    if (isBgExtension) {
        ErrLog(DSTORE_LOG, MODULE_SEGMENT,
            ErrMsg("Table (segmentId={%d, %u}, fsmMetaId = {%d, %u}) is extended by background thread.",
                m_segmentId.m_fileId, m_segmentId.m_blockId, fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId));
    } else {
        Xid currXid = thrd->GetActiveTransaction()->GetCurrentXid();
        ErrLog(DSTORE_LOG, MODULE_SEGMENT,
            ErrMsg("Table (segmentId={%d, %u}, fsmMetaId = {%d, %u}) is extended for transaction (xid={%lu, %lu})",
                m_segmentId.m_fileId, m_segmentId.m_blockId, fsmMetaPageId.m_fileId, fsmMetaPageId.m_blockId,
                (uint64_t)currXid.m_zoneId, currXid.m_logicSlotId));
    }

    StorageAssert(*targetPageId != INVALID_PAGE_ID);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::GetNewPagesForFSM(PageId *pageIdList, uint16 targetPageCount)
{
    uint16 currentPageIndex = 0;
    uint16 avaliablePageCount = 0;

    while (targetPageCount > 0) {
        BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
        HeapSegmentMetaPage *metaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
        uint16 unassignedCount = metaPage->GetUnassignedPageCount();
        if (unassignedCount == 0) {
            PageId newExtMetaPageId = INVALID_PAGE_ID;
            if (STORAGE_FUNC_FAIL(GetNewExtent(&newExtMetaPageId, segMetaPageBuf))) {
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                return DSTORE_FAIL;
            }
        }

        metaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
        /* We have unassigned pages to be used */
        avaliablePageCount = DstoreMin(metaPage->GetUnassignedPageCount(), targetPageCount);
        StorageAssert(avaliablePageCount > 0);
        /* First page is next page of addedPageId in segment meta page */
        PageId firstPageId = {metaPage->addedPageId.m_fileId,
                              metaPage->addedPageId.m_blockId + 1};
        /* Update addedPageId */
        metaPage->addedPageId.m_blockId += avaliablePageCount;

        /* Record pageId of returned pages */
        for (uint16 i = 0; i < avaliablePageCount; ++i) {
            pageIdList[currentPageIndex++] = {firstPageId.m_fileId, firstPageId.m_blockId + i};
        }
        (void)m_bufMgr->MarkDirty(segMetaPageBuf);
        if (NeedWal()) {
            AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
            WalRecordTbsDataSegmentAssignDataPages walRecord;
            bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
            walRecord.SetHeader({WAL_TBS_SEG_META_ASSIGN_DATA_PAGES, sizeof(walRecord), segMetaPageBuf->GetPageId(),
                                 metaPage->GetWalId(), metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag,
                                 segMetaPageBuf->GetFileVersion()});
            walRecord.SetDataSegmentMeta(metaPage->addedPageId);
            walWriterContext->RememberPageNeedWal(segMetaPageBuf);
            walWriterContext->PutNewWalRecord(&walRecord);
            (void)walWriterContext->EndAtomicWal();
        }
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        /* Update targetPageCount */
        StorageAssert(targetPageCount >= avaliablePageCount);
        targetPageCount -= avaliablePageCount;
    }

    return DSTORE_SUCC;
}

RetStatus HeapSegment::InitNewFsmTree(PdbId pdbId, const PageId &fsmMetaPageId, const PageId &fsmRootPageId,
    BufMgrInterface *bufMgr, bool needWal)
{
    /* needWal : false means heap temp segment ; true means heap normal segment */
    BufferPoolReadFlag flag = ((!needWal) ? BufferPoolReadFlag::CreateNewPage() : BufferPoolReadFlag());

    BufferDesc *fsmMetaPageBuf = bufMgr->Read(pdbId, fsmMetaPageId, LW_EXCLUSIVE, flag);
    if (unlikely(fsmMetaPageBuf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }

    BufferDesc *fsmRootPageBuf = bufMgr->Read(pdbId, fsmRootPageId, LW_EXCLUSIVE, flag);
    if (unlikely(fsmRootPageBuf == INVALID_BUFFER_DESC)) {
        bufMgr->UnlockAndRelease(fsmMetaPageBuf);
        return DSTORE_FAIL;
    }

    /* Initialize the FsmRootPage */
    FsmPage *fsmRootPage = static_cast<FsmPage *>(fsmRootPageBuf->GetPage());
    fsmRootPage->InitFsmPage(fsmRootPageBuf->GetPageId(), fsmMetaPageId, {INVALID_PAGE_ID, INVALID_FSM_SLOT_NUM});
    fsmRootPage->m_header.m_myself = fsmRootPageId;
    (void)bufMgr->MarkDirty(fsmRootPageBuf);

    /* Initialize the FsmMetaPage */
    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    TimestampTz accessTimestamp = GetCurrentTimestampInSecond();
    fsmMetaPage->InitFreeSpaceMapMetaPage(fsmMetaPageBuf->GetPageId(), fsmRootPageId, accessTimestamp);
    (void)bufMgr->MarkDirty(fsmMetaPageBuf);

    if (needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmRootPage->GetWalId() != walWriterContext->GetWalId());
        /* Write wal for fsm root */
        /* Init root fsm page wal */
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsInitFsmPage fsmRootWalData;
        fsmRootWalData.SetHeader({WAL_TBS_INIT_FSM_PAGE, sizeof(fsmRootWalData), fsmRootPageId, fsmRootPage->GetWalId(),
                                  fsmRootPage->GetPlsn(), fsmRootPage->GetGlsn(), glsnChangedFlag,
                                  fsmRootPageBuf->GetFileVersion()});
        fsmRootWalData.SetData(fsmMetaPageId, {INVALID_PAGE_ID, INVALID_FSM_SLOT_NUM}, 0, nullptr);
        walWriterContext->RememberPageNeedWal(fsmRootPageBuf);
        walWriterContext->PutNewWalRecord(&fsmRootWalData);

        glsnChangedFlag = (fsmMetaPage->GetWalId() != walWriterContext->GetWalId());
        /* Init fsm meta page wal */
        WalRecordTbsInitFreeSpaceMap fsmMetaWalData;
        fsmMetaWalData.SetHeader({WAL_TBS_INIT_FSM_META, sizeof(fsmMetaWalData), fsmMetaPageId, fsmMetaPage->GetWalId(),
                                  fsmMetaPage->GetPlsn(), fsmMetaPage->GetGlsn(), glsnChangedFlag,
                                  fsmMetaPageBuf->GetFileVersion()});
        fsmMetaWalData.SetData(fsmRootPageId, accessTimestamp);
        walWriterContext->RememberPageNeedWal(fsmMetaPageBuf);
        walWriterContext->PutNewWalRecord(&fsmMetaWalData);
        (void)walWriterContext->EndAtomicWal();
    }

    bufMgr->UnlockAndRelease(fsmRootPageBuf);
    bufMgr->UnlockAndRelease(fsmMetaPageBuf);

    return DSTORE_SUCC;
}

RetStatus HeapSegment::UnlinkFsmExtent(const PageId &newNextExtMetaPageId, const PageId &curFsmMetaPageId)
{
    /* Read out the FreeSpaceMapMetaPage */
    BufferDesc *fsmMetaBufferDesc = m_bufMgr->
        Read(this->GetPdbId(), curFsmMetaPageId, LW_EXCLUSIVE);
    if (unlikely(fsmMetaBufferDesc == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }

    FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBufferDesc->GetPage());
    fsmMetaPage->UnlinkExtent(newNextExtMetaPageId);
    (void) m_bufMgr->MarkDirty(fsmMetaBufferDesc);
    if (NeedWal()) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (fsmMetaPage->GetWalId() != walWriterContext->GetWalId());

        WalRecordTbsSegmentUnlinkExtent walData;
        walData.SetHeader({WAL_TBS_SEG_UNLINK_EXT, sizeof(walData), curFsmMetaPageId, fsmMetaPage->GetWalId(),
                           fsmMetaPage->GetPlsn(), fsmMetaPage->GetGlsn(), glsnChangedFlag,
                           fsmMetaBufferDesc->GetFileVersion()});
        walData.SetData(newNextExtMetaPageId, INVALID_PAGE_ID, FSM_EXT_SIZE, EXT_FSM_PAGE_TYPE);
        walWriterContext->RememberPageNeedWal(fsmMetaBufferDesc);
        walWriterContext->PutNewWalRecord(&walData);
    }
    m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::FreeFsmExtents(const PageId &firstFsmPageId, const PageId &curFsmMetaPageId,
    const PageId &usedFsmPageId)
{
    /* Now, free each extent that current FSM uses */
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(this->GetPdbId());
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr when FreeFsmExtents, pdbId %u.", this->GetPdbId()));
        return DSTORE_FAIL;
    }
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(m_tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to open tablespace %u.", m_tablespaceId));
        return DSTORE_FAIL;
    }

    PageId curFsmExtMetaPageId = firstFsmPageId;
    while (curFsmExtMetaPageId != INVALID_PAGE_ID) {
        BufferDesc *bufDesc = m_bufMgr->Read(this->GetPdbId(), curFsmExtMetaPageId, LW_SHARED);
        if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
        }
        SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(bufDesc->GetPage());
        PageId nextFsmExtMetaPageId = extMetaPage->extentMeta.nextExtMetaPageId;
        ExtentSize curExtSize = extMetaPage->extentMeta.extSize;
        m_bufMgr->UnlockAndRelease(bufDesc);
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());

        if (IsTempSegment() &&
            STORAGE_FUNC_FAIL(InvalidateBufferInExtent(curFsmExtMetaPageId, curExtSize))) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Invalid temp buffer Start(%hu, %u)added(%hu, %u)size(%hu)fail",
                curFsmExtMetaPageId.m_fileId, curFsmExtMetaPageId.m_blockId,
                usedFsmPageId.m_fileId, usedFsmPageId.m_blockId, static_cast<uint16>(curExtSize)));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(tablespace->FreeExtent(curExtSize, curFsmExtMetaPageId))) {    // Critical-Point
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Free extent(%hu, %u)fail",
                curFsmExtMetaPageId.m_fileId, curFsmExtMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(UnlinkFsmExtent(nextFsmExtMetaPageId, curFsmMetaPageId))) {    // Critical-Point
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Unlink fsm extent(%hu, %u)fail,next(%hu, %u)",
                curFsmExtMetaPageId.m_fileId, curFsmExtMetaPageId.m_blockId,
                nextFsmExtMetaPageId.m_fileId, nextFsmExtMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        (void)walWriterContext->EndAtomicWal();
        curFsmExtMetaPageId = nextFsmExtMetaPageId;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::DropHeapSegmentInternal()
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);

    /* Step 1: Scan through all FSM meta pages and Free their extents */
    HeapSegmentMetaPage *metaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    int i = 0;
    while (i < MAX_FSM_TREE_PER_RELATION) {
        PageId curFsmMetaPageId = metaPage->fsmInfos[i].fsmMetaPageId;
        if (curFsmMetaPageId == INVALID_PAGE_ID) {
            i++;
            continue;
        }

        /* Read out the FreeSpaceMapMetaPage */
        BufferDesc *fsmMetaBufferDesc = m_bufMgr->Read(this->GetPdbId(), curFsmMetaPageId, LW_SHARED);
        if (unlikely(fsmMetaBufferDesc == INVALID_BUFFER_DESC)) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }

        /* Quick check if current FSM did not use any extent */
        FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBufferDesc->GetPage());
        PageId curFsmExtMetaPageId = fsmMetaPage->fsmExtents.first;
        PageId usedFsmPage = fsmMetaPage->usedFsmPage;
        m_bufMgr->UnlockAndRelease(fsmMetaBufferDesc);
        if (curFsmExtMetaPageId == INVALID_PAGE_ID) {
            i++;
            continue;
        }

        if (STORAGE_FUNC_FAIL(FreeFsmExtents(curFsmExtMetaPageId, curFsmMetaPageId, usedFsmPage))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return DSTORE_FAIL;
        }

        i++;
    }
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    /* Step 2: Using DropSegmentInternal() to free data extent */
    if (STORAGE_FUNC_FAIL(DropSegmentInternal())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void HeapSegment::GetFsmMetaPageId(uint32 fsmId, PageId &pageId, NodeId &nodeId)
{
    HeapSegmentMetaPage *segMetaPage;
    BufferDesc *segMetaPageBuf;

    segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    pageId = segMetaPage->fsmInfos[fsmId].fsmMetaPageId;
    nodeId = segMetaPage->fsmInfos[fsmId].assignedNodeId;
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
}

bool HeapSegment::NeedUpdateFsm(uint32 prevSize, uint32 curSize) const
{
    return !(PartitionFreeSpaceMap::IsInSameFreeList(static_cast<uint16>(prevSize), static_cast<uint16>(curSize)));
}

RetStatus HeapSegment::InitHeapSegMetaInfo(PdbId pdbId, BufMgrInterface *bufMgr, SegmentType segType,
    PageId segMetaPageId, bool isReUsedFlag)
{
    BufferPoolReadFlag flag = ((segType == SegmentType::HEAP_TEMP_SEGMENT_TYPE) ?
                               BufferPoolReadFlag::CreateNewPage() : BufferPoolReadFlag());

    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    if (storagePdb == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("fail to get pdb %hhu", pdbId));
        return DSTORE_FAIL;
    }
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalId *walStreamIds = nullptr;
    uint32 walStreamCnt = walStreamMgr->GetOwnedStreamIds(&walStreamIds);
    if (walStreamCnt == 0) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("pdb %s has 0 live wal stream", storagePdb->GetPdbName()));
        return DSTORE_FAIL;
    }

    BufferDesc *segMetaPageBuf = bufMgr->Read(pdbId, segMetaPageId, LW_EXCLUSIVE, flag);
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }

    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    uint64 plsn = walStreamMgr->GetWalStream(*walStreamIds)->GetMaxAppendedPlsn();
    uint64 glsn = segMetaPage->GetGlsn();
    segMetaPage->InitHeapSegmentMetaPage(segType, segMetaPageId, FIRST_EXT_SIZE, plsn, glsn);
    PageId fsmMetaPageId = INVALID_PAGE_ID;
    /*
     * Initialize and add the first FsmMetaPage.
     * The first page after the SegmentMetaPage is always the FsmMetaPage
     */
    fsmMetaPageId = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + 1};
    PageId fsmRootPageId = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + 2};
    bool needWal = (segType == SegmentType::HEAP_SEGMENT_TYPE);
    if (STORAGE_FUNC_FAIL(HeapSegment::InitNewFsmTree(pdbId, fsmMetaPageId, fsmRootPageId, bufMgr, needWal))) {
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        return DSTORE_FAIL;
    }
    /* Record this FSM meta page in DataSegmentMetaPage */
    uint16 fsmId = segMetaPage->numFsms;
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    segMetaPage->fsmInfos[fsmId].fsmMetaPageId = fsmMetaPageId;
    segMetaPage->fsmInfos[fsmId].assignedNodeId = selfNodeId;
    segMetaPage->numFsms += 1;

    segMetaPage->InitSegmentInfo(fsmRootPageId, isReUsedFlag);
    (void)bufMgr->MarkDirty(segMetaPageBuf);

    if (needWal) {
        /* Initialize the SegmentMetaPage */
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());

        /* Init segment meta page wal */
        bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsInitHeapSegment segMetaWalData;
        segMetaWalData.SetHeader({WAL_TBS_INIT_HEAP_SEGMENT_META, sizeof(segMetaWalData), segMetaPageId,
                                  segMetaPage->GetWalId(), segMetaPage->GetPlsn(), segMetaPage->GetGlsn(),
                                  glsnChangedFlag, segMetaPageBuf->GetFileVersion()});
        segMetaWalData.SetData(fsmRootPageId, fsmMetaPageId, selfNodeId, fsmId, plsn, glsn);
        segMetaWalData.SetReUseFlag(isReUsedFlag);
        walWriterContext->RememberPageNeedWal(segMetaPageBuf);
        walWriterContext->PutNewWalRecord(&segMetaWalData);
        (void)walWriterContext->EndAtomicWal();
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Init heap segment for SegmentId (%hu, %u) success, plsn is %lu, glsn is %lu. pdb %u",
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, plsn, glsn, pdbId));

    bufMgr->UnlockAndRelease(segMetaPageBuf);

    return DSTORE_SUCC;
}

RetStatus HeapSegment::PrepareFreeSlots(PartitionFreeSpaceMap *fsm, uint16 *freeSlotCount)
{
    uint16 needExtendPageCount = 0;
    if (STORAGE_FUNC_FAIL(fsm->GetFsmStatus(freeSlotCount, &needExtendPageCount))) {
        return DSTORE_FAIL;
    }
    if (unlikely(*freeSlotCount == 0 && needExtendPageCount == 0)) {
        storage_set_error(TBS_ERROR_FSM_TREE_LEVEL_REACH_MAX);
        return DSTORE_FAIL;
    }
    /* extend fsm if current free slots count is 0 */
    if (*freeSlotCount == 0) {
        StorageAssert(needExtendPageCount > 0);
        if (STORAGE_FUNC_FAIL(ExtendFsmPages(fsm, needExtendPageCount))) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(fsm->AdjustFsmTree(needExtendPageCount))) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(fsm->GetFsmStatus(freeSlotCount, &needExtendPageCount))) {
            return DSTORE_FAIL;
        }
        if (unlikely(*freeSlotCount == 0)) {
            storage_set_error(TBS_ERROR_SEGMENT_HAS_NO_SPACE);
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapSegment::ExtendFsmPages(PartitionFreeSpaceMap *fsm, uint16 needExtendPageCount)
{
    BufferDesc *fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_SHARED);    // Critical-Point
    FreeSpaceMapMetaPage *freeSpaceMapMetaPage =
            static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    uint16 curFreeFsmPageCount = freeSpaceMapMetaPage->GetFreeFsmPageCount();
    PageId lastFsmExtMetaPageId = freeSpaceMapMetaPage->fsmExtents.last;
    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);

    if (curFreeFsmPageCount < needExtendPageCount) {
        if (STORAGE_FUNC_FAIL(GetNewFsmExtent(fsm, lastFsmExtMetaPageId))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapSegment::GetNewFsmExtent(PartitionFreeSpaceMap *fsm, const PageId &lastFsmExtMetaPageId)
{
    /* Fsm extent size is fixed EXT_SIZE_8 */
    ExtentSize fsmExtSize = FSM_EXT_SIZE;
    PageId newExtMetaPageId = INVALID_PAGE_ID;
    bool isFirstFsmExtent = (lastFsmExtMetaPageId == INVALID_PAGE_ID);

    /* Step 1: Alloc extent from tablespace */
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(this->GetPdbId());
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr when GetNewFsmExtent, pdbId %u.", this->GetPdbId()));
        return DSTORE_FAIL;
    }
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(m_tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to open tablespace %u.", m_tablespaceId));
        return DSTORE_FAIL;
    }
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(AllocExtent(
        this->GetPdbId(), tablespace->GetTablespaceId(), fsmExtSize, &newExtMetaPageId, &isReuseFlag))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    /* Step 2: Init new extent meta page */
    if (STORAGE_FUNC_FAIL(InitExtMetaPage(newExtMetaPageId, fsmExtSize))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }
    /* Step 3: Modify next pointer of last fsm extent meta page if new extent is not first fsm extent */
    if (likely(!isFirstFsmExtent)) {
        if (STORAGE_FUNC_FAIL(LinkNextExtInPrevExt(lastFsmExtMetaPageId, newExtMetaPageId))) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
            return DSTORE_FAIL;
        }
    }
    /* Step 4: Modify Data Segment Meta Page */
    if (STORAGE_FUNC_FAIL(DataSegMetaLinkFsmExtent(fsm, newExtMetaPageId, fsmExtSize))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }
    (void)walWriterContext->EndAtomicWal();
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::DataSegMetaLinkFsmExtent(PartitionFreeSpaceMap *fsm, const PageId &newExtMetaPageId,
    ExtentSize extSize)
{
    BufferDesc *fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_EXCLUSIVE);
    FreeSpaceMapMetaPage *freeSpaceMapMetaPage =
            static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());
    PageId fsmMetaPageId = fsmMetaPageBuf->GetPageId();
    freeSpaceMapMetaPage->AddFsmExtent(newExtMetaPageId);
    (void)m_bufMgr->MarkDirty(fsmMetaPageBuf);
    if (NeedWal()) {
        /* Write Wal of DataSegMetaPage */
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        bool glsnChangedFlag = (freeSpaceMapMetaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsSegmentAddExtent walData;
        walData.SetHeader({WAL_TBS_SEG_ADD_EXT, sizeof(walData), fsmMetaPageId, freeSpaceMapMetaPage->GetWalId(),
                           freeSpaceMapMetaPage->GetPlsn(), freeSpaceMapMetaPage->GetGlsn(), glsnChangedFlag,
                           fsmMetaPageBuf->GetFileVersion()});
        walData.SetData(newExtMetaPageId, extSize, EXT_FSM_PAGE_TYPE);
        walWriterContext->RememberPageNeedWal(fsmMetaPageBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::GetFreeFsmIndex(PartitionFreeSpaceMap *fsm, uint16 pageCount, FsmIndex *fsmIndexList)
{
    StorageAssert(pageCount <= PAGES_ADD_TO_FSM_PER_TIME);
    BufferDesc *fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_SHARED);
    FreeSpaceMapMetaPage *freeSpaceMapMetaPage =
            static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());

    PageId curLeafFsmPageId = freeSpaceMapMetaPage->currMap[0];
    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);
    FsmIndex firstFsmIndex;
    if (STORAGE_FUNC_FAIL(fsm->GetFirstFreeFsmIndex(curLeafFsmPageId, &firstFsmIndex))) {
        return DSTORE_FAIL;
    }
    StorageAssert(firstFsmIndex.slot + pageCount <= FSM_MAX_HWM);
    for (uint16 i = 0; i < pageCount; ++i) {
        uint16 curSlotId = firstFsmIndex.slot + i;
        fsmIndexList[i] = {firstFsmIndex.page, curSlotId};
    }
    return DSTORE_SUCC;
}

RetStatus HeapSegment::InitNewDataPageWithFsmIndex(uint16 pageCount, PageId *pageIdList,
    BufferDesc **pageBufList, FsmIndex *fsmIndexList, bool pagesIsReused)
{
    StorageAssert(pageCount <= PAGES_ADD_TO_FSM_PER_TIME);
    Size walSize = sizeof(WalRecordTbsInitDataPages) + pageCount * sizeof(WalRecordLsnInfo);
    WalRecordTbsInitDataPages *walDataPtr = (WalRecordTbsInitDataPages *)DstorePalloc(walSize);
    if (unlikely(walDataPtr == nullptr)) {
        return DSTORE_FAIL;
    }
    Size walLsnInfoSize = sizeof(WalRecordLsnInfo) * pageCount;
    WalRecordLsnInfo *preWalPointer = (WalRecordLsnInfo *)DstorePalloc(walLsnInfoSize);
    if (unlikely(preWalPointer == nullptr)) {
        DstorePfree(walDataPtr);
        return DSTORE_FAIL;
    }

    RetStatus retStatus = DSTORE_FAIL;
    if (!pagesIsReused) {
        retStatus = m_bufMgr->BatchCreateNewPage(this->GetPdbId(), pageIdList, pageCount, pageBufList);
    }
    if (pagesIsReused || STORAGE_FUNC_FAIL(retStatus)) {
        /* when m_lastExtentIsReused is true and batch new page was failing, doing it one-by-one */
        for (uint16 i = 0; i < pageCount; ++i) {
            pageBufList[i] = m_bufMgr->Read(this->GetPdbId(), pageIdList[i], LW_EXCLUSIVE);
            if (unlikely(pageBufList[i] == INVALID_BUFFER_DESC)) {
                DstorePfree(walDataPtr);
                DstorePfree(preWalPointer);
                return DSTORE_FAIL;
            }
        }
    }

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    for (uint16 i = 0; i < pageCount; ++i) {
        Page *newPage = pageBufList[i]->GetPage();
        /* reused page should retain the original lsn and walId */
        if (!pagesIsReused) {
            newPage->SetLsn(0, 0, 0, true);
        }
        preWalPointer[i].SetValue(newPage);
        m_initDataPageCallback(pageBufList[i], pageBufList[i]->GetPageId(), fsmIndexList[i]);
        (void)m_bufMgr->MarkDirty(pageBufList[i]);
        walWriterContext->RememberPageNeedWal(pageBufList[i]);
    }
    /* Write logic wal of init multiple new data pages */
    walDataPtr->SetHeader(WAL_TBS_INIT_MULTIPLE_DATA_PAGES, walSize, pageBufList[0]->GetFileVersion());
    walDataPtr->SetData(PageType::HEAP_PAGE_TYPE, pageBufList[0]->GetPageId(), fsmIndexList[0], pageCount,
                        preWalPointer);
    walWriterContext->PutNewWalRecord(walDataPtr);
    DstorePfree(walDataPtr);
    DstorePfree(preWalPointer);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::AddDataPagesToMetaPages(uint16 slotCount, uint16 dataPageCount, bool isBgExtension,
    BufferDesc *segMetaPageBuf, PageId *addPageList, uint16 &addPageCount, bool &needUpdateExtendCoeff)
{
    uint16 maxAddPageCount = DstoreMin(slotCount, dataPageCount);
    addPageCount = DstoreMin(m_maxAddNewPageCount, maxAddPageCount);
    /* Read data segment meta page in LW_EXCLUSIVE */
    if (STORAGE_FUNC_FAIL(GetFreeDataPageIds(segMetaPageBuf, addPageCount, addPageList))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }

    /*
     * Update the PartitionFreeSpaceMap stat here after extension
     * Note that we only update the extendCoefficient of fsm if current segment contains
     * EXT_SIZE_8192 extents AND we are doing foreground extension.
     */
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    needUpdateExtendCoeff = (!isBgExtension && (segMetaPage->GetExtentCount() > EXT_NUM_LINE[EXTENT_SIZE_COUNT - 1]));

    segMetaPage->AddDataPages(addPageList[0], addPageList[addPageCount - 1], addPageCount);
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);

    if (NeedWal()) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());

        bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsSegMetaAdjustDataPagesInfo walData;
        walData.SetHeader({WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO, sizeof(walData), GetSegmentMetaPageId(),
                           segMetaPage->GetWalId(), segMetaPage->GetPlsn(), segMetaPage->GetGlsn(), glsnChangedFlag,
                           segMetaPageBuf->GetFileVersion()});
        walData.SetData(segMetaPage->dataFirst, segMetaPage->dataLast, segMetaPage->dataBlockCount,
                        segMetaPage->addedPageId);
        walWriterContext->RememberPageNeedWal(segMetaPageBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    return DSTORE_SUCC;
}

RetStatus HeapSegment::AddDataPagesToFsm(PartitionFreeSpaceMap *fsm, PageId *addPageList,
    uint16 addPageCount, bool needUpdateExtendCoeff, bool pagesIsReused)
{
    BufferDesc *pageBufList[PAGES_ADD_TO_FSM_PER_TIME] = {INVALID_BUFFER_DESC};
    FsmIndex pageFsmIndex[PAGES_ADD_TO_FSM_PER_TIME];
    if (STORAGE_FUNC_FAIL(GetFreeFsmIndex(fsm, addPageCount, pageFsmIndex))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(InitNewDataPageWithFsmIndex(addPageCount, addPageList, pageBufList,
        pageFsmIndex, pagesIsReused))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }

    BufferDesc *fsmMetaPageBuf = fsm->ReadFsmMetaPageBuf(LW_SHARED);    // Critical-Point
    FreeSpaceMapMetaPage *freeSpaceMapMetaPage =
            static_cast<FreeSpaceMapMetaPage *>(fsmMetaPageBuf->GetPage());

    PageId curLeafFsmPageId = freeSpaceMapMetaPage->currMap[0];
    fsm->UnlockAndReleaseFsmMetaPageBuf(fsmMetaPageBuf);
    if (STORAGE_FUNC_FAIL(fsm->AddMultipleNewPageToFsm(curLeafFsmPageId, addPageCount, addPageList))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }

    fsm->UpdateFsmStatAfterExtend(addPageCount, needUpdateExtendCoeff);
    if (NeedWal()) {
        (void)thrd->m_walWriterContext->EndAtomicWal();
        for (uint16 i = 0; i < addPageCount; ++i) {
            m_bufMgr->UnlockAndRelease(pageBufList[i]);
        }
    }
    return DSTORE_SUCC;
}

/*
 * This function will check if current HeapSegment has any inactive FSMs. If there is,
 * we will reassign them to other Nodes who have active access to the current HeapSegment
 * based on their needs (i.e. how many free pages are managed by FSMs assigned to a node)
 */
RetStatus HeapSegment::RecycleUnusedFsm()
{
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }

    /* Step 1: Check info on HeapSegmentMetaPage to decide if there are any FSMs to recycle */
    HeapRecycleFsmContext *recycleFsmContext =
        static_cast<HeapRecycleFsmContext *>(DstorePalloc0(sizeof(HeapRecycleFsmContext)));
    if (STORAGE_VAR_NULL(recycleFsmContext)) {
        return DSTORE_FAIL;
    }
    recycleFsmContext->segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    recycleFsmContext->segMetaPage = static_cast<HeapSegmentMetaPage *>(recycleFsmContext->segMetaPageBuf->GetPage());
    if (STORAGE_FUNC_FAIL(FindRecyclableFsm())) {
        UnlockAndReleaseSegMetaPageBuf(recycleFsmContext->segMetaPageBuf);
        DstorePfreeExt(recycleFsmContext);
        return DSTORE_FAIL;
    }

    /* Release locks before doing the real work */
    UnlockAndReleaseSegMetaPageBuf(recycleFsmContext->segMetaPageBuf);

    /* If all FSMs are assigned to the same node or nothing to recycle, return success directly */
    if (recycleFsmContext->allFsmToSameNode || recycleFsmContext->numRecycleFsm == 0) {
        DstorePfreeExt(recycleFsmContext);
        return DSTORE_SUCC;
    }

    /* Do the real recycle and reassignment here */
    if (STORAGE_FUNC_FAIL(RecycleUnusedFsmInternal(*recycleFsmContext))) {
        DstorePfreeExt(recycleFsmContext);
        return DSTORE_FAIL;
    }

    /* Now, we have done the recycle. Clean up allocated resources */
    DstorePfreeExt(recycleFsmContext);
    return DSTORE_SUCC;
}

RetStatus HeapSegment::RecycleUnusedFsmInternal(HeapRecycleFsmContext &context)
{
    /* Step 1: Lock and read out the HeapSegmentMetaPage and prepare for changes */
    context.segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    context.segMetaPage = static_cast<HeapSegmentMetaPage *>(context.segMetaPageBuf->GetPage());

    /* Step 2: Read out the numTotalPages of each pending recycle FSM and create a max heap based on it */
    binaryheap *recycleFsmInfoHeap = OrderRecyclableFsmsBasedOnNumTotalPages(context);
    if (recycleFsmInfoHeap == nullptr) {
        context.recycleFsmInfoHeap = nullptr;
        UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);
        return DSTORE_FAIL;
    }

    /* If binary heap is empty, it means all FSMs are already recycled by others. Return directly here */
    if (binaryheap_empty(recycleFsmInfoHeap)) {
        BinaryheapFree(recycleFsmInfoHeap);
        UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);
        return DSTORE_SUCC;
    } else {
        /* Otherwise, save the max heap inside our context */
        context.recycleFsmInfoHeap = recycleFsmInfoHeap;
    }

    /* Step 3: Summarize information about current FSM assignment and create a max heap based on numTotalPages */
    binaryheap *nodeFsmInfoHeap = OrderNodesBasedOnNumTotalPages(context);
    if (nodeFsmInfoHeap == nullptr) {
        /* Something wrong happened to bufferMgr, fail out here */
        BinaryheapFree(recycleFsmInfoHeap);
        context.recycleFsmInfoHeap = nullptr;
        UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);
        return DSTORE_FAIL;
    }

    /*
     * If the heap is empty, that means all FSMs are recyclable and no other node have active access
     * to their assigned FSMs. If numTotalPagesSumForNodes is 0, that means all active FSMs manages no
     * page and assigned nodes do not have active access to these FSMs neither. Therefore, we just
     * stopped the recycle now since no one is actively accessing this segment.
     */
    if (binaryheap_empty(nodeFsmInfoHeap) || context.numTotalPagesSumForNodes == 0) {
        BinaryheapFree(recycleFsmInfoHeap);
        BinaryheapFree(nodeFsmInfoHeap);
        context.recycleFsmInfoHeap = nullptr;
        UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);

        return DSTORE_SUCC;
    }

    /* save the max heap inside our context for later use */
    context.nodeFsmInfoHeap = nodeFsmInfoHeap;

    /* Step 4: Based on two max heaps, reassign FSM trees now */
    if (STORAGE_FUNC_FAIL(ReassignRecyclableFsm(context))) {
        BinaryheapFree(recycleFsmInfoHeap);
        BinaryheapFree(nodeFsmInfoHeap);
        context.recycleFsmInfoHeap = nullptr;
        context.nodeFsmInfoHeap = nullptr;
        UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);
        return DSTORE_FAIL;
    }

    /* After everything, clean up resources */
    BinaryheapFree(recycleFsmInfoHeap);
    BinaryheapFree(nodeFsmInfoHeap);
    UnlockAndReleaseSegMetaPageBuf(context.segMetaPageBuf);

    return DSTORE_SUCC;
}

/*
 * This function will scan through fsmInfos array in HeapSegmentMetaPage and tries to find FSMs:
 *   1. whose newest access times is old enough based on a pre-set interval
 *   2. which is assigned to non-existing NodeId based on current MemberView
 * If we find any, record its information inside HeapRecycleFsmContext::recycleFsmInfo
 */
RetStatus HeapSegment::FindRecyclableFsm()
{
    return DSTORE_SUCC;
}

/*
 * This function will scan through fsmInfos of HeapSegmentMetaPage and collect how many pages are managed by
 * each recycable FSM. We will return a binaryheap based on the numTotalPages of each FSM which will be used later
 *
 * Also, we detect whether they are recycled or not during the time we grab the lock on the
 * HeapSegmentMetaPage. If a recyclable FSM is already recycled, we will not recycle it twice. Related information
 * will be saved in HeapRecycleFsmContext and return for later use.
 */
binaryheap *HeapSegment::OrderRecyclableFsmsBasedOnNumTotalPages(HeapRecycleFsmContext &context) const
{
    binaryheap *recycleFsmInfoHeap = BinaryheapAllocate(context.numRecycleFsm, RecycleFsmInfo::Comparator, nullptr);
    for (int i = 0; i < context.numRecycleFsm; i++) {
        /* First, check if current Recycle Fsm is already recycled by others */
        uint16 fsmId = context.recycleFsmInfo[i].fsmId;
        NodeId oldNodeId = context.recycleFsmInfo[i].fsmInfo.assignedNodeId;
        NodeId curNodeId = context.segMetaPage->fsmInfos[fsmId].assignedNodeId;
        if (curNodeId != oldNodeId) {
            /* This FSM is already reassigned, so we don't need to recycle it now */
            context.needRecycle[i] = false;
            continue;
        }

        /* Update the numTotalPages of all recycle FSMs from previously collected information */
        context.numTotalPagesSumForRecycleFsm += context.recycleFsmInfo[i].numTotalPages;

        /* Add this RecycleFsmInfo into the heap */
        BinaryheapAddUnordered(recycleFsmInfoHeap, PointerGetDatum(&context.recycleFsmInfo[i]));
        context.needRecycle[i] = true;
    }

    /* Build the heap if it is not empty */
    if (!binaryheap_empty(recycleFsmInfoHeap)) {
        BinaryheapBuild(recycleFsmInfoHeap);
    }

    return recycleFsmInfoHeap;
}

/*
 * This function will scan through fsmInfos of HeapSegmentMetaPage and collect how many pages are managed by
 * each active node. We will return a binaryheap based on the numTotalPages of each active Node which will be
 * used later.
 */
binaryheap *HeapSegment::OrderNodesBasedOnNumTotalPages(HeapRecycleFsmContext &context)
{
    binaryheap *nodeFsmInfoHeap = BinaryheapAllocate(context.segMetaPage->numFsms,
                                                     NodeFsmInfo::NumTotalPagesComparator,
                                                     nullptr);
    int curRecycleFsmIndex = 0;

    /* Loop through all existing FSMs */
    for (int i = 0; i < context.segMetaPage->numFsms; i++) {
        /*
         * Step 1. Skip FSMs that are already in our pending list and have not yet been recycled
         * Note: the sequence of FsmInfo in pendingRecycleFsm should be the same as that in
         * segMetaPage->fsmInfos since we never change the sequence of FSMs in fsmInfos.
         */
        if ((context.segMetaPage->fsmInfos[i].fsmMetaPageId ==
             context.recycleFsmInfo[curRecycleFsmIndex].fsmInfo.fsmMetaPageId) &&
            context.needRecycle[curRecycleFsmIndex]) {
            curRecycleFsmIndex++;
            continue;
        }

        /* Step 2. Read info of this FSM and get its numTotalPages */
        PageId fsmMetaPageId = context.segMetaPage->fsmInfos[i].fsmMetaPageId;
        /* its ok to read the MVCC version of the fsmMetaPage here since we only want its numTotalPage */
        BufferDesc *fsmMetaBuf = m_bufMgr->Read(this->GetPdbId(), fsmMetaPageId, LW_SHARED);
        if (fsmMetaBuf == INVALID_BUFFER_DESC) {
            BinaryheapFree(nodeFsmInfoHeap);
            return nullptr;
        }

        FreeSpaceMapMetaPage *fsmMetaPage = static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
        uint64 curNumTotalPages = fsmMetaPage->GetNumTotalPages();
        m_bufMgr->UnlockAndRelease(fsmMetaBuf);

        /* Step 3. Check if this NodeId is already in our nodeFsmInfo array */
        bool found = false;
        for (int j = 0; j < context.numNodeFsmInfo; j++) {
            if (context.nodeFsmInfo[j].nodeId == context.segMetaPage->fsmInfos[i].assignedNodeId) {
                /* Add curNumTotalPages to numTotalPages of corresponding nodeFsmInfo and continue */
                context.nodeFsmInfo[j].numTotalPages += curNumTotalPages;
                context.numTotalPagesSumForNodes += curNumTotalPages;
                found = true;
                break;
            }
        }

        /* Step 4. Add this NodeId to nodeFsmInfo array if not found */
        if (!found) {
            context.nodeFsmInfo[context.numNodeFsmInfo].nodeId = context.segMetaPage->fsmInfos[i].assignedNodeId;
            context.nodeFsmInfo[context.numNodeFsmInfo].numTotalPages = curNumTotalPages;
            context.numTotalPagesSumForNodes += curNumTotalPages;
            /* Add this NodeFsmInfo into the heap */
            BinaryheapAddUnordered(nodeFsmInfoHeap, PointerGetDatum(&context.nodeFsmInfo[context.numNodeFsmInfo]));
            context.numNodeFsmInfo++;
        }
    }

    /* Build the heap if it is not empty */
    if (!binaryheap_empty(nodeFsmInfoHeap)) {
        BinaryheapBuild(nodeFsmInfoHeap);
    }

    return nodeFsmInfoHeap;
}

/*
 * This function will reassign recycable FSMs to active nodes based on information saved in HeapRecycleFsmContext
 *
 * We reassign FSMs based on following rules:
 * 1. We assign FSMs to nodes that has the most numTotalPages since it represents higher
 *     need to free pages
 * 2. We keep reassign FSMs to the same node until we assigned a certain amount of free
 *     pages to it:
 *                          (numTotalPages per node)
 *   addedNumTotalPages = ---------------------------  * (numTotalPages of all recycled FSM)
 *                        (numTotalPages of all node)
 */
RetStatus HeapSegment::ReassignRecyclableFsm(HeapRecycleFsmContext &context)
{
    uint64 addedNumTotalPages = 0;

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    while (!binaryheap_empty(context.recycleFsmInfoHeap)) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        RecycleFsmInfo *candidateFsm = static_cast<RecycleFsmInfo *>(static_cast<void *>(DatumGetPointer(
            BinaryheapFirst(context.recycleFsmInfoHeap))));
        NodeFsmInfo *targetNode = static_cast<NodeFsmInfo *>(static_cast<void *>(DatumGetPointer(
            BinaryheapFirst(context.nodeFsmInfoHeap))));

        /* Reassign candidateFsm to targetNode on HeapSegmentMetaPage */
        uint16 fsmId = candidateFsm->fsmId;
        context.segMetaPage->fsmInfos[fsmId].assignedNodeId = targetNode->nodeId;
        (void)m_bufMgr->MarkDirty(context.segMetaPageBuf);
        /* Write WAL for reassign fsm tree here */
        GenerateWalForRecycleFsmTree(context.segMetaPageBuf, fsmId);

        /* Update the accessTimestamp of the reassigned FSM to avoid reassignment again */
        BufferDesc *fsmMetaBuf = m_bufMgr->Read(this->GetPdbId(), candidateFsm->fsmInfo.fsmMetaPageId, LW_EXCLUSIVE);
        if (fsmMetaBuf == INVALID_BUFFER_DESC) {
            (void)walWriterContext->EndAtomicWal();
            return DSTORE_FAIL;
        }
        PartitionFreeSpaceMap::UpdateFsmAccessTimestamp(fsmMetaBuf, m_bufMgr);
        m_bufMgr->UnlockAndRelease(fsmMetaBuf);

        ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
               ErrMsg("Heap segment (%hu, %u) reassign FSM (%hu, %u) from Node %u to Node %u successfully",
                      GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
                      candidateFsm->fsmInfo.fsmMetaPageId.m_fileId, candidateFsm->fsmInfo.fsmMetaPageId.m_blockId,
                      candidateFsm->fsmInfo.assignedNodeId, targetNode->nodeId));

        /* Now, decide if we should reassign next recyclable FSM to a different node. */
        addedNumTotalPages += candidateFsm->numTotalPages;
        double numTotalPageRatio = static_cast<double>(targetNode->numTotalPages) /
                                   static_cast<double>(context.numTotalPagesSumForNodes);
        uint64 numTotalPagesThreshold = static_cast<uint64>(numTotalPageRatio * context.numTotalPagesSumForRecycleFsm);
        if (addedNumTotalPages > numTotalPagesThreshold) {
            BinaryheapRemoveFirst(context.nodeFsmInfoHeap);
            addedNumTotalPages = 0;
        }
        BinaryheapRemoveFirst(context.recycleFsmInfoHeap);
        (void)walWriterContext->EndAtomicWal();
    }

    return DSTORE_SUCC;
}

/*
 * Build a scan handler of heap's fsmtree to get Segment Data Block's freespace.
 */
DiagnoseIterator *HeapSegment::HeapFsmScan()
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    DstoreMemoryContext mctx = g_dstoreCurrentMemoryContext;
    HeapFreespaceDiagnose *hfsDiag = nullptr;

    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(segMetaPageBuf->GetPage());
    hfsDiag = DstoreNew(mctx) HeapFreespaceDiagnose(m_bufMgr, m_pdbId, segMetaPage->numFsms);
    if (hfsDiag == nullptr) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Allocate memory for freespace diagnose failed."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return nullptr;
    }

    for (uint16 i = 0; i < segMetaPage->numFsms; i++) {
        if (!hfsDiag->AddOneFsmTreeTask(segMetaPage->fsmInfos[i].fsmMetaPageId)) {
            hfsDiag->End();
            delete hfsDiag;
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            return nullptr;
        }
    }
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    if (!hfsDiag->Init()) {
        hfsDiag->End();
        delete hfsDiag;
        return nullptr;
    }
    return hfsDiag;
}
