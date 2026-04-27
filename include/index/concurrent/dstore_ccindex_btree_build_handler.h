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
 * dstore_ccindex_btree_build_handler.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_ccindex_btree_build_handler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_CCINDEX_BTREE_BUILD_HANDLER_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_CCINDEX_BTREE_BUILD_HANDLER_H

#include "index/dstore_btree_build.h"
#include "index/dstore_index_handler.h"

namespace DSTORE {

static constexpr int NUM_ATTRS_DELTA_DML_INDEX = 2;
static constexpr long DEFAULT_UPDATE_INTERVAL = 500000; /* 500 ms */

enum class WaitForTrxsEndType : uint8 {
    WAIT_FOR_INDEX_META_VISIBLE_FOR_ALL = 0,
    WAIT_FOR_DELTA_DML_TRXS_END,
    WAIT_FOR_OLD_INDEX_TRXS_END,
    WAIT_FOR_OLD_INDEX_INVISIBLE_FOR_ALL,
    NO_MORE_WAIT
};

class CcindexBtrBuildHandler : public BtreeBuild {
public:
    CcindexBtrBuildHandler() = delete;
    CcindexBtrBuildHandler(const CcindexBtrBuildHandler &) = delete;
    CcindexBtrBuildHandler &operator=(const CcindexBtrBuildHandler &) = delete;

    RetStatus WaitForTrxVisibleForAll(PdbId pdbId, Xid xid);
    RetStatus BuildIndexConcurrently();
    RetStatus MergeDeltaDml(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo);
    RetStatus HandleAndMergeEachDeltaRecord(IndexTuple *deltaDmlIndexTup, TupleDesc deltaDmlIndexTupDesc);

    static CcindexBtrBuildHandler *Create(StorageRelation indexRel, IndexBuildInfo *indexBuildInfo,
                                          ScanKey scanKey, bool isLpi)
    {
        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("[CCINDEX] create index(%s): begin, snapshot csn %lu",
            indexBuildInfo->baseInfo.indexRelName, thrd->GetSnapShotCsn()));
        
        CcindexBtrBuildHandler *ccindexBtree = DstoreNew(g_dstoreCurrentMemoryContext)
                                               CcindexBtrBuildHandler(indexRel, indexBuildInfo, scanKey, isLpi);
        if (unlikely(ccindexBtree == nullptr)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to create CcindexBtrBuildHandler for creating index(%s) concurrently",
                    indexBuildInfo->baseInfo.indexRelName));
        }

        return ccindexBtree;
    }

    virtual void Destroy();

    inline void UpdateIndexRel(StorageRelation indexRel)
    {
        m_indexRel = indexRel;
    }

    StorageRelation GetIndexRel()
    {
        return m_indexRel;
    }

    inline void UpdateBtrBuildInfo(IndexBuildInfo *indexBuildInfo)
    {
        if (likely(indexBuildInfo != nullptr)) {
            m_indexBuildInfo = indexBuildInfo;
            m_indexInfo = &indexBuildInfo->baseInfo;
            return;
        }
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"indexBuildInfo\".", __FUNCTION__));
        PrintBackTrace();
    }

    inline void SetRebuildFlag()
    {
        m_isRebuild = true;
    }

    uint64 m_deltaTupleScanned = 0;
    uint64 m_deltaTupleInsert = 0;
    uint64 m_deltaTupleDelete = 0;
    uint64 m_deltaTupleSkipped = 0;

protected:
    CcindexBtrBuildHandler(StorageRelation indexRel, IndexBuildInfo *indexBuildInfo, ScanKey scanKey,
                           bool isLpi);
    virtual RetStatus WaitForAllCurrentActiveTrxsEnd(PdbId pdbId, CommitSeqNo targetCsn, TimestampTz startTime);

    bool m_isLpi;

private:
    RetStatus WaitForGlobalCsnPushToSpecificCsn(PdbId pdbId, CommitSeqNo targetCsn);
    RetStatus MergeDeltaDmlForNonLocal(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo);
    RetStatus MergeDeltaDmlForLpi(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo);

    RetStatus InitMergeScanHandler(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo);

    RetStatus DoMerge(Datum *values, bool *isnull, ItemPointer heapCtid,
                      DmlOperationTypeForCcindex tupleOperationType);

    inline bool CheckDmlOperationType(DmlOperationTypeForCcindex type)
    {
        return type == DmlOperationTypeForCcindex::DML_OPERATION_INSERT ||
            type == DmlOperationTypeForCcindex::DML_OPERATION_DELETE;
    }
    inline void SetCcindexBtrStatus(BtrCcidxStatus status)
    {
        m_indexInfo->btrIdxStatus = status;
    }

    bool m_isRebuild;
    WaitForTrxsEndType m_status;  /* default value is WAIT_FOR_INDEX_META_VISIBLE_FOR_ALL */
    IndexScanHandler *m_dmlScanHandler;
};
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_CCINDEX_BTREE_BUILD_HANDLER_H */