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
 * dstore_btree_delta_index_for_ccindex.h
 *
 * IDENTIFICATION
 *        dstore/include/index/concurrent/dstore_btree_delta_index_for_ccindex.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_DELTA_INDEX_FOR_CCINDEX_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_DELTA_INDEX_FOR_CCINDEX_H

#include "index/dstore_btree_insert.h"

namespace DSTORE {

struct BtreeInfo {
    StorageRelation indexRel;
    IndexInfo *indexInfo;
    ScanKey scanKey;
    bool isDeltaForLpi;
    int retryTimes;
};

class BtreeDeltaDmlForCcindex final : public BtreeInsert {
public:
    ~BtreeDeltaDmlForCcindex() final {};

    BtreeDeltaDmlForCcindex() = delete;
    BtreeDeltaDmlForCcindex(const BtreeDeltaDmlForCcindex &) = delete;
    BtreeDeltaDmlForCcindex &operator=(const BtreeDeltaDmlForCcindex &) = delete;

    /**
     * Update the record in DeltaDmlTable to the newest version
     *
     * @param btrInfo btree information of the DeltaDmlTable's index that we'll update for
     * @param values Datum of IndexTuple we're going to insert into DeltaDmlTable's index.
     * @param isNulls Bits map of is-null flags of IndexTuple we're going to insert into DeltaDmlTable's index.
     * @param deltaHeapCtid the heapCtid on DeltaDmlTable of the original heap record.
     * @param output duplicateDeltaRec, tells caller that we cannot insert the record for now. Caller should remove the
     *                                  old duplicateDeltaRec first, then retry inserting the record.
     */
    static RetStatus UpdateDeltaDmlRec(BtreeInfo btrInfo, Datum *values, bool *isNulls, ItemPointer deltaHeapCtid,
                                       ItemPointer duplicateDeltaRec);

    /**
     * Check if a record exist in DeltaDmlTable
     *
     * @param btrInfo btree information of the DeltaDmlTable's index that we'll search for
     * @param values Datum of IndexTuple we're going to search from DeltaDmlTable's index.
     * @param isNulls Bits map of is-null flags of IndexTuple we're going to search from DeltaDmlTable's index.
     * @param output deltaRec, tells caller that we've found the requested record.
     */
    static RetStatus CheckExistence(BtreeInfo btrInfo, Datum *values, bool *isNulls, ItemPointer deltaRec);

private:
    BtreeDeltaDmlForCcindex(BtreeInfo btrInfo);

    /**
     * Search for the previous record of the given keys and heapCtid(of the original heap Table) from DeltaDmlTable
     */
    RetStatus SearchForPreviousRecord();
    /**
     * Implementation of the DeltaDmlTable record insertion if no previous record found
     */
    RetStatus InsertDeltaDmlRec();

    void InitSearchingKeys() override;
    RetStatus CheckConflict() override;

    void SetSearchingInfo(Datum *values, bool *isNulls, ItemPointer delDmlHeapCtid);

    bool m_isOrigIndexLpi;
    ItemPointerData m_duplicateHeapCtid;

    ItemPointerData m_delDmlHeapCtid;
    ItemPointerData m_heapCtid;
    DmlOperationTypeForCcindex m_operType;
    Datum *m_values;
    bool *m_isNulls;
};

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_DELTA_INDEX_FOR_CCINDEX_H */