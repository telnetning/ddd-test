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
 * dstore_btree_split_strategy.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_split_strategy.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_STORAGE_BTREE_SPLIT_STRATEGY
#define DSTORE_STORAGE_BTREE_SPLIT_STRATEGY

#include "index/dstore_btree.h"

namespace DSTORE {

/* strategy for searching through materialized list of split points */
enum class FindSplitStrategy {
    SPLIT_DEFAULT,                /* give all split points a chance */
    SPLIT_AFTER_NEWITEM,          /* split right after new item */
    SPLIT_MANY_DUPLICATES,        /* grouped split points */
    SPLIT_SINGLE_VALUE            /* leave left page almost full */
};

constexpr int LOC_DISTANCE_LIMIT = 9;

struct SplitPoint {
    int curDelta;
    int leftPageFreeSpace;
    int rightPageFreeSpace;
    OffsetNumber firstRightOff;
    bool newItemOnLeft;
};

class BtreeSplitStrategy : public Btree {
public:
    BtrPage *m_targetPage;
    OffsetNumber m_newItemOff;
    IndexTuple *m_newItem;

    uint32 m_newItemSize;
    int m_leftPageUsableSpace;
    int m_rightPageUsableSpace;
    int m_oldDataItemsTotalSpace;

    bool m_isLeaf;
    bool m_isRightMost;
    OffsetNumber m_maxOffsetNumber;

    uint16 m_maxSplitPointsNum;
    uint16 m_nSplitPoints;
    uint32 m_minFirstRightSize;
    SplitPoint *m_splitPoints;

    BtreeSplitStrategy(StorageRelation indexRel, IndexInfo *indexInfo, BtrPage *targetPage,
                       OffsetNumber newItemOff, IndexTuple *newItem);

    ~BtreeSplitStrategy() final;

    RetStatus InitSplitPoint(uint8 addedTdCount);

    OffsetNumber FindSplitLoc(bool *newItemOnLeft);

    bool SplitAfterNewItemOff();

    double GetFillFactor(bool *useFillFactor) const;

    uint32 GetPerfectPenalty();

    FindSplitStrategy SetStrategy(uint32 *perfectPenalty);

    void RecordSplitLoc(OffsetNumber firstRightOff, bool newItemOnLeft,
                        int oldDataItemSpaceToLeft, uint32 firstRightOriginSize);

    RetStatus ScanThroughPage();
};

class BtrSplitPointSelectorBase : public BaseObject {
public:
    virtual ~BtrSplitPointSelectorBase();
    /* context data for findSplitPoint */
    IndexInfo   *m_indexInfo;
    BtrPage     *m_targetPage;
    IndexTuple  *m_newItem;
    uint32       m_newItemSize;
    OffsetNumber m_newItemOff;

    /* candidate split point data */
    uint16       m_nSplitPoints;
    SplitPoint  *m_splitPoints;

    BtrSplitPointSelectorBase(IndexInfo *indexInfo, BtreeSplitStrategy *strategy);

    uint32 GetSplitPenalty(SplitPoint *split);

    virtual OffsetNumber GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft);
};

/*
 * Naive strategy is used when we simply split after new item, which is
 * perfect when given a monotonically increasing insertions sequence.
 */
class BtrSplitPointAfterNewItemSelector : public BtrSplitPointSelectorBase {
public:
    BtrSplitPointAfterNewItemSelector(IndexInfo *indexInfo, BtreeSplitStrategy *strategy);

    OffsetNumber GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft) final;
};

/*
 * Many duplicates strategy is used when a heap TID would otherwise be
 * appended, but the page isn't completely full of logical duplicates.
 */
class BtrSplitPointManyDuplicatesSelector : public BtrSplitPointSelectorBase {
public:
    BtrSplitPointManyDuplicatesSelector(IndexInfo *indexInfo, BtreeSplitStrategy *strategy);

    OffsetNumber GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft) final;
};

/*
 * Single value strategy is used when it is impossible to avoid appending
 * a heap TID.  It arranges to leave the left page very full, using fillfactor
 */
class BtrSplitPointSingleValuesSelector : public BtrSplitPointSelectorBase {
public:
    BtrSplitPointSingleValuesSelector(IndexInfo *indexInfo, BtreeSplitStrategy *strategy);

    OffsetNumber GetBestSplitLoc(uint32 perfectpenalty, bool *newitemonleft) final;
};

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_BTREE_SPLIT_STRATEGY
