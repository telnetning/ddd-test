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
 * dstore_heap_struct.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/heap/dstore_heap_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_HANDLER_STRUCT_H
#define DSTORE_HEAP_HANDLER_STRUCT_H

#include "page/dstore_itemptr.h"
#include "transaction/dstore_transaction_struct.h"
#include "tuple/dstore_tuple_struct.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {

constexpr int DEFAULT_HEAP_FILLFACTOR = 100;

enum class HeapHandlerFailureReason : uint8_t {
    UNKNOWN = 0,
    SELF_CREATED = 1,
    SELF_MODIFIED = 2,
    UPDATED = 3,
    DELETED = 4,
    ALLOC_TD_FAILED = 5,
    DEADLOCK = 6,
    INVISIBLE_TO_SNAPSHOT = 7,
    ALLOC_TRANS_SLOT_FAILED = 8,
    CHECK_TUPLE_CHANGED_FAILED = 9,
    GET_NEWEST_CTID_FAILED = 10,
    READ_BUFFER_FAILED = 11,
    LOCK_TUP_FAILED = 12,
    UPDATE_FAILED = 13,
    DELETE_FAILED = 14,
    INVALID_PARAM = 15,
    INIT_HANDLER_FAILED = 16,
    LOB_FETCH_TUPLE_FAILED = 17,
    LOB_CTX_INTI_FAILED = 18,
    LOB_FORMAT_TUPLE_FAILED = 19,
    LOB_UPDATE_FAILED = 20,
    LOB_MAKE_TUPLE_FAILED = 21,
    SEQ_NEXTVAL_EXCEED_MAXVALUE_FAILED = 22,
    SEQ_NEXTVAL_EXCEED_MINVALUE_FAILED = 23,
    SEQ_LAST_VALUE_INVALID = 24,
    LOCK_WAIT_CANCELED = 25,
    LOCK_WAIT_TIMEOUT = 26,
    ROLLBACK_BIG_TUPLE_FAILED = 27,
    SUCCESS = 28
};

struct FailureInfo {
    ItemPointerData ctid;
    CommandId cid;
    HeapHandlerFailureReason reason;

    /* INVALID_CID = ~static_cast<CommandId>(0) */
    FailureInfo() : ctid(INVALID_ITEM_POINTER), cid(~static_cast<CommandId>(0)),
        reason(HeapHandlerFailureReason::UNKNOWN)
    {}

    inline void SetCtid(ItemPointerData failCtid)
    {
        ctid = failCtid;
    }
    inline void SetCid(uint32_t failCid)
    {
        cid = failCid;
    }
    inline void SetReason(HeapHandlerFailureReason failReason)
    {
        reason = failReason;
    }
};

struct HeapLockTupleContext {
    HeapTuple *retTup;
    bool needRetTup;
    /* allow to lock heap tuple if insert or modified by self, aka failureInfo.reason is SELF_CREATED, SELF_MODIFIED */
    bool allowLockSelf;
    SnapshotData snapshot;
    ItemPointerData ctid;
    bool executedEpq = false;
    FailureInfo failureInfo;
    HeapLockTupleContext() : retTup{nullptr}, needRetTup{false}, allowLockSelf{false} {}
};

struct HeapInsertContext {
    HeapTuple *heapTuple;
    ItemPointerData ctid;
    CommandId cid;
};

struct HeapBacthInsertContext {
    HeapInsertContext *contexts;
    uint16_t count;
};

struct HeapDeleteContext {
    ItemPointerData ctid;

    /*
     * needReturnTup is a hint bit to tell handler
     * if the caller needs a copy of tuple back to
     * update index information
     *
     * Copied old tuple will be saved in returnTup which is
     * palloced. It is caller's responsibility to pfree it
     *
     * When the deleted tuple has lobs, needDeleteLob is True
     * and returnTup will be used to store the deleted tuple
     * temporarily in order to delete lobs using the ctids in
     * it. If needReturnTup is True, return the deleted Tuple
     * with lobs appended. Otherwise, pfree returnTup before
     * return.
     */
    HeapTuple *returnTup;
    bool needReturnTup;
    bool needDeleteLob;
    bool executedEpq = false;
    Bitmapset *replicaKeyAttrs; /* used for replication to identify the deleted tuple, see FormReplicaIdentityTuple. */
    FailureInfo failureInfo;
    SnapshotData snapshot;
    CommandId cid;
    HeapDeleteContext()
        : returnTup{nullptr}, needReturnTup{false}, needDeleteLob{false}, replicaKeyAttrs{nullptr}
    {
        snapshot.Init();
        cid = FIRST_CID;
    }
};

struct HeapUpdateContext {
    ItemPointerData oldCtid;
    ItemPointerData newCtid;
    HeapTuple *newTuple;

    /*
     * needReturnOldTup is a hint bit to tell handler
     * if the caller needs a copy of old tuple back to
     * update index information
     *
     * Copied old tuple will be saved in retOldTuple which is
     * palloced. It is caller's responsibility to pfree it
     *
     * When the updated tuple has lobs, needUpdateLob is True
     * and retOldTuple will be used to store the updated tuple
     * temporarily in order to update lobs using the ctids in
     * it. If needReturnOldTup is True, return the updated
     * Tuple with lobs appended. Otherwise, pfree
     * retOldTuple before return.
     */
    HeapTuple *retOldTuple;
    bool needUpdateLob;
    bool needReturnOldTup;
    bool hasIndex;    /* for another-page update, if table is indexed, return old tuple anyway. */
    Bitmapset *replicaKeyAttrs; /* used for replication to identify the updated tuple, see FormReplicaIdentityTuple. */
    FailureInfo failureInfo;
    SnapshotData snapshot;
    CommandId cid;

    /*
    * Even if the tuple has already been locked, we still need to check tuple changed in order to excute epq.
    * However the above may cause that we can not update or delete tuple forever, therefore, we do not check
    * tuple again when epq excuted.
    */
    bool executedEpq = false;
    HeapUpdateContext()
        : newTuple{nullptr},
          retOldTuple{nullptr},
          needUpdateLob{false},
          needReturnOldTup{false},
          hasIndex{true},
          replicaKeyAttrs{nullptr}
    {
        snapshot.Init();
        cid = FIRST_CID;
    }
};

/* Sample Scan information */
struct HeapSampleScanContext {
    int curBlockNum;
    PageId curPageId;
    int lastBlockNum;
    int numLiveTuples;
    int numDeadTuples;
    int numTuples;
    HeapTuple *tuples[MAX_ITEM_OFFSET_NUMBER];

    explicit HeapSampleScanContext()
        : curBlockNum{0}, curPageId{INVALID_PAGE_ID}, lastBlockNum{0}, numLiveTuples{0}, numDeadTuples{0}, numTuples{0}
    {
        for (int i = 0; i < MAX_ITEM_OFFSET_NUMBER; i++) {
            tuples[i] = nullptr;
        }
    }

    void SetSampleBlockNum(int blocknum)
    {
        lastBlockNum = curBlockNum;
        curBlockNum = blocknum;
    }
};

struct SeqContext {
    ItemPointerData ctid;
    /*
     * The retTuple is a copy of new Tuple,
     * if failureInfo is not UNKNOWN. Otherwise,
     * Copied old tuple will be saved in retTuple to log sequence information.
     *
     * It is caller's responsibility to pfree it.
     */
    HeapTuple *retTuple;
    FailureInfo failureInfo;
    SeqContext()
        : ctid(INVALID_ITEM_POINTER),
          retTuple(nullptr)
    {
    }
};
} /* namespace DSTORE */
#endif
