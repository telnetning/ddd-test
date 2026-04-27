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
 * dstore_index_tuple.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/tuple/dstore_index_tuple.h
 *
 * ---------------------------------------------------------------------------------------
 */
/*
 * dstore_index_tuple.h
 *
 *  Created on: Mar 7, 2022
 *      Author: zanky
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_TUPLE_DSTORE_INDEX_TUPLE_H
#define SRC_GAUSSKERNEL_INCLUDE_TUPLE_DSTORE_INDEX_TUPLE_H

#include "common/log/dstore_log.h"
#include "page/dstore_itemptr.h"
#include "tuple/dstore_tupledesc.h"
#include "common/memory/dstore_mctx.h"
#include "tuple/dstore_data_tuple.h"
#include "index/dstore_index_struct.h"
#include "errorcode/dstore_index_error_code.h"

namespace DSTORE {

constexpr uint16 INDEX_SIZE_MASK = 0x1FFF;
constexpr uint16 INDEX_TUPLE_SIZE = 16;
/*
 * Notes on B-Tree tuple format, and key and non-key attributes:
 *
 * INCLUDE B-Tree indexes have non-key attributes.  These are extra
 * attributes that may be returned by index-only scans, but do not influence
 * the order of items in the index (formally, non-key attributes are not
 * considered to be part of the key space).  Non-key attributes are only
 * present in leaf index tuples whose item pointers actually point to heap
 * tuples (non-pivot tuples).  _bt_check_natts() enforces the rules
 * described here.
 *
 * Non-pivot tuple format (plain/non-posting variant):
 *
 *  m_heap_ctid | m_info | key values | INCLUDE columns, if any
 *
 * m_heap_ctid points to the heap TID, which is a tiebreaker key column as of
 * BTREE_VERSION 4.
 *
 * Non-pivot tuples complement pivot tuples, which only have key columns.
 * The sole purpose of pivot tuples is to represent how the key space is
 * separated.  In general, any B-Tree index that has more than one level
 * (i.e. any index that does not just consist of a metapage and a single
 * leaf root page) must have some number of pivot tuples, since pivot
 * tuples are used for traversing the tree.  Suffix truncation can omit
 * trailing key columns when a new pivot is formed, which makes minus
 * infinity their logical value.  Since BTREE_VERSION 4 indexes treat heap
 * TID as a trailing key column that ensures that all index tuples are
 * physically unique, it is necessary to represent heap TID as a trailing
 * key column in pivot tuples, though very often this can be truncated
 * away, just like any other key column. (Actually, the heap TID is
 * omitted rather than truncated, since its representation is different to
 * the non-pivot representation.)
 *
 * Pivot tuple format:
 *
 *  m_heap_ctid | m_info | key values | [heap TID]
 *
 * We store the number of columns present inside pivot tuples by abusing
 * their m_heap_ctid offset field, since pivot tuples never need to store a real
 * offset (pivot tuples generally store a downlink in m_heap_ctid, though).  The
 * offset field only stores the number of columns/attributes when the
 * INDEX_ALT_TID_MASK bit is set, which doesn't count the trailing heap
 * TID column sometimes stored in pivot tuples -- that's represented by
 * the presence of BT_PIVOT_HEAP_TID_ATTR.  The INDEX_ALT_TID_MASK bit in
 * m_info is always set on BTREE_VERSION 4 pivot tuples, since
 * BTreeTupleIsPivot() must work reliably on heapkeyspace versions.
 *
 * In version 2 or version 3 (!heapkeyspace) indexes, INDEX_ALT_TID_MASK
 * might not be set in pivot tuples.  BTreeTupleIsPivot() won't work
 * reliably as a result.  The number of columns stored is implicitly the
 * same as the number of columns in the index, just like any non-pivot
 * tuple. (The number of columns stored should not vary, since suffix
 * truncation of key columns is unsafe within any !heapkeyspace index.)
 *
 * The 12 least significant bits from m_heap_ctid's offset number are used to
 * represent the number of key columns within a pivot tuple.  This leaves 4
 * status bits (BT_STATUS_OFFSET_MASK bits), which are shared by all tuples
 * that have the INDEX_ALT_TID_MASK bit set (set in m_info) to store basic
 * tuple metadata.  BTreeTupleIsPivot() and BTreeTupleIsPosting() use the
 * BT_STATUS_OFFSET_MASK bits.
 *
 * Sometimes non-pivot tuples also use a representation that repurposes
 * m_heap_ctid to store metadata rather than a TID.  PostgreSQL v13 introduced a
 * new non-pivot tuple format to support deduplication: posting list
 * tuples.  Deduplication merges together multiple equal non-pivot tuples
 * into a logically equivalent, space efficient representation.  A posting
 * list is an array of ItemPointerData elements.  Non-pivot tuples are
 * merged together to form posting list tuples lazily, at the point where
 * we'd otherwise have to split a leaf page.
 *
 * Posting tuple format (alternative non-pivot tuple representation):
 *
 *  m_heap_ctid | m_info | key values | posting list (TID array)
 *
 * Posting list tuples are recognized as such by having the
 * INDEX_ALT_TID_MASK status bit set in m_info and the BT_IS_POSTING status
 * bit set in m_heap_ctid's offset number.  These flags redefine the content of
 * the posting tuple's m_heap_ctid to store the location of the posting list
 * (instead of a block number), as well as the total number of heap TIDs
 * present in the tuple (instead of a real offset number).
 *
 * The 12 least significant bits from m_heap_ctid's offset number are used to
 * represent the number of heap TIDs present in the tuple, leaving 4 status
 * bits (the BT_STATUS_OFFSET_MASK bits).  Like any non-pivot tuple, the
 * number of columns stored is always implicitly the total number in the
 * index (in practice there can never be non-key columns stored, since
 * deduplication is not supported with INCLUDE indexes).
 */
/*
 * normal leaf :
 * heapCtid | m_info | key values | INCLUDE columns
 *
 * pivot tuple
 * heapCtid | m_info | key values | [heap TID]
 * key values may be truncated ,so we need to store the number of key in the t_info
 *
 * posting tuple
 * heapCtid | m_info | key values | posting list (TID array)
 */
union IndexLink {
    ItemPointerData heapCtid; /* reference TID to heap tuple */
    struct Value {
        /*
         * pivot tuple: downlink to the lower level, only need page id
         * posting tuple: offset to the start point of the tid array
         */
        PageId lowlevelIndexpageLink;
        /*
         * pivot tuple: store the key number as they may get truncated which different from catalog
         * posting tuple: indicating the number of tid it stores
         */
        uint16 num : 12;
        uint16 hasCtidBreaker : 1; /* indicating has tid as tie breaker in pivot tuple. */
        uint16 isposting : 1;  /* indicating this is a posting list leaf tuple */
        uint16 hasTableOid : 1; /* indicating has partition oid for global partition index in pivot tuple. */
        uint16 unused : 1;   /* for future use also align with latest pg */
    } val;

    IndexLink() : heapCtid(0)
    {
    }
};

struct IndexTuple : public DataTuple {
    /* Link info for Index Tuple */
    IndexLink m_link;

    /*
     * m_info is laid out in the following fashion:
     */
    union {
        uint32 m_info;
        struct Value {
            uint32 m_hasNull : 1;               /* has nulls */
            uint32 m_hasVarwidth : 1;           /* has var-width attributes */
            uint32 m_notPlainLeaf : 1;          /* 0 normal leaf tuple, 1 :either pivot or leaf with posting list. */
            uint32 m_tupleSize : 13;            /* size of tuple */
            uint32 m_tdId : 8;                  /* point to td slot */
            uint32 m_tdStatus : 2;              /* Whether the td is shared */
            uint32 m_isDeleted : 1;             /* Whether tuple is deleted */
            uint32 m_ccindexInsForDelFlag : 1;  /* Whether tuple is inserting tuple which is deleted for ccindex */
            uint32 m_ccindexStatus : 2;         /* 0: Normal index, 1: WRITE_ONLY_INDEX, 2: IN_BUILDING_INDEX */
            /*
             * Whether the tuple has same key and ctid with the last tuple on the left page, this flag will be set only
             * in the following case: when splitting a leaf page, the first tuple on the right page is equal to
             * the last tuple on the left page (i.e., they have same key and ctid, and for GPI,
             * they should also have same tableoid, but note that they can have different including columns),
             * we then set this flag for the first tuple on the right page in this case.
             * If this flag is set, skip rollback the undo record of it when construct cr page on the right page.
             * Because it will rollback on the left page: we should avoid rollback it twice.
             */
            uint32 m_isSameWithLastLeft : 1;
            uint32 m_unused : 1;
        } val;
    } m_info;

    static int Compare(IndexTuple *tuple1, IndexTuple *tuple2, IndexInfo *indexInfo);
    static IndexTuple *FormTuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull, uint32 *tupleSize = nullptr);
    static IndexTuple *FormTuple(HeapTuple *heapTuple, IndexBuildInfo *info, Datum *values,
                                 bool *isnulls, uint32 *tupleSize = nullptr);
    IndexTuple *Truncate(TupleDesc desc, int keepNatt);
    void CheckAttIsNull(int attnum, bool &slow);
    Datum GetAttrNocache(int attnum, TupleDesc desc);

    inline void SetSize(uint32 indexSize)
    {
        /*
         * Here we make sure that the size will fit in the field reserved for it
         * in m_info.
         */
        StorageAssert((indexSize & INDEX_SIZE_MASK) == indexSize);
        m_info.val.m_tupleSize = indexSize;
    }

    /* Get size of the tuple */
    inline uint32 GetSize() const
    {
        return m_info.val.m_tupleSize;
    }

    inline void SetDeleted()
    {
        m_info.val.m_isDeleted = 1;
    }

    inline void SetNotDeleted()
    {
        m_info.val.m_isDeleted = 0;
    }

    inline bool IsDeleted() const
    {
        return m_info.val.m_isDeleted == 1;
    }

    inline void SetCcindexInsForDelFlag()
    {
        m_info.val.m_ccindexInsForDelFlag = 1;
    }
 
    inline void SetNotInsertDeletedForCCindex()
    {
        m_info.val.m_ccindexInsForDelFlag = 0;
    }
 
    inline bool IsInsertDeletedForCCindex() const
    {
        return m_info.val.m_ccindexInsForDelFlag == 1;
    }

    inline void SetCcindexStatus(BtrCcidxStatus status)
    {
        m_info.val.m_ccindexStatus = static_cast<uint32>(status);
    }

    inline BtrCcidxStatus GetCcindexStatus() const
    {
        return static_cast<BtrCcidxStatus>(m_info.val.m_ccindexStatus);
    }

    inline bool TestCcindexStatus(BtrCcidxStatus status) const
    {
        BtrCcidxStatus tupStatus = GetCcindexStatus();
        return tupStatus == status;
    }

    inline void SetSameWithLastLeft()
    {
        m_info.val.m_isSameWithLastLeft = 1;
    }

    inline void SetNotSameWithLeftPageLastTuple()
    {
        m_info.val.m_isSameWithLastLeft = 0;
    }

    inline bool IsSameWithLastLeft() const
    {
        return m_info.val.m_isSameWithLastLeft == 1;
    }

    inline void SetHeapCtid(ItemPointer ctid)
    {
        m_link.heapCtid = *ctid;
    }

    inline void SetTdId(uint8 inTdId)
    {
        m_info.val.m_tdId = inTdId;
    }

    inline uint8 GetTdId() const
    {
        return m_info.val.m_tdId;
    }

    inline uint8 GetLockerTdId() const
    {
        return INVALID_TD_SLOT;
    }

    inline void SetTdStatus(TupleTdStatus status)
    {
        m_info.val.m_tdStatus = static_cast<uint32>(status);
    }

    inline TupleTdStatus GetTdStatus() const
    {
        return static_cast<TupleTdStatus>(m_info.val.m_tdStatus);
    }

    inline bool TestTdStatus(TupleTdStatus status) const
    {
        return static_cast<TupleTdStatus>(m_info.val.m_tdStatus) == status;
    }

    inline bool HasCtidBreaker() const
    {
        return static_cast<bool>(m_link.val.hasCtidBreaker);
    }

    inline Oid* GetPivotTableOid()
    {
        if (m_link.val.hasTableOid == false) {
            return nullptr;
        }
        uint32 offset = sizeof(Oid);
        if (m_link.val.hasCtidBreaker) {
            offset += sizeof(ItemPointerData);
        }

        return static_cast<Oid *>(
            static_cast<void *>(static_cast<char *>(static_cast<void *>(this)) + GetSize() - offset));
    }

    inline uint16 GetPostingTidNum() const
    {
        return m_link.val.num;
    }

    inline ItemPointerData GetHeapCtid()
    {
        if (IsPivot()) {
            return GetPivotHeapCtid();
        }
        return m_link.heapCtid;
    }

    inline const ItemPointerData GetHeapCtid() const
    {
        if (IsPivot()) {
            return GetPivotHeapCtid();
        }
        return m_link.heapCtid;
    }

    inline Oid GetTableOid(IndexInfo *indexInfo)
    {
        Oid tableOid = DSTORE_INVALID_OID;

        if (IsPivot()) {
            Oid *tableOidPtr = GetPivotTableOid();
            if (tableOidPtr) {
                tableOid = *tableOidPtr;
            }
        } else {
            StorageAssert(indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX));
            bool isNull;
            tableOid = GetAttr(indexInfo->tableOidAtt, indexInfo->attributes, &isNull);
            StorageAssert(tableOid != DSTORE_INVALID_OID);
        }

        return tableOid;
    }

    inline static uint32 GetDataOffset(bool hasNull)
    {
        if (!hasNull) {
            return static_cast<uint32>(MAXALIGN(sizeof(IndexTuple)));
        }
        return static_cast<uint32>(MAXALIGN(sizeof(IndexTuple) + static_cast<uint32>(GetBitmapLen(INDEX_MAX_KEY_NUM))));
    }

    inline uint32 GetDataOffset() const
    {
        if (!HasNull()) {
            return static_cast<uint32>(MAXALIGN(sizeof(IndexTuple)));
        }
        return static_cast<uint32>(MAXALIGN(sizeof(IndexTuple) + static_cast<uint32>(GetBitmapLen(INDEX_MAX_KEY_NUM))));
    }

    inline char *GetValues()
    {
        return static_cast<char*>(static_cast<void *>(this)) + GetDataOffset();
    }

    inline uint32 GetValueSize() const
    {
        return GetSize() - GetDataOffset();
    }

    inline uint8 *GetNullBitmap()
    {
        if (HasNull()) {
            return (static_cast<uint8 *>(static_cast<void *>(this)) + sizeof(IndexTuple));
        }
        return nullptr;
    }

    inline bool AttrIsNull(int attnum)
    {
        StorageAssert(HasNull());
        char *bp = static_cast<char *>(static_cast<void *>(this)) + sizeof(IndexTuple);
        return DataTuple::DataTupleAttrIsNull(static_cast<uint32>(attnum), bp);
    }

    inline void SetHasNull()
    {
        m_info.val.m_hasNull = 1;
    }

    inline bool HasNull() const
    {
        return static_cast<bool>(m_info.val.m_hasNull);
    }

    inline void SetHasVariable()
    {
        m_info.val.m_hasVarwidth = 1;
    }

    inline bool HasVariable() const
    {
        return static_cast<bool>(m_info.val.m_hasVarwidth);
    }

    /* is pivot index tuple */
    /* change the tid usage, but we have 2 options. 1) pivot tuple  2)posting tuple */
    inline bool IsPivot() const
    {
        return m_info.val.m_notPlainLeaf != 0 && !m_link.val.isposting;
    }

    inline IndexTuple *Copy()
    {
        uint32 tupleSize = GetSize();
        if (unlikely(tupleSize < INDEX_TUPLE_SIZE)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("Invalid index tuple size %u. indexlink:%lu", tupleSize, m_link.heapCtid.m_placeHolder));
            storage_set_error(INDEX_ERROR_TUPLE_DAMAGED);
            return nullptr;
        }
        IndexTuple* result = static_cast<IndexTuple *>(DstorePalloc(tupleSize));
        if (unlikely(result == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when IndexTuple Copy."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return nullptr;
        }
        StorageAssert(tupleSize > 0);
        errno_t rc = memcpy_s(static_cast<void*>(result), tupleSize, static_cast<void*>(this), tupleSize);
        storage_securec_check(rc, "\0", "\0");
        return result;
    }

    /*
     * generate a new tuple with partition oid for global partition index, as all the key can not distinguish 2 tuples
     * in pivot
     */
    inline IndexTuple *CopyWithTableOid(uint16 keynum, Oid tableOid)
    {
        uint32 tupleSize = GetSize() + sizeof(Oid);
        IndexTuple* result = static_cast<IndexTuple *>(DstorePalloc(tupleSize));
        if (unlikely(result == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when IndexTuple CopyWithTableOid."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return nullptr;
        }
        errno_t rc = memcpy_s(static_cast<void*>(result), GetSize(), static_cast<void*>(this), GetSize());
        storage_securec_check(rc, "\0", "\0");
        result->SetSize(tupleSize);
        result->SetKeyNum(keynum, false, true);
        Oid* resTableOid = result->GetPivotTableOid();
        *resTableOid = tableOid;
        return result;
    }

    /*
     * generate a new tuple with tiebreaker tid, as all the key can not distinguish 2 tuples in pivot
     */
    inline IndexTuple *CopyWithTableOidAndCtid(uint16 keynum, Oid tableOid, ItemPointerData ctid)
    {
        uint32 tupleSize = GetSize() + sizeof(ItemPointerData);
        bool addTableOid = false;
        if (tableOid != DSTORE_INVALID_OID) {
            tupleSize += sizeof(Oid);
            addTableOid = true;
        }
        IndexTuple* result = static_cast<IndexTuple *>(DstorePalloc0(tupleSize));
        if (unlikely(result == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when IndexTuple CopyWithCtid."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return nullptr;
        }
        errno_t rc = memcpy_s(static_cast<void*>(result), GetSize(), static_cast<void*>(this), GetSize());
        storage_securec_check(rc, "\0", "\0");
        result->SetSize(tupleSize);
        result->SetKeyNum(keynum, true, addTableOid);
        if (addTableOid) {
            Oid* resTableOid = result->GetPivotTableOid();
            *resTableOid = tableOid;
        }
        if (likely(result->HasCtidBreaker())) {
            result->SetPivotHeapCtid(ctid);
        }
        return result;
    }

    Datum GetAttr(int attnum, TupleDesc desc, bool *isNull)
    {
        *isNull = false;
        if (!HasNull()) {
            Form_pg_attribute attr = DstoreTupleDescAttr(desc, attnum - 1);
            return (attr->attcacheoff >= 0) ? FetchAtt(attr, (GetValues() + attr->attcacheoff)) :
                                              GetAttrNocache(attnum, desc);
        }
        if (AttrIsNull(static_cast<uint16>(attnum - 1))) {
            *isNull = true;
            return static_cast<Datum>(0);
        }
        return GetAttrNocache(attnum, desc);
    }

    Oid GetAttrTableOid(int16 tableOidAttNum, TupleDesc tupleDesc)
    {
        Oid tableOid = DSTORE_INVALID_OID;
        bool isNull;
        tableOid = DatumGetUInt32(GetAttr(tableOidAttNum, tupleDesc, &isNull));
        StorageAssert(tableOid != DSTORE_INVALID_OID);
        return tableOid;
    }

    inline void DeformTuple(TupleDesc tupleDesc, Datum *values, bool *isNull)
    {
        char* tupleValues = GetValues();
        char* nullValues = static_cast<char*>(static_cast<void*>(GetNullBitmap()));
        int end = tupleDesc->natts;

        TupleAttrContext attrContext = {tupleDesc, values, isNull, 0, false};
        DisassembleDataContext context = {
            attrContext, 0, end, tupleValues, nullValues, nullptr
        };
        if (likely(!HasNull())) {
            DataTuple::DisassembleData<false>(context);
        } else {
            DataTuple::DisassembleData<true>(context);
        }
    }

    /* Set link for this index tuple */
    inline void SetLowlevelIndexpageLink(const PageId &dst)
    {
        m_link.val.lowlevelIndexpageLink = dst;
    }

    inline PageId GetLowlevelIndexpageLink() const
    {
        return m_link.val.lowlevelIndexpageLink;
    }

    inline OffsetNumber GetLinkOffset() const
    {
        return m_link.heapCtid.GetOffset();
    }

    inline void SetKeyNum(uint16 keyNum, bool heapTid, bool hasTableOid = false)
    {
        m_info.val.m_notPlainLeaf = 1;
        m_link.val.hasCtidBreaker = static_cast<uint32>(heapTid);
        if (hasTableOid) {
            m_link.val.hasTableOid = 1;
        }
        m_link.val.isposting = 0;
        m_link.val.num = keyNum;
    }

    /*
     * Get number of attributes within tuple.
     * if pivot tuple, the key may truncated
     * or leaf tuple, key number  in the meta data
     * Note that this does not include an implicit tiebreaker heap TID
     * attribute, if any.  Note also that the number of key attributes must be
     * explicitly represented in all heapkeyspace pivot tuples.
     *
     */
    inline uint16 GetKeyNum(uint16 keyNum) const
    {
        /* If it is pivot tuple return */
        if (IsPivot()) {
            return m_link.val.num;
        }
        return keyNum;
    }

    /*
     * The key value used is
     * "minus infinity", a sentinel value that's reliably less than any real
     * key value that could appear in the left page.
     */
    static inline IndexTuple *CreateMinusinfPivotTuple(IndexTuple *tup = nullptr)
    {
        if (tup == nullptr) {
            tup = static_cast<IndexTuple *>(DstorePalloc0(sizeof(IndexTuple)));
            if (unlikely(tup == nullptr)) {
                storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when CreateMinusinfPivotTuple."));
                return nullptr;
            }
        }
        tup->SetSize(sizeof(IndexTuple));
        tup->SetKeyNum(0, false);
        return tup;
    }

    inline void SetHasExternal() const
    {
        /* not use in index now */
        StorageAssert(false);
    }

    inline void SetHasInlineLobValue() const
    {
        /* not use in index now */
        StorageReleasePanic(true, MODULE_INDEX, ErrMsg("index tuple should not set has inline lob."));
    }

    inline void ResetHasInlineLobValue() const
    {
        /* not use in index now */
        StorageReleasePanic(true, MODULE_INDEX, ErrMsg("index tuple should not reset has inline lob."));
    }

    inline bool HasInlineLobValue() const
    {
        /* not use in index now */
        return false;
    }

    inline void ResetInfo()
    {
        m_info.m_info = 0;
    }

    inline void ResetAttrInfo()
    {
        m_info.val.m_hasNull = 0;
        m_info.val.m_hasVarwidth = 0;
    }

private:
    inline ItemPointerData GetPivotHeapCtid()
    {
        if (m_link.val.hasCtidBreaker) {
            return *static_cast<ItemPointer>(static_cast<void *>(
                    static_cast<char *>(static_cast<void *>(this)) + GetSize() - sizeof(ItemPointerData)));
        }
        return INVALID_ITEM_POINTER; /* heap tid is truncated */
    }

    inline const ItemPointerData GetPivotHeapCtid() const
    {
        if (m_link.val.hasCtidBreaker) {
            return *static_cast<const ItemPointerData *>(static_cast<const void *>(
                static_cast<const char *>(static_cast<const void *>(this)) + GetSize() - sizeof(ItemPointerData)));
        }
        return INVALID_ITEM_POINTER; /* heap tid is truncated */
    }

    inline void SetPivotHeapCtid(ItemPointerData heapCtid)
    {
        if (m_link.val.hasCtidBreaker) {
            ItemPointer pivotHeapCtid =  static_cast<ItemPointer>(static_cast<void *>(
                    static_cast<char *>(static_cast<void *>(this)) + GetSize() - sizeof(ItemPointerData)));
            *pivotHeapCtid = heapCtid;
        }
    }
};
static_assert(sizeof(IndexTuple) == INDEX_TUPLE_SIZE, "INDEX_TUPLE_SIZE is incorrect.");

}  // namespace DSTORE


#endif /* SRC_GAUSSKERNEL_INCLUDE_TUPLE_STORAGE_INDEX_TUPLE_H */
