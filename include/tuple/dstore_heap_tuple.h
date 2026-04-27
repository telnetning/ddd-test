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
 * dstore_heap_tuple.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/tuple/dstore_heap_tuple.h
 *
 * represent disk heap tuple format
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_TUPLE_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_TUPLE_H

#include "page/dstore_itemptr.h"
#include "tuple/dstore_data_tuple.h"
#include "transaction/dstore_transaction_types.h"

namespace DSTORE {

const uint32 MAX_LINK_TUP_CHUNKS = 0xFFFFFFFF;
const uint16 LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE = sizeof(ItemPointerData) + sizeof(uint32);

enum class HeapDiskTupLiveMode {
    TUPLE_BY_NORMAL_INSERT = 0,
    NEW_TUPLE_BY_INPLACE_UPDATE,
    NEW_TUPLE_BY_SAME_PAGE_UPDATE,
    OLD_TUPLE_BY_SAME_PAGE_UPDATE,
    OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE,
    NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE,
    TUPLE_BY_NORMAL_DELETE
};

enum class HeapDiskTupLinkInfoType {
    TUP_NO_LINK_TYPE = 0,
    TUP_LINK_FIRST_CHUNK_TYPE = 1,
    TUP_LINK_NOT_FIRST_CHUNK_TYPE = 2
};

struct HeapDiskTuple : public DataTuple {
    /*
     * DatumField is valid when tuple is formed by SQL engine,
     *     this should never be accessed or modified after PrepareTuple.
     * TupleField is valid when written to or retrieved from pages.
     * Lengths of TupleField and DatumField needs must match so that
     * we don't need to do extra copy when writing mem tuple into physical pages.
     */
    union {
        struct TupleField {
            uint8 m_tdId;
            uint8 m_lockerTdId;
            uint16 m_size;
            Xid m_xid;
        } m_tuple_info;
        struct DatumField {
            int32 m_len;    /* varlena header (do not touch directly!) */
            int32 m_typmod; /* -1, or identifier of a record type */
            Oid m_typeid;   /* composite type OID, or RECORDOID */
        } m_datum_info;
    } m_ext_info;

    union {
        uint32 m_info;
        struct Value {
            uint32 m_hasNull : 1;
            uint32 m_hasVarwidth : 1;
            uint32 m_hasExternal : 1;
            uint32 m_hasOid : 1;
            uint32 m_tdStatus : 2;
            uint32 m_liveMode : 3;
            uint32 m_linkInfo : 2;
            uint32 m_numColumn : 11;
            uint32 m_HasInlineLobValue : 1;
            /* This bit is used as a temporary flag during hash joins.
             * Marked reserved so that future disk-only 1-bit attribute can also reuse this bit. */
            uint32 m_reserved_for_sql : 1;
            uint32 m_unused : 8;
        } val;
    } m_info;

    char m_data[];

    inline uint8 GetLockerTdId() const
    {
        return m_ext_info.m_tuple_info.m_lockerTdId;
    }

    inline void SetLockerTdId(uint8 tdId)
    {
        m_ext_info.m_tuple_info.m_lockerTdId = tdId;
    }

    inline uint16 GetTupleSize() const
    {
        return m_ext_info.m_tuple_info.m_size;
    }

    inline void SetTupleSize(uint16 size)
    {
        m_ext_info.m_tuple_info.m_size = size;
    }

    inline uint8 GetTdId() const
    {
        return m_ext_info.m_tuple_info.m_tdId;
    }

    inline Xid GetXid() const
    {
        return m_ext_info.m_tuple_info.m_xid;
    }

    inline TupleTdStatus GetTdStatus() const
    {
        return static_cast<TupleTdStatus>(m_info.val.m_tdStatus);
    }

    inline char *GetData()
    {
        return m_data;
    }
    inline void *GetDiskTupleData()
    {
        return static_cast<void *>(m_data);
    }

    inline char *GetValues()
    {
        char *retAddr = static_cast<char *>(static_cast<void *>(this)) + GetValuesOffset();
        return retAddr;
    }

    static inline uint32 GetHeaderSize()
    {
        static const uint32 headerSize = offsetof(HeapDiskTuple, m_data);
        return headerSize;
    }

    inline static uint32 GetValuesOffset(uint16 attnum, bool hasNull, bool hasOid, bool isLinked)
    {
        uint32 offset = static_cast<uint32>(GetHeaderSize() +
                       (static_cast<uint8>(hasNull)) * DataTuple::GetBitmapLen(attnum) +
                       (static_cast<uint8>(hasOid)) * sizeof(Oid) +
                       (static_cast<uint8>(isLinked)) * LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE);

        return MAXALIGN(offset);
    }

    inline uint32 GetValuesOffset() const
    {
        return HeapDiskTuple::GetValuesOffset(GetNumColumn(), HasNull(), HasOid(), IsLinked());
    }

    inline uint16 GetNumColumn() const
    {
        return m_info.val.m_numColumn;
    }

    inline void SetNumColumn(uint32 attNum)
    {
        m_info.val.m_numColumn = attNum;
    }

    inline uint8 *GetNullBitmap()
    {
        if (HasNull()) {
            /* next chunk ctid + number of linked tupchunk */
            if (unlikely(IsLinked())) {
                return static_cast<uint8 *>(static_cast<void *>(m_data + LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE));
            }
            return static_cast<uint8 *>(static_cast<void *>(m_data));
        }
        return nullptr;
    }

    inline Oid GetOid() const
    {
        StorageAssert(!IsLinked() || IsFirstLinkChunk());
        if (HasOid()) {
            if (!IsLinked()) {
                return *static_cast<const Oid *>(static_cast<const void *>(
                    static_cast<const char *>(static_cast<const void *>(this)) + GetValuesOffset() - sizeof(Oid)));
            }
            if (IsFirstLinkChunk()) {
                /*
                 * We can't use GetValuesOffset() for the first chunk. When copying header and data
                 * from a unlinked tuple to the first chunk, we never recalculate the alignment with
                 * the link header considered. We just copy the HeapDiskTuple header, skip the space
                 * needed for the link header and then copy the data. The bitmap and the oid are actually
                 * copied as part of the data instead of the header. So when calculating the valuesOffset,
                 * we just need to manually add link header size to the offset.
                 */
                uint32 offset = GetValuesOffset(GetNumColumn(), HasNull(), HasOid(), false) +
                    LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE;
                return *static_cast<const Oid *>(static_cast<const void *>(
                    static_cast<const char *>(static_cast<const void *>(this)) + offset - sizeof(Oid)));
            }
        }
        return DSTORE_INVALID_OID;
    }

    inline void SetOid(Oid oid)
    {
        StorageAssert(HasOid());
        *((static_cast<Oid *>(static_cast<void *>(static_cast<char *>(static_cast<void *>(this)) +
            GetValuesOffset() - sizeof(Oid))))) = oid;
    }

    inline bool HasNull() const
    {
        return m_info.val.m_hasNull == 1;
    }

    inline bool HasExternal() const
    {
        return m_info.val.m_hasExternal == 1;
    }

    inline bool HasOid() const
    {
        return m_info.val.m_hasOid == 1;
    }

    inline void SetTdStatus(TupleTdStatus status)
    {
        m_info.val.m_tdStatus = static_cast<uint32>(status);
    }

    inline bool TestTdStatus(TupleTdStatus status) const
    {
        return m_info.val.m_tdStatus == static_cast<uint32>(status);
    }

    inline void SetLiveMode(HeapDiskTupLiveMode mode)
    {
        m_info.val.m_liveMode = static_cast<uint32>(mode);
    }

    inline bool TestLiveMode(HeapDiskTupLiveMode mode) const
    {
        return m_info.val.m_liveMode == static_cast<uint32>(mode);
    }

    inline HeapDiskTupLiveMode GetLiveMode() const
    {
        return static_cast<HeapDiskTupLiveMode>(m_info.val.m_liveMode);
    }

    inline void SetHasNull()
    {
        m_info.val.m_hasNull = 1;
    }

    inline void SetHasExternal()
    {
        m_info.val.m_hasExternal = 1;
    }

    inline void SetHasVariable()
    {
        m_info.val.m_hasVarwidth = 1;
    }

    inline void SetHasInlineLobValue()
    {
        m_info.val.m_HasInlineLobValue = 1;
    }

    inline void ResetHasInlineLobValue()
    {
        m_info.val.m_HasInlineLobValue = 0;
    }

    inline bool HasInlineLobValue() const
    {
        return static_cast<bool>(m_info.val.m_HasInlineLobValue);
    }

    inline bool HasVariable() const
    {
        return static_cast<bool>(m_info.val.m_hasVarwidth);
    }

    inline void SetHasOid()
    {
        m_info.val.m_hasOid = 1;
    }

    inline void SetTdId(uint8 tdId)
    {
        m_ext_info.m_tuple_info.m_tdId = tdId;
    }

    inline void SetXid(Xid xid)
    {
        m_ext_info.m_tuple_info.m_xid = xid;
    }

    inline bool AttrIsNull(int attrNum)
    {
        if (unlikely(attrNum > GetNumColumn())) {
            return true;
        }
        /*
         * attr cannot be null if
         *     1) this tuple does not have null column
         *     2) attrNum is system attribute number
         */
        if (!HasNull() || attrNum < 0) {
            return false;
        }

        if (unlikely(IsLinked())) {
            return DataTuple::DataTupleAttrIsNull(static_cast<uint32>(attrNum),
                m_data + LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE);
        } else {
            return DataTuple::DataTupleAttrIsNull(static_cast<uint32>(attrNum), m_data);
        }
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

    inline uint32 GetInfo() const
    {
        return m_info.m_info;
    }

    inline void SetInfo(uint32 info)
    {
        m_info.m_info = info;
    }

    inline void SetNoLink()
    {
        m_info.val.m_linkInfo = static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_NO_LINK_TYPE);
    }

    inline void SetFirstLinkChunk()
    {
        m_info.val.m_linkInfo = static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_FIRST_CHUNK_TYPE);
    }

    inline void SetNotFirstLinkChunk()
    {
        m_info.val.m_linkInfo = static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_NOT_FIRST_CHUNK_TYPE);
    }

    inline bool IsNotFirstLinkChunk() const
    {
        return m_info.val.m_linkInfo == static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_NOT_FIRST_CHUNK_TYPE);
    }

    inline bool IsLinked() const
    {
        return m_info.val.m_linkInfo != static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_NO_LINK_TYPE);
    }

    inline uint32 GetLinkInfo() const
    {
        return m_info.val.m_linkInfo;
    }

    inline bool IsFirstLinkChunk() const
    {
        return m_info.val.m_linkInfo == static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_FIRST_CHUNK_TYPE);
    }

    inline void SetNextChunkCtid(ItemPointerData ctid)
    {
        StorageAssert(IsLinked());
        ItemPointerData *nextCtid = static_cast<ItemPointerData *>(GetDiskTupleData());
        *nextCtid = ctid;
    }

    inline ItemPointerData GetNextChunkCtid()
    {
        StorageAssert(IsLinked());
        return *(static_cast<ItemPointerData *>(GetDiskTupleData()));
    }

    inline void SetNumTupChunks(uint32 numTupChunks)
    {
        uint32 *numTupChunksPtr = static_cast<uint32 *>(static_cast<void *>(m_data + sizeof(ItemPointerData)));
        *numTupChunksPtr = numTupChunks;
    }

    inline uint32 GetNumChunks() const
    {
        if (IsLinked()) {
            StorageAssert(IsFirstLinkChunk());
            return *static_cast<const uint32 *>(static_cast<const void *>(m_data + sizeof(ItemPointerData)));
        }
        return 1;
    }

    inline void SetDatumTypeId(Oid typId)
    {
        m_ext_info.m_datum_info.m_typeid = typId;
    }

    inline Oid GetDatumTypeId()
    {
        return m_ext_info.m_datum_info.m_typeid;
    }

    inline void SetDatumTypeMod(int32 typMod)
    {
        m_ext_info.m_datum_info.m_typmod = typMod;
    }

    inline Oid GetDatumTypeMod()
    {
        return m_ext_info.m_datum_info.m_typmod;
    }

    inline bool GetReservedForSql()
    {
        return static_cast<bool>(m_info.val.m_reserved_for_sql);
    }

    inline void SetReservedForSql(bool val)
    {
        m_info.val.m_reserved_for_sql = val;
    }
};

constexpr uint8 HEAP_DISK_TUP_HEADER_SIZE = static_cast<uint8>(sizeof(HeapDiskTuple));

/* When tuple is too long and it needs to be divided into multiple chunks. */
struct HeapTupChunks {
    uint32 m_chunkNum;             /* How many chunks */
    uint16 *m_chunksSize;         /* the size of each chunk */
    HeapDiskTuple **m_chunksData; /* the data of each chunk */
};

}  // namespace DSTORE

#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_TUPLE_H */
