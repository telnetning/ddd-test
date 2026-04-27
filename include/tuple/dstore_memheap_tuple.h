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
 * dstore_memheap_tuple.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/tuple/dstore_memheap_tuple.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_DSTORE_MEMHEAP_TUPLE_H
#define SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_DSTORE_MEMHEAP_TUPLE_H

#include "tuple/dstore_heap_tuple.h"

namespace DSTORE {

struct HeapTupleHeader {
    uint32 len;           /* length of data */
    uint8 type;           /* record dstore type for sql engine integration, will remove later */
    union {
        uint8 info;
        struct Value {
            uint8 isAllocMem : 1;
            uint8 isExternalMem : 1;
            uint8 unused : 6;
        } val;
    } tupleInfo;
    ItemPointerData ctid; /* point to current tid */
    Oid tableOid;
    int32 datumTypmod;    /* -1, or identifier of a record type */
    Oid datumTypeid;      /* composite type OID, or RECORDOID */
    Xid deleteXidForDebug;  /* deletionXid for show any tuple */
    Oid lobTargetOid;       /* lobLocator->relid */

    HeapTupleHeader() : len(0), type(DSTORE_TUPLE_TYPE), ctid(INVALID_ITEM_POINTER), tableOid(0), datumTypmod(0),
        datumTypeid(0), deleteXidForDebug(INVALID_XID), lobTargetOid(DSTORE_INVALID_OID)
    {
        tupleInfo.info = 0;
    }
    void Init()
    {
        len = 0;
        type = DSTORE_TUPLE_TYPE;
        tupleInfo.info = 0;
        ctid = INVALID_ITEM_POINTER;
        tableOid = 0;
        datumTypmod = 0;
        datumTypeid = 0;
        deleteXidForDebug = INVALID_XID;
        lobTargetOid = DSTORE_INVALID_OID;
    }
};

struct HeapTuple {
    HeapTupleHeader m_head;
    HeapDiskTuple *m_diskTuple;

    void Init()
    {
        m_diskTuple = nullptr;
        m_head.Init();
    }

    inline uint32 GetDiskTupleSize() const
    {
        return m_head.len;
    }

    inline void SetDiskTupleSize(uint32 size)
    {
        m_head.len = size;
        m_head.type = DSTORE_TUPLE_TYPE;
    }

    inline void SetDiskTuple(HeapDiskTuple *diskTuple)
    {
        m_diskTuple = diskTuple;
    }
    inline HeapDiskTuple *GetDiskTuple()
    {
        return m_diskTuple;
    }

    inline Oid GetTableOid() const
    {
        return m_head.tableOid;
    }
    inline Oid GetLobTargetOid() const
    {
        return m_head.lobTargetOid;
    }
    inline void SetTableOid(Oid tableOid)
    {
        m_head.tableOid = tableOid;
    }
    inline void SetLobTargetOid(Oid lobTargetOid)
    {
        m_head.lobTargetOid = lobTargetOid;
    }

    inline void SetExternalMem()
    {
        m_head.tupleInfo.val.isExternalMem = 1;
    }
    inline void SetInternalMem()
    {
        m_head.tupleInfo.val.isExternalMem = 0;
    }
    inline bool IsExternalMem() const
    {
        return m_head.tupleInfo.val.isExternalMem == 1;
    }

    inline ItemPointerData *GetCtid()
    {
        return &m_head.ctid;
    }

    inline void SetCtid(ItemPointerData ctid)
    {
        m_head.ctid = ctid;
    }

    inline void SetAllocMem()
    {
        m_head.tupleInfo.val.isAllocMem = 1;
    }
    inline bool IsAllocMem() const
    {
        return m_head.tupleInfo.val.isAllocMem == 1;
    }

    inline void SetDatumTypeId(Oid typeId)
    {
        m_head.datumTypeid = typeId;
    }
    inline Oid GetDatumTypeId() const
    {
        return m_head.datumTypeid;
    }

    inline void SetDatumTypeMod(int32 typeMod)
    {
        m_head.datumTypmod = typeMod;
    }
    inline int32 GetDatumTypeMod() const
    {
        return m_head.datumTypmod;
    }

    inline void SetDeleteXidForDebug(Xid deleteXid)
    {
        m_head.deleteXidForDebug = deleteXid;
    }

    inline Xid GetDeleteXidForDebug()
    {
        return m_head.deleteXidForDebug;
    }

    uint16 GetNumAttrs() const
    {
        return m_diskTuple->GetNumColumn();
    }

    bool AttrIsNull(int attrNum) const
    {
        return m_diskTuple->AttrIsNull(attrNum);
    }

    uint32 GetBitmapLen() const
    {
        return static_cast<uint32>(DataTuple::GetBitmapLen(m_diskTuple->GetNumColumn()));
    }

    uint32 GetValuesOffset() const
    {
        return m_diskTuple->GetValuesOffset();
    }

    bool HasNull() const
    {
        return m_diskTuple->HasNull();
    }

    bool HasExternal() const
    {
        return m_diskTuple->HasExternal();
    }

    bool HasInlineLob() const
    {
        return m_diskTuple->HasInlineLobValue();
    }

    uint8 *GetNullBitmap() const
    {
        return m_diskTuple->GetNullBitmap();
    }

    char *GetValues() const
    {
        return m_diskTuple->GetValues();
    }

    bool HasVariable() const
    {
        return m_diskTuple->HasVariable();
    }

    uint8 GetTdId() const
    {
        return m_diskTuple->GetTdId();
    }

    void SetTdId(uint8 id)
    {
        m_diskTuple->SetTdId(id);
    }

    Xid GetXid() const
    {
        return m_diskTuple->GetXid();
    }

    void SetXid(Xid xid)
    {
        m_diskTuple->SetXid(xid);
    }

    char *GetStruct() const
    {
        return m_diskTuple->GetValues();
    }

    bool HasOid() const
    {
        return m_diskTuple->HasOid();
    }

    Oid GetOid() const
    {
        return m_diskTuple->GetOid();
    }

    void SetOid(Oid oid) const
    {
        return m_diskTuple->SetOid(oid);
    }

    inline void SetDatumVarSize(uint32 diskTupSize)
    {
        DstoreSetVarSize(m_diskTuple, diskTupSize);
    }

    union Xid GetTupleXid() const;
    Datum GetSysattr(int attNum, bool *isNull);

    HeapTuple *Copy(AllocMemFunc allocMem = nullptr);
    static void Copy(HeapTuple *destTup, HeapTuple *srcTup, bool isExternalMem);

    static HeapTuple *FormTuple(TupleDesc tupleDesc, Datum *values, bool *isnull, AllocMemFunc allocMem = nullptr);
    void DeformTuple(TupleDesc tupleDesc, Datum *values, bool *isNulls);
    Datum DeformColumnData(TupleDesc tupleDesc, Form_pg_attribute att, char *tupleValues, char *&lobValues);
    void DeformTuplePart(TupleAttrContext &attrContext, int start, int end);

    void CalculateOffset(int attNum,  int natts, Form_pg_attribute *att, int& off);
    void CalculateOffsetSlow(CalculateOffsetSlowContext &context);
    bool CheckHasNull(int attNum, bool *isNull, bool &slow) const;
    void CheckHasVarAtt(int attNum, bool& slow, Form_pg_attribute *att) const;
    Datum GetAttr(int attNum, TupleDesc desc, bool *isNull, bool forceReturnLobLocator = false);
};
}

#endif /* SRC_GAUSSKERNEL_DSTORE_INCLUDE_TUPLE_STORAGE_MEMHEAP_TUPLE_H */
