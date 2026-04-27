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
 * dstore_heap_undo_struct.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_undo_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_UNDO_STRUCT_H
#define DSTORE_HEAP_UNDO_STRUCT_H

#include "common/dstore_datatype.h"
#include "page/dstore_itemptr.h"
#include "securec.h"
#include "tuple/dstore_heap_tuple.h"

namespace DSTORE {

struct UndoData {
    uint16 rawDataSize;

    inline void Init()
    {
        rawDataSize = 0;
    }

    inline void ResetRawDataSize()
    {
        rawDataSize = 0;
    }
    inline uint16 GetRawDataSize() const
    {
        return rawDataSize;
    }

    inline void AppendUndoData(char *srcData, uint16 srcSize, char *dest, uint16 destSize)
    {
        char *ptr = dest + rawDataSize;
        if (srcSize == 0) {
            /* If all columns are null */
            return;
        }
        errno_t rc = memcpy_s(ptr, destSize, srcData, srcSize);
        storage_securec_check(rc, "\0", "\0");
        rawDataSize += srcSize;
    }
} PACKED;

struct UndoDataHeapDelete : public UndoData {
    char rawData[];

    inline uint16 GetSize() const
    {
        return GetRawDataSize() + sizeof(UndoDataHeapDelete);
    }
    inline void Append(char *data, uint16 size)
    {
        UndoData::AppendUndoData(data, size, rawData, size);
    }
    inline HeapDiskTuple *GetDiskTuple()
    {
        return static_cast<HeapDiskTuple *>(static_cast<void *>(rawData));
    }
} PACKED;

struct UndoDataHeapInplaceUpdate : public UndoData {
    uint8 oldTdId;
    uint16 numDiffPos;
    uint32 oldInfo;
    uint16 oldTupleLen;
    Xid oldXid;
    char rawData[];

    inline void SetNumDiff(uint16 numDiff)
    {
        numDiffPos = numDiff;
    }
    inline void SetOldTdId(uint8 tdId)
    {
        oldTdId = tdId;
    }
    inline uint8 GetOldTdId() const
    {
        return oldTdId;
    }
    inline void Append(char *data, uint16 size)
    {
        UndoData::AppendUndoData(data, size, rawData, size);
    }
    inline uint16 GetSize() const
    {
        return GetRawDataSize() + sizeof(UndoDataHeapInplaceUpdate);
    }
    inline char *GetRawData()
    {
        return rawData;
    }
    inline uint16 GetNumDiff() const
    {
        return numDiffPos;
    }
    inline void SetOldTupleSize(uint16 len)
    {
        oldTupleLen = len;
    }
    inline uint16 GetOldTupleSize() const
    {
        return oldTupleLen;
    }
    inline void SetXid(Xid xid)
    {
        oldXid = xid;
    }
    inline Xid GetXid()
    {
        return oldXid;
    }
    void GenerateUndoData(uint16 numDiff, const uint16 *diffPos, char *oldTupleData, uint8 tdId,
        uint32 tupInfo, Xid xid);

    void UndoActionOnTuple(HeapDiskTuple *diskTupleOnPage, uint16 undoTupleSize);
} PACKED;

struct UndoDataHeapSamePageAppendUpdate : public UndoData {
    char rawData[];

    inline HeapDiskTuple *GetDiskTuple()
    {
        return static_cast<HeapDiskTuple *>(static_cast<void *>(rawData));
    }
    inline void Append(char *data, uint16 size)
    {
        UndoData::AppendUndoData(data, size, rawData, size);
    }
} PACKED;

struct UndoDataHeapAnotherPageAppendUpdate : public UndoData {
    ItemPointerData newCtid;
    char rawData[];

    inline uint16 GetSize() const
    {
        return GetRawDataSize() + sizeof(UndoDataHeapAnotherPageAppendUpdate);
    }
    inline void SetNewCtid(ItemPointerData ctid)
    {
        newCtid = ctid;
    }
    inline ItemPointerData GetNewCtid() const
    {
        return newCtid;
    }
    inline HeapDiskTuple *GetDiskTuple()
    {
        return static_cast<HeapDiskTuple *>(static_cast<void *>(rawData));
    }
    inline void Append(char *data, uint16 size)
    {
        UndoData::AppendUndoData(data, size, rawData, size);
    }
};

struct UndoDataHeapBatchInsert : public UndoData {
    char rawData[];

    inline void *GetRawData()
    {
        return static_cast<void *>(rawData);
    }
    inline uint16 GetSize() const
    {
        return GetRawDataSize() + sizeof(UndoDataHeapBatchInsert);
    }
    inline void Append(char *data, uint16 size)
    {
        UndoData::AppendUndoData(data, size, rawData, size);
    }
    inline uint16 GetItemIdRangeCount() const
    {
        /* How many itemid range (ItemIdStart, ItemIdEnd) */
        const uint16 itemIdNumPerRange = 2;
        return GetRawDataSize() / (sizeof(uint16) * itemIdNumPerRange);
    }
} PACKED;

} /* The end of namespace DSTORE */
#endif
