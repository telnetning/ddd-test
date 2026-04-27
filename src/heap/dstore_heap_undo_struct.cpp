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
 * dstore_heap_undo_struct.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_undo_struct.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_update.h"

namespace DSTORE {

void UndoDataHeapInplaceUpdate::GenerateUndoData(uint16 numDiff, const uint16 *diffPos, char *oldTupleData,
    uint8 tdId, uint32 tupInfo, Xid xid)
{
    SetNumDiff(numDiff);
    SetOldTdId(tdId);
    SetXid(xid);
    oldInfo = tupInfo;
    ResetRawDataSize();
    uint16 oldTupleDataSize = static_cast<uint16>(GetOldTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    uint16 dataEndOffset = static_cast<uint16>(oldTupleDataSize - 1);
    char *oldTupleDataEnd = oldTupleData + dataEndOffset;

    for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
        /* The start position of diff section */
        uint16 start = diffPos[i];
        /* The end position of diff section */
        uint16 end = diffPos[i + 1];
        StorageAssert(((start < end) && (end <= oldTupleDataSize)));

        /* Record into undo */
        Append(static_cast<char *>(static_cast<void *>(&start)), static_cast<uint16>(sizeof(diffPos[0])));
        Append(static_cast<char *>(static_cast<void *>(&end)), static_cast<uint16>(sizeof(diffPos[0])));
        /* The data of diff section */
        uint16 endOffset = static_cast<uint16>(end - 1);
        Append(oldTupleDataEnd - endOffset, static_cast<uint16>(end - start));
    }
}

void UndoDataHeapInplaceUpdate::UndoActionOnTuple(HeapDiskTuple *diskTupleOnPage, uint16 undoTupleSize)
{
    errno_t rc;
    uint16 pageTupleSize = diskTupleOnPage->GetTupleSize();
    uint16 pageTupleDataSize = static_cast<uint16>(pageTupleSize - HEAP_DISK_TUP_HEADER_SIZE);
    uint16 undoTupleDataSize = static_cast<uint16>(undoTupleSize - HEAP_DISK_TUP_HEADER_SIZE);
    char *tupleData = diskTupleOnPage->GetData();
    uint16 dataOffset;
    if (undoTupleSize > pageTupleSize) {
        dataOffset = static_cast<uint16>(undoTupleSize - pageTupleSize);
        rc = memmove_s(tupleData + dataOffset, pageTupleDataSize, tupleData, pageTupleDataSize);
        storage_securec_check(rc, "\0", "\0");
    } else if (undoTupleSize < pageTupleSize) {
        dataOffset = static_cast<uint16>(pageTupleSize - undoTupleSize);
        rc = memmove_s(tupleData, pageTupleDataSize, tupleData + dataOffset, undoTupleDataSize);
        storage_securec_check(rc, "\0", "\0");
    }

    uint16 start, end;
    char *diffDataPtr = GetRawData();
    uint16 numDiff = GetNumDiff();
    uint16 dataEndOffset = static_cast<uint16>(undoTupleDataSize - 1);
    char *dataEnd = tupleData + dataEndOffset;
    for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
        start = *static_cast<uint16 *>(static_cast<void *>(diffDataPtr));
        end = *static_cast<uint16 *>(static_cast<void *>(diffDataPtr + sizeof(start)));
        StorageAssert(end <= undoTupleDataSize);
        uint16 len = static_cast<uint16>(end - start);
        uint16 destOffset = static_cast<uint16>(end - 1);
        rc = memcpy_s(dataEnd - destOffset, len, diffDataPtr + sizeof(start) + sizeof(end), len);
        storage_securec_check(rc, "\0", "\0");
        diffDataPtr += sizeof(start) + sizeof(end) + end - start;
    }
    diskTupleOnPage->SetTdId(GetOldTdId());
    diskTupleOnPage->SetInfo(oldInfo);
    diskTupleOnPage->SetTupleSize(GetOldTupleSize());
    diskTupleOnPage->SetXid(GetXid());
}

}  // namespace DSTORE