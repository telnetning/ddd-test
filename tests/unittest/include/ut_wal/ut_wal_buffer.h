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
 */
#ifndef DSTORE_UT_WAL_BUFFER_H
#define DSTORE_UT_WAL_BUFFER_H
#include "wal/dstore_wal_buffer.h"
#include "common/memory/dstore_mctx.h"
#include "gtest/gtest.h"

namespace DSTORE {
    class MockWalStreamBuffer : public WalStreamBuffer {
    public:
        MockWalStreamBuffer(DstoreMemoryContext memoryContext, uint16 blockSize): WalStreamBuffer(memoryContext, blockSize) {
        }

        RetStatus Init(uint64 lastEndPlsn, uint32 lastRecordLen, uint8 *lastBlockData, uint16 lastBlockUsedSize)
        {
        }

        void ReserveInsertLocation(uint32 size, uint64 &startPlsn, uint64 endPlsn)
        {
        }

        uint8* GetBufferBlock(uint64 ptr)
        {
        }

        void MarkInsertFinish(uint64 startPos, uint64 endPos)
        {
        }

        void GetNextFlushData(uint64 targetPlsn, uint64 startPlsn, uint64 endPlsn, uint8 *&data)
        {
        }
    };
}

#endif //DSTORE_UT_WAL_BUFFER_H
