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
 * dstore_wal_utils.h
 *
 * Description:
 * Wal temp utils definition, will merge into uniformed se utils file.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_UTILS_H
#define DSTORE_WAL_UTILS_H

#include "common/dstore_datatype.h"

namespace DSTORE {
constexpr uint32 WAL_WAIT_MEMORY_AVAILABLE_TIME = 1;
class WalUtils {
public:
    static inline uint64 GetWalFileUsableBytes(uint64 walFileSize, uint32 walFileHdrSize)
    {
        return walFileSize - walFileHdrSize;
    }

    static inline uint64 GetPlsnBySegOffsetAndSegNo(uint64 segOffset, uint64 segNo, uint64 walFileSize)
    {
        return segNo * walFileSize + segOffset;
    }

    template <bool isEnd>
    static uint64 WalBytePosToPlsn(uint64 walFileSize, uint64 bytepos);

    static uint64 WalPlsnToBytePos(uint64 walFileSize, uint64 plsn);

    template <bool isEnd>
    static uint64 GetRecordPlsn(uint64 groupStartPlsn, uint64 recordOffset, uint64 walFileSize);

    static uint64 GetWalGroupStartPlsn(uint64 groupEndPlsn, uint64 recordLen, uint64 walFileSize);

    static bool TryAtomicSetBiggerU64(volatile uint64 *target, uint64 newVal);

    static bool IsOvertimed(const timespec &start, const timespec &end, const timespec &target);

    static uint64 TimeDiffInMicroseconds(const timespec& start, const timespec& end);

    static void SignalBlock();

    static RetStatus SetThreadAffinity(size_t targetCpu, const char* threadName);

    static void HandleWalThreadCpuBind(const char* threadName);

    static void HandleWalThreadCpuUnbind(const char* threadName);

    static uint64 GetFileVersion(const PdbId pdbId, const FileId fileId);

    static uint64 GetTbsVersion(const PdbId pdbId, const TablespaceId tableSpaceId);
};
}
#endif // STORAGE_WAL_UTILS_H
