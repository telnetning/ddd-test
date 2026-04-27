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
 *  checksum_impl.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/src/common/algorithm/dstore_checksum_impl.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_crc32c.h"
#include "common/algorithm/dstore_checksum_impl.h"

namespace DSTORE {
/* number of checksums to calculate in parallel */
constexpr uint32 N_SUMS = 32;
/* prime multiplier of FNV-1a hash */
constexpr uint32 FNV_PRIME = 16777619;
constexpr uint32 FNV_OFFSET = 17;

/*
 * Base offsets to initialize each of the parallel FNV hashes into a
 * different initial state.
 */
constexpr uint32 g_checksumBaseOffsets[N_SUMS] = {
    0x5B1F36E9,
    0xB8525960,
    0x02AB50AA,
    0x1DE66D2A,
    0x79FF467A,
    0x9BB9F8A3,
    0x217E7CD2,
    0x83E13D2C,
    0xF8D4474F,
    0xE39EB970,
    0x42C6AE16,
    0x993216FA,
    0x7B093B5D,
    0x98DAFF3C,
    0xF718902A,
    0x0B1C9CDB,
    0xE58F764B,
    0x187636BC,
    0x5D7B3BB1,
    0xE73DE7DE,
    0x92BEC979,
    0xCCA6C0B2,
    0x304A0979,
    0x85AA43D4,
    0x783125BB,
    0x6CA8EAA2,
    0xE407EAC6,
    0x4B5CFC3E,
    0x9FBF8C76,
    0x15CA20BE,
    0xF2CA9FD3,
    0x959BD756};

/*
 * Calculate one round of the checksum.
 */
static inline void ChecksumCalculate(uint32 *checksum, uint32 value)
{
    uint32 tmp = (*checksum) ^ value;
    *checksum = (tmp * FNV_PRIME) ^ (tmp >> FNV_OFFSET);
}

static inline uint32 ChecksumInit(uint32 seed, uint32 value)
{
    ChecksumCalculate(&seed, value);
    return seed;
}

uint32 CompFnv(const uint8* data, uint32 size)
{
    uint32 sums[N_SUMS];
    const uint32* dataArr = static_cast<const uint32*>(static_cast<const void*>(data));
    uint32 result = 0;
    uint32 i, j;

    /* ensure that the size is compatible with the algorithm */
    StorageAssert((size % (sizeof(uint32))) == 0);

    /* initialize partial checksums to their corresponding offsets */
    for (j = 0; j < N_SUMS; j++) {
        sums[j] = ChecksumInit(g_checksumBaseOffsets[j], dataArr[j]);
    }
    dataArr += N_SUMS;

    /* main checksum calculation */
    for (i = 1; i < size / (sizeof(uint32) * N_SUMS); i++) {
        for (j = 0; j < N_SUMS; j++) {
            ChecksumCalculate(&sums[j], dataArr[j]);
        }
        dataArr += N_SUMS;
    }
    /* Calculate the last part that is less than (sizeof(uint32) * N_SUMS) in length */
    uint32 leftLen = ((size % (sizeof(uint32) * N_SUMS)) / (sizeof(uint32)));
    for (i = 0; i < leftLen; i++) {
        ChecksumCalculate(&sums[i], dataArr[i]);
    }
    /* finally add in two rounds of zeroes for additional mixing */
    for (j = 0; j < N_SUMS; j++) {
        ChecksumCalculate(&sums[j], 0);
        ChecksumCalculate(&sums[j], 0);

        /* xor fold partial checksums together */
        result ^= sums[j];
    }

    return result;
}

/*
 * Compute the checksum for a openGauss buf.  The buf must be aligned on a
 * 4-byte boundary.
 *
 */
uint32 CompChecksum(const void* buf, uint32 size, CHECKSUM_ALGORITHM alg)
{
    const uint8* data = static_cast<const uint8*>(buf);
    uint32 checksum = 0;
    if (alg == CHECKSUM_FNV) {
        checksum = CompFnv(data, size);
        /*
        * Reduce to a uint16 (to fit in the pd_checksum field) with an offset of
        * one. That avoids checksums of zero, which seems like a good idea.
        */
        checksum = (checksum % UINT16_MAX) + 1;
    } else if (alg == CHECKSUM_CRC) {
        checksum = 0xFFFFFFFF;
        checksum = CompCrc32c(checksum, data, size);
    }
    return checksum;
}
}