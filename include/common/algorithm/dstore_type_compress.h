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
 * IDENTIFICATION
 *        include/common/algorithm/dstore_type_compress.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TYPE_COMPRESS_H
#define DSTORE_TYPE_COMPRESS_H

#include "common/dstore_datatype.h"

namespace DSTORE {

constexpr uint8 COMPRESSED32_MAX_BYTE = 5;
constexpr uint8 COMPRESSED64_MAX_BYTE = 10;
class VarintCompress {
public:
    static constexpr uint8 numOffset = 7;
    static constexpr bool enbaleTypeCompress = false;
    static inline uint32 Zigzag32(int32 num)
    {
        return static_cast<uint32>((num << 1) ^ (num >> 31));
    }

    static inline int32 UnZigzag32(uint32 num)
    {
        uint32 result = (num >> 1) ^ -(num & 1);
        return static_cast<int32>(result);
    }

    static inline uint64 Zigzag64(int64 num)
    {
        return static_cast<uint64>((num << 1) ^ (num >> 63));
    }

    static inline int64 UnZigzag64(uint64 num)
    {
        uint64 result = (num >> 1) ^ -(num & 1);
        return static_cast<int64>(result);
    }

    static uint8 GetSigned32CompressedSize(int32 num)
    {
        uint32 zigzagNum = Zigzag32(num);
        return GetUnsigned32CompressedSize(zigzagNum);
    }

    static uint8 CompressSigned32(int32 num, char *buf)
    {
        uint32 zigzagNum = Zigzag32(num);
        return CompressUnsigned32(zigzagNum, buf);
    }

    static int32 DecompressSigned32(const char *buf, uint8 &size)
    {
        uint32 num = DecompressUnsigned32(buf, size);
        return UnZigzag32(num);
    }

    static uint8 GetUnsigned32CompressedSize(uint32 num)
    {
        if (num < (1 << 7)) {
            return 1;
        } else if (num < (1 << (7 * 2))) {
            return 2;
        } else if (num < (1 << (7 * 3))) {
            return 3;
        } else if (num < (1 << (7 * 4))) {
            return 4;
        }
        return COMPRESSED32_MAX_BYTE;
    }

    static uint8 CompressUnsigned32(uint32 num, char *buf)
    {
        uint8 size = 0;
        buf[0] = '\0';
        uint8 byte;
        do {
            byte = num & 0x7F;
            if (num > 0x7F) {
                byte |= 0x80;
            }
            buf[size++] = static_cast<char>(byte);
            num >>= 7;
        } while (num != 0);
        return size;
    }

    static uint32 DecompressUnsigned32(const char *buf, uint8 &size)
    {
        uint8 byte;
        uint32 num = 0;
        size = 0;
        uint8 i;
        for (i = 0; i < COMPRESSED32_MAX_BYTE; i++) {
            byte = static_cast<uint8>(buf[i]);
            num |= static_cast<uint32>(byte & 0x7F) << (numOffset * i);
            if ((byte & 0x80) != 0x80) {
                break;
            }
        }
        size = i + 1;
        return num;
    }

    static uint8 GetUnsigned64CompressedSize(uint64 num)
    {
        uint32 high32Num = static_cast<uint32>(num >> 32);
        if (high32Num == 0) {
            return GetUnsigned32CompressedSize(static_cast<uint32>(num));
        }
        if (num < (1UL << (7 * 5))) {
            return 5;
        } else if (num < (1UL << (7 * 6))) {
            return 6;
        } else if (num < (1UL << (7 * 7))) {
            return 7;
        } else if (num < (1UL << (7 * 8))) {
            return 8;
        } else if (num < (1UL << (7 * 9))) {
            return 9;
        }
        return COMPRESSED64_MAX_BYTE;
    }

    static uint8 GetSigned64CompressedSize(int64 num)
    {
        uint64 zigzagNum = Zigzag64(num);
        return GetUnsigned64CompressedSize(zigzagNum);
    }

    static uint8 CompressSigned64(int64 num, char *buf)
    {
        uint64 zigzagNum = Zigzag64(num);
        return CompressUnsigned64(zigzagNum, buf);
    }

    static int64 DecompressSigned64(const char *buf, uint8 &size)
    {
        uint64 num = DecompressUnsigned64(buf, size);
        return UnZigzag64(num);
    }

    static uint8 CompressUnsigned64(uint64 num, char *buf)
    {
        uint8 size = 0;
        buf[0] = '\0';
        uint8 byte;
        do {
            byte = num & 0x7F;
            if (num > 0x7F) {
                byte |= 0x80;
            }
            buf[size++] = static_cast<char>(byte);
            num >>= 7;
        } while (num != 0);
        return size;
    }

    static uint64 DecompressUnsigned64(const char *buf, uint8 &size)
    {
        uint8 byte;
        uint64 num = 0;
        size = 0;
        uint8 i;
        for (i = 0; i < COMPRESSED64_MAX_BYTE; i++) {
            byte = static_cast<uint8>(buf[i]);
            num |= (static_cast<uint64>(byte & 0x7F)) << (static_cast<uint64>(numOffset * i));
            if ((byte & 0x80) != 0x80) {
                break;
            }
        }
        size = i + 1;
        return num;
    }
};
}  // namespace DSTORE
#endif  // STORAGE_TYPE_COMPRESS_H
