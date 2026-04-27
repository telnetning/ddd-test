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
 * dstore_lz4.h
 *
 * IDENTIFICATION
 *        include/common/instrument/lz4/dstore_lz4.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_LZ4_H
#define DSTORE_LZ4_H

/*
 * LZ4 header file should be placed in the shared third_lib
 */
extern "C" int LZ4_compressBound(int inputSize);
extern "C" int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);
extern "C" int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity);

#endif  // STORAGE_TRACE_H
