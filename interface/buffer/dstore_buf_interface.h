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
 * dstore_buf_interface.h
 *      This file declares system function handlers for bufferpool module.
 *
 * IDENTIFICATION
 *      interface/buffer/dstore_buf_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_BUF_INTERFACE
#define DSTORE_BUF_INTERFACE

namespace BufferInterface {
using namespace DSTORE;
#pragma GCC visibility push(default)

char *DoWhenBufferpoolResize(Size bufferPoolNewSize);

RetStatus BBoxBlackListGet(char*** out_chunk_addr, uint64** out_chunk_size, uint64* out_numOfChunk);

#pragma GCC visibility pop
} /* namespace BufferInterface */

#endif /* STORAGE_BUF_INTERFACE */
