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
 * dstore_buf_interface.cpp
 *      This file implements system function handlers for bufferpool module.
 *
 * IDENTIFICATION
 *      src/buffer/dstore_buf_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "buffer/dstore_buf_mgr.h"
#include "buffer/dstore_buf_interface.h"

namespace BufferInterface {
using namespace DSTORE;

char *DoWhenBufferpoolResize(Size bufferPoolNewSize)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    StringInfoData message;
    if (unlikely(!message.init())) {
        ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("Failed to initialize message to resize buffer pool."));
        return nullptr;
    }
    (void)bufMgr->DoWhenBufferpoolResize(bufferPoolNewSize, message);
    return message.data;
}

RetStatus BBoxBlackListGet(char*** out_chunk_addr, uint64** out_chunk_size, uint64* out_numOfChunk)
{
    if (!out_chunk_addr || !out_chunk_size || !out_numOfChunk) {
        return DSTORE_FAIL;
    }

    DSTORE::BufMgr* bufMgr = dynamic_cast<DSTORE::BufMgr*>(g_storageInstance->GetBufferMgr());
    if (!bufMgr) {
        return DSTORE_FAIL;
    }

    *out_chunk_addr = nullptr;
    *out_chunk_size = nullptr;
    *out_numOfChunk = 0;

    return bufMgr->BBoxBlackListGetBuffer(out_chunk_addr, out_chunk_size, out_numOfChunk);
}
}  /* namespace BufferInterface */
