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
#ifndef UT_SNAPSHOT_UTIL_H
#define UT_SNAPSHOT_UTIL_H

#include "transaction/dstore_transaction_types.h"
#include "buffer/dstore_buf.h"

#include <iostream>
#include <memory>

using namespace DSTORE;

namespace DSTORE{
    class VFSAdapter;
}

namespace UtBufferUtils {

static constexpr FileId DEFAULT_FILE_ID = 1U;

std::unique_ptr<SnapshotData> prepare_fake_snapshot(CommitSeqNo csn, CommandId cid = 0);

extern BufferDesc **prepare_buffer(uint32 size, FileId fileId = DEFAULT_FILE_ID);

extern void free_buffer(BufferDesc **buffers, Size size);

extern void prepare_fake_file(VFSAdapter *vfs, FileId fileId, uint32 blockNumber, const char *path);

extern void remove_fake_file(VFSAdapter *vfs, FileId fileId, const char *path);

} /* namespace UtBufferUtils */

#endif