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
#include "ut_buffer/ut_buffer_util.h"
#include "framework/dstore_vfs_adapter.h"
#include "vfs/vfs_interface.h"
#include "ut_utilities/ut_dstore_framework.h"

namespace UtBufferUtils {

std::unique_ptr<SnapshotData> prepare_fake_snapshot(CommitSeqNo csn, CommandId cid)
{
    std::unique_ptr<SnapshotData> snapshot(new SnapshotData());
    snapshot->SetCsn(csn);
    snapshot->SetCid(cid);
    return snapshot;
}

BufferDesc **prepare_buffer(uint32 size, FileId fileId)
{
    BufferDesc **buffers = (BufferDesc **)DstorePalloc(sizeof(BufferDesc *) * size);

    for (uint32 i = 0; i < size; i++) {
        BufBlock block = (BufBlock)DstorePallocAligned(BLCKSZ, ALIGNOF_BUFFER);
        BufferDescController *controller = (BufferDescController *)DstorePalloc(sizeof(BufferDescController));
        controller->InitController();
        buffers[i] = (BufferDesc *)DstorePalloc(sizeof(BufferDesc));
        buffers[i]->InitBufferDesc(block, controller);
        buffers[i]->bufTag = {g_defaultPdbId, {DEFAULT_FILE_ID, i}};
    }
    return buffers;
}

void free_buffer(BufferDesc **buffers, Size size)
{
    for (Size i = 0; i < size; i++) {
        DstorePfreeAligned(buffers[i]->bufBlock);
        DstorePfreeExt(buffers[i]->controller);
        DstorePfreeExt(buffers[i]);
    }

    DstorePfreeExt(buffers);
}

void prepare_fake_file(VFSAdapter *vfs, FileId fileId, uint32 blockNumber, const char *path)
{
    int ret;
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = (64 << 10);
    filePara.maxSize = (uint64) DSTORE_MAX_BLOCK_NUMBER * BLCKSZ;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;
    ret = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, UT_DEFAULT_STORESPACE_NAME);
    storage_securec_check(ret, "\0", "\0");

    ret = vfs->CreateFile(fileId, path, filePara);
    StorageAssert(ret == DSTORE_SUCC);
    ret = vfs->Extend(fileId, GetOffsetByBlockNo(blockNumber));
    StorageAssert(ret == DSTORE_SUCC);
}

void remove_fake_file(VFSAdapter *vfs, FileId fileId, const char *path)
{
    vfs->Close(fileId);
    vfs->RemoveFile(fileId, path);
}

} /* namespace UtBufferUtils */
