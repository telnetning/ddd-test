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
#include <atomic>
#include "port/dstore_port.h"
#include "wal/dstore_wal_read_buffer.h"
#include "wal/dstore_wal_file_manager.h"

#ifndef DSTORE_UT_WAL_FILE_MANAGER_MOCK_H
#define DSTORE_UT_WAL_FILE_MANAGER_MOCK_H

namespace DSTORE
{

class MockWalFile : public WalFile {
public:
    explicit MockWalFile(WalFileInitPara para) : WalFile(para) {};
    ~MockWalFile() = default;

    void RandomPreadThread(int64 successSize, AsyncIoContext *aioContext)
    {
        /* Use random sleep to imitate async, time range [0ms, 100ms] */
        usleep((random() % 100) * 1000);
        new std::thread(aioContext->callback, EOK, successSize, aioContext->asyncContext);
        DstorePfreeExt(aioContext);
    }

    RetStatus PreadAsync(void *buf, uint64_t count, int64_t offset, const AsyncIoContext *aioContext) override
    {
        int64 successSize = 0;
        /* First 3 block can read all data, the third block can read half data, the other block read nothing. */
        if (offset / WAL_READ_BUFFER_BLOCK_SIZE < 3) {
            successSize = WAL_READ_BUFFER_BLOCK_SIZE;
        } else if (offset / WAL_READ_BUFFER_BLOCK_SIZE == 3) {
            successSize = WAL_READ_BUFFER_BLOCK_SIZE / 2;
        } else {
            successSize = 0;
        }
        /* It need deep copy here to avoid aioContext is a stack object */
        AsyncIoContext *aioContextBackup = static_cast<AsyncIoContext *>(DstorePalloc(sizeof(AsyncIoContext)));
        aioContextBackup->callback = aioContext->callback;
        aioContextBackup->asyncContext = aioContext->asyncContext;
        new std::thread(&MockWalFile::RandomPreadThread, this, successSize, aioContextBackup);
        return DSTORE_SUCC;
    }

};
}
#endif
