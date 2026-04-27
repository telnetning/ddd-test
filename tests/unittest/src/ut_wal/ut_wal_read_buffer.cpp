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
#include <gtest/gtest.h>
#include <sys/time.h>
#include <thread>

#include "framework/dstore_instance.h"
#include "wal/dstore_wal_utils.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_mock/ut_wal_file_manager_mock.h"

using namespace DSTORE;
using namespace std;

class UTWalReadBuffer : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        WALBASICTEST::SetUp();
        m_walReadBuffer = DstoreNew(g_dstoreCurrentMemoryContext)WalReadBuffer(g_dstoreCurrentMemoryContext,
            m_walStream, RedoMode::RECOVERY_REDO);
        ASSERT_NE(m_walReadBuffer, nullptr);
        m_walReadBuffer->m_loadToBufferConf = {
            .loadStartPlsn = 0,
            .readBufferBlockNum = 16,
            .readBufferBlockSize = WAL_READ_BUFFER_BLOCK_SIZE,
            .readBufferSize = 16 * WAL_READ_BUFFER_BLOCK_SIZE
        };
        m_walReadBuffer->m_readBuffer =
            static_cast<uint8 *>(DstorePalloc(m_walReadBuffer->m_loadToBufferConf.readBufferSize));
        ASSERT_NE(m_walReadBuffer->m_readBuffer, nullptr);

        m_walReadBuffer->m_readBufferBlockStates = static_cast<WalFileLoadToBufferBlockState *>(DstorePalloc(
            m_walReadBuffer->m_loadToBufferConf.readBufferBlockNum * sizeof(WalFileLoadToBufferBlockState)));

        for (uint32 i = 0; i < m_walReadBuffer->m_loadToBufferConf.readBufferBlockNum; ++i) {
            m_walReadBuffer->m_readBufferBlockStates[i].recycled = true;
            m_walReadBuffer->m_readBufferBlockStates[i].readCnt = &m_walReadBuffer->m_readCnt;
            m_walReadBuffer->m_readBufferBlockStates[i].loadWorkerNeedStop = &m_walReadBuffer->m_stopLoadWorker;
        }
    }

    void TearDown() override
    {
        DstorePfree(m_walReadBuffer->m_readBuffer);
        m_walReadBuffer->m_readBuffer = nullptr;
        DstorePfree(m_walReadBuffer->m_readBufferBlockStates);
        m_walReadBuffer->m_readBufferBlockStates = nullptr;
        delete m_walReadBuffer;
        WALBASICTEST::TearDown();
    }

    WalReadBuffer *m_walReadBuffer;
    uint64 m_walFileSize = 16 * 1024 * 1024U;

};

/* Use MockWalFile class to mock PreadAsync concurrent.
 * This test case due to test: walStreamEndPlsn update result is correct or not in concurrent scene.
 */
TEST_F(UTWalReadBuffer, WalStreamEndPlsnConcurrentUpdateTest)
{
    WalFileInitPara param;
    MockWalFile *walFile = DstoreNew(g_dstoreCurrentMemoryContext)MockWalFile(param);
    ASSERT_NE(walFile, nullptr);
    uint32 curPlsn = 0;
    LoadToBufferContext context;
    uint64 eachReadBytes = WAL_READ_BUFFER_BLOCK_SIZE;
    context.walFile = walFile;
    context.eachReadBytes = &eachReadBytes;
    context.dioRw = false;
    context.dioReadAdaptor = nullptr;
    /* In this loop, it will trigger 8 times PreadAsync in LoadToBufferPageStore function,
     * read callback call will delay a random time in range [0ms, 100ms] in each pread threads.
     */
    for (uint32 readBufferBlockNo = 0; readBufferBlockNo < 8; ++readBufferBlockNo) {
        context.readBufferBlockNo = readBufferBlockNo;
        context.readBuffer = m_walReadBuffer->m_readBuffer + readBufferBlockNo * WAL_READ_BUFFER_BLOCK_SIZE;
        context.readOffset = readBufferBlockNo * WAL_READ_BUFFER_BLOCK_SIZE;
        /* Init block state read end plsn to block start plsn, update it later in pread async callback. */
        GsAtomicWriteU64(&m_walReadBuffer->m_readBufferBlockStates[readBufferBlockNo].readEndPlsn, curPlsn);
        m_walReadBuffer->m_readBufferBlockStates[readBufferBlockNo].recycled.store(false, std::memory_order_release);
        m_walReadBuffer->LoadToBufferPageStore(context);
        curPlsn += WAL_READ_BUFFER_BLOCK_SIZE;
    }

    /* Wait m_readCnt equal to 0, which means all read threads finished. */
    while (GsAtomicReadU32(&m_walReadBuffer->m_readCnt) != 0) {
        usleep(50 * 1000);
    }

    /* Expect stream end plsn is first 3 block data and the third block data len.
     * See details at MockWalFile PreadAsync function.
     */
    uint64 expectEndPlsn = WAL_READ_BUFFER_BLOCK_SIZE * 3 + WAL_READ_BUFFER_BLOCK_SIZE / 2;
    ASSERT_EQ(GsAtomicReadU64(&m_walReadBuffer->m_walStreamEndPlsn), expectEndPlsn);

    delete walFile;
}

